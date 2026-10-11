//! 查询计划：从绑定的 [`Query`](crate::analyze::Query) 生成物理计划并
//! 构建执行器树（对应 C++ 版 `planner/planner.cpp` + `common/portal.h`）。
//!
//! P5 范围（对照 C++ 版的差异）：
//! - 索引选择：等值/范围条件命中单列索引时走索引扫描（多列索引要求
//!   全列等值）；否则顺序扫描；
//! - 连接：左深树 + 嵌套循环（表顺序即用户书写顺序）；哈希/归并连接
//!   随优化器开关后续接入（C++ 默认同样是 nestloop）；
//! - 子查询在计划期求解（仅非相关子查询）；
//! - 连接顺序不做重排（C++ 的按行数重排属启发式，后续版本接入）；
//! - 聚合为真正的哈希聚合；ORDER BY 聚合查询按输出位置排序。

use crate::analyze::{BoundCond, BoundRhs, BoundSelect, BoundSetClause, Query, TabCol};
use crate::common::errors::EasyDbError;
use crate::execution::dml::{DeleteExec, IndexMaintenance, InsertExec, UpdateExec};
use crate::execution::query_execs::{
    AggItem, AggregateExec, NestedLoopJoinExec, ProjectExec, SortExec,
};
use crate::execution::scan::{IndexScanExec, SeqScanExec};
use crate::execution::{ExecCtx, Executor, ResolvedCond, ResolvedRhs, RowSchema};
use crate::parser::ast::{CompOp, OrderDir};
use crate::system::Database;
use crate::types::{ColType, Value};

/// 单表扫描的索引访问路径。
#[derive(Debug, Clone)]
pub struct IndexPlan {
    pub cols: Vec<String>,
    /// 下界键（与索引列对齐的值；None = 无界）
    pub lower: Option<Vec<Value>>,
    /// 上界键
    pub upper: Option<Vec<Value>>,
}

/// 单表扫描。
#[derive(Debug, Clone)]
pub struct ScanPlan {
    pub table: String,
    /// 可在本表行上求值的条件（走索引的条件也保留，扫描仍做残余过滤）
    pub conds: Vec<BoundCond>,
    pub index: Option<IndexPlan>,
}

/// SELECT 计划树节点。
#[derive(Debug, Clone)]
pub enum PlanNode {
    Scan(ScanPlan),
    Join {
        left: Box<PlanNode>,
        right: Box<PlanNode>,
        conds: Vec<BoundCond>,
    },
    /// 聚合（items 顺序 = SELECT 选择列顺序）
    Aggregate {
        input: Box<PlanNode>,
        group_by: Vec<TabCol>,
        items: Vec<TabCol>,
        having: Vec<BoundCond>,
    },
    /// 按源列排序（列引用对输入行求值）
    Sort {
        input: Box<PlanNode>,
        key: TabCol,
        desc: bool,
    },
    /// 按输出位置排序（聚合查询的 ORDER BY）
    SortPos {
        input: Box<PlanNode>,
        pos: usize,
        desc: bool,
    },
    Project {
        input: Box<PlanNode>,
        cols: Vec<TabCol>,
        distinct: bool,
    },
}

/// 物理计划。
#[derive(Debug, Clone)]
pub enum Plan {
    Insert {
        table: String,
        rows: Vec<Vec<Value>>,
    },
    Delete {
        table: String,
        conds: Vec<BoundCond>,
    },
    Update {
        table: String,
        clauses: Vec<BoundSetClause>,
        conds: Vec<BoundCond>,
    },
    Select {
        root: PlanNode,
        columns: Vec<String>,
    },
}

/// 生成计划（子查询在此求值）。
pub fn plan(query: Query, db: &mut Database, ctx: &ExecCtx) -> Result<Plan, EasyDbError> {
    match query {
        Query::Insert { table, values } => {
            let cols = db.table(&table)?.cols.clone();
            if values.len() != cols.len() {
                return Err(EasyDbError::InvalidValueCount);
            }
            let mut row = Vec::with_capacity(values.len());
            for (c, v) in cols.iter().zip(&values) {
                row.push(crate::execution::coerce_value_to(c.col_type(), v)?);
            }
            Ok(Plan::Insert {
                table,
                rows: vec![row],
            })
        }
        Query::Delete { table, wheres } => Ok(Plan::Delete {
            table,
            conds: wheres,
        }),
        Query::Update {
            table,
            clauses,
            wheres,
        } => Ok(Plan::Update {
            table,
            clauses,
            conds: wheres,
        }),
        Query::Select(s) => {
            let columns = output_columns(&s);
            let root = plan_select(*s, db, ctx)?;
            Ok(Plan::Select { root, columns })
        }
        other => Err(EasyDbError::Internal(format!(
            "statement {other:?} does not go through the planner"
        ))),
    }
}

/// 输出列名：别名 > 聚合函数形式 > （限定）列名。
fn output_columns(s: &BoundSelect) -> Vec<String> {
    s.items
        .iter()
        .map(|item| {
            if !item.alias.is_empty() {
                return item.alias.clone();
            }
            if item.agg != crate::types::AggregationType::None {
                let func = match item.agg {
                    crate::types::AggregationType::Count => "COUNT",
                    crate::types::AggregationType::Max => "MAX",
                    crate::types::AggregationType::Min => "MIN",
                    crate::types::AggregationType::Sum => "SUM",
                    crate::types::AggregationType::None => unreachable!(),
                };
                return format!("{func}({})", item.col);
            }
            if item.tab.is_empty() {
                item.col.clone()
            } else {
                format!("{}.{}", item.tab, item.col)
            }
        })
        .collect()
}

fn plan_select(
    mut s: BoundSelect,
    db: &mut Database,
    ctx: &ExecCtx,
) -> Result<PlanNode, EasyDbError> {
    // 0. 求解子查询（非相关）：IN → 值列表；标量 → 单值
    resolve_subqueries_select(&mut s, db, ctx)?;

    // 1. 单表扫描（条件下推 + 索引选择）；跨表条件留给连接
    let mut scans: Vec<PlanNode> = Vec::new();
    let mut join_conds: Vec<BoundCond> = Vec::new();
    let mut joined: Vec<String> = Vec::new();

    for tab in &s.tables {
        let (scan_conds, rest) = split_conds(&s.wheres, tab);
        let index = choose_index(db, tab, &scan_conds);
        scans.push(PlanNode::Scan(ScanPlan {
            table: tab.clone(),
            conds: scan_conds,
            index,
        }));
        for c in rest {
            if !join_conds.contains(&c) {
                join_conds.push(c);
            }
        }
    }

    // 2. 左深连接：连接条件在其引用表全部就绪后挂上
    let mut node = scans.remove(0);
    joined.push(s.tables[0].clone());
    while !scans.is_empty() {
        let right = scans.remove(0);
        joined.push(s.tables[joined.len()].clone());
        let (now, later): (Vec<_>, Vec<_>) = join_conds
            .into_iter()
            .partition(|c| cond_tables_ready(c, &joined));
        node = PlanNode::Join {
            left: Box::new(node),
            right: Box::new(right),
            conds: now,
        };
        join_conds = later;
    }

    // 3. 聚合判定与排序位置
    let is_agg = !s.group_by.is_empty()
        || s.items
            .iter()
            .any(|i| i.agg != crate::types::AggregationType::None)
        || !s.having.is_empty();

    let order_key = s.order_by.as_ref().map(|(k, _)| k.clone());
    let order_desc = s
        .order_by
        .as_ref()
        .is_some_and(|(_, d)| *d == OrderDir::Desc);

    if is_agg {
        // 聚合查询：ORDER BY 必须对应某个输出项，按输出位置排序
        node = PlanNode::Aggregate {
            input: Box::new(node),
            group_by: s.group_by.clone(),
            items: s.items.clone(),
            having: s.having.clone(),
        };
        if let Some(key) = &order_key {
            let pos = s
                .items
                .iter()
                .position(|i| same_col(i, key))
                .ok_or_else(|| EasyDbError::ColumnNotFound(key.col.clone()))?;
            node = PlanNode::SortPos {
                input: Box::new(node),
                pos,
                desc: order_desc,
            };
        }
        return Ok(node);
    }

    // 非聚合：先按源列排序，再投影
    if let Some(key) = &order_key {
        node = PlanNode::Sort {
            input: Box::new(node),
            key: key.clone(),
            desc: order_desc,
        };
    }

    Ok(PlanNode::Project {
        input: Box::new(node),
        cols: s.items.clone(),
        distinct: s.distinct,
    })
}

/// 求解 SELECT 中的子查询条件（IN → 值列表；标量比较 → 单值）。
fn resolve_subqueries_select(
    s: &mut BoundSelect,
    db: &mut Database,
    ctx: &ExecCtx,
) -> Result<(), EasyDbError> {
    for conds in [&mut s.wheres, &mut s.having] {
        for cond in conds.iter_mut() {
            if let BoundRhs::SubQuery(sub) = &cond.rhs {
                let Query::Select(inner) = sub.as_ref() else {
                    return Err(EasyDbError::SubqueryIllegal(
                        "only select subqueries are supported".to_string(),
                    ));
                };
                let values = db.eval_select_column(inner, ctx)?;
                cond.rhs = match cond.op {
                    CompOp::In => BoundRhs::LitList(values),
                    _ => {
                        if values.len() != 1 {
                            return Err(EasyDbError::SubqueryIllegal(
                                "scalar subquery must return exactly one row".to_string(),
                            ));
                        }
                        BoundRhs::Value(values.into_iter().next().unwrap())
                    }
                };
            }
        }
    }
    Ok(())
}

fn right_table_of(tables: &[String], idx: usize) -> String {
    tables[idx].clone()
}

/// 条件涉及的所有表是否都已进入连接树。
fn cond_tables_ready(cond: &BoundCond, joined: &[String]) -> bool {
    let mut tables = vec![cond.lhs.tab.clone()];
    if let BoundRhs::Col(c) = &cond.rhs {
        tables.push(c.tab.clone());
    }
    tables.iter().all(|t| t.is_empty() || joined.contains(t))
}

/// 把条件按“是否可在 tab 单表上求值”切分；未限定名仅在单表查询时下推。
fn split_conds(conds: &[BoundCond], tab: &str) -> (Vec<BoundCond>, Vec<BoundCond>) {
    let mut mine = Vec::new();
    let mut rest = Vec::new();
    for c in conds {
        let rhs_same_tab = match &c.rhs {
            BoundRhs::Col(col) => col.tab.is_empty() || col.tab == tab,
            _ => true,
        };
        if (c.lhs.tab == tab) && rhs_same_tab {
            mine.push(c.clone());
        } else if c.lhs.tab.is_empty() && rhs_same_tab {
            // 未限定名：多表时存在歧义风险，仅在单表场景直接下推
            mine.push(c.clone());
        } else {
            rest.push(c.clone());
        }
    }
    (mine, rest)
}

fn same_col(a: &TabCol, b: &TabCol) -> bool {
    a.tab == b.tab && a.col == b.col && a.agg == b.agg
}

/// 索引选择：等值/范围条件命中索引。
fn choose_index(db: &Database, table: &str, conds: &[BoundCond]) -> Option<IndexPlan> {
    let meta = db.table(table).ok()?;
    for ix in &meta.indexes {
        if ix.cols.len() == 1 {
            let col = &ix.cols[0];
            let mut eq: Option<Value> = None;
            let mut lower: Option<Value> = None;
            let mut upper: Option<Value> = None;
            for cond in conds {
                let tab_ok = cond.lhs.tab == table || cond.lhs.tab.is_empty();
                if !tab_ok || cond.lhs.col != *col {
                    continue;
                }
                let v = match &cond.rhs {
                    BoundRhs::Value(v) => v.clone(),
                    _ => continue,
                };
                match cond.op {
                    CompOp::Eq => eq = Some(v),
                    CompOp::Ge => lower = Some(bound_or(lower, v, std::cmp::Ordering::Greater)),
                    CompOp::Gt => lower = Some(bound_or(lower, v, std::cmp::Ordering::Greater)),
                    CompOp::Le => upper = Some(bound_or(upper, v, std::cmp::Ordering::Less)),
                    CompOp::Lt => upper = Some(bound_or(upper, v, std::cmp::Ordering::Less)),
                    _ => {}
                }
            }
            if let Some(v) = eq {
                return Some(IndexPlan {
                    cols: vec![col.clone()],
                    lower: Some(vec![v.clone()]),
                    upper: Some(vec![v]),
                });
            }
            if lower.is_some() || upper.is_some() {
                return Some(IndexPlan {
                    cols: vec![col.clone()],
                    lower: lower.map(|v| vec![v]),
                    upper: upper.map(|v| vec![v]),
                });
            }
        } else {
            // 多列索引：全部列等值 → 点查
            let mut key = Vec::with_capacity(ix.cols.len());
            let mut ok = true;
            for col in &ix.cols {
                let hit = conds.iter().find_map(|c| {
                    let tab_ok = c.lhs.tab == table || c.lhs.tab.is_empty();
                    match (&c.rhs, tab_ok && c.lhs.col == *col && c.op == CompOp::Eq) {
                        (BoundRhs::Value(v), true) => Some(v.clone()),
                        _ => None,
                    }
                });
                match hit {
                    Some(v) => key.push(v),
                    None => {
                        ok = false;
                        break;
                    }
                }
            }
            if ok {
                return Some(IndexPlan {
                    cols: ix.cols.clone(),
                    lower: Some(key.clone()),
                    upper: Some(key),
                });
            }
        }
    }
    None
}

fn bound_or(cur: Option<Value>, v: Value, _ordering: std::cmp::Ordering) -> Value {
    // M2：同列多个范围条件取最后出现的（SQL 语义下同列双范围应求交，
    // 常见用例单条件，简化处理并文档注明）
    let _ = cur;
    v
}

// ==================== 执行器树构建 ====================

/// 由计划构建执行器树（对应 C++ Portal）。
pub fn build_executor(
    node: &PlanNode,
    db: &Database,
    ctx: &ExecCtx,
) -> Result<Box<dyn Executor>, EasyDbError> {
    match node {
        PlanNode::Scan(scan) => build_scan(scan, db, ctx),
        PlanNode::Join { left, right, conds } => {
            let l = build_executor(left, db, ctx)?;
            let r = build_executor(right, db, ctx)?;
            let mut cols = l.schema().cols.clone();
            cols.extend(r.schema().cols.clone());
            let schema = RowSchema::new(cols);
            let conds = resolve_conds(conds, &schema)?;
            Ok(Box::new(NestedLoopJoinExec::new(l, r, conds, schema.cols)))
        }
        PlanNode::Aggregate {
            input,
            group_by,
            items,
            having,
        } => {
            let inp = build_executor(input, db, ctx)?;
            let in_schema = inp.schema().clone();
            let group_idx: Vec<usize> = group_by
                .iter()
                .map(|c| lookup(&in_schema, c))
                .collect::<Result<Vec<_>, _>>()?;

            let mut agg_items = Vec::with_capacity(items.len());
            let mut out_cols = Vec::with_capacity(items.len());
            for item in items {
                let name = item_display_name(item);
                match item.agg {
                    crate::types::AggregationType::None => {
                        let idx = lookup(&in_schema, item)?;
                        let key_pos = group_by
                            .iter()
                            .position(|g| same_col(g, item))
                            .unwrap_or_else(|| {
                                group_by.iter().position(|g| g.col == item.col).unwrap_or(0)
                            });
                        agg_items.push(AggItem::Group {
                            idx,
                            key_pos,
                            name: name.clone(),
                        });
                    }
                    crate::types::AggregationType::Count => {
                        let idx = if item.col == "*" {
                            None
                        } else {
                            Some(lookup(&in_schema, item)?)
                        };
                        agg_items.push(AggItem::Count {
                            idx,
                            name: name.clone(),
                        });
                    }
                    crate::types::AggregationType::Sum => {
                        let idx = lookup(&in_schema, item)?;
                        let float = matches!(
                            source_col_type(db, input, &item.col),
                            Some(ColType::Float | ColType::Double)
                        );
                        agg_items.push(AggItem::Sum {
                            idx,
                            name: name.clone(),
                            float,
                        });
                    }
                    crate::types::AggregationType::Min => {
                        let idx = lookup(&in_schema, item)?;
                        agg_items.push(AggItem::Min {
                            idx,
                            name: name.clone(),
                        });
                    }
                    crate::types::AggregationType::Max => {
                        let idx = lookup(&in_schema, item)?;
                        agg_items.push(AggItem::Max {
                            idx,
                            name: name.clone(),
                        });
                    }
                }
                out_cols.push((String::new(), name));
            }
            let out_schema = RowSchema::new(out_cols);
            // HAVING 条件按“输出项匹配”（列名 + 聚合）解析到输出位置：
            // 例如 COUNT(*) 对应 col="*"、agg=Count 的选择项
            let having = {
                let pos_of = |c: &TabCol| -> Result<usize, EasyDbError> {
                    items
                        .iter()
                        .position(|i| same_col(i, c))
                        .ok_or_else(|| EasyDbError::ColumnNotFound(c.col.clone()))
                };
                let mut resolved = Vec::with_capacity(having.len());
                for c in having {
                    let lhs = pos_of(&c.lhs)?;
                    let rhs = match &c.rhs {
                        BoundRhs::Value(v) => ResolvedRhs::Value(v.clone()),
                        BoundRhs::Col(col) => ResolvedRhs::Col(pos_of(col)?),
                        BoundRhs::LitList(vs) => ResolvedRhs::List(vs.clone()),
                        BoundRhs::SubQuery(_) => {
                            return Err(EasyDbError::Internal(
                                "subquery should have been resolved during planning".to_string(),
                            ));
                        }
                    };
                    resolved.push(ResolvedCond { lhs, op: c.op, rhs });
                }
                resolved
            };

            Ok(Box::new(AggregateExec::new(
                inp,
                group_idx,
                agg_items,
                having,
                out_schema.cols,
            )))
        }
        PlanNode::Sort { input, key, desc } => {
            let inp = build_executor(input, db, ctx)?;
            let idx = inp
                .schema()
                .index_of(&key.tab, &key.col)
                .ok_or_else(|| EasyDbError::ColumnNotFound(key.col.clone()))?;
            Ok(Box::new(SortExec::new(inp, idx, *desc)))
        }
        PlanNode::SortPos { input, pos, desc } => {
            let inp = build_executor(input, db, ctx)?;
            Ok(Box::new(SortExec::new(inp, *pos, *desc)))
        }
        PlanNode::Project {
            input,
            cols,
            distinct,
        } => {
            let inp = build_executor(input, db, ctx)?;
            let in_schema = inp.schema().clone();
            let mut indices = Vec::with_capacity(cols.len());
            let mut out_cols = Vec::with_capacity(cols.len());
            for c in cols {
                indices.push(lookup(&in_schema, c)?);
                let name = item_display_name(c);
                out_cols.push((String::new(), name));
            }
            Ok(Box::new(ProjectExec::new(
                inp, indices, out_cols, *distinct,
            )))
        }
    }
}

fn item_display_name(item: &TabCol) -> String {
    if !item.alias.is_empty() {
        return item.alias.clone();
    }
    if item.agg != crate::types::AggregationType::None {
        let func = match item.agg {
            crate::types::AggregationType::Count => "COUNT",
            crate::types::AggregationType::Max => "MAX",
            crate::types::AggregationType::Min => "MIN",
            crate::types::AggregationType::Sum => "SUM",
            crate::types::AggregationType::None => unreachable!(),
        };
        return format!("{func}({})", item.col);
    }
    if item.tab.is_empty() {
        item.col.clone()
    } else {
        format!("{}.{}", item.tab, item.col)
    }
}

/// DML 扫描计划构建：为表生成条件下推 + 索引选择的扫描节点。
pub fn plan_table_scan(
    db: &Database,
    table: &str,
    conds: &[BoundCond],
) -> Result<PlanNode, EasyDbError> {
    let (mine, _rest) = split_conds(conds, table);
    let index = choose_index(db, table, &mine);
    Ok(PlanNode::Scan(ScanPlan {
        table: table.to_string(),
        conds: mine,
        index,
    }))
}

pub fn build_dml_scan(
    db: &Database,
    table: &str,
    conds: &[BoundCond],
    ctx: &ExecCtx,
) -> Result<Box<dyn Executor>, EasyDbError> {
    build_executor(&plan_table_scan(db, table, conds)?, db, ctx)
}

pub fn index_maintenance(db: &Database, table: &str) -> Result<Vec<IndexMaintenance>, EasyDbError> {
    let meta = db.table(table)?;
    let mut out = Vec::new();
    for ix in &meta.indexes {
        let mut col_idx = Vec::with_capacity(ix.cols.len());
        let mut col_types = Vec::with_capacity(ix.cols.len());
        let mut col_lens = Vec::with_capacity(ix.cols.len());
        for name in &ix.cols {
            let col = meta
                .cols
                .iter()
                .find(|c| c.name() == *name)
                .ok_or_else(|| EasyDbError::Internal("index column missing".to_string()))?;
            col_idx.push(
                meta.cols
                    .iter()
                    .position(|c| c.name() == *name)
                    .unwrap_or(0),
            );
            col_types.push(col.col_type());
            col_lens.push(col.storage_size() as usize);
        }
        out.push(IndexMaintenance {
            handle: ix.handle.clone(),
            col_idx,
            col_types,
            col_lens,
        });
    }
    Ok(out)
}

pub fn build_insert(
    db: &Database,
    table: &str,
    rows: Vec<Vec<Value>>,
    ctx: &ExecCtx,
) -> Result<InsertExec, EasyDbError> {
    let meta = db.table(table)?;
    Ok(InsertExec::new(
        meta.fh.clone(),
        table,
        meta.cols.clone(),
        rows,
        index_maintenance(db, table)?,
        ctx,
    ))
}

pub fn build_delete(
    db: &Database,
    table: &str,
    conds: &[BoundCond],
    ctx: &ExecCtx,
) -> Result<DeleteExec, EasyDbError> {
    let meta = db.table(table)?;
    let scan = build_dml_scan(db, table, conds, ctx)?;
    Ok(DeleteExec::new(
        scan,
        meta.fh.clone(),
        table,
        index_maintenance(db, table)?,
        ctx,
    ))
}

pub fn build_update(
    db: &Database,
    table: &str,
    clauses: &[BoundSetClause],
    conds: &[BoundCond],
    ctx: &ExecCtx,
) -> Result<UpdateExec, EasyDbError> {
    let meta = db.table(table)?;
    let scan = build_dml_scan(db, table, conds, ctx)?;
    let schema_cols: Vec<(String, String)> = meta
        .cols
        .iter()
        .map(|c| (c.tab_name().to_string(), c.name().to_string()))
        .collect();
    let row_schema = RowSchema::new(schema_cols);
    let mut resolved = Vec::with_capacity(clauses.len());
    for clause in clauses {
        let idx = row_schema
            .index_of(&clause.col.tab, &clause.col.col)
            .ok_or_else(|| EasyDbError::ColumnNotFound(clause.col.col.clone()))?;
        resolved.push((idx, clause.clone()));
    }
    Ok(UpdateExec::new(
        scan,
        meta.fh.clone(),
        table,
        meta.cols.clone(),
        resolved,
        index_maintenance(db, table)?,
        ctx,
    ))
}

fn source_col_type(db: &Database, node: &PlanNode, col: &str) -> Option<ColType> {
    match node {
        PlanNode::Scan(s) => db
            .table(&s.table)
            .ok()?
            .cols
            .iter()
            .find(|c| c.name() == col)
            .map(|c| c.col_type()),
        PlanNode::Join { left, right, .. } => {
            source_col_type(db, left, col).or_else(|| source_col_type(db, right, col))
        }
        PlanNode::Sort { input, .. }
        | PlanNode::SortPos { input, .. }
        | PlanNode::Project { input, .. }
        | PlanNode::Aggregate { input, .. } => source_col_type(db, input, col),
    }
}

fn lookup(schema: &RowSchema, c: &TabCol) -> Result<usize, EasyDbError> {
    schema
        .index_of(&c.tab, &c.col)
        .ok_or_else(|| EasyDbError::ColumnNotFound(c.col.clone()))
}

fn resolve_conds(
    conds: &[BoundCond],
    schema: &RowSchema,
) -> Result<Vec<ResolvedCond>, EasyDbError> {
    let mut out = Vec::with_capacity(conds.len());
    for c in conds {
        let lhs = lookup(schema, &c.lhs)?;
        let rhs = match &c.rhs {
            BoundRhs::Value(v) => ResolvedRhs::Value(v.clone()),
            BoundRhs::Col(col) => ResolvedRhs::Col(lookup(schema, col)?),
            BoundRhs::LitList(vs) => ResolvedRhs::List(vs.clone()),
            BoundRhs::SubQuery(_) => {
                return Err(EasyDbError::Internal(
                    "subquery should have been resolved during planning".to_string(),
                ));
            }
        };
        out.push(ResolvedCond { lhs, op: c.op, rhs });
    }
    Ok(out)
}

fn build_scan(
    scan: &ScanPlan,
    db: &Database,
    ctx: &ExecCtx,
) -> Result<Box<dyn Executor>, EasyDbError> {
    let meta = db.table(&scan.table)?;
    let fh = meta.fh.clone();
    let cols = meta.cols.clone();
    let schema = RowSchema::new(
        cols.iter()
            .map(|c| (c.tab_name().to_string(), c.name().to_string()))
            .collect(),
    );
    let conds = resolve_conds(&scan.conds, &schema)?;

    if let Some(ix) = &scan.index {
        let index_meta = meta
            .indexes
            .iter()
            .find(|m| m.cols == ix.cols)
            .ok_or_else(|| EasyDbError::Internal("index meta missing".to_string()))?;
        let encode = |vals: &Option<Vec<Value>>| -> Result<Option<Vec<u8>>, EasyDbError> {
            match vals {
                Some(v) => {
                    let types: Vec<ColType> = ix
                        .cols
                        .iter()
                        .map(|name| {
                            cols.iter()
                                .find(|c| c.name() == *name)
                                .map(|c| c.col_type())
                                .unwrap_or(ColType::Int)
                        })
                        .collect();
                    let lens: Vec<usize> = ix
                        .cols
                        .iter()
                        .map(|name| {
                            cols.iter()
                                .find(|c| c.name() == *name)
                                .map(|c| c.storage_size() as usize)
                                .unwrap_or(0)
                        })
                        .collect();
                    Ok(Some(crate::index::ix_defs::encode_key(&types, &lens, v)?))
                }
                None => Ok(None),
            }
        };
        let lower = encode(&ix.lower)?;
        let upper = encode(&ix.upper)?;
        Ok(Box::new(IndexScanExec::new(
            index_meta.handle.clone(),
            fh,
            cols,
            lower,
            upper,
            conds,
            ctx,
            &scan.table,
        )?))
    } else {
        Ok(Box::new(SeqScanExec::new(
            fh,
            cols,
            conds,
            ctx,
            &scan.table,
        )))
    }
}

// right_table_of 保留给连接顺序重排（后续版本）
#[allow(dead_code)]
fn _unused(tables: &[String], idx: usize) -> String {
    right_table_of(tables, idx)
}
