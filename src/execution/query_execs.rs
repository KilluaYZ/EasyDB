//! 查询算子：投影 / 排序 / 聚合 / 连接。
//!
//! 对应 C++ 版 executor_projection / executor_sort / executor_aggregation /
//! executor_nestedloop_join。差异：
//! - 聚合为真正的哈希聚合（按分组键累加输入管道），支持 GROUP BY +
//!   HAVING；C++ 版是退化的全表聚合（无分组执行）；
//! - 连接实现嵌套循环（右子树物化），哈希/归并连接随优化器开关后续接入。

use std::collections::VecDeque;

use super::{Executor, ResolvedCond, Row, RowSchema, cmp_values, eval_cond};
use crate::common::errors::EasyDbError;
use crate::common::rid::Rid;
use crate::types::{AggregationType, Value};

/// 投影（选择列 + UNIQUE 去重）。
pub struct ProjectExec {
    input: Box<dyn Executor>,
    indices: Vec<usize>,
    schema: RowSchema,
    distinct: bool,
    seen: Vec<Vec<Value>>,
}

impl ProjectExec {
    pub fn new(
        input: Box<dyn Executor>,
        indices: Vec<usize>,
        out_cols: Vec<(String, String)>,
        distinct: bool,
    ) -> Self {
        Self {
            input,
            indices,
            schema: RowSchema::new(out_cols),
            distinct,
            seen: Vec::new(),
        }
    }
}

impl Executor for ProjectExec {
    fn schema(&self) -> &RowSchema {
        &self.schema
    }

    fn next(&mut self) -> Result<Option<Row>, EasyDbError> {
        loop {
            let Some(row) = self.input.next()? else {
                return Ok(None);
            };
            let values: Vec<Value> = self
                .indices
                .iter()
                .map(|&i| row.values[i].clone())
                .collect();
            if self.distinct {
                if self.seen.contains(&values) {
                    continue;
                }
                self.seen.push(values.clone());
            }
            return Ok(Some(Row {
                rid: Rid::INVALID,
                values,
            }));
        }
    }
}

/// 过滤（未被下推的残余条件）。
pub struct FilterExec {
    input: Box<dyn Executor>,
    conds: Vec<ResolvedCond>,
}

impl FilterExec {
    pub fn new(input: Box<dyn Executor>, conds: Vec<ResolvedCond>) -> Self {
        Self { input, conds }
    }
}

impl Executor for FilterExec {
    fn schema(&self) -> &RowSchema {
        self.input.schema()
    }

    fn next(&mut self) -> Result<Option<Row>, EasyDbError> {
        loop {
            let Some(row) = self.input.next()? else {
                return Ok(None);
            };
            let mut pass = true;
            for cond in &self.conds {
                if !eval_cond(cond, &row)? {
                    pass = false;
                    break;
                }
            }
            if pass {
                return Ok(Some(row));
            }
        }
    }
}

/// 排序：首取时物化全部输入并按键排序（稳定）。
pub struct SortExec {
    input: Box<dyn Executor>,
    key_idx: usize,
    desc: bool,
    buffer: Option<VecDeque<Row>>,
}

impl SortExec {
    pub fn new(input: Box<dyn Executor>, key_idx: usize, desc: bool) -> Self {
        Self {
            input,
            key_idx,
            desc,
            buffer: None,
        }
    }
}

impl Executor for SortExec {
    fn schema(&self) -> &RowSchema {
        self.input.schema()
    }

    fn next(&mut self) -> Result<Option<Row>, EasyDbError> {
        if self.buffer.is_none() {
            let mut rows = Vec::new();
            while let Some(row) = self.input.next()? {
                rows.push(row);
            }
            let key = self.key_idx;
            let desc = self.desc;
            rows.sort_by(|a, b| {
                let ord =
                    cmp_values(&a.values[key], &b.values[key]).unwrap_or(std::cmp::Ordering::Equal);
                if desc { ord.reverse() } else { ord }
            });
            self.buffer = Some(VecDeque::from(rows));
        }
        Ok(self.buffer.as_mut().unwrap().pop_front())
    }
}

/// 聚合输出项：分组列或聚合函数（顺序跟随 SELECT 选择列）。
#[derive(Debug, Clone)]
pub enum AggItem {
    /// 分组列：`idx` 为输入行下标，`key_pos` 为分组键中的位置
    Group {
        idx: usize,
        key_pos: usize,
        name: String,
    },
    Count {
        idx: Option<usize>,
        name: String,
    },
    /// `float` 标记源列是否浮点族（决定累加器类型）
    Sum {
        idx: usize,
        name: String,
        float: bool,
    },
    Min {
        idx: usize,
        name: String,
    },
    Max {
        idx: usize,
        name: String,
    },
}

/// HAVING 条件：对聚合输出行求值的已解析条件。
pub struct AggregateExec {
    input: Box<dyn Executor>,
    in_schema: RowSchema,
    out_schema: RowSchema,
    group_by: Vec<usize>,
    items: Vec<AggItem>,
    having: Vec<ResolvedCond>,
    groups: Option<Vec<Vec<Value>>>, // 每组输出行（按 SELECT 项顺序）
    cursor: usize,
}

impl AggregateExec {
    pub fn new(
        input: Box<dyn Executor>,
        group_by: Vec<usize>,
        items: Vec<AggItem>,
        having: Vec<ResolvedCond>,
        out_cols: Vec<(String, String)>,
    ) -> Self {
        let in_schema = input.schema().clone();
        Self {
            input,
            in_schema,
            out_schema: RowSchema::new(out_cols),
            group_by,
            items,
            having,
            groups: None,
            cursor: 0,
        }
    }

    fn accumulate(&mut self) -> Result<(), EasyDbError> {
        let mut keys: Vec<Vec<Value>> = Vec::new();
        let mut accs: Vec<Vec<AccState>> = Vec::new();
        while let Some(row) = self.input.next()? {
            let key: Vec<Value> = self
                .group_by
                .iter()
                .map(|&i| row.values[i].clone())
                .collect();
            let pos = keys.iter().position(|k| *k == key);
            let idx = match pos {
                Some(i) => i,
                None => {
                    keys.push(key);
                    accs.push(
                        self.items
                            .iter()
                            .map(|item| AccState::new(item, &self.in_schema))
                            .collect(),
                    );
                    keys.len() - 1
                }
            };
            for (i, item) in self.items.iter().enumerate() {
                accs[idx][i].update(item, &row.values);
            }
        }
        // 输出行严格按 SELECT 项顺序：分组列取键值，聚合列取累加值
        let mut out = Vec::with_capacity(accs.len());
        for (i, acc) in accs.iter().enumerate() {
            let mut row = Vec::with_capacity(self.items.len());
            for (j, item) in self.items.iter().enumerate() {
                match item {
                    AggItem::Group { key_pos, .. } => row.push(keys[i][*key_pos].clone()),
                    other => row.push(acc[j].emit(other)),
                }
            }
            out.push(row);
        }
        self.groups = Some(out);
        Ok(())
    }
}

impl Executor for AggregateExec {
    fn schema(&self) -> &RowSchema {
        &self.out_schema
    }

    fn next(&mut self) -> Result<Option<Row>, EasyDbError> {
        if self.groups.is_none() {
            self.accumulate()?;
        }
        let groups = self.groups.as_ref().unwrap();
        while self.cursor < groups.len() {
            let row = &groups[self.cursor];
            self.cursor += 1;
            let full = Row {
                rid: Rid::INVALID,
                values: row.clone(),
            };
            let mut pass = true;
            for cond in &self.having {
                if !eval_cond(cond, &full)? {
                    pass = false;
                    break;
                }
            }
            if pass {
                return Ok(Some(full));
            }
        }
        Ok(None)
    }
}

/// 单组聚合状态。
enum AccState {
    Count(u64),
    /// 数值和：整型列以 i64 累加（输出 Int），浮点列以 f64（输出 Double）
    SumInt(i64),
    SumFloat(f64),
    Min(Option<Value>),
    Max(Option<Value>),
    /// 分组列占位（值直接来自分组键）
    Group,
}

impl AccState {
    fn new(item: &AggItem, _schema: &RowSchema) -> Self {
        match item {
            AggItem::Group { .. } => AccState::Group,
            AggItem::Count { .. } => AccState::Count(0),
            AggItem::Sum { float, .. } => {
                if *float {
                    AccState::SumFloat(0.0)
                } else {
                    AccState::SumInt(0)
                }
            }
            AggItem::Min { .. } => AccState::Min(None),
            AggItem::Max { .. } => AccState::Max(None),
        }
    }

    fn update(&mut self, item: &AggItem, row: &[Value]) {
        match (self, item) {
            (AccState::Count(n), _) => *n += 1,
            (AccState::Group, _) => {}
            (AccState::SumInt(acc), AggItem::Sum { idx, .. }) => match &row[*idx] {
                Value::Int(v) => *acc += i64::from(*v),
                Value::Long(v) => *acc += *v,
                _ => {}
            },
            (AccState::SumFloat(acc), AggItem::Sum { idx, .. }) => {
                *acc += match &row[*idx] {
                    Value::Int(v) => i64::from(*v) as f64,
                    Value::Long(v) => *v as f64,
                    Value::Float(v) | Value::Double(v) => *v,
                    _ => 0.0,
                };
            }
            (AccState::Min(cur), AggItem::Min { idx, .. }) => {
                let v = &row[*idx];
                let better = cur.as_ref().is_none_or(|c| {
                    cmp_values(v, c).unwrap_or(std::cmp::Ordering::Greater)
                        == std::cmp::Ordering::Less
                });
                if better {
                    *cur = Some(v.clone());
                }
            }
            (AccState::Max(cur), AggItem::Max { idx, .. }) => {
                let v = &row[*idx];
                let better = cur.as_ref().is_none_or(|c| {
                    cmp_values(v, c).unwrap_or(std::cmp::Ordering::Less)
                        == std::cmp::Ordering::Greater
                });
                if better {
                    *cur = Some(v.clone());
                }
            }
            _ => {}
        }
    }

    fn emit(&self, _item: &AggItem) -> Value {
        match self {
            AccState::Group => Value::Int(0),
            AccState::Count(n) => Value::Int(*n as i32),
            // 整型和按 C++ 口径回填 Int；浮点和为 Double
            AccState::SumInt(n) => Value::Int(*n as i32),
            AccState::SumFloat(n) => Value::Double(*n),
            // 空组的最值（无 NULL 语义）：数值零值
            AccState::Min(v) | AccState::Max(v) => v.clone().unwrap_or(Value::Int(0)),
        }
    }
}

/// 聚合项对应的输出 AggregationType（供上层构造 schema/输出名）。
pub fn item_agg_type(item: &AggItem) -> AggregationType {
    match item {
        AggItem::Group { .. } => AggregationType::None,
        AggItem::Count { .. } => AggregationType::Count,
        AggItem::Sum { .. } => AggregationType::Sum,
        AggItem::Min { .. } => AggregationType::Min,
        AggItem::Max { .. } => AggregationType::Max,
    }
}

/// 嵌套循环连接：左驱动，右子树物化后反复扫描。
pub struct NestedLoopJoinExec {
    left: Box<dyn Executor>,
    right: Box<dyn Executor>,
    conds: Vec<ResolvedCond>,
    schema: RowSchema,
    right_rows: Option<Vec<Row>>,
    right_pos: usize,
    cur_left: Option<Row>,
    done: bool,
}

impl NestedLoopJoinExec {
    pub fn new(
        left: Box<dyn Executor>,
        right: Box<dyn Executor>,
        conds: Vec<ResolvedCond>,
        out_cols: Vec<(String, String)>,
    ) -> Self {
        Self {
            left,
            right,
            conds,
            schema: RowSchema::new(out_cols),
            right_rows: None,
            right_pos: 0,
            cur_left: None,
            done: false,
        }
    }
}

impl Executor for NestedLoopJoinExec {
    fn schema(&self) -> &RowSchema {
        &self.schema
    }

    fn next(&mut self) -> Result<Option<Row>, EasyDbError> {
        if self.done {
            return Ok(None);
        }
        if self.right_rows.is_none() {
            let mut rows = Vec::new();
            while let Some(r) = self.right.next()? {
                rows.push(r);
            }
            self.right_rows = Some(rows);
        }
        let right_rows = self.right_rows.as_ref().unwrap();

        loop {
            if self.cur_left.is_none() {
                match self.left.next()? {
                    Some(l) => {
                        self.cur_left = Some(l);
                        self.right_pos = 0;
                    }
                    None => {
                        self.done = true;
                        return Ok(None);
                    }
                }
            }
            let left = self.cur_left.as_ref().unwrap();
            while self.right_pos < right_rows.len() {
                let right = &right_rows[self.right_pos];
                self.right_pos += 1;
                let mut values = left.values.clone();
                values.extend(right.values.iter().cloned());
                let combined = Row {
                    rid: left.rid,
                    values,
                };
                let mut pass = true;
                for cond in &self.conds {
                    if !eval_cond(cond, &combined)? {
                        pass = false;
                        break;
                    }
                }
                if pass {
                    return Ok(Some(combined));
                }
            }
            self.cur_left = None;
        }
    }
}
