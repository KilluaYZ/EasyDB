//! SQL 词法器。
//!
//! 对应 C++ 版 `src-cpp/parser/lex.l`（flex）：
//! - 关键字大小写不敏感（与 `%option caseless` 一致），关键字优先于标识符；
//! - 数字带符号（`-5` 是一个整数字面量，flex 最长匹配语义）；
//! - 字符串为单引号包裹、不允许内部出现引号（`'[^']*'`）；
//! - 路径字面量：`[A-Za-z0-9._/-]+\.(csv|tbl)`；
//! - 支持 `--` 行注释与 `/* */` 块注释。

use std::fmt;

/// 词法单元。
#[derive(Debug, Clone, PartialEq)]
pub enum Tok {
    // ---- 关键字（大小写不敏感）----
    Show,
    Tables,
    Create,
    Table,
    Drop,
    Desc,
    Insert,
    Into,
    Values,
    Delete,
    From,
    Asc,
    Order,
    By,
    As,
    Count,
    Max,
    Min,
    Sum,
    Group,
    Having,
    In,
    Where,
    Update,
    Set,
    Select,
    KwInt,
    Char,
    Varchar,
    KwFloat,
    Datetime,
    NotNull,
    Index,
    Hash,
    And,
    Join,
    Exit,
    Help,
    Begin,
    Commit,
    Abort,
    Rollback,
    Unique,
    EnableNestLoop,
    EnableSortMerge,
    EnableHashJoin,
    EnableOptimizer,
    StaticCheckpoint,
    Load,
    OutputFile,
    Transaction,
    Isolation,
    Level,
    Read,
    Uncommitted,
    Committed,
    Repeatable,
    Serializable,
    // ---- 字面量 ----
    Ident(String),
    Str(String),
    IntLit(i32),
    FloatLit(f64),
    Bool(bool),
    // ---- 运算符 / 分隔符 ----
    Semi,
    LParen,
    RParen,
    Comma,
    Star,
    Dot,
    Eq,
    Lt,
    Gt,
    Le,
    Ge,
    Ne,
    Plus,
    Minus,
    Slash,
    /// 输入结束（对应 C++ T_EOF）
    Eof,
}

impl fmt::Display for Tok {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Tok::Ident(s) => write!(f, "identifier `{s}`"),
            Tok::Str(s) => write!(f, "string '{s}'"),
            Tok::IntLit(n) => write!(f, "integer {n}"),
            Tok::FloatLit(n) => write!(f, "float {n}"),
            Tok::Bool(b) => write!(f, "bool {b}"),
            Tok::Eof => write!(f, "end of input"),
            other => write!(f, "token {other:?}"),
        }
    }
}

/// 词法错误：意外字符。
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unexpected character {ch:?} at line {line} column {col}")]
pub struct LexError {
    pub ch: char,
    pub line: usize,
    pub col: usize,
}

/// 带位置的词法单元：(起点, Tok, 终点)。
pub type Spanned = (usize, Tok, usize);

/// 关键字表（小写）；`NOT NULL` 两个词在扫描器里特判。
fn keyword(word: &str) -> Option<Tok> {
    Some(match word {
        "show" => Tok::Show,
        "tables" => Tok::Tables,
        "create" => Tok::Create,
        "table" => Tok::Table,
        "drop" => Tok::Drop,
        "desc" => Tok::Desc,
        "insert" => Tok::Insert,
        "into" => Tok::Into,
        "values" => Tok::Values,
        "delete" => Tok::Delete,
        "from" => Tok::From,
        "asc" => Tok::Asc,
        "order" => Tok::Order,
        "by" => Tok::By,
        "as" => Tok::As,
        "count" => Tok::Count,
        "max" => Tok::Max,
        "min" => Tok::Min,
        "sum" => Tok::Sum,
        "group" => Tok::Group,
        "having" => Tok::Having,
        "in" => Tok::In,
        "where" => Tok::Where,
        "update" => Tok::Update,
        "set" => Tok::Set,
        "select" => Tok::Select,
        "int" | "integer" => Tok::KwInt,
        "char" => Tok::Char,
        "varchar" => Tok::Varchar,
        "float" => Tok::KwFloat,
        "datetime" => Tok::Datetime,
        "unique" => Tok::Unique,
        "index" => Tok::Index,
        "hash" => Tok::Hash,
        "and" => Tok::And,
        "join" => Tok::Join,
        "exit" => Tok::Exit,
        "help" => Tok::Help,
        "begin" => Tok::Begin,
        "commit" => Tok::Commit,
        "abort" => Tok::Abort,
        "rollback" => Tok::Rollback,
        "enable_nestloop" => Tok::EnableNestLoop,
        "enable_sortmerge" => Tok::EnableSortMerge,
        "enable_hashjoin" => Tok::EnableHashJoin,
        "enable_optimizer" => Tok::EnableOptimizer,
        "static_checkpoint" => Tok::StaticCheckpoint,
        "load" => Tok::Load,
        "output_file" => Tok::OutputFile,
        "transaction" => Tok::Transaction,
        "isolation" => Tok::Isolation,
        "level" => Tok::Level,
        "read" => Tok::Read,
        "uncommitted" => Tok::Uncommitted,
        "committed" => Tok::Committed,
        "repeatable" => Tok::Repeatable,
        "serializable" => Tok::Serializable,
        "on" | "true" => Tok::Bool(true),
        "off" | "false" => Tok::Bool(false),
        _ => return None,
    })
}

fn is_ident_start(c: char) -> bool {
    c.is_ascii_alphabetic()
}

fn is_ident_continue(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

/// 是否为路径字面量段字符（flex: {alpha}|{digit}|_|\.|\/|\-）。
fn is_path_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '/' | '-')
}

struct Lexer<'a> {
    src: &'a [u8],
    pos: usize,
    line: usize,
    col: usize,
}

impl<'a> Lexer<'a> {
    fn peek_char(&self) -> Option<char> {
        self.src.get(self.pos).copied().map(|b| b as char)
    }

    fn bump(&mut self) -> Option<char> {
        let c = self.peek_char()?;
        self.pos += 1;
        if c == '\n' {
            self.line += 1;
            self.col = 1;
        } else {
            self.col += 1;
        }
        Some(c)
    }

    fn eat_while(&mut self, pred: impl Fn(char) -> bool) -> usize {
        let start = self.pos;
        while let Some(c) = self.peek_char() {
            if pred(c) {
                self.bump();
            } else {
                break;
            }
        }
        self.pos - start
    }

    fn skip_trivia(&mut self) -> Result<(), LexError> {
        loop {
            match self.peek_char() {
                Some(c) if c.is_ascii_whitespace() => {
                    self.bump();
                }
                Some('-') if self.src.get(self.pos + 1) == Some(&b'-') => {
                    // 行注释：到行尾
                    while let Some(c) = self.peek_char() {
                        if c == '\n' {
                            break;
                        }
                        self.bump();
                    }
                }
                Some('/') if self.src.get(self.pos + 1) == Some(&b'*') => {
                    // 块注释：到最近的 */
                    self.bump();
                    self.bump();
                    loop {
                        match self.peek_char() {
                            None => break,
                            Some('*') if self.src.get(self.pos + 1) == Some(&b'/') => {
                                self.bump();
                                self.bump();
                                break;
                            }
                            _ => {
                                self.bump();
                            }
                        }
                    }
                }
                _ => return Ok(()),
            }
        }
    }

    /// 从当前词首尝试路径字面量：连续路径字符且以 .csv/.tbl 结尾。
    fn try_path(&mut self) -> Option<String> {
        let start = self.pos;
        let mut end = start;
        while let Some(c) = self.src.get(end).copied().map(|b| b as char) {
            if is_path_char(c) {
                end += 1;
            } else {
                break;
            }
        }
        let raw = std::str::from_utf8(&self.src[start..end]).ok()?.to_string();
        if (raw.ends_with(".csv") || raw.ends_with(".tbl"))
            && raw.chars().any(|c| c == '/' || c == '.' || c == '-')
            && raw
                .chars()
                .next()
                .is_some_and(|c| c.is_ascii_alphanumeric())
        {
            for _ in 0..raw.len() {
                self.bump();
            }
            Some(raw)
        } else {
            None
        }
    }

    fn next_token(&mut self) -> Result<Option<Spanned>, LexError> {
        self.skip_trivia()?;
        let start = self.pos;
        let Some(c) = self.peek_char() else {
            return Ok(Some((start, Tok::Eof, start)));
        };

        // 标识符 / 关键字 / 路径 / NOT NULL
        if is_ident_start(c) {
            if let Some(path) = self.try_path() {
                return Ok(Some((start, Tok::Str(path), self.pos)));
            }
            let len = self.eat_while(is_ident_continue);
            let word = std::str::from_utf8(&self.src[start..start + len])
                .unwrap()
                .to_string();
            // NOT NULL（两词一词法单元）
            if word.eq_ignore_ascii_case("not") {
                let mut probe = Lexer {
                    src: self.src,
                    pos: self.pos,
                    line: self.line,
                    col: self.col,
                };
                let ws = probe.eat_while(|c| c == ' ' || c == '\t');
                let next_len = probe.eat_while(is_ident_continue);
                if ws > 0
                    && next_len == 4
                    && std::str::from_utf8(&self.src[probe.pos - 4..probe.pos])
                        .unwrap()
                        .eq_ignore_ascii_case("null")
                {
                    self.pos = probe.pos;
                    self.line = probe.line;
                    self.col = probe.col;
                    return Ok(Some((start, Tok::NotNull, self.pos)));
                }
            }
            if let Some(kw) = keyword(&word.to_ascii_lowercase()) {
                return Ok(Some((start, kw, self.pos)));
            }
            return Ok(Some((start, Tok::Ident(word), self.pos)));
        }

        // 数字（带符号，flex 最长匹配：-5 是整数字面量）
        if c.is_ascii_digit()
            || ((c == '-' || c == '+')
                && self
                    .src
                    .get(self.pos + 1)
                    .is_some_and(|b| b.is_ascii_digit()))
        {
            if c == '-' || c == '+' {
                self.bump();
            }
            self.eat_while(|c| c.is_ascii_digit());
            if self.peek_char() == Some('.') {
                self.bump();
                self.eat_while(|c| c.is_ascii_digit());
                let text = std::str::from_utf8(&self.src[start..self.pos]).unwrap();
                let v: f64 = text.parse().map_err(|_| LexError {
                    ch: c,
                    line: self.line,
                    col: self.col,
                })?;
                return Ok(Some((start, Tok::FloatLit(v), self.pos)));
            }
            let text = std::str::from_utf8(&self.src[start..self.pos]).unwrap();
            let v: i32 = text.parse().map_err(|_| LexError {
                ch: c,
                line: self.line,
                col: self.col,
            })?;
            return Ok(Some((start, Tok::IntLit(v), self.pos)));
        }

        // 字符串：'[^']*'
        if c == '\'' {
            self.bump();
            let str_start = self.pos;
            while let Some(ch) = self.peek_char() {
                if ch == '\'' {
                    break;
                }
                self.bump();
            }
            if self.peek_char() != Some('\'') {
                return Err(LexError {
                    ch: '\'',
                    line: self.line,
                    col: self.col,
                });
            }
            let text = std::str::from_utf8(&self.src[str_start..self.pos])
                .map_err(|_| LexError {
                    ch: c,
                    line: self.line,
                    col: self.col,
                })?
                .to_string();
            self.bump(); // 收尾引号
            return Ok(Some((start, Tok::Str(text), self.pos)));
        }

        // 运算符与分隔符
        let tok = match c {
            ';' => Tok::Semi,
            '(' => Tok::LParen,
            ')' => Tok::RParen,
            ',' => Tok::Comma,
            '*' => Tok::Star,
            '.' => Tok::Dot,
            '=' => Tok::Eq,
            '>' => {
                if self.src.get(self.pos + 1) == Some(&b'=') {
                    self.bump();
                    Tok::Ge
                } else {
                    Tok::Gt
                }
            }
            '<' => match self.src.get(self.pos + 1) {
                Some(&b'=') => {
                    self.bump();
                    Tok::Le
                }
                Some(&b'>') => {
                    self.bump();
                    Tok::Ne
                }
                _ => Tok::Lt,
            },
            '!' => {
                if self.src.get(self.pos + 1) == Some(&b'=') {
                    self.bump();
                    Tok::Ne
                } else {
                    return Err(LexError {
                        ch: c,
                        line: self.line,
                        col: self.col,
                    });
                }
            }
            '+' => Tok::Plus,
            '-' => Tok::Minus,
            '/' => Tok::Slash,
            _ => {
                return Err(LexError {
                    ch: c,
                    line: self.line,
                    col: self.col,
                });
            }
        };
        self.bump();
        Ok(Some((start, tok, self.pos)))
    }
}

/// 词法分析入口：整段 SQL → 词法单元流（以 [`Tok::Eof`] 结尾）。
pub fn tokenize(src: &str) -> Result<Vec<Spanned>, LexError> {
    let mut lx = Lexer {
        src: src.as_bytes(),
        pos: 0,
        line: 1,
        col: 1,
    };
    let mut out = Vec::new();
    loop {
        let tok = lx.next_token()?;
        let eof = tok.as_ref().is_some_and(|(_, t, _)| *t == Tok::Eof);
        if let Some(t) = tok {
            out.push(t);
        }
        if eof {
            return Ok(out);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn toks(src: &str) -> Vec<Tok> {
        tokenize(src)
            .unwrap()
            .into_iter()
            .map(|(_, t, _)| t)
            .collect()
    }

    #[test]
    fn keywords_case_insensitive() {
        assert_eq!(
            toks("SeLeCt * FrOm t"),
            vec![
                Tok::Select,
                Tok::Star,
                Tok::From,
                Tok::Ident("t".into()),
                Tok::Eof
            ]
        );
    }

    #[test]
    fn signed_numbers_and_floats() {
        assert_eq!(
            toks("-5 3.25 +7"),
            vec![
                Tok::IntLit(-5),
                Tok::FloatLit(3.25),
                Tok::IntLit(7),
                Tok::Eof
            ]
        );
        // flex 语义：`5.` 也是浮点
        assert_eq!(toks("5."), vec![Tok::FloatLit(5.0), Tok::Eof]);
    }

    #[test]
    fn strings_and_comments() {
        assert_eq!(
            toks("'a b' -- trailing\n/* block */ 'c'"),
            vec![Tok::Str("a b".into()), Tok::Str("c".into()), Tok::Eof]
        );
    }

    #[test]
    fn operators() {
        assert_eq!(
            toks(">= <= <> != = < > ; ( ) , * ."),
            vec![
                Tok::Ge,
                Tok::Le,
                Tok::Ne,
                Tok::Ne,
                Tok::Eq,
                Tok::Lt,
                Tok::Gt,
                Tok::Semi,
                Tok::LParen,
                Tok::RParen,
                Tok::Comma,
                Tok::Star,
                Tok::Dot,
                Tok::Eof,
            ]
        );
    }

    #[test]
    fn not_null_and_bools() {
        assert_eq!(
            toks("name VARCHAR(8) NOT NULL, x INT"),
            vec![
                Tok::Ident("name".into()),
                Tok::Varchar,
                Tok::LParen,
                Tok::IntLit(8),
                Tok::RParen,
                Tok::NotNull,
                Tok::Comma,
                Tok::Ident("x".into()),
                Tok::KwInt,
                Tok::Eof,
            ]
        );
        assert_eq!(
            toks("SET ENABLE_NESTLOOP ON"),
            vec![Tok::Set, Tok::EnableNestLoop, Tok::Bool(true), Tok::Eof]
        );
        assert_eq!(
            toks("SET ENABLE_NESTLOOP = FALSE"),
            vec![
                Tok::Set,
                Tok::EnableNestLoop,
                Tok::Eq,
                Tok::Bool(false),
                Tok::Eof
            ]
        );
    }

    #[test]
    fn paths() {
        assert_eq!(
            toks("LOAD test_data/t1.csv INTO t1"),
            vec![
                Tok::Load,
                Tok::Str("test_data/t1.csv".into()),
                Tok::Into,
                Tok::Ident("t1".into()),
                Tok::Eof,
            ]
        );
    }

    #[test]
    fn unexpected_char_errors() {
        assert!(tokenize("select @").is_err());
        // 未闭合字符串
        assert!(tokenize("'abc").is_err());
    }
}
