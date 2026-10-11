//! EasyDB TUI 客户端：ratatui + crossterm + tui-textarea。
//!
//! 布局：上方为结果区（滚动查看历史输出），下方为 SQL 编辑器，
//! 底部状态栏（连接信息 / 快捷键提示）。
//!
//! 快捷键：
//! - `Enter` 执行编辑器中的 SQL（缺分号自动补齐）；`Alt+Enter` 换行
//! - `Alt+↑` / `Alt+↓` 翻阅历史；`PgUp` / `PgDn` 滚动结果区
//! - `Esc` 退出

use crossterm::event::{
    self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyModifiers,
};
use ratatui::prelude::*;
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};
use std::io::{Stdout, stdout};
use tui_textarea::TextArea;

use easydb::client::{ClientConn, ClientError};

fn main() {
    let mut host = String::from("127.0.0.1");
    let mut port: u16 = 8765;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-p" => {
                port = args.next().and_then(|v| v.parse().ok()).unwrap_or_else(|| {
                    eprintln!("invalid port");
                    std::process::exit(1);
                });
            }
            "-h" => {
                host = args.next().unwrap_or_else(|| {
                    eprintln!("-h requires a host");
                    std::process::exit(1);
                });
            }
            other => {
                eprintln!("usage: easydb_client [-h host] [-p port]  (got {other})");
                std::process::exit(1);
            }
        }
    }

    let conn = match ClientConn::connect(&host, port) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("failed to connect to {host}:{port}: {e}");
            std::process::exit(1);
        }
    };

    let _ = crossterm::terminal::enable_raw_mode();
    let mut stdout = stdout();
    let _ = crossterm::execute!(
        stdout,
        crossterm::terminal::EnterAlternateScreen,
        EnableMouseCapture
    );
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = ratatui::Terminal::new(backend).unwrap();

    let result = run_app(&mut terminal, conn, format!("{host}:{port}"));

    let _ = crossterm::execute!(
        terminal.backend_mut(),
        crossterm::terminal::LeaveAlternateScreen,
        DisableMouseCapture
    );
    let _ = crossterm::terminal::disable_raw_mode();

    if let Err(e) = result {
        match e {
            AppError::Io(e) => eprintln!("connection error: {e}"),
            AppError::Exit => println!("Bye."),
        }
    }
}

enum AppError {
    Io(std::io::Error),
    Exit,
}

impl From<std::io::Error> for AppError {
    fn from(e: std::io::Error) -> Self {
        AppError::Io(e)
    }
}

#[derive(Default)]
struct App {
    /// 服务端响应（按次累积展示）
    output: String,
    history: Vec<String>,
    history_pos: Option<usize>,
    /// 滚动偏移（结果区自底向上）
    scroll: u16,
    finished: bool,
}

fn run_app(
    terminal: &mut ratatui::Terminal<CrosstermBackend<Stdout>>,
    mut conn: ClientConn,
    addr: String,
) -> Result<(), AppError> {
    let mut textarea = TextArea::default();
    textarea.set_block(
        Block::default()
            .borders(Borders::ALL)
            .title(" SQL (Enter 执行 / Alt+Enter 换行) "),
    );
    let mut app = App {
        output: format!("已连接 {addr} — Esc 退出，Enter 执行，Alt+↑/↓ 历史，PgUp/PgDn 滚动\n"),
        ..Default::default()
    };

    loop {
        terminal.draw(|f| {
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Min(5),
                    Constraint::Length(7),
                    Constraint::Length(1),
                ])
                .split(f.area());

            let results = Paragraph::new(app.output.as_str())
                .block(Block::default().borders(Borders::ALL).title(" Results "))
                .wrap(Wrap { trim: false })
                .scroll((app.scroll, 0));
            f.render_widget(results, chunks[0]);

            let _ = &textarea;
            f.render_widget(&textarea, chunks[1]);

            let hint = Line::from(vec![
                Span::styled(
                    format!(" {} ", addr),
                    Style::default().fg(Color::Black).bg(Color::Green),
                ),
                Span::raw(
                    " Enter 执行 · Alt+Enter 换行 · Alt+↑/↓ 历史 · PgUp/PgDn 滚动 · Esc 退出 ",
                ),
            ]);
            f.render_widget(Paragraph::new(hint), chunks[2]);
        })?;

        // 结果区默认滚到底部（最新输出可见）
        let total_lines = app.output.lines().count() as u16;
        app.scroll = app.scroll.min(total_lines.saturating_sub(1));

        if let Event::Key(key) = event::read()? {
            if key.kind == event::KeyEventKind::Repeat {
                // 长按重复保留默认行为
            }
            match (key.modifiers, key.code) {
                (KeyModifiers::NONE, KeyCode::Esc) => {
                    app.finished = true;
                }
                (KeyModifiers::NONE, KeyCode::PageDown) => {
                    app.scroll = app.scroll.saturating_add(5);
                }
                (KeyModifiers::NONE, KeyCode::PageUp) => {
                    app.scroll = app.scroll.saturating_sub(5);
                }
                (KeyModifiers::ALT, KeyCode::Up) => {
                    if let Some(sql) = app.history_prev() {
                        textarea = TextArea::default();
                        textarea.insert_str(sql);
                        textarea.set_block(
                            Block::default()
                                .borders(Borders::ALL)
                                .title(" SQL (Enter 执行 / Alt+Enter 换行) "),
                        );
                    }
                }
                (KeyModifiers::ALT, KeyCode::Down) => {
                    if let Some(sql) = app.history_next() {
                        textarea = TextArea::default();
                        textarea.insert_str(sql);
                        textarea.set_block(
                            Block::default()
                                .borders(Borders::ALL)
                                .title(" SQL (Enter 执行 / Alt+Enter 换行) "),
                        );
                    }
                }
                (KeyModifiers::NONE, KeyCode::Enter) => {
                    let sql = textarea.lines().join(" ");
                    textarea = TextArea::default();
                    textarea.set_block(
                        Block::default()
                            .borders(Borders::ALL)
                            .title(" SQL (Enter 执行 / Alt+Enter 换行) "),
                    );
                    if sql.trim().is_empty() {
                        continue;
                    }
                    app.history.push(sql.trim().to_string());
                    app.history_pos = None;
                    app.output.push_str("\n> ");
                    app.output.push_str(sql.trim());
                    app.output.push('\n');
                    match conn.execute(&sql) {
                        Ok(resp) => {
                            if !resp.is_empty() {
                                app.output.push_str(&resp);
                                app.output.push('\n');
                            }
                            // 滚到底部（draw 时按内容行数夹取）
                            app.scroll = u16::MAX;
                        }
                        Err(ClientError::Exit) => {
                            app.finished = true;
                        }
                        Err(ClientError::Io(e)) => {
                            app.output.push_str(&format!("connection error: {e}\n"));
                            app.scroll = u16::MAX;
                            // 连接已断开则退出循环
                            return Err(AppError::Io(e));
                        }
                    }
                }
                _ => {
                    // 其余按键交给编辑器处理
                    textarea.input(key);
                }
            }
            if app.finished {
                return Err(AppError::Exit);
            }
        }
    }
}

impl App {
    fn history_prev(&mut self) -> Option<String> {
        if self.history.is_empty() {
            return None;
        }
        let pos = match self.history_pos {
            None => self.history.len() - 1,
            Some(0) => return None,
            Some(p) => p - 1,
        };
        self.history_pos = Some(pos);
        self.history.get(pos).cloned()
    }

    fn history_next(&mut self) -> Option<String> {
        let pos = self.history_pos?;
        let next = if pos + 1 >= self.history.len() {
            self.history_pos = None;
            return Some(String::new());
        } else {
            pos + 1
        };
        self.history_pos = Some(next);
        self.history.get(next).cloned()
    }
}
