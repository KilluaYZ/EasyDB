//! EasyDB 服务端：TCP 文本协议（与 C++ 版客户端兼容）。
//!
//! 协议：客户端发送 SQL 文本并以 NUL 结尾；服务端返回纯文本响应并以
//! NUL 结尾（SELECT 为 RecordPrinter 风格表格，DML/DDL 成功为空文本，
//! 错误为错误信息 + 换行）。`exit`/`bye` 断开连接。
//!
//! 并发模型：共享 `Arc<Mutex<Database>>`，一连接一线程——数据库级互斥
//! 与 C++ 版"缓冲池全局大锁"的串行化粒度相当；行级冲突由事务层
//! wait-die 锁管理器保证（P6）。

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use clap::Parser;
use easydb::system::{Database, Session, format_output};

const BUFFER_LENGTH: usize = 8192;

#[derive(Debug, Parser)]
#[command(name = "easydb_server", about = "EasyDB TCP server")]
struct Args {
    /// TCP port to listen on.
    #[arg(short, long, default_value_t = 8765, value_name = "PORT")]
    port: u16,

    /// Directory used to store the database files.
    #[arg(
        short = 'd',
        long = "db-dir",
        default_value = "test.db",
        value_name = "DIR"
    )]
    db_dir: PathBuf,
}

fn main() {
    let args = Args::parse();

    let addr = format!("127.0.0.1:{}", args.port);
    let listener = TcpListener::bind(&addr).unwrap_or_else(|e| {
        eprintln!("failed to bind {addr}: {e}");
        std::process::exit(1);
    });
    let db = Arc::new(Mutex::new(Database::open(&args.db_dir).unwrap_or_else(
        |e| {
            eprintln!("failed to open database at {}: {e}", args.db_dir.display());
            std::process::exit(1);
        },
    )));
    println!(
        "EasyDB (Rust) listening on {addr}, data dir '{}'",
        args.db_dir.display()
    );

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let db = db.clone();
                std::thread::spawn(move || {
                    if let Err(e) = serve_connection(stream, db) {
                        eprintln!("connection error: {e}");
                    }
                    println!("client connection closed");
                });
            }
            Err(e) => eprintln!("accept error: {e}"),
        }
    }
}

/// 服务一个客户端连接：循环读取 SQL（NUL 结尾），执行并回送文本。
fn serve_connection(stream: TcpStream, db: Arc<Mutex<Database>>) -> std::io::Result<()> {
    let mut stream = stream;
    let mut session = Session::new();
    let mut buf = [0u8; BUFFER_LENGTH];
    let mut acc: Vec<u8> = Vec::new();
    loop {
        let n = stream.read(&mut buf)?;
        if n == 0 {
            db.lock()
                .unwrap_or_else(|e| e.into_inner())
                .close_session(&mut session);
            return Ok(()); // 对端关闭
        }
        acc.extend_from_slice(&buf[..n]);
        // 协议要求 NUL 结尾；未读到完整语句则继续读（TCP 可能分段）
        let Some(stmt_end) = acc.iter().position(|&b| b == 0) else {
            continue;
        };
        let sql = String::from_utf8_lossy(&acc[..stmt_end]).to_string();
        acc.drain(..=stmt_end);
        let trimmed = sql.trim();
        if trimmed == "exit" || trimmed == "exit;" || trimmed == "bye" || trimmed == "bye;" {
            db.lock()
                .unwrap_or_else(|e| e.into_inner())
                .close_session(&mut session);
            return Ok(());
        }

        let response = {
            let mut db = db.lock().unwrap_or_else(|e| e.into_inner());
            match db.run_sql_in_session(&mut session, trimmed) {
                Ok(out) => format_output(&out),
                // C++ 版错误路径：错误信息 + 换行
                Err(e) => format!("{e}\n"),
            }
        };
        stream.write_all(response.as_bytes())?;
        stream.write_all(b"\0")?;
        stream.flush()?;
    }
}
