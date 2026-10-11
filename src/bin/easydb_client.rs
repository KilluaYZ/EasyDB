//! EasyDB 命令行客户端：连接服务端，读取 SQL 并打印执行结果。
//!
//! 每条以分号结尾的语句会立即执行；未完成的语句可以跨多行输入。
//! rustyline 提供上下键历史、行编辑、Ctrl-C 和 Ctrl-D 支持。

use std::error::Error;

use clap::Parser;
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;

use easydb::client::{ClientConn, ClientError};

#[derive(Debug, Parser)]
#[command(name = "easydb_client", about = "EasyDB command-line client")]
struct Args {
    /// EasyDB server host name or IP address.
    #[arg(short = 'H', long, default_value = "127.0.0.1", value_name = "HOST")]
    host: String,

    /// EasyDB server TCP port.
    #[arg(short, long, default_value_t = 8765, value_name = "PORT")]
    port: u16,
}

fn main() {
    let args = Args::parse();
    let mut conn = match ClientConn::connect(&args.host, args.port) {
        Ok(conn) => conn,
        Err(e) => {
            eprintln!("failed to connect to {}:{}: {e}", args.host, args.port);
            std::process::exit(1);
        }
    };

    println!("Connected to EasyDB at {}:{}.", args.host, args.port);
    println!("Type SQL statements terminated by ';'. Type 'exit' or press Ctrl-D to quit.");

    if let Err(e) = run_repl(&mut conn) {
        eprintln!("client error: {e}");
        std::process::exit(1);
    }
}

fn run_repl(conn: &mut ClientConn) -> Result<(), Box<dyn Error>> {
    let mut editor = DefaultEditor::new()?;
    let mut statement = String::new();

    loop {
        let prompt = if statement.trim().is_empty() {
            "easydb> "
        } else {
            "    -> "
        };

        let line = match editor.readline(prompt) {
            Ok(line) => line,
            Err(ReadlineError::Interrupted) => {
                statement.clear();
                println!("^C");
                continue;
            }
            Err(ReadlineError::Eof) => {
                if !statement.trim().is_empty() {
                    execute_statement(conn, &statement)?;
                }
                println!();
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        };

        let trimmed = line.trim();
        if statement.trim().is_empty() && (trimmed == "exit" || trimmed == "bye") {
            println!("Bye.");
            return Ok(());
        }

        statement.push_str(&line);
        statement.push('\n');
        if statement.trim_end().ends_with(';') {
            let sql = statement.trim().to_string();
            let should_exit = matches!(sql.as_str(), "exit;" | "bye;");
            if !sql.is_empty() {
                editor.add_history_entry(sql.as_str())?;
                execute_statement(conn, &sql)?;
            }
            statement.clear();
            if should_exit {
                return Ok(());
            }
        }
    }
}

fn execute_statement(conn: &mut ClientConn, statement: &str) -> Result<(), Box<dyn Error>> {
    match conn.execute(statement) {
        Ok(output) => {
            if !output.is_empty() {
                print!("{output}");
                if !output.ends_with('\n') {
                    println!();
                }
            }
            Ok(())
        }
        Err(ClientError::Exit) => {
            println!("Bye.");
            Ok(())
        }
        Err(ClientError::Io(e)) => Err(e.into()),
    }
}
