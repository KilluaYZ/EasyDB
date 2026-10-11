//! 客户端连接：与服务端交换 SQL/响应文本的协议封装。
//!
//! 协议与 C++ 版客户端一致：发送 SQL 文本 + NUL；接收纯文本响应，
//! 读到 NUL（或对端关闭）为止。

use std::io::{Read, Write};
use std::net::TcpStream;

/// 与服务端的连接。
pub struct ClientConn {
    stream: TcpStream,
}

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// 退出/断开指令
    #[error("exit")]
    Exit,
}

impl ClientConn {
    /// 连接 `host:port`。
    pub fn connect(host: &str, port: u16) -> Result<Self, ClientError> {
        let stream = TcpStream::connect((host, port))?;
        Ok(Self { stream })
    }

    /// 执行一条 SQL，返回服务端响应文本。
    /// `exit`/`bye` 返回 [`ClientError::Exit`]（连接由调用方关闭）。
    pub fn execute(&mut self, sql: &str) -> Result<String, ClientError> {
        let trimmed = sql.trim();
        if trimmed == "exit" || trimmed == "exit;" || trimmed == "bye" || trimmed == "bye;" {
            return Err(ClientError::Exit);
        }
        let mut stmt = trimmed.to_string();
        // 语句缺分号时自动补齐（HELP/EXIT 除外）
        if !stmt.ends_with(';') && !stmt.is_empty() {
            stmt.push(';');
        }
        self.stream.write_all(stmt.as_bytes())?;
        self.stream.write_all(b"\0")?;
        self.stream.flush()?;

        // 读到 NUL 为止（TCP 可能分段，必须循环读）
        let mut acc = Vec::new();
        let mut chunk = [0u8; 8192];
        loop {
            let n = self.stream.read(&mut chunk)?;
            if n == 0 {
                return Ok(String::from_utf8_lossy(&acc).to_string()); // 对端关闭
            }
            acc.extend_from_slice(&chunk[..n]);
            if acc.contains(&0) {
                let end = acc.iter().position(|&b| b == 0).unwrap_or(acc.len());
                return Ok(String::from_utf8_lossy(&acc[..end]).to_string());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};

    /// 最小回声服务端：收到 SQL 回一条固定文本 + NUL。
    fn echo_server() -> (u16, Arc<Mutex<Vec<String>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let received = Arc::new(Mutex::new(Vec::new()));
        let recv2 = received.clone();
        std::thread::spawn(move || {
            let Ok((mut stream, _)) = listener.accept() else {
                return;
            };
            let mut buf = [0u8; 8192];
            let mut acc = Vec::new();
            loop {
                let n = match stream.read(&mut buf) {
                    Ok(0) | Err(_) => return,
                    Ok(n) => n,
                };
                acc.extend_from_slice(&buf[..n]);
                let Some(end) = acc.iter().position(|&b| b == 0) else {
                    continue;
                };
                let sql = String::from_utf8_lossy(&acc[..end]).to_string();
                acc.drain(..=end);
                recv2.lock().unwrap().push(sql.clone());
                let resp = format!("resp-for: {sql}");
                let _ = stream.write_all(resp.as_bytes());
                let _ = stream.write_all(b"\0");
            }
        });
        (port, received)
    }

    #[test]
    fn execute_roundtrip_and_semicolon_autofill() {
        let (port, received) = echo_server();
        let mut conn = ClientConn::connect("127.0.0.1", port).unwrap();

        // 缺分号自动补齐
        let resp = conn.execute("SELECT 1").unwrap();
        assert_eq!(resp, "resp-for: SELECT 1;");
        // 已有分号保持不变
        let resp = conn.execute("SELECT 2;").unwrap();
        assert_eq!(resp, "resp-for: SELECT 2;");
        assert_eq!(
            received.lock().unwrap().as_slice(),
            ["SELECT 1;", "SELECT 2;"]
        );
    }

    #[test]
    fn exit_returns_error_marker() {
        let (port, _received) = echo_server();
        let mut conn = ClientConn::connect("127.0.0.1", port).unwrap();
        assert!(matches!(conn.execute("exit"), Err(ClientError::Exit)));
        assert!(matches!(conn.execute("bye;"), Err(ClientError::Exit)));
    }
}
