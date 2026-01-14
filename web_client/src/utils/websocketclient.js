let host_root_path = "station.killuayz.top:8080/";
// let host_root_path = import.meta.env.VITE_APP_API_HOST;
export class WebSocketClient {
    url_path = "ws://" + host_root_path + "echo";
    // url_path = import.meta.env.VITE_APP_WS_HOST;
    websocket = null;
    callBackFuncs = [];
    messageQueue = []; // 消息队列，用于在连接建立前缓存消息
    isConnecting = false; // 是否正在连接中

    connect() {
        // 如果已经连接或正在连接，直接返回
        if (this.websocket != null && this.websocket.readyState === WebSocket.OPEN) {
            return Promise.resolve();
        }
        
        // 如果正在连接中，等待连接完成
        if (this.isConnecting) {
            return new Promise((resolve) => {
                const checkConnection = () => {
                    if (this.websocket && this.websocket.readyState === WebSocket.OPEN) {
                        resolve();
                    } else if (this.websocket && this.websocket.readyState === WebSocket.CLOSED) {
                        // 连接失败，重新连接
                        this.isConnecting = false;
                        this.connect().then(resolve);
                    } else {
                        setTimeout(checkConnection, 100);
                    }
                };
                checkConnection();
            });
        }

        this.isConnecting = true;
        
        return new Promise((resolve, reject) => {
            // 关闭旧连接（如果存在）
            if (this.websocket != null) {
                this.websocket.close();
            }

            this.websocket = new WebSocket(this.url_path);
            
            this.websocket.onopen = () => {
                console.log("WebSocket connected successfully");
                this.isConnecting = false;
                // 发送队列中的消息
                while (this.messageQueue.length > 0) {
                    const msg = this.messageQueue.shift();
                    this.websocket.send(msg);
                }
                resolve();
            };

            this.websocket.onerror = (error) => {
                console.error("WebSocket connection error:", error);
                this.isConnecting = false;
                reject(error);
            };

            this.websocket.onclose = (event) => {
                console.log("WebSocket closed:", event.code, event.reason);
                this.isConnecting = false;
                // 如果非正常关闭，可以尝试重连
                if (event.code !== 1000) {
                    console.log("WebSocket closed unexpectedly, may need to reconnect");
                }
            };

            this.websocket.onmessage = (event) => {
                this.callBackFuncs.forEach(func => {
                    func(event);
                })
            }
        });
    }

    close() {
        if (this.websocket != null) {
            this.websocket.close();
            this.websocket = null;
        }
        this.messageQueue = [];
        this.isConnecting = false;
    }

    send(msg) {
        if (this.websocket && this.websocket.readyState === WebSocket.OPEN) {
            this.websocket.send(msg);
        } else if (this.websocket && this.websocket.readyState === WebSocket.CONNECTING) {
            // 连接中，将消息加入队列
            console.log("WebSocket is connecting, queuing message");
            this.messageQueue.push(msg);
        } else {
            // 未连接，尝试连接后发送
            console.log("WebSocket is not connected, attempting to connect...");
            this.messageQueue.push(msg);
            this.connect().catch(error => {
                console.error("Failed to connect WebSocket:", error);
            });
        }
    }

    addOnMessageCallBackFunc(func) {
        this.callBackFuncs.push(func);
    }
}
