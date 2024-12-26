let host_root_path = "killuayz.top:6032/";
// let host_root_path = import.meta.env.VITE_APP_API_HOST;
export class WebSocketClient {
    url_path = "ws://" + host_root_path + "echo";
    // url_path = import.meta.env.VITE_APP_WS_HOST;
    websocket = null;
    callBackFuncs = [];

    connect() {
        if (this.websocket == null || this.websocket.readyState !== WebSocket.OPEN) {
            this.websocket = new WebSocket(this.url_path);
            this.websocket.onmessage = (event) => {
                this.callBackFuncs.forEach(func => {
                    func(event);
                })
            }
        }
    }


    close() {
        if (this.websocket != null) {
            this.websocket.close()
        }
    }

    send(msg) {
        if (this.websocket.readyState === WebSocket.OPEN) {
            this.websocket.send(msg)
        } else {
            console.error("WebSocket is not open. Cannot send message.")
        }
    }

    addOnMessageCallBackFunc(func) {
        this.callBackFuncs.push(func);
    }
}
