#!python3
import asyncio
import websockets
import threading
import socket

CPP_SERVER_IP = "localhost"
CPP_SERVER_PORT = 8765
SCK_MAP = {}

# 处理与外部 WebSocket 服务器的连接（现在改成异步与线程池结合）
async def forward_to_external_server(websocket, path):
    try:
        client_socket = SCK_MAP[websocket.remote_address]
        async for message in websocket:
            print(f"Forwarding message to external server: {message}")

            # 使用 asyncio.to_thread 将同步的 I/O 操作移到线程池中
            response = await asyncio.to_thread(handle_external_server, client_socket, message)

            # 将外部服务器的响应返回给客户端
            await websocket.send(response)

    except Exception as e:
        print(f"Error in forward_to_external_server: {e}")
    finally:
        print(f"Connection with client {websocket.remote_address} closed.")

# 处理外部服务器的同步交互
def handle_external_server(client_socket, message):
    client_socket.sendall(message.encode('utf-8'))
    response = client_socket.recv(819200)
    return response.decode('utf-8')

# WebSocket 服务器处理逻辑
async def handle_client_connection(websocket, path):
    print(f"Client connected: {websocket.remote_address}")
    
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_address = (CPP_SERVER_IP, CPP_SERVER_PORT)
    client_socket.connect(server_address)
    SCK_MAP[websocket.remote_address] = client_socket
    
    # 创建一个协程来处理和外部服务器的连接
    await forward_to_external_server(websocket, path)

# 为每个客户端连接启动独立线程
def start_websocket_server():
    # 为子线程创建并设置事件循环
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # 创建 WebSocket 服务器，监听端口并处理客户端连接
    server = websockets.serve(handle_client_connection, "localhost", 8080)

    # 启动 WebSocket 服务器
    loop.run_until_complete(server)
    print("WebSocket server started on ws://localhost:8080")

    # 保持服务器运行
    loop.run_forever()

# 启动 WebSocket 服务器 
def start_server_in_thread():
    server_thread = threading.Thread(target=start_websocket_server)
    server_thread.daemon = True
    server_thread.start()
    

if __name__ == "__main__": 
    start_server_in_thread() # 主程序可以执行其他任务，这里就模拟一个阻塞操作 
    while True: 
        pass # 这里可以处理其他任务或执行其他代码