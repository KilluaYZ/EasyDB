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
    client_socket = None
    try:
        if websocket.remote_address not in SCK_MAP:
            print(f"Error: No socket found for {websocket.remote_address}")
            return
        
        client_socket = SCK_MAP[websocket.remote_address]
        async for message in websocket:
            print(f"Forwarding message to external server: {message}")

            try:
                # 使用 asyncio.to_thread 将同步的 I/O 操作移到线程池中
                response = await asyncio.to_thread(handle_external_server, client_socket, message)

                # 将外部服务器的响应返回给客户端
                await websocket.send(response)
            except websockets.exceptions.ConnectionClosed:
                print("WebSocket connection closed by client")
                break
            except Exception as e:
                print(f"Error processing message: {e}")
                import traceback
                traceback.print_exc()
                # 继续处理下一条消息，不中断连接

    except websockets.exceptions.ConnectionClosed:
        print(f"WebSocket connection closed: {websocket.remote_address}")
    except Exception as e:
        print(f"Error in forward_to_external_server: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print(f"Connection with client {websocket.remote_address} closed.")
        # 清理 socket 连接
        if websocket.remote_address in SCK_MAP:
            try:
                if client_socket:
                    client_socket.close()
            except:
                pass
            del SCK_MAP[websocket.remote_address]

# 处理外部服务器的同步交互
def handle_external_server(client_socket, message):
    try:
        client_socket.sendall(message.encode('utf-8'))
        response = client_socket.recv(819200)
        if not response:
            raise ConnectionError("Connection closed by server")
        return response.decode('utf-8')
    except Exception as e:
        print(f"Error in handle_external_server: {e}")
        raise

# WebSocket 服务器处理逻辑
async def handle_client_connection(connection):
    # websockets 12.0+ 版本 API：回调函数只接收一个 connection 参数
    websocket = connection
    # 在 websockets 16.0 中，尝试获取路径信息（如果可用）
    path = '/'
    try:
        # 尝试多种方式获取路径
        if hasattr(connection, 'request') and hasattr(connection.request, 'path'):
            path = connection.request.path
        elif hasattr(connection, 'path'):
            path = connection.path
    except Exception as e:
        print(f"Could not get path: {e}")
        path = '/'
    
    print(f"Client connected: {websocket.remote_address}, path: {path}")
    
    # 可以在这里处理不同的路径，目前接受所有路径（包括 /echo）
    # if path != "/echo":
    #     await websocket.close(code=1008, reason="Invalid path")
    #     return
    
    try:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_address = (CPP_SERVER_IP, CPP_SERVER_PORT)
        client_socket.connect(server_address)
        SCK_MAP[websocket.remote_address] = client_socket
        
        # 创建一个协程来处理和外部服务器的连接
        await forward_to_external_server(websocket, path)
    except Exception as e:
        print(f"Error in handle_client_connection: {e}")
        import traceback
        traceback.print_exc()
        await websocket.close(code=1011, reason=f"Server error: {str(e)}")

# 异步启动 WebSocket 服务器
async def start_websocket_server_async():
    # 创建 WebSocket 服务器，监听端口并处理客户端连接
    # 使用 "0.0.0.0" 允许外部连接，如果只需要本地访问可以使用 "localhost"
    async with websockets.serve(handle_client_connection, "0.0.0.0", 8080):
        print("WebSocket server started on ws://0.0.0.0:8080 (accessible from any interface)")
        # 保持服务器运行
        await asyncio.Future()  # 永远等待

# 为每个客户端连接启动独立线程
def start_websocket_server():
    # 为子线程创建并设置事件循环
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    # 在事件循环中运行服务器
    loop.run_until_complete(start_websocket_server_async())

# 启动 WebSocket 服务器 
def start_server_in_thread():
    server_thread = threading.Thread(target=start_websocket_server)
    server_thread.daemon = True
    server_thread.start()
    

if __name__ == "__main__": 
    start_server_in_thread() # 主程序可以执行其他任务，这里就模拟一个阻塞操作 
    while True: 
        pass # 这里可以处理其他任务或执行其他代码