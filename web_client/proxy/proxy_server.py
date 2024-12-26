# #!python3
# import socket 
# from aiohttp import web
# import asyncio
# CPP_SERVER_IP = "localhost"
# CPP_SERVER_PORT = 8765
# client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# server_address = (CPP_SERVER_IP, CPP_SERVER_PORT)
# client_socket.connect(server_address)

# async def ws_handle(request):
#     ws = web.WebSocketResponse()
#     await ws.prepare(request)
#     # async for msg in ws:
#     #     print(f"received message :{msg}")
#     #     client_socket.sendall(msg.data.encode('utf-8'))
#     #     response = client_socket.recv(819200)
#     #     await ws.send_str(response.decode('utf-8'))
#     # return ws
#     with self.resource:
#         while not ws.closed:
#             try:
#                 item = await asyncio.wait_for(self.resource.queue.get(), 2)
#             except asyncio.TimeoutError:
#                 ws.ping()
#                 continue
#             except asyncio.CancelledError:
#                 break

#             if item['action'] == 'cleared':
#                 data = {'action': 'cleared'}
#             elif item['action'] == 'received':
#                 data = {
#                     'action': 'received',
#                     'msg': serialize_message(item['msg']),
#                 }
#             else:
#                 data = None

#             if data:
#                 ws.send_str(json.dumps(data))
#         return ws

# app = web.Application()
# app.add_routes([web.get('/echo', ws_handle)])
# web.run_app(app)

#!python3
import asyncio
import websockets
import threading
import socket 
CPP_SERVER_IP = "localhost"
CPP_SERVER_PORT = 8765
SCK_MAP = {}


# 处理与外部 WebSocket 服务器的连接
async def forward_to_external_server(websocket, path):
    try:
        # # 连接到外部 WebSocket 服务器
        # async with websockets.connect(external_server_url) as external_ws:
        #     print(f"Connected to external server: {external_server_url}")

        #     # 启动两个任务来转发消息
        #     # 一个任务是从客户端读取消息并转发到外部服务器
        #     async def forward_from_client_to_external():
        #         async for message in websocket:
        #             print(f"Forwarding message to external server: {message}")
        #             await external_ws.send(message)

        #     # 另一个任务是从外部服务器读取消息并转发到客户端
        #     async def forward_from_external_to_client():
        #         async for message in external_ws:
        #             print(f"Forwarding message to client: {message}")
        #             await websocket.send(message)

            # 同时运行两个任务
            # await asyncio.gather(forward_from_client_to_external(), forward_from_external_to_client())
        client_socket = SCK_MAP[websocket.remote_address]
        async for message in websocket:
            print(f"Forwarding message to external server: {message}")
            client_socket.sendall(message.encode('utf-8'))
            response = client_socket.recv(819200)
            await websocket.send(response.decode('utf-8'))

    except Exception as e:
        print(f"Error in forward_to_external_server: {e}")
    finally:
        print(f"Connection with client {websocket.remote_address} closed.")

# WebSocket 服务器处理逻辑
async def handle_client_connection(websocket, path):
    # 这里假设每个 Web 客户端连接有一个对应的外部 WebSocket 服务器地址
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
    server_thread.daemon = True # 设置为守护线程，程序退出时自动结束 
    server_thread.start() 

if __name__ == "__main__": 
    start_server_in_thread() # 主程序可以执行其他任务，这里就模拟一个阻塞操作 
    while True: 
        pass # 这里可以处理其他任务或执行其他代码
