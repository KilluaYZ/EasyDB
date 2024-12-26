#!python3
import socket 
from aiohttp import web

CPP_SERVER_IP = "localhost"
CPP_SERVER_PORT = 8765
client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_address = (CPP_SERVER_IP, CPP_SERVER_PORT)
client_socket.connect(server_address)

async def ws_handle(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    async for msg in ws:
        print(f"received message :{msg}")
        client_socket.sendall(msg.data.encode('utf-8'))
        response = client_socket.recv(819200)
        await ws.send_str(response.decode('utf-8'))
    return ws

app = web.Application()
app.add_routes([web.get('/echo', ws_handle)])
web.run_app(app)