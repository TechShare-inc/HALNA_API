import asyncio
import websockets
import json
import ssl
import time
import os

async def on_message(websocket):
    async for message in websocket:
        if isinstance(message, bytes):
            await handle_binary_message(message)
        else:
            await handle_text_message(websocket, message)

async def handle_text_message(websocket, message):
    print(f"Received command: {message}")
    data = json.loads(message)
    # メッセージタイプに応じた処理を行う
    if 'navigation' in data:
        await handle_navigation_command(websocket, data['navigation'])
    elif 'cmd_vel' in data:
        await handle_cmd_vel_command(websocket, data['cmd_vel'])
    elif 'param' in data:
        await handle_param_command(websocket, data['param'])
    elif 'graph' in data:
        await handle_graph_command(websocket, data['graph'])
    elif 'rosbag' in data:
        await handle_rosbag_command(websocket, data['rosbag'])
    elif 'picture' in data:
        await handle_picture_command(websocket, data['picture'])
    elif 'process' in data:
        await handle_process_command(websocket, data['process'])
    else:
        print("Unknown command received")

async def handle_binary_message(message):
    print("Binary message received (not expected in client)")

async def handle_navigation_command(websocket, command):
    msg_type = command.get('msg_type')
    if msg_type == 'NAVIGATION':
        x = command.get('x')
        y = command.get('y')
        th = command.get('th')
        print(f"Navigating to ({x}, {y}, {th})")
        await asyncio.sleep(1)
        response = create_base_message("NAVIGATION_RESPONSE")
        response.update({
            "destination": command.get('sender'),
            "command_id": command.get('command_id'),
            "result": True
        })
        await send_msg(websocket, response)

async def handle_cmd_vel_command(websocket, command):
    linear_x = command.get('linear_x')
    linear_y = command.get('linear_y')
    angular_z = command.get('angular_z')
    print(f"Moving with linear_x={linear_x}, linear_y={linear_y}, angular_z={angular_z}")

async def handle_param_command(websocket, command):
    print(f"Updating parameters: {command}")
    response = create_base_message("PARAM")
    response.update({
        "destination": command.get('sender'),
        "command_id": "0",
        "type": "updated_param",
        "msg": command
    })
    await send_msg(websocket, response)

async def handle_graph_command(websocket, command):
    print(f"Graph command received: {command}")

async def handle_rosbag_command(websocket, command):
    print(f"Rosbag command received: {command}")
    if command.get('type') == 'get_rosbags':
        response = create_base_message("ROSBAG")
        response.update({
            "destination": command.get('sender'),
            "command_id": "0",
            "type": "rosbag_name_list",
            "msg": ["rosbag1.bag", "rosbag2.bag"]
        })
        await send_msg(websocket, response)

async def handle_picture_command(websocket, command):
    print(f"Picture command received: {command}")
    response = create_base_message("MESSAGE")
    response.update({
        "destination": command.get('sender'),
        "command_id": "0",
        "msg": "Received all the patrol pictures",
        "error": False
    })
    await send_msg(websocket, response)

async def handle_process_command(websocket, command):
    print(f"Process command received: {command}")
    response = create_base_message("MESSAGE")
    response.update({
        "destination": command.get('sender'),
        "command_id": "0",
        "msg": f"Process '{command.get('systemctl')}' executed",
        "error": False
    })
    await send_msg(websocket, response)

async def send_graph(websocket):
    node_file = os.path.join("dummy_graph", "node", "original_graph_node.txt")
    edge_file = os.path.join("dummy_graph", "edge", "original_graph_edge.txt")
    extenstions = ["node", "edge"]
    files = [node_file, edge_file]
    for i in range(len(extenstions)):
        with open(files[i], "rb") as file:
            file_name = "graph_data:original_graph_" + extenstions[i] + ".txt"
            file_data = file.read()
            header = file_name.encode('utf-8')
            floor_name = "floor_0".encode('utf-8')
            map_name_bytes = "map_001".encode('utf-8')
            combined_message = header + b'\0' + map_name_bytes + b'\0' + floor_name + b'\0' + file_data
            await websocket.send(combined_message)
        time.sleep(0.2)

async def send_initial_messages(websocket):
    initial_message = create_base_message("INNITIAL_CONNECTION")
    initial_message.update({
        "destination": "server",
        "map_id": "map_001",
        "ip_address": "192.168.1.2",
        "building_number": 0,
        "floor_level": 0
    })
    await send_msg(websocket, initial_message)
    await send_graph(websocket)



def create_base_message(msg_type):
    return {
        "sender": "robot1",
        "timestamp": time.time(),
        "msgtype": msg_type,
    }

async def send_msg(websocket, payload):
    message = json.dumps({"message": payload})
    print(f"Sending: {message}")
    await websocket.send(message)

async def main():
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    uri = "wss://localhost:5000/sample/robot1/"

    async with websockets.connect(uri, ssl=ssl_context) as websocket:
        await send_initial_messages(websocket)
        await on_message(websocket)

if __name__ == "__main__":
    asyncio.run(main())
