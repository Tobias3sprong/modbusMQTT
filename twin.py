import json
import time
import struct
import threading
from struct import unpack
from pymodbus.client.serial import ModbusSerialClient
from pymodbus.client.tcp import ModbusTcpClient
from paho.mqtt import client as mqtt_client

# Load credentials
json_file_path = r".secrets/credentials.json"
with open(json_file_path, "r") as f:
    credentials = json.load(f)

def on_connect(client, userdata, flags, rc):
    if rc == 0 and client.is_connected():
        print("Connected to MQTT broker")

def on_disconnect(client, userdata, rc):
    print(f"MQTT disconnected with result code: {rc}")
    if rc != 0:
        print("Unexpected disconnection. Attempting to reconnect...")
        try:
            client.reconnect()
        except Exception as e:
            print(f"Reconnect failed: {e}")

# MODBUS
# Set up modbus RTU
comapA = ModbusSerialClient(
    port='/dev/ttyUSB0',
    stopbits=1,
    bytesize=8,
    parity='N',
    baudrate=19200,
    timeout=0.3
)

comapB = ModbusSerialClient(
    port='/dev/ttyUSB1',
    stopbits=1,
    bytesize=8,
    parity='N',
    baudrate=19200,
    timeout=0.3
)
# Set up modbus TCP
teltonika = ModbusTcpClient(
    host="localhost",
    port=502
)

def modbusConnect(comap):
    print(f"Attempting to connect to {comap}")
    while not comap.connect():
        print("Modbus initialisation failed")
        time.sleep(1)

def modbusTcpConnect(teltonika):
    print("Attempting to connect to Modbus TCP server...")
    while not teltonika.connect():
        print("Modbus TCP initialisation failed")
        time.sleep(1)

def discover_slave_id(modbus_client, start=1, end=247):
    for slave in range(start, end + 1):
        print(f"Testing slave id: {slave} ...")
        try:
            response = modbus_client.read_holding_registers(3000, count=8, slave=slave)
            if response is not None and hasattr(response, 'registers') and len(response.registers) > 0:
                print(f"Device found on slave id {slave}")
                return slave
            else:
                print(f"No valid response from slave id {slave}")
        except Exception as e:
            print(f"Error testing slave id {slave}: {e}")
        time.sleep(0.2)
    print("Device not found in the specified slave id range.")
    return None

def modbusMessage(comap, slaveID):
    try:
        response1 = comap.read_holding_registers(3000, count=8, slave=slaveID)
        byte_data = b''.join(struct.pack('>H', reg) for reg in response1.registers)
        decoded_string = byte_data.decode('utf-8').split("\x00")[0]

        response2 = comap.read_holding_registers(12, count=6, slave=slaveID)
        block1 = ''.join('{:04x}'.format(b) for b in response2.registers)

        response3 = comap.read_holding_registers(103, count=21, slave=slaveID)
        block2 = ''.join('{:04x}'.format(b) for b in response3.registers)

        response4 = comap.read_holding_registers(162, count=6, slave=slaveID)
        block3 = ''.join('{:04x}'.format(b) for b in response4.registers)

        response5 = comap.read_holding_registers(248, count=108, slave=slaveID)
        block4 = ''.join('{:04x}'.format(b) for b in response5.registers)

        message = {
            "timestamp": time.time(),
            "gensetName": decoded_string,
            "dataBlock1": block1,
            "dataBlock2": block2,
            "dataBlock3": block3,
            "dataBlock4": block4,
            "FW": "0.7.0",
        }
        print(message)
    except Exception as e:
        print(f"Error in modbusMessage for slave {slaveID}: {e}")
        raise

def teltonikaMessage():
    try:
        response = teltonika.read_holding_registers(143, count=4)
        if not hasattr(response, 'registers'):
            raise ValueError("No registers in response")
        latlon = response.registers
        # Remove or comment out teltonika.close() for persistent connections.
        teltonika.close()
        combined = (latlon[0] << 16) | latlon[1]
        bytes_data = combined.to_bytes(4, byteorder='big')
        latitude = unpack('>f', bytes_data)[0]

        combined = (latlon[2] << 16) | latlon[3]
        bytes_data = combined.to_bytes(4, byteorder='big')
        longitude = unpack('>f', bytes_data)[0]

        message = {
            "timestamp": time.time(),
            "latitude": latitude,
            "longitude": longitude
        }
        print(message)
    except Exception as e:
        print(f"Error in teltonikaMessage: {e}")
        raise

# MQTT Setup
BROKER = credentials["broker"]
PORT = credentials["port"]
USERNAME = credentials["username"]
PASSWORD = credentials["password"]
topicData = "ET/genlogger/data"

client = mqtt_client.Client()
client.username_pw_set(USERNAME, PASSWORD)
client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.connect(BROKER, PORT, keepalive=10)
client.loop_start()

def comap_loop(comap):
    modbusConnect(comap)
    slave_id = discover_slave_id(comap, start=1, end=10)
    if slave_id is None:
        print("No slave found. Exiting.")
        return
    while True:
        try:
            modbusMessage(comap, slave_id)
        except Exception as e:
            print(f"Error occurred: {e}. Reconnecting and rediscovering slave id...")
            modbusConnect(comap)
            new_slave_id = discover_slave_id(comap, start=1, end=10)
            if new_slave_id is None:
                print("No slave found after reconnection. Exiting loop.")
                break  # Or continue trying, depending on your needs.
            else:
                slave_id = new_slave_id
        time.sleep(1)

def teltonika_loop():
    modbusTcpConnect(teltonika)
    while True:
        try:
            teltonikaMessage()
        except Exception:
            modbusTcpConnect(teltonika)
        time.sleep(1)

if __name__ == "__main__":
    thread_modbusA = threading.Thread(target=comap_loop, args=(comapA,), daemon=True)
    thread_modbusB = threading.Thread(target=comap_loop, args=(comapB,), daemon=True)
    thread_teltonika = threading.Thread(target=teltonika_loop, daemon=True)
    
    thread_modbusA.start()
    thread_modbusB.start()
    thread_teltonika.start()

    while True:
        time.sleep(10)
