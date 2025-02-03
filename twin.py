import json
import time
import struct
import threading
from struct import unpack
from pymodbus.client.serial import ModbusSerialClient
from pymodbus.client.tcp import ModbusTcpClient
from paho.mqtt import client as mqtt_client
#l Load credentials
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
    # Keep trying until connection is successful
    while not comap.connect():
        print("Modbus initialisation failed")
        time.sleep(1)

def modbusTcpConnect(teltonika):
    print("Attempting to connect to Modbus TCP server...")
    # Keep trying until connection is successful
    while not teltonika.connect():
        print("Modbus TCP initialisation failed")
        time.sleep(1)

def discover_slave_id(modbus_client, start=1, end=247):
    """
    Attempts to discover the slave id by trying a read on each candidate slave.
    
    Parameters:
      modbus_client - an instance of ModbusSerialClient that is already connected.
      start - the first slave id to try (default 1).
      end   - the last slave id to try (default 247).
      
    Returns:
      The slave id if found, or None otherwise.
    """
    for slave in range(start, end + 1):
        print(f"Testing slave id: {slave} ...")
        try:
            # Adjust register address and count as needed for your device.
            response = modbus_client.read_holding_registers(3000, count=8, slave=slave)
            if response is not None and hasattr(response, 'registers') and len(response.registers) > 0:
                print(f"Device found on slave id {slave}")
                return slave
            else:
                print(f"No valid response from slave id {slave}")
        except Exception as e:
            print(f"Error testing slave id {slave}: {e}")
        time.sleep(0.2)  # Slight pause between tries
    print("Device not found in the specified slave id range.")
    return None

def modbusMessage(comap, slaveID):
    try:
        # Read the genset name
        response1 = comap.read_holding_registers(3000, count=8, slave=slaveID) #Genset Name
        byte_data = b''.join(struct.pack('>H', reg) for reg in response1.registers)
        decoded_string = byte_data.decode('utf-8').split("\x00")[0]

        # Read the data blocks
        response2 = comap.read_holding_registers(12, count=6, slave=slaveID) #First block
        block1 = ''.join('{:04x}'.format(b) for b in response2.registers)
        response3 = comap.read_holding_registers(103, count=21, slave=slaveID)  #Second block
        block2 = ''.join('{:04x}'.format(b) for b in response3.registers)
        response4 = comap.read_holding_registers(162, count=6, slave=slaveID)  #Second block
        block3 = ''.join('{:04x}'.format(b) for b in response4.registers)
        response5 = comap.read_holding_registers(248, count=108, slave=slaveID)  #Second block
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
        # Reraise to let the loop handle reconnection.
        print(f"Error in modbusMessage for slave {slaveID}: {e}")
        raise

def teltonikaMessage():
    try:
        response = teltonika.read_holding_registers(143, count=4)  # Read 4 registers
        if not hasattr(response, 'registers'):
            raise ValueError("No registers in response")
        latlon = response.registers
        teltonika.close()
        # Combine the registers into a single 32-bit float value (big-endian) and unpack it to a float value (latitude)
        combined = (latlon[0] << 16) | latlon[1]
        bytes_data = combined.to_bytes(4, byteorder='big')
        latitude = unpack('>f', bytes_data)[0]  # '>' = big-endian float
        # Repeat for the longitude
        combined = (latlon[2] << 16) | latlon[3]
        bytes_data = combined.to_bytes(4, byteorder='big')
        longitude = unpack('>f', bytes_data)[0]  # '>' = big-endian float
        message = {
            "timestamp": time.time(),
            "latitude": latitude,
            "longitude": longitude
        }
        print(message)
        teltonika.close()
    except Exception as e:
        print(f"Error in teltonikaMessage: {e}")
        raise  # Let the loop catch and reconnect if necessary

BROKER = credentials["broker"] 
PORT = credentials["port"]
USERNAME = credentials["username"]
PASSWORD = credentials["password"]
topicData = "ET/genlogger/data"

client = mqtt_client.Client()
client.username_pw_set(USERNAME, PASSWORD)
client.on_connect = on_connect
client.on_disconnect = on_disconnect  # Attach the on_disconnect callback
client.connect(BROKER, PORT, keepalive=10)
client.loop_start()

def comap_loop(comap):
    # Establish initial connection on the bus.
    modbusConnect(comap)
    slave_id = discover_slave_id(comap, start=1, end=10)
    while slave_id is None:
        print("No slave found. Exiting.")
        return
    while True:
        try:
            modbusMessage(comap, slave_id)
        except Exception as e:
            print(f"Error occurred: {e}. Reconnecting...")
            modbusConnect(comap)
        time.sleep(1)

def comap_loop(comap):
    # Establish initial connection on the bus.
    modbusConnect(comap)
    slave_id = discover_slave_id(comap, start=1, end=10)
    if slave_id is None:
        print("No slave found. Exiting.")
        return
    while slave_id is not None:
        try:
            modbusMessage(comap, slave_id)
        except Exception as e:
            print(f"Error occurred: {e}. Reconnecting...")
            modbusConnect(comap)
        time.sleep(1)

def teltonika_loop():
    modbusTcpConnect(teltonika)
    while True:
        try:
            teltonikaMessage()
        except Exception:
            # If reading fails, try reconnecting
            modbusTcpConnect(teltonika)
        time.sleep(1)

if __name__ == "__main__":
    thread_modbusA = threading.Thread(target=comap_loop, args=(comapA,), daemon=True)
    thread_modbusB = threading.Thread(target=comap_loop, args=(comapB,), daemon=True)
    thread_teltonika = threading.Thread(target=teltonika_loop, daemon=True)
    
    thread_modbusA.start()
    #thread_modbusB.start()
    #thread_teltonika.start()

    while True:
        time.sleep(10)