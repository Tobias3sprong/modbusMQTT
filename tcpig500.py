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
comapA = ModbusTcpClient(
    host='10.0.0.248',
    port=502,
    timeout=0.1,
    retries=1
)
comapA.transaction_retries = 1  # Set the number of retries for Modbus operations

def modbusConnect(comap):
    print(f"Attempting to connect to {comap}")
    while not comap.connect():
        print("Modbus initialisation failed")
        time.sleep(1)

def discover_slave_id(modbus_client, start=1, end=247):
    while True:
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
            #time.sleep(0.2)
        print("Device not found in the specified slave id range. Restarting discovery...")
        time.sleep(5)

def modbusMessage(comap, slaveID):
    try:
        genSetNameResponse = comap.read_holding_registers(3019, count=8, slave=slaveID)
        byte_data = b''.join(struct.pack('>H', reg) for reg in genSetNameResponse.registers)
        genSetName = byte_data.decode('utf-8').split("\x00")[0]

        dataBlock1Response = comap.read_holding_registers(1019, count=68, slave=slaveID)
        dataBlock1 =''.join('{:04x}'.format(b) for b in dataBlock1Response.registers)

        dataBlock2Response = comap.read_holding_registers(1258, count=2, slave=slaveID)
        dataBlock2 =''.join('{:04x}'.format(b) for b in dataBlock2Response.registers)


        message = {
            "timestamp": time.time(),
            "gensetName": genSetName,
            "FW": "0.7.0",
            "dataBlock1": dataBlock1 + dataBlock2,
        }
        print(message)
        client.publish(genData, json.dumps(message))
    except Exception as e:
        print(f"Error in modbusMessage for slave {slaveID}: {e}")
        raise


# MQTT Setup
BROKER = credentials["broker"]
PORT = credentials["port"]
USERNAME = credentials["username"]
PASSWORD = credentials["password"]
genData = "ET/genlogger/ig500/data"
modemData = "ET/modemlogger/data"

client = mqtt_client.Client()
client.username_pw_set(USERNAME, PASSWORD)
client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.connect(BROKER, PORT, keepalive=10)
client.loop_start()

def comap_loop(comap):
    modbusConnect(comap)
    # discover_slave_id will loop internally until a slave is found.
    #slave_id = discover_slave_id(comap, start=1, end=32)
    
    while True:
        try:
            modbusMessage(comap, 21)
        except Exception as e:
            print(f"Error occurred: {e}. Reconnecting and rediscovering slave id...")
            modbusConnect(comap)
            # This call will loop internally until it finds a valid slave id.
            #slave_id = discover_slave_id(comap, start=1, end=32)
            #print(f"Rediscovered slave id: {slave_id}")
        time.sleep(1)


if __name__ == "__main__":
    thread_modbusA = threading.Thread(target=comap_loop, args=(comapA,), daemon=True)

    
    thread_modbusA.start()


    while True:
        time.sleep(10)
