import json
import time
import struct
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
comapAslave = 3

comapB = ModbusSerialClient(
    port='/dev/ttyUSB1',
    stopbits=1,
    bytesize=8,
    parity='N',
    baudrate=19200,
    timeout=0.3
)
comapBslave = 4

# Set up modbus TCP
teltonika = ModbusTcpClient(
    host="localhost",
    port=502
)

def modbusConnect(ComapA):
    while ComapA.connect() == False:
        print("Modbus Set A initialisation failed")
        time.sleep(2)

def modbusConnect(ComapB):
    while ComapB.connect() == False:
        print("Modbus Set B initialisation failed")
        time.sleep(2)

def modbusTcpConnect(teltonika):
    print("Attempting to connect to Modbus TCP server...")
    while not teltonika.connect():
        time.sleep(2)  # Wait for 2 seconds before retrying



        # Gecombineerde registers printen
        #print(combined_registers)


def modbusMessageA():
    try:
        # Read the genset name
        response1 = comapA.read_holding_registers(3000, count=8, slave=comapAslave) #Genset Name
        byte_data = b''.join(struct.pack('>H', reg) for reg in response1.registers)
        decoded_string = byte_data.decode('utf-8').strip('\x00').split("\x00")[0]

        # Read the data blocks
        response2 = comapA.read_holding_registers(12, count=6, slave=comapAslave) #First block
        block1 = ''.join('{:04x}'.format(b) for b in response2.registers)
        response3 = comapA.read_holding_registers(103, count=21, slave=comapAslave)  #Second block
        block2 = ''.join('{:04x}'.format(b) for b in response3.registers)
        response4 = comapA.read_holding_registers(162, count=6, slave=comapAslave)  #Second block
        block3 = ''.join('{:04x}'.format(b) for b in response4.registers)
        response5 = comapA.read_holding_registers(248, count=108, slave=comapAslave)  #Second block
        block4 = ''.join('{:04x}'.format(b) for b in response5.registers)

        message = {
            "timestamp": time.time(),
            "gensetName": decoded_string,
            "dataBlock1": block1,
            "dataBlock2": block2,
            "dataBlock3": block3,
            "dataBlock4": block4,
            #"RSSI": RSSI,
            #"IMSI": int(IMSI),  # Add the full IMSI as a readable string
            #"IP": WanIP,
            "FW": "0.7.0",
        #    "gpsLat": gpsLat.registers[0] +
        }
        print(message)
    except Exception as e:
        print(f"Error: {e}")    

def modbusMessageB():
    try:
        # Read the genset name
        response1 = comapB.read_holding_registers(3000, count=8, slave=comapBslave) #Genset Name
        byte_data = b''.join(struct.pack('>H', reg) for reg in response1.registers)
        decoded_string = byte_data.decode('utf-8')

        # Read the data blocks
        response2 = comapB.read_holding_registers(12, count=6, slave=comapBslave) #First block
        block1 = ''.join('{:04x}'.format(b) for b in response2.registers)
        response3 = comapB.read_holding_registers(103, count=21, slave=comapBslave)  #Second block
        block2 = ''.join('{:04x}'.format(b) for b in response3.registers)
        response4 = comapB.read_holding_registers(162, count=6, slave=comapBslave)  #Second block
        block3 = ''.join('{:04x}'.format(b) for b in response4.registers)
        response5 = comapB.read_holding_registers(248, count=108, slave=comapBslave)  #Second block
        block4 = ''.join('{:04x}'.format(b) for b in response5.registers)

        message = {
            "timestamp": time.time(),
            "gensetName": decoded_string.split('\x00')[0], #Comap outputs some weird characters at the end of the string
            "dataBlock1": block1,
            "dataBlock2": block2,
            "dataBlock3": block3,
            "dataBlock4": block4,
            "FW": "0.7.0"
        }
        result = client.publish(topicData, json.dumps(message))
        status = result.rc
        if status != 0:
            print(f'Failed to send log message to topic {topicData}')
        print(message)
    except Exception as e:
            print(f"Error: {e}")    

def teltonikaMessage():
    try:
        response = teltonika.read_holding_registers(143, count=4)  # Read 4 registers
        if hasattr(response, 'registers'):
            latlon = response.registers  # Extract the register values
        else:
            raise ValueError("No registers in response") # Raise an error if there are no registers
        teltonika.close()
        # Combine the registers into a single 32-bit float value (big-endian) and unpack it to a float value (latitude)
        combined = (latlon[0] << 16) | latlon[1]
        bytes_data = combined.to_bytes(4, byteorder='big')
        latitude = unpack('>f', bytes_data)[0]  # '>' = big-endian float
        # Repeat for the longitude
        combined = (latlon[2] << 16) | latlon[3]
        bytes_data = combined.to_bytes(4, byteorder='big')
        longitude = unpack('>f', bytes_data)[0]  # '>' = big-endian float
        print(f"Latitude: {latitude}" + f"Longitude: {longitude}")
        teltonika.close()

    except Exception as e:
        print(f"Error: {e}")

BROKER = credentials["broker"] 
PORT = credentials["port"]
USERNAME = credentials["username"]
PASSWORD = credentials["password"]
topicData = "ET/genlogger/data"
msgCount = 0



flag_connected = True
lastLogMessage = ""

client = mqtt_client.Client()
client.username_pw_set(USERNAME, PASSWORD)
client.on_connect = on_connect
#client.will_set(topicLog, "Disconnected", retain=True)  # Optional: Set a last will message
client.connect(BROKER, PORT, keepalive=10)  # Increased the keepalive interval
time.sleep(2)
client.loop_start()
time.sleep(2)

try:
    while True:
        modbusMessageA()
        modbusMessageB()
        teltonikaMessage()
        time.sleep(1)
except Exception as e:
    print(f"Error: {e}")
