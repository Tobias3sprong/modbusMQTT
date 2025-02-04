import json
import time
import struct
import threading
from pymodbus.client.serial import ModbusSerialClient
from pymodbus.client.tcp import ModbusTcpClient
from paho.mqtt import client as mqtt_client

#l Load credentials
json_file_path = r".secrets/credentials.json"
with open(json_file_path, "r") as f:
    credentials = json.load(f)

# MODBUS
# Set up modbus RTU
modbusclient = ModbusSerialClient(
    port='/dev/ttyHS0',
    stopbits=1,
    bytesize=8,
    parity='N',
    baudrate=19200,
    timeout=0.3
)
# Set up modbus TCP
tcpClient = ModbusTcpClient(
    host="localhost",
    port=502
)

routerSerial = "0000000000000000"

def modbusConnect(modbusclient):
    while modbusclient.connect() == False:
        logMQTT(client, topicLog, "Modbus RTU initialisation failed")
        time.sleep(1)


def modbusTcpConnect(tcpClient):
    print("Attempting to connect to Modbus TCP server...")
    while not tcpClient.connect():
        logMQTT(client, topicLog, "Modbus TCP initialisation failed, retrying...")
        time.sleep(1)  # Wait for 2 seconds before retrying
    logMQTT(client, topicLog, "Successfully connected to Modbus TCP server!")


def logMQTT(client, topicLog, logMessage):
    global lastLogMessage
    if not logMessage == lastLogMessage:
        message = {
            "timestamp": time.time(),
            "log": logMessage,
        }
        print(str(time.time()) + "\t->\t" + logMessage)
        lastLogMessage = logMessage
        result = client.publish(topicLog, json.dumps(message))
        status = result.rc
        if status != 0:
            print(f'Failed to send log message to topic {topicLog}')


def on_connect(client, userdata, flags, rc):
    if rc == 0 and client.is_connected():
        client.subscribe(topicReset)
        client.subscribe(topicConfig)


def on_disconnect(client, userdata, rc):
    print(f"MQTT disconnected with result code: {rc}")
    if rc != 0:
        try:
            client.reconnect()
        except Exception as e:
            print(f"Reconnect failed: {e}")

def resetVoltage():
    try:
        modbusclient.write_registers(int(0x2700), [0x5AA5], slave=1)
        modbusclient.write_registers(int(0x2400), [0x14], slave=1)
    except Exception as e:
        logMQTT(client, topicLog, f"Resetting min/max voltage has failed: {e}")
    else:
        logMQTT(client, topicLog, "Min/max voltage has been reset")
def resetCurrent():
    try:
        modbusclient.write_registers(int(0x2700), [0x5AA5], slave=1)
        modbusclient.write_registers(int(0x2400), [0xA], slave=1)
    except Exception as e:
        logMQTT(client, topicLog, f"Resetting min/max current has failed: {e}")
    else:
        logMQTT(client, topicLog, "Min/max current has been reset")
def rebootModem():
    try:
        tcpClient.write_register(206, 1)
    except:
        logMQTT(client, topicLog, "Reboot failed")
    else:
        logMQTT(client, topicLog, "Rebooting modem...")

sendInterval = 10

def on_message(client, userdata, msg):
    global sendInterval
    if msg.topic == topicReset:
        if msg.payload.decode() == 'current':
            print("reset current")
            resetCurrent()
        elif msg.payload.decode() == 'voltage':
            print("reset voltage")
            resetVoltage()
        elif msg.payload.decode() == 'modem':
            rebootModem()
        else:
            print(f'Received `{msg.payload.decode()}` from `{msg.topic}` topic')
    if msg.topic == topicConfig:
        try:
            config = json.loads(msg.payload.decode())
            sendInterval = config["sendInterval"]
            logMQTT(client, topicLog, "Received config message - new config is applied")
        except Exception as error:
            print("An error occurred:", error)  # An error occurred: name 'x' is not defined
            logMQTT(client, topicLog, "Received invalid config message")


def publishPowerlog(client):
    global routerSerial
    try:
        block1 = modbusclient.read_holding_registers(int(0x1000), count=122, slave=1)
        ct = modbusclient.read_holding_registers(int(0x1200),count=1, slave=1)
        hexString = ''.join('{:04x}'.format(b) for b in block1.registers)
        hexStringCT = ''.join('{:04x}'.format(b) for b in ct.registers)
        message = {
            "timestamp": time.time(),
            "routerSerial": routerSerial,
            "slaveID": 1,
            "rtuData": hexString + hexStringCT,
        }
        result = client.publish(topicPower, json.dumps(message))
        status = result[0]
        if not status == 0:
            print(f'Failed to send message to topic {topicPower}')
    except Exception as e:
        logMQTT(client, topicLog, f"Modbus connection error - Check wiring or modbus slave: {str(e)}")

def publishModemlog(client):
    global routerSerial
    try:
        rssiData = ''.join('{:02x}'.format(b) for b in tcpClient.read_holding_registers(4, count=1).registers)
        rssi = int(rssiData, 16) - 0x10000 if int(rssiData, 16) > 0x7FFF else int(rssiData, 16)
        tcpClient.close()
        imsiData = tcpClient.read_holding_registers(348,count=8)
        imsi = bytes.fromhex(''.join('{:02x}'.format(b) for b in imsiData.registers))[:-1].decode("ASCII")
        tcpClient.close()
        wanipData = tcpClient.read_holding_registers(139,count=2)  # WAN IP address registers   
        wanipint = (wanipData.registers[0] << 16) | wanipData.registers[1] 
        wanip = '.'.join(str((wanipint >> (8 * i)) & 0xFF) for i in range(3, -1, -1))
        tcpClient.close()
        serialData = tcpClient.read_holding_registers(39, count=16)
        serialByteData = b''.join(struct.pack('>H', reg) for reg in serialData.registers)
        newRouterSerial = serialByteData.decode('ascii').split('\00')[0]
        if not newRouterSerial == routerSerial:
            routerSerial = newRouterSerial
            client.subscribe(topicReset)
            client.subscribe(topicConfig)
        tcpClient.close()
        # Convert to a dotted quad IP string
        message = {
            "timestamp": time.time(),
            "modemSerial": int(routerSerial),
            "RSSI": rssi,
            "IMSI": int(imsi),  # Add the full IMSI as a readable string
            "IP": wanip,
            "FW": "1.0.2"
        }
        result = client.publish(topicModem, json.dumps(message))
        status = result[0]
        if not status == 0:
            print(f'Failed to send message to topic {topicModem}')
    except Exception as e:
        logMQTT(client, topicLog, f"Modbus connection error - Check wiring or modbus slave: {str(e)}")


def powerLoop():
    modbusConnect(modbusclient)
    while True:
        try:
            publishPowerlog(client)
        except Exception:
            modbusConnect(modbusclient)
        time.sleep(sendInterval)

def modemLoop():
    modbusTcpConnect(tcpClient)
    while True:
        try:
            publishModemlog(client)
        except Exception:
            modbusTcpConnect(tcpClient)
        time.sleep(60)
# MQTT

BROKER = credentials["broker"] 
PORT = credentials["port"]
USERNAME = credentials["username"]
PASSWORD = credentials["password"]
topicPower = "ET/powerlogger/data"
topicModem = "ET/modemlogger/data"
topicReset = "ET/powerlogger/"+routerSerial+"/reset"
topicConfig = "ET/powerlogger/"+routerSerial+"/config"
topicLog = "ET/modemlogger/"+routerSerial+"/log"
msgCount = 0



flag_connected = True
lastLogMessage = ""
client = mqtt_client.Client()
client.username_pw_set(USERNAME, PASSWORD)
client.on_connect = on_connect
client.on_message = on_message
client.on_disconnect = on_disconnect  # Moved this line to the connect_mqtt function
client.will_set(topicLog, "Disconnected", retain=True)  # Optional: Set a last will message
client.connect(BROKER, PORT, keepalive=10)  # Increased the keepalive interval
client.loop_start()
time.sleep(2)

if __name__ == "__main__":
    
    thread_modemLoop = threading.Thread(target=modemLoop, daemon=True)
    thread_powerLoop = threading.Thread(target=powerLoop, daemon=True)
    
    thread_modemLoop.start()
    time.sleep(5)
    thread_powerLoop.start()
    

    while True:
        time.sleep(10)
