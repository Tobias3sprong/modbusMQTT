import json
import time
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
    port='/dev/ttyUSB0',
    stopbits=1,
    bytesize=8,
    parity='N',
    baudrate=9600,
    timeout=0.3
)
# Set up modbus TCP
tcpClient = ModbusTcpClient(
    host="localhost",
    port=502
)



def modbusConnect(modbusclient):
    while modbusclient.connect() == False:
        logMQTT(client, topicLog, "Modbus RTU initialisation failed")
        time.sleep(2)


def modbusTcpConnect(tcpClient):
    print("Attempting to connect to Modbus TCP server...")
    while not tcpClient.connect():
        logMQTT(client, topicLog, "Modbus TCP initialisation failed, retrying...")
        time.sleep(2)  # Wait for 2 seconds before retrying
    logMQTT(client, topicLog, "Successfully connected to Modbus TCP server! WAN IP is: "+ WanIP)


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
        logMQTT(client,topicLog, "Connected to broker!")
        client.subscribe(topicReset)
        client.subscribe(topicConfig)


def on_disconnect(client, userdata, rc):
    while True:
        time.sleep(10)
        try:
            client.reconnect()
            return
        except Exception as err:
            print("Failed to reconnect to MQTT")
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

def connect_mqtt():
    try:

        return client
    except Exception as e:
        print("MQTT connection failed:", e)


def publish(client):
    global deviceID, IMSI, WanIP
    try:
        # Resultaten van de drie requests
        response1 = modbusclient.read_holding_registers(2, count=125, slave=3)
        response2 = modbusclient.read_holding_registers(162, count=6, slave=3)
        response3 = modbusclient.read_holding_registers(3000, count=16, slave=3)

        # Registers samenvoegen
        combined_registers = response1.registers + response2.registers + response3.registers

        # Gecombineerde registers printen
        print(combined_registers)
    except Exception as e:
        logMQTT(client, topicLog, f"Modbus connection error - Check wiring or modbus slave: {str(e)}")
    time.sleep(sendInterval)


if tcpClient.connect():
    global IMSI, WanIP
    IMSIreg = tcpClient.read_holding_registers(348,count=8)
    IMSI = bytes.fromhex(''.join('{:02x}'.format(b) for b in IMSIreg.registers))[:-1].decode("ASCII")
    WanIPreg = tcpClient.read_holding_registers(139,count=2)  # WAN IP address registers    
    if WanIPreg.isError():
        print("Failed to read WAN IP from registers.")
        print(WanIPreg)
        WanIP = "0.0.0.0"  # Default in case of failure
    else:
        #Combine the two registers into a 32-bit value
        wan_ip_int = (WanIPreg.registers[0] << 16) | WanIPreg.registers[1]
        # Convert to a dotted quad IP string
        WanIP = '.'.join(str((wan_ip_int >> (8 * i)) & 0xFF) for i in range(3, -1, -1))
        print(f"WAN IP: {WanIP}")



# MQTT

BROKER = credentials["broker"] 
PORT = credentials["port"]
USERNAME = credentials["username"]
PASSWORD = credentials["password"]
topicData = "ET/powerlogger/data"
topicReset = "ET/powerlogger/"+IMSI+"/reset"
topicConfig = "ET/powerlogger/"+IMSI+"/config"
topicLog = "ET/powerlogger/"+IMSI+"/log"
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
time.sleep(2)
client.loop_start()
time.sleep(2)
try:
    while True:
        if tcpClient.connect() == False:
            modbusTcpConnect(tcpClient)

        if modbusclient.connect() == False:
            modbusConnect(modbusclient)
        else:
            publish(client)
except Exception as e:
    print(f"Error: {e}")
    tcpClient.close()
    modbusclient.close()
except KeyboardInterrupt :
    tcpClient.close()
    modbusclient.close()