import json
import time
from pymodbus.client.serial import ModbusSerialClient
from pymodbus.client.tcp import ModbusTcpClient
from paho.mqtt import client as mqtt_client






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



def modbusConnect(modbusclient):
    while modbusclient.connect() == False:
        logMQTT(client, topicLog, "Modbus RTU initialisation failed")
        time.sleep(2)


def modbusTcpConnect(tcpClient):
    while tcpClient.connect() == False:
        logMQTT(client, topicLog, "Modbus TCP initialisation failed")
        time.sleep(2)




def logMQTT(client, topicLog, logMessage):
    global lastLogMessage
    if not logMessage == lastLogMessage:
        message = x = {
            "timestamp": time.time(),
            "log": logMessage,
        }
        print(str(time.time()) + "\t->\t" + logMessage)
        lastLogMessage = logMessage
        result = client.publish(topicLog, json.dumps(message))
        status = result[0]
        if not status == 0:
            print(f'Failed to send log message to topic {topicData}')


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
        modbusclient.write_registers(int(0x2700), int(0x5AA5), 1)
        modbusclient.write_registers(int(0x2400), int(0x14), 1)
    except:
        logMQTT(client, topicLog, "Resetting min/max voltage has failed")
    else:
        logMQTT(client, topicLog, "Min/max voltage has been reset")
def resetCurrent():
    try:
        modbusclient.write_registers(int(0x2700), int(0x5AA5), 1)
        modbusclient.write_registers(int(0x2400), int(0xA), 1)
    except:
        logMQTT(client, topicLog, "Resetting min/max current has failed")
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
    global sendInterval, deviceID
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
            deviceID = config["deviceID"]
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
    global deviceID
    try:
        block1 = modbusclient.read_holding_registers(int(0x1000), 122, 1)
        ct = modbusclient.read_holding_registers(int(0x1200), 1, 1)
        hexString = ''.join('{:04x}'.format(b) for b in block1.registers)
        print(str(time.time()) + "\t->\t" + hexString + str(ct.registers[0]))
        tcpData = ''.join('{:02x}'.format(b) for b in tcpClient.read_holding_registers(4, 1).registers)
        message = x = {
            "deviceID" : deviceID,
            "timestamp": time.time(),
            "rtuData": hexString + str(ct.registers[0]),
            "tcpData": tcpData
        }
        result = client.publish(topicData, json.dumps(message))
        status = result[0]
        if not status == 0:
            print(f'Failed to send message to topic {topicData}')
    except:
        logMQTT(client, topicLog, "Modbus connection error - Check wiring or modbus slave")
    time.sleep(sendInterval)

if tcpClient.connect() == True:
    IMSIreg = tcpClient.read_holding_registers(348, 8)
    IMSI = bytes.fromhex(''.join('{:02x}'.format(b) for b in IMSIreg.registers))[:-1].decode("ASCII")


# MQTT
BROKER = 'mqtt.event-things.io'
PORT = 1883
USERNAME = 'eventthings_client'
PASSWORD = 'letq9trZSCdXGZj'
topicData = "ET/powerlogger/data"
topicReset = "ET/powerlogger/"+IMSI+"/reset"
topicConfig = "ET/powerlogger/"+IMSI+"/config"
topicLog = "ET/powerlogger/"+IMSI+"/log"
msgCount = 0
deviceID = 99
IMSI = 00000000000000000

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

while True:
    if tcpClient.connect() == False:
        modbusTcpConnect(tcpClient)


    if modbusclient.connect() == False:
        modbusConnect(modbusclient)
    else:
        publish(client)