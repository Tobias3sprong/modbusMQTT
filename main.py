import json
import time
from datetime import datetime
from pymodbus.client import ModbusSerialClient
from paho.mqtt import client as mqtt_client
from dataclasses import dataclass



# MODBUS
# Set up modbus

modbusclient = ModbusSerialClient(
    method='rtu',
    port='/dev/tty.usbserial-A10NBLZ7',
    #port='#/dev/ttyHS0',
    stopbits=1,
    bytesize=8,
    parity='N',
    baudrate=19200,
    timeout=0.3
)



def modbusConnect(modbusclient):
    while modbusclient.connect() == False:
        logMQTT(client, topicLog, "Modbus initialisation failed")
        time.sleep(2)




# MQTT
BROKER = '93.119.7.13'
PORT = 1883
topicData = "ET/emdx/1/data"
topicReset = "ET/emdx/1/reset"
topicConfig = "ET/emdx/1/config"
topicLog = "ET/emdx/1/log"
msgCount = 0

USERNAME = 'tobias'
PASSWORD = 'perensap'
flag_connected = True






lastLogMessage = ""
def logMQTT(client, topicLog, logMessage):
    global lastLogMessage
    if not logMessage == lastLogMessage:
        message = x = {
            "timestamp": time.time(),
            "log": logMessage,
        }
        print(str(time.time()) + ": " + logMessage)
        lastLogMessage = logMessage
        result = client.publish(topicLog, json.dumps(message))
        status = result[0]
        if not status == 0:
            print(f'Failed to send log message to topic {topicData}')


def on_connect(client, userdata, flags, rc):
    global flag_connected
    if rc == 0 and client.is_connected():
        print("Connected to MQTT Broker!")
        flag_connected = True
        client.subscribe(topicReset)
        client.subscribe(topicConfig)
    else:
        print(f'Failed to connect, return code {rc}')


def on_disconnect(client, userdata, rc):
    print("Disconnected")
    global flag_connected
    flag_connected = False
    while True:
        time.sleep(10)
        try:
            client.reconnect()
            return
        except Exception as err:
            print("failed")
def resetVoltage():
    try:
        modbusclient.write_registers(int(0x2700), int(0x5AA5), 1)
        modbusclient.write_registers(int(0x2400), int(0x14), 1)
    except:
        print("Something went wrong writing modbus registers")
        logMQTT(client, topicLog, "Resetting min/max voltage has failed")
    else:
        print("reset!")
        logMQTT(client, topicLog, "Min/max voltage has been reset")
def resetCurrent():
    try:
        modbusclient.write_registers(int(0x2700), int(0x5AA5), 1)
        modbusclient.write_registers(int(0x2400), int(0xA), 1)
    except:
        print("Something went wrong writing modbus registers")
        logMQTT(client, topicLog, "Resetting min/max current has failed")
    else:
        print("reset!")
        logMQTT(client, topicLog, "Min/max current has been reset")

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
        else:
            print(f'Received `{msg.payload.decode()}` from `{msg.topic}` topic')
    if msg.topic == topicConfig:
        try:
            config = json.loads(msg.payload.decode())
            sendInterval = config["sendInterval"]
            logMQTT(client, topicLog, "Received config file - new config is applied")
        except:
            logMQTT(client, topicLog, "Received invalid config message")

def connect_mqtt():
    try:

        return client
    except Exception as e:
        print("MQTT connection failed:", e)


def publish(client):
    try:
        block1 = modbusclient.read_holding_registers(int(0x1000), 122, 1)
        ct = modbusclient.read_holding_registers(int(0x1200), 1, 1)
        hexString = ''.join(format(x, '02x') for x in block1.registers)
        print(str(time.time()) + ": " + hexString + str(ct.registers[0]))
        message = x = {
            "timestamp": time.time(),
            "data": hexString + str(ct.registers[0]),
        }
        result = client.publish(topicData, json.dumps(message))
        status = result[0]
        if not status == 0:
            print(f'Failed to send message to topic {topicData}')
    except:
        logMQTT(client, topicLog, "Modbus connection error - Check wiring or modbus slave")
    time.sleep(sendInterval)

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
    if modbusclient.connect() == False:
        logMQTT(client, topicLog, "Modbus initialisation failed")
        modbusConnect(modbusclient)
    else:
        publish(client)