import logging
import time
from pymodbus.client import ModbusSerialClient
from paho.mqtt import client as mqtt_client
from dataclasses import dataclass

#MODBUS
#Set up modbus
modbusclient = ModbusSerialClient(
    method='rtu',
    port='/dev/tty.usbserial-00810',
    #port='#/dev/ttyHS0',
    stopbits=1,
    bytesize=8,
    parity='N',
    baudrate=19200,
    timeout=0.3
)

def modbusConnect(modbusclient):
    while modbusclient.connect() == False:
            print(modbusclient.connect())
            print("modbus communication failed")
            time.sleep(2)

modbusConnect(modbusclient)

#MQTT
BROKER = 'mqtt.t3techniek.nl'
PORT = 1883
topicData = "ET/emdx/1/data"
topicReset = "ET/emdx/1/reset"
topicLog = "ET/emdx/1/log"

USERNAME = 'tobias'
PASSWORD = 'perensap'
FLAG_EXIT = False

def logMQTT(client, topicLog, logMessage):
    result = client.publish(topicLog, logMessage)
    status = result[0]
    if status == 0:
        print(f'Send log message to topic `{topicLog}`')
    else:
        print(f'Failed to send log message to topic {topicData}')

def resetVoltage():
    try:
        modbusclient.write_registers(int(0x2700), int(0x5AA5), 1)
        modbusclient.write_registers(int(0x2400), int(0x14), 1)
    except:
        print("Something went wrong writing modbus registers")
    else:
        print("reset!")
def resetCurrent():
    try:
        modbusclient.write_registers(int(0x2700), int(0x5AA5), 1)
        modbusclient.write_registers(int(0x2400), int(0xA), 1)
    except:
        print("Something went wrong writing modbus registers")
    else:
        print("reset!")

def on_connect(client, userdata, flags, rc):
    if rc == 0 and client.is_connected():
        print("Connected to MQTT Broker!")
        client.subscribe(topicReset)
    else:
        print(f'Failed to connect, return code {rc}')

def on_disconnect(client, userdata, rc):
    logging.info("Disconnected with result code: %s", rc)
    reconnect_count, reconnect_delay = 0
    while True:
        logging.info("Reconnecting in %d seconds...", reconnect_delay)
        time.sleep(10)
        try:
            client.reconnect()
            logging.info("Reconnected successfully!")
            return
        except Exception as err:
            logging.error("%s. Reconnect failed. Retrying...", err)

def on_message(client, userdata, msg):
    if msg.payload.decode() == 'current':
        print("reset current")
        logMQTT(client,topicLog, "Min/max current has been reset")
        resetCurrent()
    elif msg.payload.decode() == 'voltage':
        print("reset voltage")
        logMQTT(client,topicLog, "Min/max voltage has been reset")
        resetVoltage()
    else:
        print(f'Received `{msg.payload.decode()}` from `{msg.topic}` topic')


def connect_mqtt():
    client = mqtt_client.Client()
    client.username_pw_set(USERNAME, PASSWORD)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(BROKER, PORT, keepalive=3)
    client.on_disconnect = on_disconnect
    return client

def publish(client):
    while not FLAG_EXIT:
        if not client.is_connected():
            logging.error("publish: MQTT client is not connected!")
            time.sleep(1)
            continue
        value = modbusclient.read_holding_registers(int(0x1000), 122, 1)
        hexString = ''.join(format(x, '02x') for x in value.registers)
        print(hexString)
        result = client.publish(topicData, hexString)
        # result: [0, 1]
        status = result[0]
        if status == 0:
            print(f'Send message to topic `{topicData}`')
        else:
            print(f'Failed to send message to topic {topicData}')
        time.sleep(1)

def run():
    #logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s',
    #                    level=logging.DEBUG)
    client = connect_mqtt()
    client.loop_start()
    time.sleep(1)
    if client.is_connected():
        publish(client)
    else:
        client.loop_stop()


if __name__ == '__main__':
    run()