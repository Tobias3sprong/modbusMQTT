import logging
import time
from pymodbus.client import ModbusSerialClient
from paho.mqtt import client as mqtt_client
from dataclasses import dataclass

#MODBUS
#Set up modbus #/dev/ttyHS0 /dev/tty.usbserial-A10NBLZ7
modbusclient = ModbusSerialClient(method='rtu', port='/dev/tty.usbserial-A10NBLZ7', stopbits=1, bytesize=8, parity='N', baudrate=19200, timeout=0.3)

def modbusConnect(modbusclient):
    while modbusclient.connect() == False:
            print(modbusclient.connect())
            print("modbus communication failed")
            time.sleep(2)

modbusConnect(modbusclient)
@dataclass
class LeGrandReading:
    voltage_l1: float
    voltage_l2: float
    voltage_l3: float
    current_l1: float
    current_l2: float
    current_l3: float
    current_n: float
    voltage_l1_l2: float
    voltage_l2_l3: float
    voltage_l3_l1: float
    power_active: float
    power_apparent: float
    power_reactive: float
    power_active_sign: bool

def decode_reading() -> LeGrandReading:
    value = modbusclient.read_holding_registers(int(0x1000), 122, 1)
    return LeGrandReading(
        voltage_l1=int(value.registers[1]+(value.registers[0] << 16))/1000,
        voltage_l2=int(value.registers[3]+(value.registers[2] << 16))/1000,
        voltage_l3=int(value.registers[5]+(value.registers[4] << 16))/1000,
        current_l1=int(value.registers[7]+(value.registers[6] << 16))/1000,
        current_l2=int(value.registers[9]+(value.registers[8] << 16))/1000,
        current_l3=int(value.registers[11]+(value.registers[10] << 16))/1000,
        current_n=int(value.registers[13]+(value.registers[12] << 16))/1000,
        voltage_l1_l2=int(value.registers[15]+(value.registers[14] << 16))/1000,
        voltage_l2_l3=int(value.registers[17]+(value.registers[16] << 16))/1000,
        voltage_l3_l1=int(value.registers[19]+(value.registers[18] << 16))/1000,
        power_active=int(value.registers[21]+(value.registers[20] << 16))/100,
        power_reactive=int(value.registers[23]+(value.registers[22] << 16))/1000,
        power_apparent=int(value.registers[25]+(value.registers[24] << 16))/1000,
        power_active_sign=int(value.registers[26])
    )

#MQTT
BROKER = 'mqtt.t3techniek.nl'
PORT = 1883
topicData = "test/data"
topicReset = "test/reset"
USERNAME = 'tobias'
PASSWORD = 'perensap'

FLAG_EXIT = False

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
        resetCurrent()
    elif msg.payload.decode() == 'voltage':
        print("reset voltage")
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
        reading = decode_reading()
        result = client.publish(topicData, str(reading))
        print(reading)
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