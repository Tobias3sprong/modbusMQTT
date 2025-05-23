import json
import time
import struct
import threading
import logging
from struct import unpack
from pymodbus.client.serial import ModbusSerialClient
from pymodbus.client.tcp import ModbusTcpClient
from paho.mqtt import client as mqtt_client
import requests

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("error_log.txt"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load credentials
json_file_path = r".secrets/credentials.json"
with open(json_file_path, "r") as f:
    credentials = json.load(f)

def on_connect(client, userdata, flags, rc):
    if rc == 0 and client.is_connected():
        logger.info("Connected to MQTT broker")
        import requests

def send_wan_ip():
    while True:
        try:
            response = requests.get('https://api.ipify.org?format=json')
            wanIP = response.json()['ip']
            logger.info(f"Current WAN IP: {wanIP}")
            client.publish("TwinsetIP", wanIP)
        except Exception as e:
            logger.error(f"Error getting/sending WAN IP: {e}")
        time.sleep(60*15)  # 15 minutes

def on_disconnect(client, userdata, rc):
    logger.info(f"MQTT disconnected with result code: {rc}")
    if rc != 0:
        logger.warning("Unexpected disconnection. Attempting to reconnect...")
        try:
            client.reconnect()
        except Exception as e:
            logger.error(f"Reconnect failed: {e}")

# MODBUS
# Set up modbus RTU
comapA = ModbusSerialClient(
    port='/dev/usb_serial_4a1e13f0',
    stopbits=1,
    bytesize=8,
    parity='N',
    baudrate=19200,
    timeout=0.1,
    retries=1
)
comapA.transaction_retries = 1  # Set the number of retries for Modbus operations

comapB = ModbusSerialClient(
    port='/dev/usb_serial_708d229b',
    stopbits=1,
    bytesize=8,
    parity='N',
    baudrate=19200,
    timeout=0.1,
    retries=1
)
comapB.transaction_retries = 1  # Set the number of retries for Modbus operations
# Set up modbus TCP
teltonika = ModbusTcpClient(
    host="127.0.0.1",
    port=502
)

intelimains = ModbusTcpClient(
    host="192.168.1.222",
    port=502
)

# Set up modbus RTU for powerlogger
powerlogger = ModbusSerialClient(
    port='/dev/usb_serial_ca4780cf',
    stopbits=1,
    bytesize=8,
    parity='N',
    baudrate=19200,
    timeout=0.3,
    retries=1
)
powerlogger.transaction_retries = 1  # Set the number of retries for Modbus operations
routerSerial = ""
def modbusConnect(comap):
    logger.info(f"Attempting to connect to {comap}")
    while not comap.connect():
        logger.error("Modbus initialisation failed")
        time.sleep(1)

def modbusTcpConnect(teltonika):
    logger.info("Attempting to connect to Modbus TCP server...")
    while not teltonika.connect():
        logger.error("Modbus TCP initialisation failed")
        time.sleep(1)

def modbusTcpConnect(intelimains):
    logger.info("Attempting to connect to IM Modbus TCP server...")
    while not intelimains.connect():
        logger.error("Modbus TCP IM initialisation failed")
        time.sleep(1)

def discover_slave_id(plModbus_client, start=1, end=247):
    while True:
        for slave in range(start, end + 1):
            logger.info(f"Testing slave id: {slave} ...")
            try:
                response = plModbus_client.read_holding_registers(3000, count=8, slave=slave)
                if response is not None and hasattr(response, 'registers') and len(response.registers) > 0:
                    logger.info(f"Device found on slave id {slave}")
                    return slave
                else:
                    logger.debug(f"No valid response from slave id {slave}")
            except Exception as e:
                logger.error(f"Error testing slave id {slave}: {e}")
            #time.sleep(0.2)
        logger.warning("Device not found in the specified slave id range. Restarting discovery...")
        time.sleep(5)

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
        logger.info(message)
        client.publish(genData, json.dumps(message))
    except Exception as e:
        logger.error(f"Error in modbusMessage for slave {slaveID}: {e}")
        raise

def teltonikaMessage():
    global routerSerial
    try:
        response = teltonika.read_holding_registers(143, count=4)
        if not hasattr(response, 'registers'):
            raise ValueError("No registers in response")
        latlon = response.registers
        # Remove or comment out teltonika.close() for persistent connections.
        teltonika.close()
        response = teltonika.read_holding_registers(39, count=16)
        if hasattr(response, 'registers'):
            # Pack each register as a big-endian unsigned short (2 bytes)
            byte_data = b''.join(struct.pack('>H', reg) for reg in response.registers)
            # Decode as UTF-8 and strip any null characters
            routerSerial = byte_data.decode('ascii').split('\00')[0]
            teltonika.close() 
            logger.info(routerSerial)
        else:
            logger.warning("No registers found in response")
        combined = (latlon[0] << 16) | latlon[1]
        bytes_data = combined.to_bytes(4, byteorder='big')
        latitude = unpack('>f', bytes_data)[0]

        combined = (latlon[2] << 16) | latlon[3]
        bytes_data = combined.to_bytes(4, byteorder='big')
        longitude = unpack('>f', bytes_data)[0]

        message = {
            "timestamp": time.time(),
            "routerSerial": routerSerial,
            "latitude": latitude,
            "longitude": longitude
        }
        logger.info(message)
        client.publish(modemData, json.dumps(message))
    except Exception as e:
        logger.error(f"Error in teltonikaMessage: {e}")
        raise

def intelimainsMessage():
    try:
        controllerNameResponse = intelimains.read_holding_registers(1324, count=16)
        byte_data = b''.join(struct.pack('>H', reg) for reg in controllerNameResponse.registers)
        controllerName = byte_data.decode('utf-8').split("\x00")[0]

        response1 = intelimains.read_holding_registers(1001, count=43)
        block1 = ''.join('{:04x}'.format(b) for b in response1.registers)

        response2 = intelimains.read_holding_registers(1319, count=1)
        block2 = ''.join('{:04x}'.format(b) for b in response2.registers)

        message = {
            "timestamp": time.time(),
            "controllerName": controllerName,
            "data": block1 + block2,
        }
        logger.info(message)
        client.publish(intelimainsData, json.dumps(message))
    except Exception as e:
        logger.error(f"Error in intelimainsMessage: {e}")
        raise

def check_powerlogger_slave(slave_id):
    """Check if a powerlogger slave is active at the given address"""
    try:
        # Try to read registers that should be present on a powerlogger
        logger.info(f"Checking powerlogger slave {slave_id}")
        response = powerlogger.read_holding_registers(int(0x1200), count=1, slave=slave_id)
        logger.debug(f"Response from slave {slave_id}: {response}")
        if response is not None and hasattr(response, 'registers') and len(response.registers) > 0:
            logger.info(f"Valid response from slave {slave_id}: {response.registers}")
            return True
        elif response is not None and hasattr(response, 'registers') and len(response.registers) == 0:
            logger.debug(f"Empty response from slave {slave_id}")
            return False
        logger.debug(f"No valid response from slave {slave_id}")
        return False
    except Exception as e:
        logger.error(f"Error in check_powerlogger_slave for slave {slave_id}: {e}")
        return False

def publish_powerlog(client, slave_id, routerSerial):
    """Publish powerlogger data for a specific slave ID"""
    try:
        logger.info(f"Reading registers for slave {slave_id}")
        block1 = powerlogger.read_holding_registers(int(0x1000), count=122, slave=slave_id)
        if block1 is None:
            raise Exception("No response received")
        if not hasattr(block1, 'registers'):
            raise Exception("Response has no registers")
            
        ct = powerlogger.read_holding_registers(int(0x1200), count=1, slave=slave_id)
        if ct is None:
            raise Exception("No response received for CT registers")
        if not hasattr(ct, 'registers'):
            raise Exception("CT response has no registers")
            
        hexString = ''.join('{:04x}'.format(b) for b in block1.registers)
        hexStringCT = ''.join('{:04x}'.format(b) for b in ct.registers)
        message = {
            "timestamp": time.time(),
            "slaveID": slave_id,
            "routerSerial": routerSerial,
            "rtuData": hexString[:156] + hexString[344:] + hexStringCT,
        }
        logger.info(f"Publishing message for slave {slave_id}: {message}")
        result = client.publish(powerData, json.dumps(message))
        status = result[0]
        if status != 0:
            logger.error(f'Failed to send message to topic {powerData} for slave {slave_id}')
    except Exception as e:
        logger.error(f"Error in publish_powerlog for slave {slave_id}: {e}")
        raise

def powerlogger_loop():
    """Main loop for powerlogger - handles multiple slave IDs"""
    active_slaves = set()
    while True:
        try:
            # Check for new devices on addresses 1-5
            for slave_id in range(1, 6):
                if slave_id not in active_slaves:
                    logger.info(f"Checking for new device at slave ID {slave_id}")
                    if check_powerlogger_slave(slave_id):
                        logger.info(f"Found new powerlogger device at slave ID {slave_id}")
                        active_slaves.add(slave_id)
                elif slave_id in active_slaves:
                    logger.debug(f"Verifying existing device at slave ID {slave_id}")
                    if not check_powerlogger_slave(slave_id):
                        logger.warning(f"Lost connection to powerlogger at slave ID {slave_id}")
                        active_slaves.remove(slave_id)
            
            # Get router serial from teltonika
            try:
                # Ensure we have a connection to the Teltonika device
                if not teltonika.connected:
                    logger.info("Reconnecting to Teltonika device...")
                    teltonika.connect()
            except Exception as e:
                logger.error(f"Error in powerlogger_loop: {e}")
                time.sleep(5)  # Wait before retrying
            
            # Poll active slaves
            for slave_id in active_slaves:
                try:
                    publish_powerlog(client, slave_id, routerSerial)
                except Exception as e:
                    logger.error(f"Error polling slave {slave_id}: {e}")
                    # Don't remove immediately, let the next check_powerlogger_slave decide
                time.sleep(1)  # Small delay between polling each device
            
            # If no active slaves, wait a bit longer before checking again
            if not active_slaves:
                logger.info("No active powerlogger devices found, waiting...")
                time.sleep(10)
            else:
                logger.info(f"Active powerlogger devices: {active_slaves}")
                time.sleep(10)
                
        except Exception as e:
            logger.error(f"Error in powerlogger_loop: {e}")
            time.sleep(5)  # Wait before retrying

# MQTT Setup
BROKER = credentials["broker"]
PORT = credentials["port"]
USERNAME = credentials["username"]
PASSWORD = credentials["password"]
genData = "ET/genlogger/data"
modemData = "ET/modemlogger/data"
powerData = "ET/powerlogger/data"
intelimainsData = "ET/intelimains/data"
client = mqtt_client.Client()
client.username_pw_set(USERNAME, PASSWORD)
client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.connect(BROKER, PORT, keepalive=10)
client.loop_start()

def comap_loop(comap):
    modbusConnect(comap)
    # discover_slave_id will loop internally until a slave is found.
    slave_id = discover_slave_id(comap, start=1, end=32)
    
    while True:
        try:
            modbusMessage(comap, slave_id)
        except Exception as e:
            logger.error(f"Error occurred: {e}. Reconnecting and rediscovering slave id...")
            modbusConnect(comap)    
            # This call will loop internally until it finds a valid slave id.
            slave_id = discover_slave_id(comap, start=1, end=32)
            logger.info(f"Rediscovered slave id: {slave_id}")
        time.sleep(10)

def teltonika_loop():
    modbusTcpConnect(teltonika)
    while True:
        try:
            teltonikaMessage()
        except Exception:
            modbusTcpConnect(teltonika)
        time.sleep(30)

def intelimains_loop():
    modbusTcpConnect(intelimains)
    while True:
        try:
            intelimainsMessage()
        except Exception:
            modbusTcpConnect(intelimains)
        time.sleep(5)
        
if __name__ == "__main__":
    logger.info("Starting application")
    thread_modbusA = threading.Thread(target=comap_loop, args=(comapA,), daemon=True)
    thread_modbusB = threading.Thread(target=comap_loop, args=(comapB,), daemon=True)
    thread_teltonika = threading.Thread(target=teltonika_loop, daemon=True)
    thread_wan_ip = threading.Thread(target=send_wan_ip, daemon=True)
    thread_powerlogger = threading.Thread(target=powerlogger_loop, daemon=True)
    thread_intelimains = threading.Thread(target=intelimains_loop, daemon=True)
    thread_modbusA.start()
    thread_modbusB.start()
    thread_teltonika.start()
    thread_wan_ip.start()
    thread_powerlogger.start()
    thread_intelimains.start()

    while True:
        time.sleep(10)
