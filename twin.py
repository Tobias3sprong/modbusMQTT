import json
import time
import struct
import threading
from struct import unpack
from pymodbus.client.serial import ModbusSerialClient
from pymodbus.client.tcp import ModbusTcpClient
from paho.mqtt import client as mqtt_client
import requests
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("twin.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Network status tracking
network_status = {
    "online": False,
    "last_check": 0,
    "check_interval": 30,  # seconds
    "reconnect_attempts": 0,
    "max_reconnect_interval": 300  # seconds
}

# Thread synchronization locks
teltonika_lock = threading.Lock()
mqtt_lock = threading.Lock()

# Load credentials
try:
    json_file_path = r".secrets/credentials.json"
    with open(json_file_path, "r") as f:
        credentials = json.load(f)
except Exception as e:
    logger.critical(f"Failed to load credentials: {e}")
    exit(1)

def on_connect(client, userdata, flags, rc):
    if rc == 0 and client.is_connected():
        logger.info("Connected to MQTT broker")
        with network_status_lock:
            network_status["online"] = True
            network_status["reconnect_attempts"] = 0
    else:
        logger.error(f"Failed to connect to MQTT broker with result code: {rc}")

def check_network():
    """Check network connectivity and update status"""
    global network_status
    current_time = time.time()
    
    # Only check periodically to avoid too many checks
    if current_time - network_status["last_check"] < network_status["check_interval"]:
        return network_status["online"]
        
    with network_status_lock:
        network_status["last_check"] = current_time
        try:
            # Try to connect to a reliable service
            response = requests.get('https://api.ipify.org', timeout=5)
            if response.status_code == 200:
                if not network_status["online"]:
                    logger.info("Network is back online")
                network_status["online"] = True
                network_status["reconnect_attempts"] = 0
                return True
        except Exception:
            if network_status["online"]:
                logger.warning("Network appears to be offline")
            network_status["online"] = False
            return False

def send_wan_ip():
    """Send WAN IP periodically"""
    while True:
        try:
            if check_network() and client.is_connected():
                response = requests.get('https://api.ipify.org?format=json', timeout=5)
                wanIP = response.json()['ip']
                logger.info(f"Current WAN IP: {wanIP}")
                
                with mqtt_lock:
                    result = client.publish("TwinsetIP", wanIP)
                    if result[0] != 0:
                        logger.warning(f"Failed to publish WAN IP: {result}")
            else:
                logger.debug("Network offline or MQTT disconnected - skipping WAN IP update")
        except Exception as e:
            logger.error(f"Error getting/sending WAN IP: {e}")
        time.sleep(60*15)  # 15 minutes

def get_backoff_time():
    """Calculate exponential backoff time for reconnection attempts"""
    with network_status_lock:
        network_status["reconnect_attempts"] += 1
        backoff = min(30 * (2 ** (network_status["reconnect_attempts"] - 1)), 
                      network_status["max_reconnect_interval"])
    return backoff

def on_disconnect(client, userdata, rc):
    logger.warning(f"MQTT disconnected with result code: {rc}")
    with network_status_lock:
        network_status["online"] = False
    
    if rc != 0:
        logger.warning("Unexpected disconnection. Will attempt to reconnect...")

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

# Global variables
routerSerial = ""
network_status_lock = threading.Lock()

def safe_publish(topic, message, client_obj):
    """Safely publish a message with proper error handling"""
    try:
        if not client_obj.is_connected():
            logger.warning(f"MQTT not connected when trying to publish to {topic}")
            return False
            
        with mqtt_lock:
            result = client_obj.publish(topic, message)
            if result[0] != 0:
                logger.error(f"Failed to publish to {topic}: {result}")
                return False
        return True
    except Exception as e:
        logger.error(f"Error publishing to {topic}: {e}")
        return False

def modbusConnect(comap):
    logger.info(f"Attempting to connect to {comap}")
    retry_count = 0
    while not comap.connect():
        retry_count += 1
        wait_time = min(5 * retry_count, 60)  # Exponential backoff capped at 60 seconds
        logger.error(f"Modbus initialization failed, retrying in {wait_time} seconds")
        time.sleep(wait_time)

def modbusTcpConnect(client):
    device_name = getattr(client, 'host', 'unknown TCP client')
    logger.info(f"Attempting to connect to Modbus TCP server at {device_name}...")
    retry_count = 0
    while not client.connect():
        retry_count += 1
        wait_time = min(5 * retry_count, 60)  # Exponential backoff capped at 60 seconds
        logger.error(f"Modbus TCP initialization failed for {device_name}, retrying in {wait_time} seconds")
        time.sleep(wait_time)

def discover_slave_id(plModbus_client, start=1, end=247):
    while True:
        for slave in range(start, end + 1):
            logger.debug(f"Testing slave id: {slave} ...")
            try:
                response = plModbus_client.read_holding_registers(3000, count=8, slave=slave)
                if response is not None and hasattr(response, 'registers') and len(response.registers) > 0:
                    logger.info(f"Device found on slave id {slave}")
                    return slave
                else:
                    logger.debug(f"No valid response from slave id {slave}")
            except Exception as e:
                logger.debug(f"Error testing slave id {slave}: {e}")
        logger.warning("Device not found in the specified slave id range. Restarting discovery...")
        time.sleep(5)

def modbusMessage(comap, slaveID):
    try:
        response1 = comap.read_holding_registers(3000, count=8, slave=slaveID)
        if not hasattr(response1, 'registers'):
            raise Exception(f"Invalid response from slave {slaveID}")
            
        byte_data = b''.join(struct.pack('>H', reg) for reg in response1.registers)
        decoded_string = byte_data.decode('utf-8').split("\x00")[0]

        response2 = comap.read_holding_registers(12, count=6, slave=slaveID)
        if not hasattr(response2, 'registers'):
            raise Exception("Invalid response for block1")
        block1 = ''.join('{:04x}'.format(b) for b in response2.registers)

        response3 = comap.read_holding_registers(103, count=21, slave=slaveID)
        if not hasattr(response3, 'registers'):
            raise Exception("Invalid response for block2")
        block2 = ''.join('{:04x}'.format(b) for b in response3.registers)

        response4 = comap.read_holding_registers(162, count=6, slave=slaveID)
        if not hasattr(response4, 'registers'):
            raise Exception("Invalid response for block3")
        block3 = ''.join('{:04x}'.format(b) for b in response4.registers)

        response5 = comap.read_holding_registers(248, count=108, slave=slaveID)
        if not hasattr(response5, 'registers'):
            raise Exception("Invalid response for block4")
        block4 = ''.join('{:04x}'.format(b) for b in response5.registers)

        message = {
            "timestamp": time.time(),
            "gensetName": decoded_string,
            "dataBlock1": block1,
            "dataBlock2": block2,
            "dataBlock3": block3,
            "dataBlock4": block4,
            "FW": "0.7.1",  # Updated version
        }
        logger.info(f"Genset data: {decoded_string}")
        safe_publish(genData, json.dumps(message), client)
    except Exception as e:
        logger.error(f"Error in modbusMessage for slave {slaveID}: {e}")
        raise

def teltonikaMessage():
    global routerSerial
    try:
        with teltonika_lock:
            if not teltonika.connected:
                logger.warning("Teltonika not connected, reconnecting...")
                teltonika.connect()
                
            response = teltonika.read_holding_registers(143, count=4)
            if not hasattr(response, 'registers'):
                raise ValueError("No registers in response for latlon")
            latlon = response.registers
            
            # Get router serial in a separate request without closing the connection
            serial_response = teltonika.read_holding_registers(39, count=16)
            if hasattr(serial_response, 'registers'):
                # Pack each register as a big-endian unsigned short (2 bytes)
                byte_data = b''.join(struct.pack('>H', reg) for reg in serial_response.registers)
                # Decode as ASCII and strip any null characters
                routerSerial = byte_data.decode('ascii').split('\00')[0]
                logger.debug(f"Router serial: {routerSerial}")
            else:
                logger.warning("No valid response for router serial")
                
        # Process latitude and longitude
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
        logger.info(f"Teltonika location: Lat {latitude}, Lon {longitude}")
        safe_publish(modemData, json.dumps(message), client)
    except Exception as e:
        logger.error(f"Error in teltonikaMessage: {e}")
        raise

def intelimainsMessage():
    try:
        if not intelimains.connected:
            logger.warning("Intelimains not connected, reconnecting...")
            intelimains.connect()
            
        controllerNameResponse = intelimains.read_holding_registers(1324, count=16)
        if not hasattr(controllerNameResponse, 'registers'):
            raise ValueError("No registers in response for controller name")
            
        byte_data = b''.join(struct.pack('>H', reg) for reg in controllerNameResponse.registers)
        controllerName = byte_data.decode('utf-8').split("\x00")[0]

        response1 = intelimains.read_holding_registers(1001, count=43)
        if not hasattr(response1, 'registers'):
            raise ValueError("No registers in response for block1")
        block1 = ''.join('{:04x}'.format(b) for b in response1.registers)

        response2 = intelimains.read_holding_registers(1319, count=1)
        if not hasattr(response2, 'registers'):
            raise ValueError("No registers in response for block2")
        block2 = ''.join('{:04x}'.format(b) for b in response2.registers)

        message = {
            "timestamp": time.time(),
            "controllerName": controllerName,
            "data": block1 + block2,
        }
        logger.info(f"Intelimains data: {controllerName}")
        safe_publish(intelimainsData, json.dumps(message), client)
    except Exception as e:
        logger.error(f"Error in intelimainsMessage: {e}")
        raise

def check_powerlogger_slave(slave_id):
    """Check if a powerlogger slave is active at the given address"""
    try:
        # Try to read registers that should be present on a powerlogger
        logger.debug(f"Checking powerlogger slave {slave_id}")
        response = powerlogger.read_holding_registers(int(0x1200), count=1, slave=slave_id)
        if response is not None and hasattr(response, 'registers') and len(response.registers) > 0:
            logger.debug(f"Valid response from slave {slave_id}: {response.registers}")
            return True
        logger.debug(f"No valid response from slave {slave_id}")
        return False
    except Exception as e:
        logger.debug(f"Error in check_powerlogger_slave for slave {slave_id}: {e}")
        return False

def publish_powerlog(client_obj, slave_id, router_serial):
    """Publish powerlogger data for a specific slave ID"""
    try:
        logger.debug(f"Reading registers for slave {slave_id}")
        block1 = powerlogger.read_holding_registers(int(0x1000), count=122, slave=slave_id)
        if block1 is None or not hasattr(block1, 'registers'):
            raise Exception(f"No valid response received for slave {slave_id}")
            
        ct = powerlogger.read_holding_registers(int(0x1200), count=1, slave=slave_id)
        if ct is None or not hasattr(ct, 'registers'):
            raise Exception(f"No valid CT response for slave {slave_id}")
            
        hexString = ''.join('{:04x}'.format(b) for b in block1.registers)
        hexStringCT = ''.join('{:04x}'.format(b) for b in ct.registers)
        message = {
            "timestamp": time.time(),
            "slaveID": slave_id,
            "routerSerial": router_serial,
            "rtuData": hexString[:156] + hexString[344:] + hexStringCT,
        }
        logger.debug(f"Publishing message for slave {slave_id}")
        safe_publish(powerData, json.dumps(message), client_obj)
    except Exception as e:
        logger.error(f"Error in publish_powerlog for slave {slave_id}: {e}")
        raise

def powerlogger_loop():
    """Main loop for powerlogger - handles multiple slave IDs"""
    active_slaves = set()
    last_slave_check = 0
    check_interval = 60  # Check for new slaves every 60 seconds
    
    while True:
        try:
            current_time = time.time()
            
            # Only check for new slaves periodically
            if current_time - last_slave_check > check_interval:
                logger.info("Checking for powerlogger slave devices...")
                last_slave_check = current_time
                
                # Check for new devices on addresses 1-5
                for slave_id in range(1, 6):
                    if slave_id not in active_slaves:
                        if check_powerlogger_slave(slave_id):
                            logger.info(f"Found new powerlogger device at slave ID {slave_id}")
                            active_slaves.add(slave_id)
                    elif slave_id in active_slaves:
                        if not check_powerlogger_slave(slave_id):
                            logger.warning(f"Lost connection to powerlogger at slave ID {slave_id}")
                            active_slaves.remove(slave_id)
            
            # Poll active slaves
            if active_slaves:
                logger.debug(f"Active powerlogger devices: {active_slaves}")
                for slave_id in list(active_slaves):  # Use a copy to avoid modification during iteration
                    try:
                        # Use the global routerSerial or a fallback value
                        current_router_serial = routerSerial if routerSerial else "unknown"
                        publish_powerlog(client, slave_id, current_router_serial)
                    except Exception as e:
                        logger.error(f"Error polling slave {slave_id}: {e}")
                    time.sleep(1)  # Small delay between polling each device
                
                # Main polling interval
                time.sleep(10)
            else:
                logger.info("No active powerlogger devices found, waiting...")
                time.sleep(30)  # Longer wait when no devices found
                
        except Exception as e:
            logger.error(f"Error in powerlogger_loop: {e}")
            time.sleep(10)  # Wait before retrying

# MQTT Setup
BROKER = credentials["broker"]
PORT = credentials["port"]
USERNAME = credentials["username"]
PASSWORD = credentials["password"]
genData = "ET/genlogger/data"
modemData = "ET/modemlogger/data"
powerData = "ET/powerlogger/data"
intelimainsData = "ET/intelimains/data"

# Configure MQTT client with proper reconnection parameters
client = mqtt_client.Client()
client.username_pw_set(USERNAME, PASSWORD)
client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.reconnect_delay_set(min_delay=1, max_delay=120)  # Exponential backoff for reconnection

# Try to establish initial MQTT connection
try:
    client.connect(BROKER, PORT, keepalive=60)
    client.loop_start()
except Exception as e:
    logger.error(f"Failed to connect to MQTT broker: {e}")
    # Proceed anyway - the loop_start and reconnect logic will handle reconnection

def comap_loop(comap):
    """Main loop for Comap controller communication"""
    retry_interval = 5
    
    while True:
        try:
            if not comap.connected:
                modbusConnect(comap)
                
            # discover_slave_id will loop internally until a slave is found
            slave_id = discover_slave_id(comap, start=1, end=32)
            logger.info(f"Discovered slave id: {slave_id}")
            
            # Main polling loop for this slave
            consecutive_errors = 0
            while True:
                try:
                    modbusMessage(comap, slave_id)
                    consecutive_errors = 0  # Reset error counter on success
                    time.sleep(10)
                except Exception as e:
                    consecutive_errors += 1
                    logger.error(f"Error in comap_loop: {e}")
                    
                    # If we have multiple consecutive errors, break to outer loop to reconnect
                    if consecutive_errors >= 3:
                        logger.warning(f"Multiple consecutive errors, reconnecting Modbus...")
                        break
                        
                    # Otherwise, short delay and retry
                    time.sleep(retry_interval)
        
        except Exception as e:
            logger.error(f"Critical error in comap_loop: {e}")
            time.sleep(retry_interval)

def teltonika_loop():
    """Main loop for Teltonika device communication"""
    retry_interval = 5
    
    while True:
        try:
            if not teltonika.connected:
                modbusTcpConnect(teltonika)
                
            # Main polling loop
            consecutive_errors = 0
            while True:
                try:
                    if check_network():
                        teltonikaMessage()
                        consecutive_errors = 0  # Reset on success
                    else:
                        logger.warning("Network appears to be down, skipping teltonika message")
                    time.sleep(30)
                except Exception as e:
                    consecutive_errors += 1
                    logger.error(f"Error in teltonika_loop: {e}")
                    
                    # If we have multiple consecutive errors, reconnect
                    if consecutive_errors >= 3:
                        logger.warning("Multiple teltonika errors, reconnecting...")
                        break
                        
                    # Wait before retry with exponential backoff
                    backoff_time = min(retry_interval * (2 ** (consecutive_errors - 1)), 60)
                    time.sleep(backoff_time)
        
        except Exception as e:
            logger.error(f"Critical error in teltonika_loop: {e}")
            time.sleep(retry_interval)

def intelimains_loop():
    """Main loop for intelimains device communication"""
    retry_interval = 5
    
    while True:
        try:
            if not intelimains.connected:
                modbusTcpConnect(intelimains)
                
            # Main polling loop
            consecutive_errors = 0
            while True:
                try:
                    intelimainsMessage()
                    consecutive_errors = 0  # Reset on success
                    time.sleep(5)
                except Exception as e:
                    consecutive_errors += 1
                    logger.error(f"Error in intelimains_loop: {e}")
                    
                    # If we have multiple consecutive errors, reconnect
                    if consecutive_errors >= 3:
                        logger.warning("Multiple intelimains errors, reconnecting...")
                        break
                        
                    # Wait before retry
                    time.sleep(retry_interval)
        
        except Exception as e:
            logger.error(f"Critical error in intelimains_loop: {e}")
            time.sleep(retry_interval)
        
if __name__ == "__main__":
    logger.info("Starting twin.py application")
    
    # Create and start all threads
    threads = []
    
    thread_modbusA = threading.Thread(target=comap_loop, args=(comapA,), daemon=True, name="ComapA-Thread")
    thread_modbusB = threading.Thread(target=comap_loop, args=(comapB,), daemon=True, name="ComapB-Thread")
    thread_teltonika = threading.Thread(target=teltonika_loop, daemon=True, name="Teltonika-Thread")
    thread_wan_ip = threading.Thread(target=send_wan_ip, daemon=True, name="WanIP-Thread")
    thread_powerlogger = threading.Thread(target=powerlogger_loop, daemon=True, name="PowerLogger-Thread")
    thread_intelimains = threading.Thread(target=intelimains_loop, daemon=True, name="InteliMains-Thread")
    
    threads.extend([
        thread_modbusA, thread_modbusB, thread_teltonika, 
        thread_wan_ip, thread_powerlogger, thread_intelimains
    ])
    
    # Start all threads
    for thread in threads:
        thread.start()
        logger.info(f"Started thread: {thread.name}")
    
    # Main thread watchdog
    try:
        while True:
            # Check if all threads are still alive
            for thread in threads:
                if not thread.is_alive():
                    logger.critical(f"Thread {thread.name} died unexpectedly!")
            
            # Log active status periodically
            logger.info("Main process still running, all threads active")
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("Application terminated by user")
    except Exception as e:
        logger.critical(f"Critical error in main thread: {e}")
