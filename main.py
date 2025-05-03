import json
import time
import struct
import random
import threading
from pymodbus.client.serial import ModbusSerialClient
from pymodbus.client.tcp import ModbusTcpClient
from paho.mqtt import client as mqtt_client
import uuid
#l Load credentials
json_file_path = r".secrets/credentials.json"
with open(json_file_path, "r") as f:
    credentials = json.load(f)

# MODBUS
# Set up modbus RTU
modbusclient = ModbusSerialClient(
    port='/dev/ttyHS0', # for production use /dev/ttyHS0
    stopbits=1,
    bytesize=8,
    parity='N',
    baudrate=19200,
    timeout=0.3 
)
# Set up modbus TCP
tcpClient = ModbusTcpClient(
    host="localhost",  #localhost for production use
    port=502
)

routerSerial = "0000000000000000"
# These topics will be properly defined after getting routerSerial
topicReset = None
topicConfig = None
topicLog = "ET/modemlogger/log"  # Temporary log topic until we get the serial

# Variables for voltage and current aggregation
voltage_l1_min = float('inf')
voltage_l1_max = float('-inf')
voltage_l1_sum = 0
voltage_l2_min = float('inf')
voltage_l2_max = float('-inf')
voltage_l2_sum = 0
voltage_l3_min = float('inf')
voltage_l3_max = float('-inf')
voltage_l3_sum = 0
current_l1_min = float('inf')
current_l1_max = float('-inf')
current_l1_sum = 0
current_l2_min = float('inf')
current_l2_max = float('-inf')
current_l2_sum = 0
current_l3_min = float('inf')
current_l3_max = float('-inf')
current_l3_sum = 0
sample_count = 0
polling_active = True

def getRouterSerial():
    global routerSerial, topicReset, topicConfig, topicLog, client
    try:
        print("Attempting to connect to Modbus TCP server to get router serial...")
        if tcpClient.connect():
            serialData = tcpClient.read_holding_registers(39, count=16)
            serialByteData = b''.join(struct.pack('>H', reg) for reg in serialData.registers)
            routerSerial = serialByteData.decode('ascii').split('\00')[0]
            
            # Now that we have the router serial, update the topic definitions
            topicReset = f"ET/powerlogger/{routerSerial}/reset"
            topicConfig = f"ET/powerlogger/{routerSerial}/config"
            topicLog = f"ET/modemlogger/{routerSerial}/log"
            
            # Update the will message with the proper topic
            client.will_set(topicLog, json.dumps({"timestamp": time.time(), "routerSerial": routerSerial, "log": "Disconnected"}), retain=True)
            
            # Close connection temporarily
            tcpClient.close()
            return True
    except Exception as e:
        print(f"Failed to get router serial: {e}")
        return False

def modbusConnect(modbusclient):
    while modbusclient.connect() == False:
        logMQTT(client, topicLog, "Modbus RTU initialisation failed")
        time.sleep(1)


def modbusTcpConnect(tcpClient):
    global routerSerial, topicReset, topicConfig, topicLog
    print("Attempting to connect to Modbus TCP server...")
    while not tcpClient.connect():
        logMQTT(client, topicLog, "Modbus TCP initialisation failed, retrying...")
        time.sleep(1)  # Wait for 2 seconds before retrying
    
    # Only update the router serial if it hasn't been fetched already
    if routerSerial == "0000000000000000":
        serialData = tcpClient.read_holding_registers(39, count=16)
        serialByteData = b''.join(struct.pack('>H', reg) for reg in serialData.registers)
        routerSerial = serialByteData.decode('ascii').split('\00')[0]
        
        # Now that we have the router serial, update the topic definitions
        topicReset = f"ET/powerlogger/{routerSerial}/reset"
        topicConfig = f"ET/powerlogger/{routerSerial}/config"
        topicLog = f"ET/modemlogger/{routerSerial}/log"
    
    # Subscribe to topics with proper serial number
    client.subscribe(topicReset)
    client.subscribe(topicConfig)
    logMQTT(client, topicLog, "Successfully connected to Modbus TCP server!")

def send_master_unlock(slaveid):
    result = modbusclient.write_registers(address=0x2700, values=[0x5AA5], slave=slaveid)
    if result.isError():
        print(f"Error sending Master Unlock Key: {result}")
        return False
    return True

def save_to_eeprom(slaveid):
    if not send_master_unlock(slaveid):
        return False
    
    result = modbusclient.write_registers(address=0x2600, values=[0x000A], slave=slaveid)
    if result.isError():
        print(f"Error saving to EEPROM: {result}")
        return False
    return True

def setSerialNumber(slaveid):
    try:
        # Check current value
        check_result = modbusclient.read_holding_registers(address=0x2213, count=1, slave=slaveid)
        if check_result.isError():
            print(f"Error reading register 0x2213: {check_result}")
            return False
            
        current_value = check_result.registers[0]
        print(f"Register 0x2213 value: {hex(current_value)}")
        
        # Only modify if value is 0
        if current_value != 0x4d2:
            print("Register is not 0. No modification needed.")
            return True
        
        # Read all registers in the group
        read_result = modbusclient.read_holding_registers(address=0x2200, count=24, slave=slaveid)
        if read_result.isError():
            print(f"Error reading register group: {read_result}")
            return False
        
        # Update register
        values = read_result.registers.copy()
        new_value = random.randint(1, 9999)
        values[19] = new_value  # Register 0x2213
        
        # Write and save changes
        if not send_master_unlock(slaveid) or \
           modbusclient.write_registers(address=0x2200, values=values, slave=slaveid).isError() or \
           not save_to_eeprom(slaveid):
            print("Failed to write or save changes")
            return False
            
        # Verify change
        verify = modbusclient.read_holding_registers(address=0x2213, count=1, slave=slaveid)
        if verify.isError() or verify.registers[0] != new_value:
            print("Verification failed")
            return False
            
        print(f"Successfully updated register 0x2213 to {new_value} ({hex(new_value)})")
        return True
        
    except Exception as e:
        print(f"Error: {e}")
        return False
    
def insertStandardSettings(slaveid):
    try:
        # Read current values
        read_result = modbusclient.read_holding_registers(address=0x2000, count=16, slave=slaveid)
        if read_result.isError():
            print(f"Error reading register group: {read_result}")
            return False
        
        values = read_result.registers.copy()
        
        values[5] = 0
        values[8] = 2
        values[10] = 0
        values[11] = 0
        values[12] = 0
        values[13] = 0
        values[14] = 0
        values[15] = 0

        # Write and save changes
        if not send_master_unlock(slaveid) or \
           modbusclient.write_registers(address=0x2000, values=values, slave=slaveid).isError() or \
           not save_to_eeprom(slaveid):
            print("Failed to write or save changes")
            return False
            
        print(f"Successfully updated standard settings")
        return True
        
    except Exception as e:
        print(f"Error: {e}")
        return False


def logMQTT(client, topic, logMessage):
    global lastLogMessage, routerSerial
    if topic is None:
        print(str(time.time()) + "\t->\t" + logMessage + " (Not sent to broker - no topic)")
        return
        
    if not logMessage == lastLogMessage:
        message = {
            "timestamp": time.time(),
            "routerSerial": routerSerial if routerSerial != "0000000000000000" else "unknown",
            "log": logMessage,
        }
        print(str(time.time()) + "\t->\t" + logMessage)
        lastLogMessage = logMessage
        try:
            result = client.publish(topic, json.dumps(message))
            status = result.rc
            if status != 0:
                print(f'Failed to send log message to topic {topic}, status code: {status}')
        except Exception as e:
            print(f'Error publishing log message: {str(e)}')

def on_connect(client, userdata, flags, rc):
    if rc == 0 and client.is_connected():
        # Only subscribe if topics are properly defined
        if topicReset and topicConfig:
            client.subscribe(topicReset)
            client.subscribe(topicConfig)
            print(f"Subscribed to {topicReset} and {topicConfig}")

def on_disconnect(client, userdata, rc):
    print(f"MQTT disconnected with result code: {rc}")
    if rc != 0:
        print("Unexpected disconnection. Attempting to reconnect...")
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
    print(f"Message received on topic: {msg.topic}")
    
    if msg.topic == topicReset:
        payload = msg.payload.decode()
        print(f"Reset action requested: {payload}")
        if payload == 'current':
            resetCurrent()
        elif payload == 'voltage':
            resetVoltage()
        elif payload == 'modem':
            rebootModem()
        else:
            print(f'Unknown reset command: {payload}')
            
    elif msg.topic == topicConfig:
        try:
            payload = msg.payload.decode()
            print(f"Config update received: {payload}")
            config = json.loads(payload)
            sendInterval = config["sendInterval"]
            logMQTT(client, topicLog, f"Config updated - sendInterval set to {sendInterval}")
        except Exception as error:
            print(f"Error processing config message: {error}")
            logMQTT(client, topicLog, f"Invalid config message: {str(error)}")
    else:
        print(f"Received message on unexpected topic: {msg.topic}")

def poll_voltage_and_current(slaveid=1):
    global voltage_l1_min, voltage_l1_max, voltage_l1_sum
    global voltage_l2_min, voltage_l2_max, voltage_l2_sum
    global voltage_l3_min, voltage_l3_max, voltage_l3_sum
    global current_l1_min, current_l1_max, current_l1_sum
    global current_l2_min, current_l2_max, current_l2_sum
    global current_l3_min, current_l3_max, current_l3_sum
    global sample_count
    
    try:
        # Read voltage and current registers (block1)
        block1 = modbusclient.read_holding_registers(int(0x1000), count=14, slave=slaveid)
        
        if block1.isError():
            print("Error reading voltage and current registers")
            return
        
        # Extract values (assuming registers contain raw values that might need scaling)
        # Voltage L1 (registers 0-1)
        voltage_l1 = (block1.registers[0] << 16 | block1.registers[1]) / 1000.0  # assuming voltage in V with scaling
        
        # Voltage L2 (registers 2-3)
        voltage_l2 = (block1.registers[2] << 16 | block1.registers[3]) / 1000.0
        
        # Voltage L3 (registers 4-5)
        voltage_l3 = (block1.registers[4] << 16 | block1.registers[5]) / 1000.0
        
        # Current L1 (registers 6-7)
        current_l1 = (block1.registers[6] << 16 | block1.registers[7]) / 1000.0  # assuming current in A with scaling
        
        # Current L2 (registers 8-9)
        current_l2 = (block1.registers[8] << 16 | block1.registers[9]) / 1000.0
        
        # Current L3 (registers 10-11)
        current_l3 = (block1.registers[10] << 16 | block1.registers[11]) / 1000.0
        
        # Update min, max, and sum for each value
        voltage_l1_min = min(voltage_l1_min, voltage_l1)
        voltage_l1_max = max(voltage_l1_max, voltage_l1)
        voltage_l1_sum += voltage_l1
        
        voltage_l2_min = min(voltage_l2_min, voltage_l2)
        voltage_l2_max = max(voltage_l2_max, voltage_l2)
        voltage_l2_sum += voltage_l2
        
        voltage_l3_min = min(voltage_l3_min, voltage_l3)
        voltage_l3_max = max(voltage_l3_max, voltage_l3)
        voltage_l3_sum += voltage_l3
        
        current_l1_min = min(current_l1_min, current_l1)
        current_l1_max = max(current_l1_max, current_l1)
        current_l1_sum += current_l1
        
        current_l2_min = min(current_l2_min, current_l2)
        current_l2_max = max(current_l2_max, current_l2)
        current_l2_sum += current_l2
        
        current_l3_min = min(current_l3_min, current_l3)
        current_l3_max = max(current_l3_max, current_l3)
        current_l3_sum += current_l3
        
        sample_count += 1
        
    except Exception as e:
        print(f"Error polling voltage and current: {e}")

def reset_aggregation():
    global voltage_l1_min, voltage_l1_max, voltage_l1_sum
    global voltage_l2_min, voltage_l2_max, voltage_l2_sum
    global voltage_l3_min, voltage_l3_max, voltage_l3_sum
    global current_l1_min, current_l1_max, current_l1_sum
    global current_l2_min, current_l2_max, current_l2_sum
    global current_l3_min, current_l3_max, current_l3_sum
    global sample_count
    
    voltage_l1_min = float('inf')
    voltage_l1_max = float('-inf')
    voltage_l1_sum = 0
    voltage_l2_min = float('inf')
    voltage_l2_max = float('-inf')
    voltage_l2_sum = 0
    voltage_l3_min = float('inf')
    voltage_l3_max = float('-inf')
    voltage_l3_sum = 0
    current_l1_min = float('inf')
    current_l1_max = float('-inf')
    current_l1_sum = 0
    current_l2_min = float('inf')
    current_l2_max = float('-inf')
    current_l2_sum = 0
    current_l3_min = float('inf')
    current_l3_max = float('-inf')
    current_l3_sum = 0
    sample_count = 0

def publishPowerlog(client):
    """
    Publish power data in binary format instead of hex strings.
    All the same data is included, just more efficiently encoded.
    """
    global routerSerial, sample_count
    global voltage_l1_min, voltage_l1_max, voltage_l1_sum
    global voltage_l2_min, voltage_l2_max, voltage_l2_sum
    global voltage_l3_min, voltage_l3_max, voltage_l3_sum
    global current_l1_min, current_l1_max, current_l1_sum
    global current_l2_min, current_l2_max, current_l2_sum
    global current_l3_min, current_l3_max, current_l3_sum
    
    try:
        # Read block1 (voltage and current) for inclusion in the message
        block1 = modbusclient.read_holding_registers(int(0x1000), count=14, slave=1)
        
        # Read other registers needed for the power message
        block2 = modbusclient.read_holding_registers(int(0x1014), count=10, slave=1) # power and reactive power and real power
        block3 = modbusclient.read_holding_registers(int(0x1026), count=1, slave=1) # frequency
        block4 = modbusclient.read_holding_registers(int(0x101c), count=2, slave=1) # consumed energy 
        block5 = modbusclient.read_holding_registers(int(0x1020), count=2, slave=1) # delivered energy
        block6 = modbusclient.read_holding_registers(int(0x1024), count=2, slave=1) # power factor and sector of power factor
        block7 = modbusclient.read_holding_registers(int(0x1200), count=1, slave=1) # CT ratio
        block8 = modbusclient.read_holding_registers(int(0x2213), count=1, slave=1) # serial number
        
        # Calculate averages if samples exist
        if sample_count > 0:
            voltage_l1_avg = voltage_l1_sum / sample_count
            voltage_l2_avg = voltage_l2_sum / sample_count
            voltage_l3_avg = voltage_l3_sum / sample_count
            current_l1_avg = current_l1_sum / sample_count
            current_l2_avg = current_l2_sum / sample_count
            current_l3_avg = current_l3_sum / sample_count
        else:
            voltage_l1_avg = voltage_l2_avg = voltage_l3_avg = 0
            current_l1_avg = current_l2_avg = current_l3_avg = 0
        
        # Combine all register blocks into a single variable for legacy compatibility
        all_registers = block8.registers + block1.registers + block2.registers + block3.registers + block4.registers + \
                        block5.registers + block6.registers + block7.registers
        
        # Initialize binary data
        binary_data = bytearray()
        
        # Add timestamp (4 bytes)
        timestamp = int(time.time())
        binary_data.extend(struct.pack('>I', timestamp))
        
        # Add register data
        for reg in all_registers:
            binary_data.extend(struct.pack('>H', reg))
        
        # Add aggregated values
        aggregated_values = [
            int(voltage_l1_min * 1000) if voltage_l1_min != float('inf') else 0,
            int(voltage_l1_max * 1000) if voltage_l1_max != float('-inf') else 0,
            int(voltage_l1_avg * 1000),
            int(voltage_l2_min * 1000) if voltage_l2_min != float('inf') else 0,
            int(voltage_l2_max * 1000) if voltage_l2_max != float('-inf') else 0,
            int(voltage_l2_avg * 1000),
            int(voltage_l3_min * 1000) if voltage_l3_min != float('inf') else 0,
            int(voltage_l3_max * 1000) if voltage_l3_max != float('-inf') else 0,
            int(voltage_l3_avg * 1000),
            int(current_l1_min * 1000) if current_l1_min != float('inf') else 0,
            int(current_l1_max * 1000) if current_l1_max != float('-inf') else 0,
            int(current_l1_avg * 1000),
            int(current_l2_min * 1000) if current_l2_min != float('inf') else 0,
            int(current_l2_max * 1000) if current_l2_max != float('-inf') else 0,
            int(current_l2_avg * 1000),
            int(current_l3_min * 1000) if current_l3_min != float('inf') else 0,
            int(current_l3_max * 1000) if current_l3_max != float('-inf') else 0,
            int(current_l3_avg * 1000),
            sample_count
        ]
        for value in aggregated_values:
            binary_data.extend(struct.pack('>I', value & 0xffffffff))
        
        print(f"Binary data size: {len(binary_data)} bytes")
        result = client.publish(topicPower, binary_data, qos=1)
        status = result[0]
        if not status == 0:
            print(f'Failed to send message to topic {topicPower}')
            
        # Reset aggregation after sending
        reset_aggregation()
        
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

def voltage_current_polling():
    global polling_active
    while polling_active:
        try:
            poll_voltage_and_current()
            time.sleep(0.2)  # Poll at 2Hz (twice per second)
        except Exception as e:
            print(f"Error in polling thread: {e}")
            time.sleep(1)  # Wait a bit longer if there's an error

def powerLoop():
    global polling_active
    modbusConnect(modbusclient)
    
    # Start the polling thread
    polling_thread = threading.Thread(target=voltage_current_polling, daemon=True)
    polling_thread.start()
    
    while True:
        try:
            publishPowerlog(client)
        except Exception:
            modbusConnect(modbusclient)
        time.sleep(sendInterval)

def modemLoop():
    global topicLog
    modbusTcpConnect(tcpClient)
    
    # Log a connection message
    logMQTT(client, topicLog, "Node is connected to broker with proper topics!")
    
    while True:
        try:
            publishModemlog(client)
        except Exception as e:
            logMQTT(client, topicLog, f"Modem loop error: {str(e)}")
            modbusTcpConnect(tcpClient)
        time.sleep(300)

# MQTT

# These values get set up after all functions are defined
flag_connected = False  # Initially not connected
lastLogMessage = ""
client_id = f"client-{uuid.uuid4()}"
client = None  # Will be initialized after function definitions

BROKER = credentials["broker"] 
PORT = credentials["port"]
USERNAME = credentials["username"]
PASSWORD = credentials["password"]
topicPower = "ET/powerlogger/data"
topicModem = "ET/modemlogger/data"
# These topics will be properly defined after getting routerSerial
topicReset = None
topicConfig = None
topicLog = "ET/modemlogger/log"  # Temporary log topic until we get the serial

# Initialize MQTT client after all functions are defined
client = mqtt_client.Client(client_id=client_id, clean_session=False)
client.username_pw_set(USERNAME, PASSWORD)
client.on_connect = on_connect
client.on_message = on_message
client.on_disconnect = on_disconnect

# Connect to MQTT broker
client.connect(BROKER, PORT, keepalive=60)  # Increased keepalive for better stability
client.loop_start()

if __name__ == "__main__":
    
    # First try to get the router serial
    if getRouterSerial():
        print(f"Router serial obtained: {routerSerial}")
    else:
        print("Could not get router serial at startup, will retry later")
    
    # Now connect to Modbus
    modbusConnect(modbusclient)
    
    # Call setup functions once with slave ID 1
    slaveid = 1
    if setSerialNumber(slaveid):
        logMQTT(client, topicLog, f"Serial number set for slave {slaveid}")
    else:
        logMQTT(client, topicLog, f"Failed to set serial number for slave {slaveid}")
        
    if insertStandardSettings(slaveid):
        logMQTT(client, topicLog, f"Standard settings applied for slave {slaveid}")
    else:
        logMQTT(client, topicLog, f"Failed to apply standard settings for slave {slaveid}")
    time.sleep(10) #wait for the modem to reboot
    thread_modemLoop = threading.Thread(target=modemLoop, daemon=True)
    thread_powerLoop = threading.Thread(target=powerLoop, daemon=True)
    
    thread_modemLoop.start()
    thread_powerLoop.start()
    

    while True:
        time.sleep(10)

        