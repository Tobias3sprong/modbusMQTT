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
# Set up modbus RTU for production use
modbusclient = ModbusSerialClient(
    port='/dev/ttyHS0', # for production use /dev/ttyHS0, local /dev/tty.usbserial-FTWJW5L4
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

#functions for emdx
def emdx_send_master_unlock(slaveid):
    result = modbusclient.write_registers(address=0x2700, values=[0x5AA5], slave=slaveid)
    if result.isError():
        print(f"Error sending Master Unlock Key: {result}")
        return False
    return True

def emdx_save_to_eeprom(slaveid):
    if not emdx_send_master_unlock(slaveid):
        return False
    
    result = modbusclient.write_registers(address=0x2600, values=[0x000A], slave=slaveid)
    if result.isError():
        print(f"Error saving to EEPROM: {result}")
        return False
    return True

def emdx_setSerialNumber(slaveid):
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
            print("Register is not 1234. No modification needed.")
            return True
        
        # Read all registers in the group
        read_result = modbusclient.read_holding_registers(address=0x2200, count=24, slave=slaveid)
        if read_result.isError():
            print(f"Error reading register group: {read_result}")
            return False
        
        # Update register
        values = read_result.registers.copy()
        new_value = random.randint(1, 9999)
        values[10] = 1
        values[19] = new_value  # Register 0x2213
        
        
        # Write and save changes
        if not emdx_send_master_unlock(slaveid) or \
           modbusclient.write_registers(address=0x2200, values=values, slave=slaveid).isError() or \
           not emdx_save_to_eeprom(slaveid):
            print("Failed to write or save changes")
            return False
        
        print("Waiting 10 seconds for reboot")
        time.sleep(10)
        
                # Read all registers in the group
        read_result = modbusclient.read_holding_registers(address=0x2200, count=24, slave=slaveid)
        if read_result.isError():
            print(f"Error reading register group: {read_result}")
            return False
        
        # Update register
        values = read_result.registers.copy()
        values[10] = 0
        
        
        # Write and save changes
        if not emdx_send_master_unlock(slaveid) or \
           modbusclient.write_registers(address=0x2200, values=values, slave=slaveid).isError() or \
           not emdx_save_to_eeprom(slaveid):
            print("Failed to write or save changes")
            return False
            
        # Verify change
        verify = modbusclient.read_holding_registers(address=0x2213, count=1, slave=slaveid)
        if verify.isError() or verify.registers[0] != new_value:
            print("Verification failed")
            return False
            
        print(f"Successfully randomised serial number")
        return True
        
    except Exception as e:
        print(f"Error: {e}")
        return False
    
def emdx_insertStandardSettings(slaveid):
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
        if not emdx_send_master_unlock(slaveid) or \
           modbusclient.write_registers(address=0x2000, values=values, slave=slaveid).isError() or \
           not emdx_save_to_eeprom(slaveid):
            print("Failed to write or save changes")
            return False
            
        print(f"Successfully updated standard settings")
        return True
        
    except Exception as e:
        print(f"Error: {e}")
        return False
#end of emdx functions

#mqtt functions
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
#end of mqtt functions

#functions for modem
def rebootModem():
    try:
        tcpClient.write_register(206, 1)
    except:
        logMQTT(client, topicLog, "Reboot failed, or pending...")
    else:
        logMQTT(client, topicLog, "Rebooting modem...")

sendInterval = 10

def on_message(client, userdata, msg):
    global sendInterval
    print(f"Message received on topic: {msg.topic}")
    
    if msg.topic == topicReset:
        payload = msg.payload.decode()
        print(f"Reset action requested: {payload}")
        if payload == 'modem':
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
        # Initialize variables for readings
        voltage_l1 = voltage_l2 = voltage_l3 = 0
        current_l1 = current_l2 = current_l3 = 0
        
        # Read voltage and current registers based on connected device type
        if emdx_connected:
            # For EMDX, use the provided slaveid (typically 1)
            block1 = modbusclient.read_holding_registers(int(0x1000), count=14, slave=slaveid)
            if block1.isError():
                print("Error reading EMDX voltage and current registers")
                return
                
            # Extract values with EMDX scaling
            voltage_l1 = (block1.registers[0] << 16 | block1.registers[1]) / 1000.0  
            voltage_l2 = (block1.registers[2] << 16 | block1.registers[3]) / 1000.0
            voltage_l3 = (block1.registers[4] << 16 | block1.registers[5]) / 1000.0
            current_l1 = (block1.registers[6] << 16 | block1.registers[7]) / 1000.0  
            current_l2 = (block1.registers[8] << 16 | block1.registers[9]) / 1000.0
            current_l3 = (block1.registers[10] << 16 | block1.registers[11]) / 1000.0
            
        elif rmu_connected:
            # For RMU/UMG, always use slave ID 49 - override the parameter
            rmu_slaveid = 49
            
            # Read voltage registers - first block
            voltage_block = modbusclient.read_holding_registers(19000, count=6, slave=rmu_slaveid)
            if voltage_block.isError():
                print("Error reading RMU voltage registers")
                return
                
            # Read current registers - separate block
            current_block = modbusclient.read_holding_registers(19012, count=8, slave=rmu_slaveid)
            if current_block.isError():
                print("Error reading RMU current registers")
                return
                
            voltage_l1 = struct.unpack('>f', struct.pack('>HH', voltage_block.registers[0], voltage_block.registers[1]))[0]
            voltage_l2 = struct.unpack('>f', struct.pack('>HH', voltage_block.registers[2], voltage_block.registers[3]))[0]
            voltage_l3 = struct.unpack('>f', struct.pack('>HH', voltage_block.registers[4], voltage_block.registers[5]))[0]
                        
            # Extract current values (using same method)
            current_l1 = struct.unpack('>f', struct.pack('>HH', current_block.registers[0], current_block.registers[1]))[0]
            current_l2 = struct.unpack('>f', struct.pack('>HH', current_block.registers[2], current_block.registers[3]))[0]
            current_l3 = struct.unpack('>f', struct.pack('>HH', current_block.registers[4], current_block.registers[5]))[0]

        else:
            return
        
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
    Publish power data in a standardized binary format compatible with the JavaScript parser.
    Implements optimized data collection from yanitza.py while maintaining the same output format.
    """
    global routerSerial, sample_count
    global voltage_l1_min, voltage_l1_max, voltage_l1_sum
    global voltage_l2_min, voltage_l2_max, voltage_l2_sum
    global voltage_l3_min, voltage_l3_max, voltage_l3_sum
    global current_l1_min, current_l1_max, current_l1_sum
    global current_l2_min, current_l2_max, current_l2_sum
    global current_l3_min, current_l3_max, current_l3_sum
    
    try:
        # Read device-specific registers and convert to standardized format
        if emdx_connected:
            # --- Optimized EMDX data collection based on yanitza.py ---
            slaveid = 1
            
            # Read serial number
            block8 = modbusclient.read_holding_registers(int(0x2213), count=1, slave=slaveid)
            if block8.isError():
                print("Error reading EMDX serial number register")
                return
            device_serial = block8.registers[0]
            
            # Read voltage and current registers
            block1 = modbusclient.read_holding_registers(int(0x1000), count=14, slave=slaveid)
            if block1.isError():
                print("Error reading EMDX voltage and current registers")
                return
                
            # Extract voltage values
            voltage_l1 = (block1.registers[0] << 16 | block1.registers[1]) / 1000.0
            voltage_l2 = (block1.registers[2] << 16 | block1.registers[3]) / 1000.0
            voltage_l3 = (block1.registers[4] << 16 | block1.registers[5]) / 1000.0
            
            # Extract current values
            current_l1 = (block1.registers[6] << 16 | block1.registers[7]) / 1000.0
            current_l2 = (block1.registers[8] << 16 | block1.registers[9]) / 1000.0
            current_l3 = (block1.registers[10] << 16 | block1.registers[11]) / 1000.0
            current_n = (block1.registers[12] << 16 | block1.registers[13]) / 1000.0
            
            # Read power values
            block2 = modbusclient.read_holding_registers(int(0x1014), count=10, slave=slaveid)
            if block2.isError():
                print("Error reading EMDX power registers")
                return
            

            
            # Read frequency
            block3 = modbusclient.read_holding_registers(int(0x1026), count=1, slave=slaveid)
            if block3.isError():
                print("Error reading EMDX frequency register")
                return
            frequency = block3.registers[0] / 10.0
            
            # Read energy values
            block4 = modbusclient.read_holding_registers(int(0x101c), count=2, slave=slaveid)
            if block4.isError():
                print("Error reading EMDX consumed energy registers")
                return
            
            block5 = modbusclient.read_holding_registers(int(0x1020), count=2, slave=slaveid)
            if block5.isError():
                print("Error reading EMDX delivered energy registers")
                return
            
            # Read power factor
            block6 = modbusclient.read_holding_registers(int(0x1024), count=2, slave=slaveid)
            if block6.isError():
                print("Error reading EMDX power factor registers")
                return
            
            # Read CT ratio
            block7 = modbusclient.read_holding_registers(int(0x1200), count=1, slave=slaveid)
            if block7.isError():
                print("Error reading EMDX CT ratio register")
                return
            
            active_power = (block2.registers[0] << 16 | block2.registers[1]) / 1000.0
            reactive_power = (block2.registers[2] << 16 | block2.registers[3]) / 1000.0
            apparent_power = (block2.registers[4] << 16 | block2.registers[5]) / 1000.0
            ct_ratio = block7.registers[0]
            if ct_ratio < 5000:
                active_power = active_power * 0.01
                reactive_power = reactive_power * 0.01
                apparent_power = apparent_power * 0.01
            sign_active = block2.registers[6]
            sign_reactive = block2.registers[7]
            chained_voltage_l1l2 = (block2.registers[8] << 16 | block2.registers[9]) / 1000.0

            # Read operating hours
            block9 = modbusclient.read_holding_registers(int(0x106E), count=1, slave=slaveid)
            if not block9.isError():
                operating_hours = block9.registers[0]
            
            # Process values
            power_factor = block6.registers[0] / 1000.0
            sector_power_factor = block6.registers[1]

            consumed_energy = (block4.registers[0] << 16 | block4.registers[1]) / 1000.0
            delivered_energy = (block5.registers[0] << 16 | block5.registers[1]) / 1000.0
            
        elif rmu_connected:
            # --- Optimized RMU data collection based on yanitza.py ---
            slaveid = 49
            
            # Read all consecutive registers from 19000 to 19085 in one request
            main_registers = modbusclient.read_holding_registers(19000, count=86, slave=slaveid)
            if main_registers.isError():
                print("Error reading main RMU registers")
                return
                
            # Read the non-consecutive registers separately
            block7 = modbusclient.read_holding_registers(600, count=2, slave=slaveid)  # CT ratio
            if block7.isError():
                print("Error reading RMU CT ratio registers")
                return
                
            block8 = modbusclient.read_holding_registers(911, count=2, slave=slaveid)  # Serial number
            if block8.isError():
                print("Error reading RMU serial number")
                return
                
            block9 = modbusclient.read_holding_registers(394, count=2, slave=slaveid)  # Operating hours
            if block9.isError():
                print("Error reading RMU operating hours")
                return
            
            # Process serial number
            device_serial = (block8.registers[0] << 16) | block8.registers[1]
            
            # Extract voltage values
            voltage_l1 = struct.unpack('>f', struct.pack('>HH', main_registers.registers[0], main_registers.registers[1]))[0]
            voltage_l2 = struct.unpack('>f', struct.pack('>HH', main_registers.registers[2], main_registers.registers[3]))[0]
            voltage_l3 = struct.unpack('>f', struct.pack('>HH', main_registers.registers[4], main_registers.registers[5]))[0]
            
            # Extract current values - offset by 12 from start (19012-19000)
            current_l1 = struct.unpack('>f', struct.pack('>HH', main_registers.registers[12], main_registers.registers[13]))[0]
            current_l2 = struct.unpack('>f', struct.pack('>HH', main_registers.registers[14], main_registers.registers[15]))[0]
            current_l3 = struct.unpack('>f', struct.pack('>HH', main_registers.registers[16], main_registers.registers[17]))[0]
            current_n = struct.unpack('>f', struct.pack('>HH', main_registers.registers[18], main_registers.registers[19]))[0]
            
            # Power values (offsets calculated from their original addresses)
            active_power = struct.unpack('>f', struct.pack('>HH', main_registers.registers[26], main_registers.registers[27]))[0]
            reactive_power = struct.unpack('>f', struct.pack('>HH', main_registers.registers[32], main_registers.registers[33]))[0]
            apparent_power = struct.unpack('>f', struct.pack('>HH', main_registers.registers[38], main_registers.registers[39]))[0]
            
            # Frequency (original block3) - offset by 50 from start (19050-19000)
            raw_freq_bytes = struct.pack('>HH', main_registers.registers[50], main_registers.registers[51])
            frequency = struct.unpack('>f', raw_freq_bytes)[0]
            
            # Energy values and power factor
            consumed_energy = struct.unpack('>f', struct.pack('>HH', main_registers.registers[68], main_registers.registers[69]))[0]
            delivered_energy = struct.unpack('>f', struct.pack('>HH', main_registers.registers[76], main_registers.registers[77]))[0]
            power_factor = struct.unpack('>f', struct.pack('>HH', main_registers.registers[84], main_registers.registers[85]))[0]
            
            sector_power_factor = 0  # May not be available
            ct_ratio = block7.registers[0]  # Use primary CT ratio
            
            # Get operating hours
            operating_hours = round(struct.unpack('>I', struct.pack('>HH', block9.registers[0], block9.registers[1]))[0] / 3600, 1)
            
            # Set signs to 0 as they might not be directly available
            sign_active = 0
            sign_reactive = 0
            
            # No chained voltage in RMU
            chained_voltage_l1l2 = 0
        else:
            return
        
        # Update min, max, and sum for each value for aggregation
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
        
        # Calculate aggregated values
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
        
        # Initialize binary data buffer
        binary_data = bytearray()
        
        # Add timestamp (4 bytes)
        timestamp = int(time.time())
        binary_data.extend(struct.pack('>I', timestamp))
        
        # Add registers in exact order expected by JavaScript parser
        registers = [
            # Block 8 - Serial number (2 registers)
            device_serial >> 16,         # High word
            device_serial & 0xFFFF,      # Low word
            
            # Block 1 - Voltage and current (14 registers)
            # Phase 1 voltage (2 registers)
            int(voltage_l1 * 1000) >> 16,
            int(voltage_l1 * 1000) & 0xFFFF,
            
            # Phase 2 voltage (2 registers)
            int(voltage_l2 * 1000) >> 16,
            int(voltage_l2 * 1000) & 0xFFFF,
            
            # Phase 3 voltage (2 registers)
            int(voltage_l3 * 1000) >> 16,
            int(voltage_l3 * 1000) & 0xFFFF,
            
            # Phase 1 current (2 registers)
            int(current_l1 * 1000) >> 16,
            int(current_l1 * 1000) & 0xFFFF,
            
            # Phase 2 current (2 registers)
            int(current_l2 * 1000) >> 16,
            int(current_l2 * 1000) & 0xFFFF,
            
            # Phase 3 current (2 registers)
            int(current_l3 * 1000) >> 16,
            int(current_l3 * 1000) & 0xFFFF,
            
            # Neutral current (2 registers)
            int(current_n * 1000) >> 16,
            int(current_n * 1000) & 0xFFFF,
            
            # Block 2 - Power values (10 registers)
            # 3-phase active power (2 registers)
            int(active_power * 100) >> 16,
            int(active_power * 100) & 0xFFFF,
            
            # 3-phase reactive power (2 registers)
            int(reactive_power * 100) >> 16,
            int(reactive_power * 100) & 0xFFFF,
            
            # 3-phase apparent power (2 registers)
            int(apparent_power * 100) >> 16,
            int(apparent_power * 100) & 0xFFFF,
            
            # Sign of active power (1 register)
            sign_active,
            
            # Sign of reactive power (1 register)
            sign_reactive,
            
            # Chained voltage L1-L2 (2 registers)
            int(chained_voltage_l1l2 * 1000) >> 16,
            int(chained_voltage_l1l2 * 1000) & 0xFFFF,
            
            # Block 3 - Frequency (1 register)
            int(min(100, max(0, frequency)) * 10),  # Limit frequency to valid range (0-100 Hz)
            
            # Block 4 - Consumed energy (2 registers)
            int(consumed_energy * 1000) >> 16,
            int(consumed_energy * 1000) & 0xFFFF,
            
            # Block 5 - Delivered energy (2 registers)
            int(delivered_energy * 1000) >> 16,
            int(delivered_energy * 1000) & 0xFFFF,
            
            # Block 6 - Power factor (2 registers)
            int(power_factor * 100),
            sector_power_factor,
            
            # Block 7 - CT ratio (1 register)
            ct_ratio,
            
            # Add Operating Hours (1 register)
            int(operating_hours)
        ]
        
        # Add all registers to binary data
        for reg in registers:
            binary_data.extend(struct.pack('>H', reg & 0xFFFF))
        
        # Add aggregated values (19 values, 4 bytes each)
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
        if status != 0:
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

def emdx_check_serialnumber(slaveid):
    try:
        emdx_serialnumber = modbusclient.read_holding_registers(int(0x2213), count=1, slave=slaveid)
        if emdx_serialnumber.isError():
            print(f"No EMDX slave found at address {slaveid}")
            return False
        print(f"Serial number read for slave {slaveid}: {emdx_serialnumber.registers[0]}")
        return True
    except Exception as e:
        print(f"Error reading serial number for slave {slaveid}: {e}")
        return False

def rmu_check_serialnumber(slaveid=49):
    try:
        # Always use slave ID 49 for RMU/UMG devices
        rmu_slaveid = 49
        
        # Read 2 registers for the serial number (spans 2 registers)
        rmu_serialnumber = modbusclient.read_holding_registers(911, count=2, slave=rmu_slaveid)
        if rmu_serialnumber.isError():
            print(f"No RMU slave found at address {rmu_slaveid}")
            return False
            
        # Combine the two registers to form the complete serial number
        # High word (register 0) << 16 | Low word (register 1)
        serial_number = (rmu_serialnumber.registers[0] << 16) | rmu_serialnumber.registers[1]
        print(f"Serial number read for slave {rmu_slaveid}: {serial_number}")
        return True
    except Exception as e:
        print(f"Error reading serial number for RMU: {e}")
        return False

def rmu_update_ct_settings(primary=400, secondary=1):
    """Update the CT (Current Transformer) settings
    
    Args:
        primary (int): Primary current in A (default: 400)
        secondary (int): Secondary current in A (default: 1)
    """
    print(f"\nUpdating CT settings to {primary}A/{secondary}A")
    
    # Update Primary CT setting (register 600)
    print(f"Writing Primary CT setting: {primary}A")
    primary_result = modbusclient.write_registers(address=600, values=[primary], slave=49)
    if primary_result.isError():
        print(f"Error updating Primary CT setting: {primary_result}")
        return False
    
    # Update Secondary CT setting (register 601)
    print(f"Writing Secondary CT setting: {secondary}A")
    secondary_result = modbusclient.write_registers(address=601, values=[secondary], slave=49)
    if secondary_result.isError():
        print(f"Error updating Secondary CT setting: {secondary_result}")
        return False
    
    # Verify the settings by reading them back
    print("\nVerifying CT settings:")
    
    # Read Primary CT setting
    primary_read = modbusclient.read_holding_registers(address=600, count=1, slave=49)
    if primary_read.isError():
        print(f"Error reading Primary CT setting: {primary_read}")
        return False
    primary_value = primary_read.registers[0]
    print(f"Primary CT setting: {primary_value}A (expected: {primary}A)")
    
    # Read Secondary CT setting
    secondary_read = modbusclient.read_holding_registers(address=601, count=1, slave=49)
    if secondary_read.isError():
        print(f"Error reading Secondary CT setting: {secondary_read}")
        return False
    secondary_value = secondary_read.registers[0]
    print(f"Secondary CT setting: {secondary_value}A (expected: {secondary}A)")
    
    # Check if values match
    if primary_value == primary and secondary_value == secondary:
        print("\nCT settings verified successfully!")
        return True
    else:
        print("\nWarning: CT settings verification failed!")
        print("Please check the device and try again.")
        return False

if __name__ == "__main__":
    
    # First try to get the router serial

    if getRouterSerial():
        print(f"Router serial obtained: {routerSerial}")
    else:
        print("Could not get router serial at startup, will retry later")

    # Now connect to Modbus
    modbusConnect(modbusclient)

    # Keep trying until at least one device is connected
    emdx_connected = False
    rmu_connected = False
    print("Waiting for devices to connect...")
    while not emdx_connected and not rmu_connected:
        # Try to connect to EMDX
        if emdx_check_serialnumber(1):
            print(f"Serial number read for slave 1")
            emdx_connected = True
        else:
            print(f"Failed to read serial number for slave 1")
        
        print("--------------------------------")
        
        # Try to connect to RMU
        if rmu_check_serialnumber(49):
            print(f"Serial number read for slave 49")
            rmu_connected = True
        else:
            print(f"Failed to read serial number for slave 49")
        
        print("--------------------------------")
        
        # If neither device is connected, wait and retry
        if not emdx_connected and not rmu_connected:
            print("No devices connected. Retrying in 10 seconds...")
            time.sleep(10)
            # Reconnect Modbus before retrying
            modbusConnect(modbusclient)
    
    print("Device connection established. Continuing...")

    # Call setup functions once with slave ID 1
    if emdx_connected:
        slaveid = 1
        if emdx_setSerialNumber(slaveid):
            logMQTT(client, topicLog, f"Serial number set for slave {slaveid}")
        else:
            logMQTT(client, topicLog, f"Failed to set serial number for slave {slaveid}")
            
        if emdx_insertStandardSettings(slaveid):
            logMQTT(client, topicLog, f"Standard settings applied for slave {slaveid}")
        else:
            logMQTT(client, topicLog, f"Failed to apply standard settings for slave {slaveid}")
        time.sleep(10) #wait for the modem to reboot

    if rmu_connected:
        slaveid = 49
        print(f"RMU connected")
        rmu_update_ct_settings(400,1)
    thread_modemLoop = threading.Thread(target=modemLoop, daemon=True)
    thread_powerLoop = threading.Thread(target=powerLoop, daemon=True)
    
    thread_modemLoop.start()
    thread_powerLoop.start()
    

    while True:
        time.sleep(10)
        