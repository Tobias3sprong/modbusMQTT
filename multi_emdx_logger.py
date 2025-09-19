import json
import time
import struct
import threading
from pymodbus.client.serial import ModbusSerialClient
from paho.mqtt import client as mqtt_client
import uuid
from datetime import datetime

# Load credentials
json_file_path = r".secrets/credentials.json"
with open(json_file_path, "r") as f:
    credentials = json.load(f)

# Configuration
LOGGER_COUNT = 5
LOGGER_ADDRESSES = [1, 2, 3, 4, 5]  # Modbus addresses for the 5 loggers
SEND_INTERVAL = 10  # seconds between data sends
RECONNECT_DELAY = 5  # seconds to wait before reconnecting failed loggers
POLLING_INTERVAL = 0.2  # seconds between voltage/current polls

# Global variables
routerSerial = "0000000000000000"
topicLog = "ET/modemlogger/log"
lastLogMessage = ""

# Logger status tracking
logger_status = {}
logger_data = {}
logger_threads = {}
logger_lock = threading.Lock()

# MQTT client setup
client_id = f"multi-logger-{uuid.uuid4()}"
client = None

# Modbus client setup
modbusclient = ModbusSerialClient(
    port='/dev/tty.usbserial-FT9LBZ90',  # for production use /dev/ttyHS0, local /dev/tty.usbserial-FTWJW5L4
    stopbits=1,
    bytesize=8,
    parity='N',
    baudrate=19200,
    timeout=0.3
)

def logMQTT(client, topic, logMessage):
    """Log message to MQTT broker"""
    global lastLogMessage, routerSerial
    if topic is None:
        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} -> {logMessage} (Not sent to broker - no topic)")
        return
        
    if not logMessage == lastLogMessage:
        message = {
            "timestamp": time.time(),
            "routerSerial": routerSerial if routerSerial != "0000000000000000" else "unknown",
            "log": logMessage,
        }
        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} -> {logMessage}")
        lastLogMessage = logMessage
        try:
            result = client.publish(topic, json.dumps(message))
            status = result.rc
            if status != 0:
                print(f'Failed to send log message to topic {topic}, status code: {status}')
        except Exception as e:
            print(f'Error publishing log message: {str(e)}')

def modbusConnect():
    """Connect to Modbus RTU"""
    while not modbusclient.connect():
        logMQTT(client, topicLog, "Modbus RTU initialization failed")
        time.sleep(1)

def emdx_check_connection(slaveid):
    """Check if EMDX logger is connected and responsive"""
    try:
        result = modbusclient.read_holding_registers(int(0x2213), count=1, slave=slaveid)
        if result.isError():
            return False
        return True
    except Exception as e:
        print(f"Error checking connection for slave {slaveid}: {e}")
        return False

def emdx_read_data(slaveid):
    """Read all data from EMDX logger"""
    try:
        # Read serial number
        block8 = modbusclient.read_holding_registers(int(0x2213), count=1, slave=slaveid)
        if block8.isError():
            return None
        device_serial = block8.registers[0]
        
        # Read voltage and current registers
        block1 = modbusclient.read_holding_registers(int(0x1000), count=14, slave=slaveid)
        if block1.isError():
            return None
            
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
            return None
        
        active_power = (block2.registers[0] << 16 | block2.registers[1]) / 1000.0
        reactive_power = (block2.registers[2] << 16 | block2.registers[3]) / 1000.0
        apparent_power = (block2.registers[4] << 16 | block2.registers[5]) / 1000.0
        
        # Read frequency
        block3 = modbusclient.read_holding_registers(int(0x1026), count=1, slave=slaveid)
        if block3.isError():
            return None
        frequency = block3.registers[0] / 10.0
        
        # Read energy values
        block4 = modbusclient.read_holding_registers(int(0x101c), count=2, slave=slaveid)
        if block4.isError():
            return None
        
        block5 = modbusclient.read_holding_registers(int(0x1020), count=2, slave=slaveid)
        if block5.isError():
            return None
        
        # Read power factor
        block6 = modbusclient.read_holding_registers(int(0x1024), count=2, slave=slaveid)
        if block6.isError():
            return None
        
        # Read CT ratio
        block7 = modbusclient.read_holding_registers(int(0x1200), count=1, slave=slaveid)
        if block7.isError():
            return None
        
        # Read operating hours
        block9 = modbusclient.read_holding_registers(int(0x106E), count=1, slave=slaveid)
        operating_hours = block9.registers[0] if not block9.isError() else 0
        
        # Process values
        power_factor = block6.registers[0] / 1000.0
        sector_power_factor = block6.registers[1]
        ct_ratio = block7.registers[0]
        
        # Scale energy values based on CT ratio
        consumed_energy = scale_energy_by_ct_ratio((block4.registers[0] << 16 | block4.registers[1]), ct_ratio)
        delivered_energy = scale_energy_by_ct_ratio((block5.registers[0] << 16 | block5.registers[1]), ct_ratio)
        
        # Apply power scaling for low CT ratios
        if ct_ratio < 5000:
            active_power = active_power * 0.01
            reactive_power = reactive_power * 0.01
            apparent_power = apparent_power * 0.01
        
        sign_active = block2.registers[6]
        sign_reactive = block2.registers[7]
        chained_voltage_l1l2 = (block2.registers[8] << 16 | block2.registers[9]) / 1000.0
        
        return {
            'device_serial': device_serial,
            'voltage_l1': voltage_l1,
            'voltage_l2': voltage_l2,
            'voltage_l3': voltage_l3,
            'current_l1': current_l1,
            'current_l2': current_l2,
            'current_l3': current_l3,
            'current_n': current_n,
            'active_power': active_power,
            'reactive_power': reactive_power,
            'apparent_power': apparent_power,
            'frequency': frequency,
            'consumed_energy': consumed_energy,
            'delivered_energy': delivered_energy,
            'power_factor': power_factor,
            'sector_power_factor': sector_power_factor,
            'ct_ratio': ct_ratio,
            'operating_hours': operating_hours,
            'sign_active': sign_active,
            'sign_reactive': sign_reactive,
            'chained_voltage_l1l2': chained_voltage_l1l2
        }
        
    except Exception as e:
        print(f"Error reading data from slave {slaveid}: {e}")
        return None

def scale_energy_by_ct_ratio(energy_value, ct_ratio):
    """Scale energy values based on CT ratio ranges"""
    if ct_ratio >= 100000:
        return energy_value * 1000  # 1.000.000 Wh
    elif ct_ratio >= 10000:
        return energy_value * 100   # 100.000 Wh
    elif ct_ratio >= 1000:
        return energy_value * 10    # 10.000 Wh
    elif ct_ratio >= 100:
        return energy_value * 1     # 1.000 Wh
    elif ct_ratio >= 10:
        return energy_value / 10    # 100 Wh
    elif ct_ratio >= 1:
        return energy_value / 100   # 10 Wh
    else:
        return energy_value / 1000  # No scaling for ct_ratio < 1

# EMDX initialization functions
def emdx_send_master_unlock(slaveid):
    """Send master unlock key to EMDX device"""
    result = modbusclient.write_registers(address=0x2700, values=[0x5AA5], slave=slaveid)
    if result.isError():
        print(f"Error sending Master Unlock Key: {result}")
        return False
    return True

def emdx_save_to_eeprom(slaveid):
    """Save current settings to EEPROM"""
    if not emdx_send_master_unlock(slaveid):
        return False
    
    result = modbusclient.write_registers(address=0x2600, values=[0x000A], slave=slaveid)
    if result.isError():
        print(f"Error saving to EEPROM: {result}")
        return False
    return True

def emdx_setSerialNumber(slaveid):
    """Set random serial number for EMDX device"""
    import random
    try:
        # Check current value
        check_result = modbusclient.read_holding_registers(address=0x2213, count=1, slave=slaveid)
        if check_result.isError():
            print(f"Error reading register 0x2213: {check_result}")
            return False
            
        current_value = check_result.registers[0]
        print(f"Register 0x2213 value: {hex(current_value)}")
        
        # Only modify if value is 0x4d2 (1234)
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
    """Insert standard settings for EMDX device"""
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

def publish_logger_data(slaveid, data):
    """Publish logger data to MQTT"""
    try:
        # Create binary data format (same as original main.py)
        binary_data = bytearray()
        
        # Add timestamp (4 bytes)
        timestamp = int(time.time())
        binary_data.extend(struct.pack('>I', timestamp))
        
        # Add registers in exact order expected by JavaScript parser
        registers = [
            # Block 8 - Serial number (2 registers)
            data['device_serial'] >> 16,         # High word
            data['device_serial'] & 0xFFFF,      # Low word
            
            # Block 1 - Voltage and current (14 registers)
            # Phase 1 voltage (2 registers)
            int(data['voltage_l1'] * 1000) >> 16,
            int(data['voltage_l1'] * 1000) & 0xFFFF,
            
            # Phase 2 voltage (2 registers)
            int(data['voltage_l2'] * 1000) >> 16,
            int(data['voltage_l2'] * 1000) & 0xFFFF,
            
            # Phase 3 voltage (2 registers)
            int(data['voltage_l3'] * 1000) >> 16,
            int(data['voltage_l3'] * 1000) & 0xFFFF,
            
            # Phase 1 current (2 registers)
            int(data['current_l1'] * 1000) >> 16,
            int(data['current_l1'] * 1000) & 0xFFFF,
            
            # Phase 2 current (2 registers)
            int(data['current_l2'] * 1000) >> 16,
            int(data['current_l2'] * 1000) & 0xFFFF,
            
            # Phase 3 current (2 registers)
            int(data['current_l3'] * 1000) >> 16,
            int(data['current_l3'] * 1000) & 0xFFFF,
            
            # Neutral current (2 registers)
            int(data['current_n'] * 1000) >> 16,
            int(data['current_n'] * 1000) & 0xFFFF,
            
            # Block 2 - Power values (10 registers)
            # 3-phase active power (2 registers)
            int(data['active_power'] * 100) >> 16,
            int(data['active_power'] * 100) & 0xFFFF,
            
            # 3-phase reactive power (2 registers)
            int(data['reactive_power'] * 100) >> 16,
            int(data['reactive_power'] * 100) & 0xFFFF,
            
            # 3-phase apparent power (2 registers)
            int(data['apparent_power'] * 100) >> 16,
            int(data['apparent_power'] * 100) & 0xFFFF,
            
            # Sign of active power (1 register)
            data['sign_active'],
            
            # Sign of reactive power (1 register)
            data['sign_reactive'],
            
            # Chained voltage L1-L2 (2 registers)
            int(data['chained_voltage_l1l2'] * 1000) >> 16,
            int(data['chained_voltage_l1l2'] * 1000) & 0xFFFF,
            
            # Block 3 - Frequency (1 register)
            int(min(100, max(0, data['frequency'])) * 10),  # Limit frequency to valid range (0-100 Hz)
            
            # Block 4 - Consumed energy (2 registers)
            int(data['consumed_energy']) >> 16,
            int(data['consumed_energy']) & 0xFFFF,
            
            # Block 5 - Delivered energy (2 registers)
            int(data['delivered_energy']) >> 16,
            int(data['delivered_energy']) & 0xFFFF,
            
            # Block 6 - Power factor (2 registers)
            int(data['power_factor'] * 100),
            data['sector_power_factor'],
            
            # Block 7 - CT ratio (1 register)
            data['ct_ratio'],
            
            # Add Operating Hours (1 register)
            int(data['operating_hours'])
        ]
        
        # Add all registers to binary data
        for reg in registers:
            binary_data.extend(struct.pack('>H', reg & 0xFFFF))
        
        # Add aggregated values (19 values, 4 bytes each) - simplified for multi-logger
        aggregated_values = [
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1  # sample_count = 1
        ]
        
        for value in aggregated_values:
            binary_data.extend(struct.pack('>I', value & 0xffffffff))
        
        # Publish to logger-specific topic
        topic = f"ET/powerlogger/data"
        result = client.publish(topic, binary_data, qos=1)
        status = result[0]
        if status != 0:
            print(f'Failed to send message to topic {topic}')
        else:
            print(f"Published data for logger {slaveid} to {topic}")
            
    except Exception as e:
        print(f"Error publishing data for logger {slaveid}: {e}")

def logger_monitor_thread(slaveid):
    """Monitor individual logger and handle reconnection"""
    global logger_status, logger_data
    
    # Track if initialization has been performed for this logger
    initialization_done = False
    
    while True:
        try:
            # Check if logger is connected
            if emdx_check_connection(slaveid):
                if not logger_status.get(slaveid, {}).get('connected', False):
                    # Logger just came online
                    with logger_lock:
                        logger_status[slaveid] = {'connected': True, 'last_seen': time.time()}
                    logMQTT(client, topicLog, f"Logger {slaveid} connected")
                    
                    # Perform EMDX initialization on first connection
                    if not initialization_done:
                        logMQTT(client, topicLog, f"Initializing EMDX logger {slaveid}...")
                        
                        # Set serial number
                        if emdx_setSerialNumber(slaveid):
                            logMQTT(client, topicLog, f"Serial number set for logger {slaveid}")
                        else:
                            logMQTT(client, topicLog, f"Failed to set serial number for logger {slaveid}")
                        
                        # Insert standard settings
                        if emdx_insertStandardSettings(slaveid):
                            logMQTT(client, topicLog, f"Standard settings applied for logger {slaveid}")
                        else:
                            logMQTT(client, topicLog, f"Failed to apply standard settings for logger {slaveid}")
                        
                        initialization_done = True
                        logMQTT(client, topicLog, f"EMDX initialization completed for logger {slaveid}")
                
                # Read data from logger
                data = emdx_read_data(slaveid)
                if data:
                    with logger_lock:
                        logger_data[slaveid] = data
                        logger_status[slaveid]['last_seen'] = time.time()
                    
                    # Publish data
                    publish_logger_data(slaveid, data)
                else:
                    print(f"Failed to read data from logger {slaveid}")
                    
            else:
                # Logger is offline
                if logger_status.get(slaveid, {}).get('connected', False):
                    # Logger just went offline
                    with logger_lock:
                        logger_status[slaveid] = {'connected': False, 'last_seen': time.time()}
                    logMQTT(client, topicLog, f"Logger {slaveid} disconnected")
                    # Reset initialization flag when logger goes offline
                    initialization_done = False
                
                print(f"Logger {slaveid} is offline, retrying in {RECONNECT_DELAY} seconds...")
                time.sleep(RECONNECT_DELAY)
                continue
                
        except Exception as e:
            print(f"Error in logger {slaveid} monitor thread: {e}")
            with logger_lock:
                logger_status[slaveid] = {'connected': False, 'last_seen': time.time()}
            time.sleep(RECONNECT_DELAY)
            continue
        
        # Wait before next check
        time.sleep(SEND_INTERVAL)

def status_monitor_thread():
    """Monitor and log status of all loggers"""
    while True:
        try:
            with logger_lock:
                connected_count = sum(1 for status in logger_status.values() if status.get('connected', False))
                total_count = len(LOGGER_ADDRESSES)
                
            if connected_count != total_count:
                offline_loggers = []
                for slaveid in LOGGER_ADDRESSES:
                    if not logger_status.get(slaveid, {}).get('connected', False):
                        offline_loggers.append(str(slaveid))
                
                logMQTT(client, topicLog, f"Status: {connected_count}/{total_count} loggers online. Offline: {', '.join(offline_loggers)}")
            else:
                logMQTT(client, topicLog, f"Status: All {total_count} loggers online")
                
        except Exception as e:
            print(f"Error in status monitor thread: {e}")
        
        time.sleep(30)  # Status update every 30 seconds

def on_connect(client, userdata, flags, rc):
    """MQTT connection callback"""
    if rc == 0 and client.is_connected():
        print("Connected to MQTT broker")
        logMQTT(client, topicLog, "Multi-logger system connected to MQTT broker")

def on_disconnect(client, userdata, rc):
    """MQTT disconnection callback"""
    print(f"MQTT disconnected with result code: {rc}")
    if rc != 0:
        print("Unexpected disconnection. Attempting to reconnect...")
        try:
            client.reconnect()
        except Exception as e:
            print(f"Reconnect failed: {e}")

def main():
    """Main function"""
    global client, routerSerial
    
    print("Starting Multi-Logger EMDX Power Logger System")
    print(f"Monitoring {LOGGER_COUNT} loggers at addresses: {LOGGER_ADDRESSES}")
    
    # Initialize MQTT client
    client = mqtt_client.Client(client_id=client_id, clean_session=False)
    client.username_pw_set(credentials["username"], credentials["password"])
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    
    # Connect to MQTT broker
    client.connect(credentials["broker"], credentials["port"], keepalive=60)
    client.loop_start()
    
    # Wait for MQTT connection
    time.sleep(2)
    
    # Connect to Modbus
    modbusConnect()
    logMQTT(client, topicLog, "Modbus RTU connected")
    
    # Initialize logger status
    for slaveid in LOGGER_ADDRESSES:
        logger_status[slaveid] = {'connected': False, 'last_seen': 0}
    
    # Start individual logger monitor threads
    for slaveid in LOGGER_ADDRESSES:
        thread = threading.Thread(target=logger_monitor_thread, args=(slaveid,), daemon=True)
        thread.start()
        logger_threads[slaveid] = thread
        print(f"Started monitor thread for logger {slaveid}")
    
    # Start status monitor thread
    status_thread = threading.Thread(target=status_monitor_thread, daemon=True)
    status_thread.start()
    
    logMQTT(client, topicLog, f"Multi-logger system started - monitoring {LOGGER_COUNT} loggers")
    
    # Keep main thread alive
    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        print("\nShutting down multi-logger system...")
        logMQTT(client, topicLog, "Multi-logger system shutting down")

if __name__ == "__main__":
    main()