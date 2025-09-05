import json
import time
import struct
import random
import threading
from pymodbus.client.serial import ModbusSerialClient

modbusclient = ModbusSerialClient(
    port='/dev/tty.usbserial-FTWJW5L4', # for production use /dev/ttyHS0
    stopbits=1,
    bytesize=8,
    parity='N',
    baudrate=19200,
    timeout=0.3 
)

try:
    modbusclient.connect()
except Exception as e:
    print(f"Error connecting to Modbus: {e}")
    exit()

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
#        if current_value != 0x4d2:
#            print("Register is not 0. No modification needed.")
#            return True
        
        # Read all registers in the group
        read_result = modbusclient.read_holding_registers(address=0x2200, count=24, slave=slaveid)
        if read_result.isError():
            print(f"Error reading register group: {read_result}")
            return False
        
        print(f"Read result: {read_result.registers}")
        # Update register
        values = read_result.registers.copy()
        new_value = random.randint(1, 9999)
        values[10] = 0
        values[19] = new_value  # Register 0x2213
        print(f"Setting new value: {new_value}")
        
        # Write and save changes
        if not send_master_unlock(slaveid):
            print("Failed to unlock device")
            return False
            
        write_result = modbusclient.write_registers(address=0x2200, values=values, slave=slaveid)
        if write_result.isError():
            print(f"Failed to write registers: {write_result}")
            return False
            
        if not save_to_eeprom(slaveid):
            print("Failed to save to EEPROM")
            return False
            
        # Verify change
        verify = modbusclient.read_holding_registers(address=0x2213, count=1, slave=slaveid)
        print(f"Verify result: {verify.registers}, Expected: {new_value}")
        if verify.isError() or verify.registers[0] != new_value:
            print(f"Verification failed: Got {verify.registers[0]}, Expected {new_value}")
            return False
            
        print(f"Successfully updated register 0x2213 to {new_value} ({hex(new_value)})")
        return True
        
    except Exception as e:
        print(f"Error: {e}")
        return False

try:
    check_result = modbusclient.read_holding_registers(address=0x2213, count=1, slave=1)
    if check_result.isError():
        print(f"Error reading register 0x2213: {check_result}")
    current_value = check_result.registers[0]
    setSerialNumber(1)
except Exception as e:
    print(f"Error reading register 0x2213: {e}")

