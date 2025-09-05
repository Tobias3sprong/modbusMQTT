
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

modbusclient.write_registers(address=0x2200, values=[0x04D2], slave=1)
