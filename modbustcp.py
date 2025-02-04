from pymodbus.client.tcp import ModbusTcpClient

tcpClient = ModbusTcpClient(
    host="77.170.196.120",
    port=502
)
if tcpClient.connect():
    IMSIreg = tcpClient.read_holding_registers(348,count=8) #, 8, slave=1
    IMSI = bytes.fromhex(''.join('{:02x}'.format(b) for b in IMSIreg.registers))[:-1].decode("ASCII")
    print(IMSI)
    WanIPreg = tcpClient.read_holding_registers(139,count=2)  #, 2, slave=1) WAN IP address registers
    print(f"Response: {WanIPreg}")
    if WanIPreg.isError():
        print("Failed to read WAN IP from registers.")
        WanIP = "0.0.0.0"  # Default in case of failure
    else:
        #Combine the two registers into a 32-bit value
        wan_ip_int = (WanIPreg.registers[0] << 16) | WanIPreg.registers[1]
        # Convert to a dotted quad IP string
        WanIP = '.'.join(str((wan_ip_int >> (8 * i)) & 0xFF) for i in range(3, -1, -1))
        print(f"WAN IP: {WanIP}")
