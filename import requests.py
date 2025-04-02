import requests

def get_wan_ip():
    response = requests.get('https://api.ipify.org?format=json')
    return response.json()['ip']

wanIP = get_wan_ip()
print(wanIP)