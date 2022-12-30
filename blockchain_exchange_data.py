"""
This file contains the code needed to retrieve the <> data from https://exchange.blockchain.com/. In order to run
this code you need to sign up there and retrieve an API secret that you insert 

"""


# enter "ipconfig getifaddr en0" into a Mac terminal to retrieve your local IP address
# 192.168.178.148
# enter "curl ifconfig.me" into a Max terminal to retrieve your public IP address
# 89.247.174.99


# Simple python websocket client
# https://github.com/websocket-client/websocket-client

# import os
# os.system("pip3 install websocket-client")

from websocket import create_connection
options = {}
options['origin'] = 'https://exchange.blockchain.com'
url = "wss://ws.blockchain.info/mercury-gateway/v1/ws"
ws = create_connection(url, **options)
# substitute API_SECRET for your API secret
# msg = '{"token": "{API_SECRET}", "channel": "auth"}'
msg = '{"token": "eyJhbGciOiJFUzI1NiIsInR5cCI6IkFQSSJ9.eyJhdWQiOiJtZXJjdXJ5IiwidWlkIjoiYThjMjVjMDQtYmUwMi00NGQyLWI1MmQtZjJjNDhiNTkxY2U5IiwiaXNzIjoiYmxvY2tjaGFpbiIsInJkbyI6ZmFsc2UsImlhdCI6MTY3MjQyMjg1OCwianRpIjoiZTcxNGY3MzgtYWZhZC00NDAzLTk3MTgtMDZlNjM0YTNjOTdjIiwic2VxIjo2NDMyOTAxLCJ3ZGwiOnRydWV9.IDiPYRDd/6ap8XMWMQJjUy5JQtScPXWL/9ULLPXFtUtPUUP+OmDl30hz2NZiKO4+2G3PScpb6698D0j5JC8nah0=", "action": "subscribe", "channel": "auth"}'
ws.send(msg)
result =  ws.recv()
print(result)
# this should return: { "seqnum":0, "event":"subscribed", "channel":"auth", "readOnly":false }




ws.close()