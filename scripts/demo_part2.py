#!/usr/bin/python3
import msgpackrpc

client_1 = msgpackrpc.Client(msgpackrpc.Address("34.229.144.99", 5057))
client_2 = msgpackrpc.Client(msgpackrpc.Address("54.167.68.153", 5057))
client_3 = msgpackrpc.Client(msgpackrpc.Address("52.71.148.87", 5057))
client_4 = msgpackrpc.Client(msgpackrpc.Address("34.226.200.9", 5057))

print("34.229.144.99 's successor : " + client_1.call("get_successor", 0)[0].decode())
print("54.167.68.153 's successor : " + client_2.call("get_successor", 0)[0].decode())
print("52.71.148.87 's successor : " + client_3.call("get_successor", 0)[0].decode())
print("34.226.200.9 's successor : " + client_4.call("get_successor", 0)[0].decode())