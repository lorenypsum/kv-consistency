from random import choice
from typing import Dict, List, Tuple
from socket import socket, AF_INET, SOCK_STREAM
from message import Message


class Client:
    def __init__(self, addresses: List[Tuple[str, int]]):
        self.addresses = addresses
        self.timestamps: Dict[str, int] = {}

    def start(self):
        while True:
            cmd = input("Digite seu comando (INIT, GET, PUT): ")
            if cmd == "INIT":
                pass
            elif cmd == "GET":
                self.handle_get()
            elif cmd == "PUT":
                self.handle_put()

    def handle_get(self):
        key = input("Digite a chave: ")
        timestamp = self.timestamps.get(key, 0)
        res, (ip, port) = self.send(Message("GET", key=key, timestamp=timestamp))
        if res.type == "GET_OK":
            print(
                f"GET_OK key:{key} value:{res.value} obtido do servidor {ip}:{port}, meu timestamp {timestamp} e do servidor {res.timestamp}"
            )
            self.timestamps[key] = res.timestamp
        else:
            print(res.type)

    def handle_put(self):
        key = input("Digite a chave: ")
        val = input("Digite o valor: ")
        res, (ip, port) = self.send(Message("PUT", key=key, value=val))
        print(
            f"PUT_OK key:{key} value:{val} timestamp:{res.timestamp} realizada no servidor {ip}:{port}"
        )
        self.timestamps[key] = res.timestamp

    def send(self, msg: Message):
        address = choice(self.addresses)
        with socket(AF_INET, SOCK_STREAM) as sock:
            sock.connect(address)
            sock.sendall(msg.encode())
            response = Message.decode(sock.recv(4096))
            return response, address


# programa principal

addresses: List[Tuple[str, int]] = []
for i in range(3):
    ip = (
        input(f"Insira o IP do {i + 1}o servidor (default: 127.0.0.1): ") or "127.0.0.1"
    )
    port = (
        input(f"Insira a porta do {i + 1}o servidor: (default: {10097 + i})")
        or f"{10097 + i}"
    )
    addresses.append((ip, int(port)))

Client(addresses).start()
