import time
from typing import Dict, List, Optional, Tuple
from socket import socket, AF_INET, SOCK_STREAM
from threading import Thread
from message import Message


class Server:
    def __init__(
        self,
        address: Tuple[str, int],
        leader_address: Tuple[str, int],
        other_addresses: List[Tuple[str, int]],
    ):
        self.address = address
        self.leader_address = leader_address
        self.other_addresses = other_addresses
        self.hash_table: Dict[str, Tuple[str, int]] = {}

    def is_leader(self):
        return self.address == self.leader_address

    def start(self):
        server_socket = socket(AF_INET, SOCK_STREAM)
        server_socket.bind(self.address)
        server_socket.listen()
        while True:
            client_socket, (ip, port) = server_socket.accept()
            Thread(target=self.handle_request, args=(client_socket, ip, port)).start()

    def handle_request(self, socket: socket, ip: str, port: int):
        with socket:
            msg = Message.decode(socket.recv(4096))
            if msg.type == "GET":
                if not msg.key:
                    raise RuntimeError("Mensagem GET sem chave.")
                response = self.handle_get(msg.key, msg.timestamp)
                print(
                    f"Cliente {ip}:{port} GET key:{msg.key} ts:{msg.timestamp}. Meu ts é {response.timestamp}, portanto devolvendo {response.value}"
                )
            elif msg.type == "PUT":
                if not msg.key:
                    raise RuntimeError("Mensagem PUT sem chave.")
                if not msg.value:
                    raise RuntimeError("Mensagem PUT sem valor.")
                print(f"Cliente {ip}:{port} PUT key:{msg.key} value:{msg.value}")
                if self.is_leader():
                    response = self.handle_put(msg.key, msg.value)
                    print(
                        f"Enviando PUT_OK ao Cliente {ip}:{port} da key:{msg.key} ts:{response.timestamp}"
                    )
                else:
                    response = self.forward_message_to_leader(msg)
            elif msg.type == "REPLICATION":
                if not msg.key:
                    raise RuntimeError("Mensagem REPLICATION sem chave.")
                if not msg.value:
                    raise RuntimeError("Mensagem REPLICATION sem valor.")
                if not msg.timestamp:
                    raise RuntimeError("Mensagem REPLICATION sem timestamp.")
                print(f"REPLICATION key:{msg.key} value:{msg.value} ts:{msg.timestamp}")
                response = self.handle_replication(msg.key, msg.value, msg.timestamp)
            socket.sendall(response.encode())

    def handle_get(self, key: str, ts: Optional[int]):
        if key in self.hash_table:
            value, timestamp = self.hash_table[key]
            if not ts or timestamp >= ts:
                return Message("GET_OK", value=value, timestamp=timestamp)
            else:
                return Message("TRY_OTHER_SERVER_OR_LATER", None, None, None)
        else:
            return Message("GET_OK")

    def handle_put(self, key: str, value: str):
        timestamp = int(time.time())
        self.hash_table[key] = (value, timestamp)
        self.replicate(key, value, timestamp)
        return Message("PUT_OK", timestamp=timestamp)

    def handle_replication(self, key: str, value: str, timestamp: int):
        self.hash_table[key] = (value, timestamp)
        return Message("REPLICATION_OK")

    def forward_message_to_leader(self, msg: Message):
        with socket(AF_INET, SOCK_STREAM) as leader_socket:
            leader_socket.connect(self.leader_address)
            leader_socket.sendall(msg.encode())
            return Message.decode(leader_socket.recv(4096))

    def replicate(self, key: str, value: str, timestamp: int):
        msg = Message("REPLICATION", key, value, timestamp)
        for address in self.other_addresses:
            if address != self.address:
                with socket(AF_INET, SOCK_STREAM) as replication_socket:
                    replication_socket.connect(address)
                    replication_socket.sendall(msg.encode())
                    Message.decode(replication_socket.recv(4096))


# programa principal

ip = input("Insira o IP do servidor (default: 127.0.0.1): ") or "127.0.0.1"
port = input("Insira a porta do servidor: (default: 10097)") or "10097"
address = (ip, int(port))

leader_ip = input("Insira o IP do líder (default: 127.0.0.1): ") or "127.0.0.1"
leader_port = input("Insira a porta do líder: (default: 10099)") or "10099"
leader_address = (leader_ip, int(leader_port))

other_ip = input("Insira o IP do outro servidor (default: 127.0.0.1): ") or "127.0.0.1"
other_port = input("Insira a porta do outro servidor (default: 10098): ") or "10098"
other_addresses = [(other_ip, int(other_port))]
if address == leader_address:
    other_ip = (
        input("Insira o IP do outro servidor (default: 127.0.0.1): ") or "127.0.0.1"
    )
    other_port = input("Insira a porta do outro servidor: ")
    other_addresses.append((other_ip, int(other_port)))

Server(address, leader_address, other_addresses).start()
