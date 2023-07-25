from random import choice
from typing import Dict, List, Tuple
from socket import socket, AF_INET, SOCK_STREAM

# Importa a classe Message de um módulo chamado 'message'
from message import Message

# Classe do cliente
class Client:
    # Inicializa o cliente com uma lista de endereços (tuplas de IP e porta) dos servidores
    def __init__(self, addresses: List[Tuple[str, int]]):
        self.addresses = addresses
        # Dicionário para armazenar timestamps das chaves
        self.timestamps: Dict[str, int] = {}

    # Inicia o loop principal do cliente para aceitar comandos do usuário
    def start(self):
        while True:
            # Solicita o comando ao usuário
            cmd = input("Digite seu comando (INIT, GET, PUT): ")
            if cmd == "INIT":
                pass
            elif cmd == "GET":
                self.handle_get()  # Chama o método para lidar com o comando GET
            elif cmd == "PUT":
                self.handle_put()  # Chama o método para lidar com o comando PUT

    # Lida com o comando GET
    def handle_get(self):
        # Solicita a chave ao usuário
        key = input("Digite a chave: ")
        # Obtém o timestamp da chave armazenado no dicionário
        timestamp = self.timestamps.get(key, 0)
        res, (ip, port) = self.send(
            Message("GET", key=key, timestamp=timestamp))
        # Envia a mensagem GET ao servidor selecionado, passando a chave e o timestamp
        if res.type == "GET_OK":
            # Imprime a resposta do servidor em caso de sucesso e atualiza o timestamp local
            print(
                f"GET_OK key:{key} value:{res.value} obtido do servidor {ip}:{port}, meu timestamp {timestamp} e do servidor {res.timestamp}"
            )
            self.timestamps[key] = res.timestamp
        else:
            # Em caso de erro, imprime o tipo de resposta
            print(res.type)

    # Lida com o comando PUT
    def handle_put(self):
        # Solicita a chave ao usuário
        key = input("Digite a chave: ")
        # Solicita o valor ao usuário
        val = input("Digite o valor: ")
        res, (ip, port) = self.send(Message("PUT", key=key, value=val))
        # Envia a mensagem PUT ao servidor selecionado, passando a chave e o valor
        print(
            f"PUT_OK key:{key} value:{val} timestamp:{res.timestamp} realizada no servidor {ip}:{port}"
        )
        # Imprime a resposta do servidor em caso de sucesso e atualiza o timestamp local
        self.timestamps[key] = res.timestamp

    # Envia a mensagem para um servidor selecionado aleatoriamente
    def send(self, msg: Message):
        # Escolhe aleatoriamente um endereço da lista
        address = choice(self.addresses)
        with socket(AF_INET, SOCK_STREAM) as sock:
            # Conecta ao servidor usando o endereço selecionado
            sock.connect(address)
            # Envia a mensagem codificada como bytes
            sock.sendall(msg.encode())
            # Recebe e decodifica a resposta do servidor
            response = Message.decode(sock.recv(4096))
            return response, address  # Retorna a resposta e o endereço do servidor


# Programa principal

addresses: List[Tuple[str, int]] = []

# Obtém informações sobre os servidores (IP e porta) do usuário e os armazena em uma lista
for i in range(3):
    ip = (
        input(
            f"Insira o IP do {i + 1}o servidor (default: 127.0.0.1): ") or "127.0.0.1"
    )
    port = (
        input(f"Insira a porta do {i + 1}o servidor: (default: {10097 + i})")
        or f"{10097 + i}"
    )
    addresses.append((ip, int(port)))

# Inicia o cliente com os endereços dos servidores e inicia o loop principal do cliente
Client(addresses).start()
