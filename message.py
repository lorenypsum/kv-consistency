from typing import Literal, Optional
from dataclasses import dataclass, asdict
import json

# Define uma classe de dados (dataclass) chamada 'Message' que representa uma mensagem
@dataclass
class Message:
    # O campo 'type' é uma Literal String, o que significa que só pode ter um dos valores especificados.
    type: Literal[
        "GET",
        "GET_OK",
        "PUT",
        "PUT_OK",
        "REPLICATION",
        "REPLICATION_OK",
        "TRY_OTHER_SERVER_OR_LATER",
    ]

    # O campo 'key' é uma string opcional que representa a chave da mensagem (usada para operações GET e PUT).
    key: Optional[str] = None

    # O campo 'value' é uma string opcional que representa o valor da mensagem (usado para operações PUT).
    value: Optional[str] = None

    # O campo 'timestamp' é um inteiro opcional que representa o carimbo de data/hora da mensagem.
    timestamp: Optional[int] = None

    # Método para codificar a mensagem em formato JSON e retornar os bytes resultantes.
    def encode(self):

        return json.dumps(asdict(self)).encode()

    # Método estático para decodificar a mensagem JSON de bytes e criar uma instância da classe 'Message'.
    @staticmethod
    def decode(bytes: bytes):

        return Message(**json.loads(bytes.decode()))
