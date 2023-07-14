from typing import Literal, Optional
from dataclasses import dataclass, asdict
import json


@dataclass
class Message:
    type: Literal[
        "GET",
        "GET_OK",
        "PUT",
        "PUT_OK",
        "REPLICATION",
        "REPLICATION_OK",
        "TRY_OTHER_SERVER_OR_LATER",
    ]
    key: Optional[str] = None
    value: Optional[str] = None
    timestamp: Optional[int] = None

    def encode(self):
        return json.dumps(asdict(self)).encode()

    @staticmethod
    def decode(bytes: bytes):
        return Message(**json.loads(bytes.decode()))
