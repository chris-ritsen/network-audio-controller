from pydantic import BaseModel
from enum import Enum

class ChannelType(str, Enum):
    RX = "rx"
    TX = "tx"

class DanteChannel(BaseModel):
    type: ChannelType
    number: int
    name: str
    friendly_name: str|None = None

    status_code: int|None=None
    volume: int|None = None

    def __str__(self):
        if self.friendly_name:
            name = self.friendly_name
        else:
            name = self.name

        if self.volume and self.volume != 254:
            text = f"{self.number}:{name} [{self.volume}]"
        else:
            text = f"{self.number}:{name}"

        return text
