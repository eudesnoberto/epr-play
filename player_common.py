# Estados do player (VLC ou MPV) para uso no play.py sem acoplar a lib específica.
from enum import IntEnum


class PlayerState(IntEnum):
    NONE = 0
    OPENING = 1
    BUFFERING = 2
    PLAYING = 3
    PAUSED = 4
    STOPPED = 5
    ENDED = 6
    ERROR = 7
