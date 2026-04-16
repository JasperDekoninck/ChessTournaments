from ..base import BaseClass


class StatManager(BaseClass):
    """Compatibility shim for historical manager JSON payloads."""

    def __init__(self) -> "StatManager":
        super().__init__()
