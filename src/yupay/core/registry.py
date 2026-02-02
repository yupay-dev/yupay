from typing import Dict, Any, Type, Protocol
import click
from rich.console import Console
from rich.status import Status


class DomainHandler(Protocol):
    """
    Protocol that any Domain Handler must implement.
    """

    def execute(self, config: Dict[str, Any], sink: Any, status: Status, console: Console) -> None:
        pass


class DomainRegistry:
    """
    Central registry for available Yupay domains.
    """
    _handlers: Dict[str, Type[DomainHandler]] = {}

    @classmethod
    def register(cls, domain: str):
        """Decorator to register a domain handler."""
        def decorator(handler_cls):
            cls._handlers[domain] = handler_cls
            return handler_cls
        return decorator

    @classmethod
    def get_handler(cls, domain: str) -> Type[DomainHandler]:
        return cls._handlers.get(domain)

    @classmethod
    def list_domains(cls):
        return list(cls._handlers.keys())
