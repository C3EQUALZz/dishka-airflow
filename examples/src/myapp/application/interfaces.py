"""Ports the application layer depends on (implemented by infrastructure)."""

from abc import abstractmethod
from typing import Protocol

from myapp.domain.entities import Greeting


class GreetingRepository(Protocol):
    @abstractmethod
    def save(self, greeting: Greeting) -> None: ...
