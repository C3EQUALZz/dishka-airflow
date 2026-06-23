"""Application use cases."""

from dataclasses import dataclass

from myapp.application.interfaces import GreetingRepository
from myapp.domain.entities import Greeting


@dataclass
class GreetingService:
    repository: GreetingRepository
    prefix: str

    def greet(self, recipient: str) -> Greeting:
        greeting = Greeting(
            recipient=recipient,
            message=f"{self.prefix}, {recipient}!",
        )
        self.repository.save(greeting)
        return greeting
