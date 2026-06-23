"""Adapters implementing the application ports."""

import logging

from myapp.application.interfaces import GreetingRepository
from myapp.domain.entities import Greeting

_logger = logging.getLogger("myapp")


class LoggingGreetingRepository(GreetingRepository):
    def save(self, greeting: Greeting) -> None:
        _logger.info(
            "saved greeting for %s: %s",
            greeting.recipient,
            greeting.message,
        )
