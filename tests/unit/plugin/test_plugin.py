"""DishkaPlugin wiring."""

from dishka import make_container

from dishka_airflow import AirflowProvider, DishkaPlugin
from dishka_airflow._listener import _DishkaListener


def test_plugin_with_container_registers_listener() -> None:
    app_container = make_container(AirflowProvider())

    class _Plugin(DishkaPlugin):
        name = "test_dishka_plugin"
        container = app_container

    assert len(_Plugin.listeners) == 1
    assert isinstance(_Plugin.listeners[0], _DishkaListener)
    app_container.close()


def test_plugin_without_container_registers_no_listener() -> None:
    class _Plugin(DishkaPlugin):
        name = "empty_dishka_plugin"

    assert _Plugin.listeners == []
