from typing import cast

from airflow.sdk.types import TaskInstance

from tests.common import FakeTaskInstance


def make_task_instance(task_id: str = "demo_task") -> TaskInstance:
    return cast("TaskInstance", FakeTaskInstance(task_id))
