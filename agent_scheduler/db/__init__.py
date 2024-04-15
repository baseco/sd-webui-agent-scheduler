from pathlib import Path
from sqlalchemy import create_engine, inspect, text, String, Text

from .base import Base, metadata, database_url, database_schema
from .app_state import AppStateKey, AppState, AppStateManager
from .task import TaskStatus, Task, TaskManager
from .model_usage_view import ModelUsageView

version = "2"

state_manager = AppStateManager()
task_manager = TaskManager()


def init():
    engine = create_engine(
        database_url, connect_args={
            "options": "-csearch_path={},metrics".format(database_schema)}
    )

    metadata.create_all(engine)

    state_manager.set_value(AppStateKey.Version, version)
    # check if app state exists
    if state_manager.get_value(AppStateKey.QueueState) is None:
        # create app state
        state_manager.set_value(AppStateKey.QueueState, "running")

    inspector = inspect(engine)
    with engine.connect() as conn:
        task_columns = inspector.get_columns("task")
        # add result column
        if not any(col["name"] == "result" for col in task_columns):
            conn.execute(text("ALTER TABLE task ADD COLUMN result TEXT"))

        # add api_task_id column
        if not any(col["name"] == "api_task_id" for col in task_columns):
            conn.execute(
                text("ALTER TABLE task ADD COLUMN api_task_id VARCHAR(64)"))

        # add api_task_callback column
        if not any(col["name"] == "api_task_callback" for col in task_columns):
            conn.execute(
                text("ALTER TABLE task ADD COLUMN api_task_callback VARCHAR(255)")
            )

        # add name column
        if not any(col["name"] == "name" for col in task_columns):
            conn.execute(text("ALTER TABLE task ADD COLUMN name VARCHAR(255)"))

        # add bookmarked column
        if not any(col["name"] == "bookmarked" for col in task_columns):
            conn.execute(
                text("ALTER TABLE task ADD COLUMN bookmarked BOOLEAN DEFAULT FALSE")
            )

        params_column = next(
            col for col in task_columns if col["name"] == "params")
        if version > "1" and not isinstance(params_column["type"], Text):
            transaction = conn.begin()
            conn.execute(
                text(
                    """
                    CREATE TABLE task_temp (
                        id VARCHAR(64) NOT NULL,
                        type VARCHAR(20) NOT NULL,
                        params TEXT NOT NULL,
                        script_params BYTEA NOT NULL,
                        priority INTEGER NOT NULL,
                        status VARCHAR(20) NOT NULL,
                        worker_id VARCHAR(64) NOT NULL,
                        ack_tag INTEGER NOT NULL,
                        created_at TIMESTAMPTZ DEFAULT (NOW()) NOT NULL,
                        updated_at TIMESTAMPTZ DEFAULT (NOW()) NOT NULL,
                        started_at TIMESTAMPTZ DEFAULT (NULL),
                        finished_at TIMESTAMPTZ DEFAULT (NULL),
                        result TEXT,
                        generation_time_seconds double precision generated always as (EXTRACT(epoch FROM (finished_at - started_at))) stored,
                        queue_wait_seconds      double precision generated always as (EXTRACT(epoch FROM (started_at - created_at))) stored
                        PRIMARY KEY (id)
                    )"""
                )
            )
            conn.execute(text("INSERT INTO task_temp SELECT * FROM task"))
            conn.execute(text("DROP TABLE task"))
            conn.execute(text("ALTER TABLE task_temp RENAME TO task"))
            transaction.commit()

        conn.close()


__all__ = [
    "init",
    "Base",
    "metadata",
    "AppStateKey",
    "AppState",
    "TaskStatus",
    "Task",
    "task_manager",
    "state_manager",
]
