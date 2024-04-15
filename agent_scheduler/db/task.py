import json
import base64
from enum import Enum
from datetime import datetime, timezone
from typing import Optional, Union, List, Dict

from sqlalchemy import (
    TypeDecorator,
    Computed,
    Column,
    String,
    Text,
    BigInteger,
    Float,
    DateTime as DateTimeImpl,
    LargeBinary,
    Boolean,
    text,
    func,
)
from sqlalchemy.orm import Session

from .base import BaseTableManager, Base, env_worker_id
from ..models import TaskModel

class DateTime(TypeDecorator):
    impl = DateTimeImpl
    cache_ok = True

    def process_bind_param(self, value: Optional[datetime], _):
        if value is None:
            return None
        return value.astimezone(timezone.utc)

    def process_result_value(self, value: Optional[datetime], _):
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)


class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"
    INTERRUPTED = "interrupted"


class Task(TaskModel):
    script_params: bytes = None
    params: str

    def __init__(self, **kwargs):
        priority = kwargs.pop("priority", int(datetime.now(timezone.utc).timestamp() * 1000))
        worker_id = kwargs.pop("worker_id", env_worker_id)
        super().__init__(priority=priority, worker_id=worker_id, **kwargs)

    class Config(TaskModel.__config__):
        exclude = ["script_params"]

    @staticmethod
    def from_table(table: "TaskTable"):
        return Task(
            id=table.id,
            api_task_id=table.api_task_id,
            api_task_callback=table.api_task_callback,
            name=table.name,
            type=table.type,
            params=table.params,
            script_params=table.script_params,
            priority=table.priority,
            # TODO: check if worker_id should be migrated
            worker_id=table.worker_id,
            ack_tag=table.ack_tag,
            status=table.status,
            result=table.result,
            bookmarked=table.bookmarked,
            created_at=table.created_at,
            updated_at=table.updated_at,
            started_at=table.started_at,
            finished_at=table.finished_at,
        )

    def to_table(self):
        return TaskTable(
            id=self.id,
            api_task_id=self.api_task_id,
            api_task_callback=self.api_task_callback,
            name=self.name,
            type=self.type,
            params=self.params,
            script_params=self.script_params,
            priority=self.priority,
            worker_id=self.worker_id,
            status=self.status,
            result=self.result,
            ack_tag=self.ack_tag,
            bookmarked=self.bookmarked,
            started_at=self.started_at,
            finished_at=self.finished_at,
        )

    def from_json(json_obj: Dict):
        return Task(
            id=json_obj.get("id"),
            api_task_id=json_obj.get("api_task_id", None),
            api_task_callback=json_obj.get("api_task_callback", None),
            name=json_obj.get("name", None),
            type=json_obj.get("type"),
            status=json_obj.get("status", TaskStatus.PENDING),
            params=json.dumps(json_obj.get("params")),
            script_params=base64.b64decode(json_obj.get("script_params")),
            priority=json_obj.get("priority", int(datetime.now(timezone.utc).timestamp() * 1000)),
            ack_tag=json_obj.get("ack_tag", None),
            worker_id=json_obj.get("worker_id", None),
            result=json_obj.get("result", None),
            bookmarked=json_obj.get("bookmarked", False),
            created_at=datetime.fromtimestamp(json_obj.get("created_at", datetime.now(timezone.utc).timestamp())),
            updated_at=datetime.fromtimestamp(json_obj.get("updated_at", datetime.now(timezone.utc).timestamp())),
            started_at=datetime.fromtimestamp(json_obj.get("started_at", None)),
            finished_at=datetime.fromtimestamp(json_obj.get("finished_at", None)),
            generated_time_seconds=json_obj.get("generated_time_seconds", None),
            queue_wait_seconds=json_obj.get("queue_wait_seconds", None)
        )

    def to_json(self):
        return {
            "id": self.id,
            "api_task_id": self.api_task_id,
            "api_task_callback": self.api_task_callback,
            "name": self.name,
            "type": self.type,
            "status": self.status,
            "params": json.loads(self.params),
            "script_params": base64.b64encode(self.script_params).decode("utf-8"),
            "priority": self.priority,
            "worker_id": self.worker_id,
            "result": self.result,
            "ack_tag": self.ack_tag,
            "bookmarked": self.bookmarked,
            "created_at": int(self.created_at.timestamp()),
            "updated_at": int(self.updated_at.timestamp()),
            "started_at": int(self.started_at.timestamp()) if self.started_at else None,
            "finished_at": int(self.finished_at.timestamp()) if self.finished_at else None,
            "generated_time_seconds": self.generation_time_seconds,
            "queue_wait_seconds": self.queue_wait_seconds,
        }


class TaskTable(Base):
    __tablename__ = "task"

    id = Column(String(64), primary_key=True)
    api_task_id = Column(String(64), nullable=True)
    api_task_callback = Column(String(255), nullable=True)
    name = Column(String(255), nullable=True)
    type = Column(String(20), nullable=False)  # txt2img or img2txt
    params = Column(Text, nullable=False)  # task args
    script_params = Column(LargeBinary, nullable=False)  # script args
    priority = Column(BigInteger, nullable=False)
    worker_id = Column(String(64), nullable=False)
    status = Column(String(20), nullable=False, default="pending")  # pending, running, done, failed
    result = Column(Text)  # task result
    bookmarked = Column(Boolean, nullable=True, default=False)
    ack_tag = Column(BigInteger, nullable=True)
    created_at = Column(
        DateTime,
        nullable=False,
        server_default=text("NOW()"),
    )
    updated_at = Column(
        DateTime,
        nullable=False,
        server_default=text("NOW()"),
        onupdate=text("NOW()"),
    )
    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)
    generation_time_seconds = Column(Float, Computed("EXTRACT(EPOCH FROM (finished_at - started_at))"))
    queue_wait_seconds = Column(Float, Computed("EXTRACT(EPOCH FROM (started_at - created_at))"))

    def __repr__(self):
        return f"Task(id={self.id!r}, type={self.type!r}, params={self.params!r}, status={self.status!r}, created_at={self.created_at!r})"


class TaskManager(BaseTableManager):
    def get_task(self, id: str) -> Union[TaskTable, None]:
        session = Session(self.engine)
        try:
            task = session.get(TaskTable, id)

            return Task.from_table(task) if task else None
        except Exception as e:
            print(f"Exception getting task from database: {e}")
            raise e
        finally:
            session.close()

    def get_task_position(self, id: str) -> int:
        session = Session(self.engine)
        try:
            task = session.get(TaskTable, id)
            if task:
                return (
                    session.query(func.count(TaskTable.id))
                    .filter(TaskTable.status == TaskStatus.PENDING)
                    .filter(TaskTable.priority < task.priority)
                    .scalar()
                )
            else:
                raise Exception(f"Task with id {id} not found")
        except Exception as e:
            print(f"Exception getting task position from database: {e}")
            raise e
        finally:
            session.close()

    def get_tasks(
        self,
        type: str = None,
        status: Union[str, List[str]] = None,
        bookmarked: bool = None,
        api_task_id: str = None,
        limit: int = None,
        offset: int = None,
        order: str = "asc",
    ) -> List[TaskTable]:
        session = Session(self.engine)
        try:
            query = session.query(TaskTable)
            # TODO: change this logic before launching this extension externally
            query.filter(TaskTable.worker_id == env_worker_id)
            if type:
                query = query.filter(TaskTable.type == type)

            if status is not None:
                if isinstance(status, list):
                    query = query.filter(TaskTable.status.in_(status))
                else:
                    query = query.filter(TaskTable.status == status)

            if api_task_id:
                query = query.filter(TaskTable.api_task_id == api_task_id)

            if bookmarked == True:
                query = query.filter(TaskTable.bookmarked == bookmarked)
            else:
                query = query.order_by(TaskTable.bookmarked.asc())

            query = query.order_by(TaskTable.priority.asc() if order == "asc" else TaskTable.priority.desc())

            if limit:
                query = query.limit(limit)

            if offset:
                query = query.offset(offset)

            all = query.all()
            return [Task.from_table(t) for t in all]
        except Exception as e:
            print(f"Exception getting tasks from database: {e}")
            raise e
        finally:
            session.close()

    def count_tasks(
        self,
        type: str = None,
        status: Union[str, List[str]] = None,
        api_task_id: str = None,
    ) -> int:
        session = Session(self.engine)
        try:
            query = session.query(TaskTable)
            if type:
                query = query.filter(TaskTable.type == type)

            if status is not None:
                if isinstance(status, list):
                    query = query.filter(TaskTable.status.in_(status))
                else:
                    query = query.filter(TaskTable.status == status)

            if api_task_id:
                query = query.filter(TaskTable.api_task_id == api_task_id)

            return query.count()
        except Exception as e:
            print(f"Exception counting tasks from database: {e}")
            raise e
        finally:
            session.close()

    def add_task(self, task: Task) -> TaskTable:
        session = Session(self.engine)
        try:
            item = task.to_table()
            session.add(item)
            session.commit()
            session.close()
            return True
        except Exception as e:
            session.rollback()
            try:
                task = session.query(TaskTable).filter(TaskTable.id == task.id).first()
            except Exception as e:
                return False
            # running / pending idempotent
            if task.status != TaskStatus.DONE and task.status != TaskStatus.FAILED:
                session.merge(item)
                session.commit()
                return True
            session.close()
            return False

    def update_task(self, task: Task) -> TaskTable:
        session = Session(self.engine)
        try:
            current = session.get(TaskTable, task.id)
            if current is None:
                raise Exception(f"Task with id {id} not found")

            session.merge(task.to_table())
            session.commit()
            return task

        except Exception as e:
            print(f"Exception updating task in database: {e}")
            raise e
        finally:
            session.close()

    def prioritize_task(self, id: str, priority: int) -> TaskTable:
        """0 means move to top, -1 means move to bottom, otherwise set the exact priority"""

        session = Session(self.engine)
        try:
            result = session.get(TaskTable, id)
            if result:
                if priority == 0:
                    result.priority = self.__get_min_priority(status=TaskStatus.PENDING) - 1
                elif priority == -1:
                    result.priority = int(datetime.now(timezone.utc).timestamp() * 1000)
                else:
                    self.__move_tasks_down(priority)
                    session.execute(text("SELECT 1"))
                    result.priority = priority

                session.commit()
                return result
            else:
                raise Exception(f"Task with id {id} not found")
        except Exception as e:
            print(f"Exception updating task in database: {e}")
            raise e
        finally:
            session.close()

    def delete_task(self, id: str):
        session = Session(self.engine)
        try:
            result = session.get(TaskTable, id)
            if result:
                session.delete(result)
                session.commit()
            else:
                raise Exception(f"Task with id {id} not found")
        except Exception as e:
            print(f"Exception deleting task from database: {e}")
            raise e
        finally:
            session.close()

    def delete_tasks(
        self,
        before: datetime = None,
        status: Union[str, List[str]] = [
            TaskStatus.DONE,
            TaskStatus.FAILED,
            TaskStatus.INTERRUPTED,
        ],
    ):
        session = Session(self.engine)
        try:
            query = session.query(TaskTable).filter(TaskTable.bookmarked == False)

            if before:
                query = query.filter(TaskTable.created_at < before)

            if status is not None:
                if isinstance(status, list):
                    query = query.filter(TaskTable.status.in_(status))
                else:
                    query = query.filter(TaskTable.status == status)

            deleted_rows = query.delete()
            session.commit()

            return deleted_rows
        except Exception as e:
            print(f"Exception deleting tasks from database: {e}")
            raise e
        finally:
            session.close()

    def __get_min_priority(self, status: str = None) -> int:
        session = Session(self.engine)
        try:
            query = session.query(func.min(TaskTable.priority))
            if status is not None:
                query = query.filter(TaskTable.status == status)

            min_priority = query.scalar()
            return min_priority if min_priority else 0
        except Exception as e:
            print(f"Exception getting min priority from database: {e}")
            raise e
        finally:
            session.close()

    def __move_tasks_down(self, priority: int):
        session = Session(self.engine)
        try:
            session.query(TaskTable).filter(TaskTable.priority >= priority).update(
                {TaskTable.priority: TaskTable.priority + 1}
            )
            session.commit()
        except Exception as e:
            print(f"Exception moving tasks down in database: {e}")
            raise e
        finally:
            session.close()
