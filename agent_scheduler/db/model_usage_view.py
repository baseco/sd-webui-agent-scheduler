from sqlalchemy import (
    text,
)
from sqlalchemy.orm import Session

from .base import BaseTableManager


class ModelUsageView(BaseTableManager):
    def model_usage(self, last_x: str):
        if last_x not in ["7_day", "30_day", "5_min"]:
            raise ValueError("Invalid last_x value")
        session = Session(self.engine)
        return session.execute(
            text(
                "select model, weight::integer from metrics.model_usage_{}".format(
                    last_x
                )
            )
        ).fetchall()
