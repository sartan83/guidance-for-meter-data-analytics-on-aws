from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

TRUE_VALUES = {"TRUE", "1", "YES", "Y"}

DEFAULT_PARAMS: dict[str, str] = {
    "catalog": "mda_dev",
    "volumes_root": "/Volumes",
    "use_autoloader": "TRUE",
    "run_anomaly": "TRUE",
    "run_training": "TRUE",
    "run_inference": "TRUE",
    "use_weather": "FALSE",
    "forecast_period": "7",
    "training_time_frame": "395",
    "model_name": "mda_forecast",
    "model_alias": "champion",
    "experiment_path": "/Shared/mda/mda_forecast",
    "registry_uri": "databricks-uc",
    "holidays_country": "US",
    "reference_time": "",
    "run_id": "",
}


def as_bool(value: str | bool | None) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().upper() in TRUE_VALUES


@dataclass(frozen=True)
class Layout:
    """Unity Catalog names for the medallion layers and their volumes."""

    catalog: str
    volumes_root: str = "/Volumes"
    bronze_schema: str = "bronze"
    silver_schema: str = "silver"
    gold_schema: str = "gold"

    def bronze(self, table: str) -> str:
        return f"{self.catalog}.{self.bronze_schema}.{table}"

    def silver(self, table: str) -> str:
        return f"{self.catalog}.{self.silver_schema}.{table}"

    def gold(self, table: str) -> str:
        return f"{self.catalog}.{self.gold_schema}.{table}"

    @property
    def landing(self) -> str:
        return f"{self.volumes_root}/{self.catalog}/{self.bronze_schema}/landing"

    def checkpoint(self, name: str) -> str:
        return f"{self.volumes_root}/{self.catalog}/{self.bronze_schema}/checkpoints/{name}"

    @property
    def model_name(self) -> str:
        return self.gold("mda_forecast")


@dataclass(frozen=True)
class TaskParams:
    values: Mapping[str, str] = field(default_factory=dict)

    def get(self, name: str) -> str:
        value = self.values.get(name)
        if value is None or value == "":
            return DEFAULT_PARAMS.get(name, "")
        return str(value)

    def flag(self, name: str) -> bool:
        return as_bool(self.get(name))

    def int(self, name: str) -> int:
        return int(self.get(name))

    @property
    def layout(self) -> Layout:
        return Layout(catalog=self.get("catalog"), volumes_root=self.get("volumes_root"))

    @property
    def registered_model(self) -> str:
        return self.layout.gold(self.get("model_name"))


def params_from_widgets(dbutils, names=None) -> TaskParams:
    """Read job parameters (pushed down to notebook widgets by Databricks Workflows)."""
    values: dict[str, str] = {}
    for name in names or DEFAULT_PARAMS:
        try:
            values[name] = dbutils.widgets.get(name)
        except Exception:  # widget not defined for this task: fall back to the default
            continue
    return TaskParams(values)


def set_task_value(key: str, value) -> None:
    """dbutils.jobs.taskValues.set when running inside a Databricks job; no-op locally."""
    try:
        from databricks.sdk.runtime import dbutils  # type: ignore

        dbutils.jobs.taskValues.set(key=key, value=value)
    except Exception:
        pass
