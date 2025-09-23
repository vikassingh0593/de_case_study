import pandas as pd
from pyspark.sql.types import StructType, StructField, DoubleType, LongType, StringType
from pyspark.sql.functions import col, from_unixtime, to_timestamp
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from typing import Dict, Iterable
from pathlib import Path
from typing import Any, Dict, Union
import yaml
import sys
import yaml
from datetime import datetime
import __init__

spark = SparkSession.builder.appName("de_case_study").getOrCreate()
dbutils = DBUtils(spark)

def read_parameters_yml() -> Dict[str, Any]:
    """
    Reads config/parameters.yml relative to the first sys.path entry (expected to be 'src').
    """
    if not sys.path:
        raise RuntimeError("sys.path is empty")
    src_dir = Path(sys.path[0])
    p = (src_dir/ "src" / "config" / "parameters.yml").resolve()  # join using pathlib [web:191]
    with p.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}  # safe_load returns None on empty docs [web:276]
    if not isinstance(data, dict):
        raise ValueError(f"YAML root must be a mapping, got {type(data).__name__}")
    return data



config = read_parameters_yml()

APIKEY = config.get("apiKey")

class UCSetup:
    def __init__(
        self,
        spark,
        dbutils,
        catalogs: Iterable[str] = ("dev", "qa", "prod"),
        dev_schemas: Iterable[str] = ("bronze", "silver", "gold"),
        infra_schema: str = "infra",
        volume_name: str = "pipeline_artifacts",
    ):
        self.spark = spark
        self.dbutils = dbutils
        self.catalogs = tuple(catalogs)
        self.dev_schemas = tuple(dev_schemas)
        self.infra_schema = infra_schema
        self.volume_name = volume_name

    # ---------- CATALOGS ----------
    def drop_catalogs(self) -> None:
        for cat in self.catalogs:
            self.spark.sql(f"DROP CATALOG IF EXISTS {cat} CASCADE")
            print(f"Dropped catalog (if existed): {cat}")  # [web:222]

    def create_catalogs(self) -> None:
        for cat in self.catalogs:
            self.spark.sql(f"CREATE CATALOG IF NOT EXISTS {cat}")
            print(f"Ensured catalog exists: {cat}")  # [web:222]

    # ---------- DEV SCHEMAS ----------
    def drop_dev_schemas(self) -> None:
        for sch in self.dev_schemas:
            self.spark.sql(f"DROP SCHEMA IF EXISTS dev.{sch} CASCADE")
            print(f"Dropped schema (if existed): dev.{sch}")  # [web:230]

    def create_dev_schemas(self) -> None:
        for sch in self.dev_schemas:
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS dev.{sch}")
            print(f"Ensured schema exists: dev.{sch}")  # [web:230]

    # ---------- INFRA + VOLUME ----------
    def ensure_infra_and_volume(self) -> str:
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS dev.{self.infra_schema}")
        self.spark.sql(
            f"CREATE VOLUME IF NOT EXISTS dev.{self.infra_schema}.{self.volume_name}"
        )
        base = f"dbfs:/Volumes/dev/{self.infra_schema}/{self.volume_name}"
        print(f"Ensured volume: dev.{self.infra_schema}.{self.volume_name} at {base}")  # [web:223]
        return base

    # ---------- PATHS UNDER VOLUME ----------
    def get_paths(self, base: str | None = None) -> Dict[str, str]:
        if base is None:
            base = f"dbfs:/Volumes/dev/{self.infra_schema}/{self.volume_name}"
        return {
            "base": base,
            "checkpoint_path": f"{base}/checkpoint",
            "logs_path": f"{base}/logs",
            "landing_zone_path": f"{base}/landing_zone",
            "watermark_path": f"{base}/watermark",
        }

    def clean_paths(self, paths: Dict[str, str]) -> None:
        for name, path in paths.items():
            if name == "base":
                continue
            try:
                self.dbutils.fs.rm(path, True)
                print(f"Deleted: {path}")
            except Exception:
                print(f"Skipped delete (not found): {path}")  # [web:220][web:224]

    def create_paths(self, paths: Dict[str, str]) -> None:
        for name, path in paths.items():
            if name == "base":
                continue
            self.dbutils.fs.mkdirs(path)
            print(f"Created: {path}")  # [web:220][web:221]

    # ---------- CONVENIENCE WORKFLOWS ----------
    def reset_env(self) -> Dict[str, str]:
        """
        Full reset: drop catalogs, recreate catalogs, create dev schemas,
        ensure volume, clean and recreate folders.
        """
        self.drop_catalogs()
        self.create_catalogs()
        self.drop_dev_schemas()
        self.create_dev_schemas()
        base = self.ensure_infra_and_volume()
        paths = self.get_paths(base)
        self.clean_paths(paths)
        self.create_paths(paths)
        print("Environment reset.")
        print(f"Paths created {paths}")


# helper = UCSetup(spark, dbutils)
# helper.reset_env()
