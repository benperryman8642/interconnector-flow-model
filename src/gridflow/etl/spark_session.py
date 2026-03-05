"""SparkSession builder (local placeholder)"""
from pyspark.sql import SparkSession


def build_spark(app_name: str = "gridflow-forecast") -> SparkSession:
    return SparkSession.builder.appName(app_name).master("local[*]").getOrCreate()
