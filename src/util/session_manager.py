from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pathlib import Path
import os


class SessionManager:
    """
    Centralized Spark session builder with Delta support.
    """

    _spark = None

    @classmethod
    def get_spark(cls, app_name: str = "spark-app") -> SparkSession:
        """
        Returns a configured SparkSession.
        Creates it if it does not already exist.
        """

        if cls._spark is not None:
            return cls._spark

        os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

        builder = (
            SparkSession.builder
            .appName(app_name)
            .master("local[*]")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )

        cls._spark = configure_spark_with_delta_pip(builder).getOrCreate()

        return cls._spark

    @classmethod
    def stop_spark(cls):
        """
        Stops the SparkSession if running.
        """
        if cls._spark:
            cls._spark.stop()
            cls._spark = None