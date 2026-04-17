from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

class ReaderFactory:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read_csv(self, path: str, header: bool = True, infer_schema: bool = True) -> DataFrame:
        return (
            self.spark.read
                .option("header", header)
                .option("inferSchema", infer_schema)
                .csv(path)
        )

    def read_delta(self, path: str) -> DataFrame:
        return self.spark.read.format("delta").load(path)

    def read_volume(self, path: str) -> DataFrame:
        return self.spark.read.format("delta").load(path)
