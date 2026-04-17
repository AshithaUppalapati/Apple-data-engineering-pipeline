from pyspark.sql import DataFrame

class LoaderFactory:

    def write_delta(self, df: DataFrame, path: str, mode: str = "overwrite"):
        (
            df.write
              .format("delta")
              .mode(mode)
              .save(path)
        )
