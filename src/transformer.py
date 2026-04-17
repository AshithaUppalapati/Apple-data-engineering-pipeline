from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

class Transformer:

    def normalize_product_names(self, df: DataFrame) -> DataFrame:
        df_clean = (
            df.withColumn("product_name", F.trim(F.lower("product_name")))
              .withColumn(
                  "product_name",
                  F.when(F.col("product_name").like("%iphone%"), "iphone")
                   .when(F.col("product_name").like("%airpods%"), "airpods")
                   .when(F.col("product_name").like("%macbook%"), "macbook")
                   .when(F.col("product_name").like("%ipad%"), "ipad")
                   .otherwise(F.col("product_name"))
              )
        )
        return df_clean

    def join_silver(self, txn: DataFrame, cust: DataFrame, prod: DataFrame) -> DataFrame:
        return (
            txn.join(cust, "customer_id", "inner")
               .join(prod, "product_name", "left")
        )

    def add_purchase_sequence(self, df: DataFrame) -> DataFrame:
        w = Window.partitionBy("customer_id").orderBy("transaction_date")
        return df.withColumn("prev_product", F.lag("product_name").over(w))

    def filter_airpods_after_iphone(self, df: DataFrame) -> DataFrame:
        return df.filter(
            (F.col("product_name") == "airpods") &
            (F.col("prev_product") == "iphone")
        )
