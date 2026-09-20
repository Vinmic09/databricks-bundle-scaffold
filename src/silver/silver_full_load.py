from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StructType


class SilverFullLoad:

    INGESTED_DATE_COLUMN = "_ingested_date"

    def __init__(
        self,
        spark: SparkSession,
        source_table: str,
        target_table: str,
    ):
        self.spark = spark
        self.source_table = source_table
        self.target_table = target_table

    def run(self) -> None:

        source_df = self.spark.table(self.source_table)

        latest_file_df = self._get_latest_file(source_df)

        flattened_df = self._flatten_structs(latest_file_df)

        flattened_df = self._explode_arrays(flattened_df)

        (
            flattened_df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(self.target_table)
        )

    def _get_latest_file(
        self,
        df: DataFrame,
    ) -> DataFrame:

        latest_date = (
            df
            .select(
                F.max(self.INGESTED_DATE_COLUMN).alias(
                    "_latest_ingested_date"
                )
            )
            .first()["_latest_ingested_date"]
        )

        return df.filter(
            F.col(self.INGESTED_DATE_COLUMN) == latest_date
        )

    def _flatten_structs(
        self,
        df: DataFrame,
    ) -> DataFrame:

        while True:

            struct_columns = [
                field.name
                for field in df.schema.fields
                if isinstance(field.dataType, StructType)
            ]

            if not struct_columns:
                break

            column = struct_columns[0]

            nested_fields = [
                F.col(
                    f"`{column}`.`{field.name}`"
                ).alias(field.name)
                for field in df.schema[column].dataType.fields
            ]

            df = (
                df
                .drop(column)
                .select("*", *nested_fields)
            )

        return df

    def _explode_arrays(
        self,
        df: DataFrame,
    ) -> DataFrame:

        while True:

            array_columns = [
                field.name
                for field in df.schema.fields
                if isinstance(field.dataType, ArrayType)
            ]

            if not array_columns:
                break

            column = array_columns[0]

            df = df.withColumn(
                column,
                F.explode_outer(F.col(f"`{column}`")),
            )

        return df