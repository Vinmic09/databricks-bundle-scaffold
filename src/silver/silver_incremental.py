from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StructType


class SilverIncremental:

    def __init__(
        self,
        spark: SparkSession,
        source_table: str,
        target_table: str,
        primary_key: str,
        last_updated: str,
        scd_type: str,
    ):
        self.spark = spark
        self.source_table = source_table
        self.target_table = target_table
        self.primary_key = primary_key
        self.last_updated = last_updated
        self.scd_type = scd_type.upper()

    def run(self) -> None:

        source_df = self.spark.table(self.source_table)

        # Flatten nested STRUCT columns
        source_df = self._flatten_structs(source_df)

        # Explode ARRAY columns
        source_df = self._explode_arrays(source_df)

        target = DeltaTable.forName(
            self.spark,
            self.target_table,
        )

        (
            target.alias("target")
            .merge(
                source_df.alias("source"),
                f"target.`{self.primary_key}` = "
                f"source.`{self.primary_key}` "
                f"AND target._is_current = true",
            )
            .whenMatchedUpdate(
                condition=(
                    f"source.`{self.last_updated}` > "
                    f"target.`{self.last_updated}`"
                ),
                set={
                    "_effective_to": f"source.`{self.last_updated}`",
                    "_is_current": "false",
                },
            )
            .execute()
        )

        new_records = (
            source_df
            .withColumn(
                "_effective_from",
                F.col(self.last_updated),
            )
            .withColumn(
                "_effective_to",
                F.lit(None).cast("timestamp"),
            )
            .withColumn(
                "_is_current",
                F.lit(True),
            )
        )

        if self.scd_type == "SCD1":
            new_records = new_records.filter(
                F.col("_is_current") == True
            )

        (
            new_records.write
            .format("delta")
            .mode("append")
            .saveAsTable(self.target_table)
        )

    def _flatten_structs(self, df):

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

    def _explode_arrays(self, df):

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
                F.explode_outer(
                    F.col(f"`{column}`")
                ),
            )

        return df