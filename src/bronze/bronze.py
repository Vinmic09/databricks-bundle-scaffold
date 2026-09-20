from pyspark.sql import functions as F

class Bronze:
    def __init__(
        self,
        spark,
        source_path: str,
        target_table: str,
        checkpoint_path: str,
        schema_location: str,
        file_format: str,
    ):
        self.spark = spark
        self.source_path = source_path
        self.target_table = target_table
        self.checkpoint_path = checkpoint_path
        self.schema_location = schema_location
        self.file_format = file_format

    def run(self) -> None:
        self._validate_parameters()
        df = self._read_source()
        df_bronze = self._add_bronze_metadata(df)
        self._write_bronze(df_bronze)
    def _validate_parameters(self) -> None:
        required_parameters = {
            "SOURCE_PATH": self.source_path,
            "TARGET_TABLE": self.target_table,
            "CHECKPOINT_PATH": self.checkpoint_path,
            "SCHEMA_LOCATION": self.schema_location,
            "FILE_FORMAT": self.file_format,
        }
        missing_parameters = [
            name
            for name, value in required_parameters.items()
            if not value
        ]
        if missing_parameters:
            raise ValueError(
                f"Missing required parameters: {', '.join(missing_parameters)}"
            )

    def _read_source(self):
        return (
            self.spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", self.file_format)
            .option("cloudFiles.schemaLocation",self.schema_location,)
            .load(self.source_path)
        )

    def _add_bronze_metadata(self, df):
        return (
            df
            .withColumn("_ingestion_timestamp",F.current_timestamp(),)
            .withColumn("_source_file",F.col("_metadata.file_path"),)
            .withColumn("_source_file_name",F.col("_metadata.file_name"),)
            .withColumn("_source_file_size",F.col("_metadata.file_size"),)
            .withColumn("_source_file_modification_time",F.col("_metadata.file_modification_time"),)
            .withColumn("_ingested_date",F.to_timestamp(F.split(F.split(F.col("_source_file_name"),"_",).getItem(-1),"\\.",).getItem(0),"yyyyMMddHHmmssSSS",),)
        )
    def _write_bronze(self, df):
        (
            df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", self.checkpoint_path)
            .toTable(self.target_table)
        )