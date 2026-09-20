from bronze import Bronze

bronze = Bronze(
    spark=spark,
    source_path=dbutils.widgets.get("SOURCE_PATH"),
    target_table=dbutils.widgets.get("TARGET_TABLE"),
    checkpoint_path=dbutils.widgets.get("CHECKPOINT_PATH"),
    schema_location=dbutils.widgets.get("SCHEMA_LOCATION"),
    file_format=dbutils.widgets.get("FILE_FORMAT"),
)
bronze.run()