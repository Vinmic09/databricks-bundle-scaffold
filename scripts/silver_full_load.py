from silver_full_load import SilverFullLoad

silver = SilverFullLoad(
    spark=spark,
    source_table=dbutils.widgets.get("SOURCE_TABLE"),
    target_table=dbutils.widgets.get("TARGET_TABLE"),
)

silver.run()