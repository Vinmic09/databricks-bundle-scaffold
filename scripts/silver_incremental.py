from silver_incremental import SilverIncremental

silver = SilverIncremental(
    spark=spark,
    source_table=dbutils.widgets.get("SOURCE_TABLE"),
    target_table=dbutils.widgets.get("TARGET_TABLE"),
    primary_key=dbutils.widgets.get("PRIMARY_KEY"),
    last_updated=dbutils.widgets.get("LAST_UPDATED"), #column name in source table that indicates when the record was last updated
    scd_type=dbutils.widgets.get("SCD_TYPE"),
)
silver.run()