###########################################
## Bronze merge template
###########################################

# Import Libs
from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp
from pyspark.sql import Row

# Initialise Variables
source_table = "" # staging table
target_table = "bronze." # bronze schema table
key_column   = "" # UID col
date_col   = "" # Date col to increment on
snapshot_offset_days = -1 # Look back days

# Load Tables
staged = spark.table(source_table)  # Read only data frame
target = DeltaTable.forName(spark, target_table) # Full access

# Soft Delete Window
snapshot_expr = f"date_add(current_date(), {snapshot_offset_days})" # Builds a SQL expression

# Merge
(
    target.alias("t") # Bronze layer
    .merge(
        staged.alias("s"), # Staging table
        f"t.{key_column} = s.{key_column}" # Matching condition
    )
    .whenMatchedUpdate(
        condition="s.LockVersion IS DISTINCT FROM t.LockVersion", # Only update when staging lock version is different to bronze lock version
        set={c: f"s.{c}" for c in staged.columns}) # When UID = UID all the cols are updated - if previously deleted then deleted status is removed
    .whenNotMatchedInsert(values={c: f"s.{c}" for c in staged.columns}) # New records from staging
    .whenNotMatchedBySourceUpdate( # Records that exsist in the bronze layer but do not appear in staging (ie they have been deleted in source)
        condition=f"""
            t.IsDeleted = false
            AND t.{date_col} >= {snapshot_expr}
        """,
        set={
            "IsDeleted": "true",
            "DeletedDate": "current_timestamp()"
        }
    )
    .execute() # Trigger the merge
)

print("Merge complete")

# Merge metrics
metrics = (
    DeltaTable.forName(spark, target_table)
    .history(1)
    .select("operationMetrics")
    .collect()[0]["operationMetrics"]
)


rows_inserted = int(metrics.get("numTargetRowsInserted", 0))
rows_updated  = int(metrics.get("numTargetRowsUpdated", 0))
rows_deleted  = int(metrics.get("numTargetRowsDeleted", 0))
rows_written = rows_inserted + rows_updated + rows_deleted

print("Merge metrics recorded")

## Write merge metrics to logs
metrics_df = spark.createDataFrame([
    Row(
        target_table  = target_table,
        rows_inserted = rows_inserted,
        rows_updated  = rows_updated,
        rows_deleted  = rows_deleted,
        rows_written  = rows_written
    )
]).withColumn("load_timestamp", current_timestamp())


metrics_df.write.format("delta") \
    .mode("append") \
    .saveAsTable("dbo.merge_metrics")

# Maintenane
spark.sql(f"OPTIMIZE {target_table}")
spark.sql(f"VACUUM {target_table} RETAIN 168 HOURS")

print("Optimize and vacuum done")
print("Merge complete")
