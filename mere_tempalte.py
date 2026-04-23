###########################################
## Bronze merge template
###########################################

# Import Libs
from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp
from pyspark.sql import Row

# Initialise Variables
source_table = "dbo.STAGE"
target_table = "bronze.BRONZE"

key_column = "UID"
date_col = "DATECOL"
snapshot_offset_days = -90

# Load Tables
staged = spark.table(source_table)                 # Read-only DataFrame
target = DeltaTable.forName(spark, target_table)   # Delta table reference

# Column handling
staged_cols = staged.columns
target_cols = spark.table(target_table).columns

# Only columns common to both staging and bronze
common_cols = [c for c in staged_cols if c in target_cols]

# Bronze-only system columns (not in staging)
BRONZE_IS_DELETED = "IsDeleted"
BRONZE_DELETED_DATE = "DeletedDate"

# ============================================
# Soft Delete Window
# ============================================
snapshot_expr = f"date_add(current_date(), {snapshot_offset_days})"

# ============================================
# Merge
# ============================================
(
    target.alias("t")
    .merge(
        staged.alias("s"),
        f"t.{key_column} = s.{key_column}"
    )

    # --------------------------------------------
    # UPDATE: only update when LockVersion changes
    # (does NOT touch bronze-only columns)
    # --------------------------------------------
    .whenMatchedUpdate(
        condition="s.LockVersion IS DISTINCT FROM t.LockVersion",
        set={c: f"s.{c}" for c in common_cols}
    )

    # --------------------------------------------
    # INSERT: explicitly set bronze system defaults
    # --------------------------------------------
    .whenNotMatchedInsert(
        values={
            **{c: f"s.{c}" for c in common_cols},
            BRONZE_IS_DELETED: "false",
            BRONZE_DELETED_DATE: "NULL"
        }
    )

    # --------------------------------------------
    # SOFT DELETE: missing from staging
    # --------------------------------------------
    .whenNotMatchedBySourceUpdate(
        condition=f"""
            t.{BRONZE_IS_DELETED} = false
            AND t.{date_col} >= {snapshot_expr}
        """,
        set={
            BRONZE_IS_DELETED: "true",
            BRONZE_DELETED_DATE: "current_timestamp()"
        }
    )

    .execute()
)

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
rows_written  = rows_inserted + rows_updated + rows_deleted
stage_rows    = staged.count()

# ============================================
# Write merge metrics to logs
# ============================================
metrics_df = spark.createDataFrame([
    Row(
        target_table  = target_table,
        stage_rows    = stage_rows,
        rows_inserted = rows_inserted,
        rows_updated  = rows_updated,
        rows_deleted  = rows_deleted,
        rows_written  = rows_written
    )
]).withColumn("load_timestamp", current_timestamp())

metrics_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("dbo.merge_metrics")

# Maintenane
spark.sql(f"OPTIMIZE {target_table}")
spark.sql(f"VACUUM {target_table} RETAIN 168 HOURS")

print("Merge completed. Optimize + Vacuum done.")
