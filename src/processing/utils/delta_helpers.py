from pyspark.sql import SparkSession, DataFrame
from delta.tables import DeltaTable


def write_delta(df: DataFrame, path: str, mode: str = "append", partition_by: list[str] | None = None) -> None:
    writer = df.write.format("delta").mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)


def upsert_delta(spark: SparkSession, df: DataFrame, path: str, merge_condition: str, update_set: dict, insert_set: dict) -> None:
    if DeltaTable.isDeltaTable(spark, path):
        dt = DeltaTable.forPath(spark, path)
        (
            dt.alias("target")
            .merge(df.alias("source"), merge_condition)
            .whenMatchedUpdate(set=update_set)
            .whenNotMatchedInsert(values=insert_set)
            .execute()
        )
    else:
        write_delta(df, path, mode="overwrite")


def optimize_delta(spark: SparkSession, path: str, z_order_cols: list[str] | None = None) -> None:
    dt = DeltaTable.forPath(spark, path)
    if z_order_cols:
        dt.optimize().executeZOrderBy(*z_order_cols)
    else:
        dt.optimize().executeCompaction()


def vacuum_delta(spark: SparkSession, path: str, retention_hours: int = 168) -> None:
    dt = DeltaTable.forPath(spark, path)
    dt.vacuum(retention_hours)
