import dlt
from pyspark.sql.functions import col

RAW_PATH = "abfss://raw@stfelixsynapse01.dfs.core.windows.net"

@dlt.table(
    name="clientes",
    comment="Bronze - clientes crudos desde Synapse Copy Activity (parquet)"
)
def clientes():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", f"{RAW_PATH}/_schemas/clientes")
        .load(f"{RAW_PATH}/clientes_bronze/")
    )

@dlt.table(
    name="pedidos",
    comment="Bronze - pedidos crudos desde Synapse Copy Activity (parquet)"
)
def pedidos():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", f"{RAW_PATH}/_schemas/pedidos")
        .load(f"{RAW_PATH}/pedidos_bronze/")
    )
