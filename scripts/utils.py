
def read_parquet(spark, path):
    return spark.read.format("parquet").load(path)

def read_csv(spark, path):
    return spark.read.format("csv").option("header", True).load(path)

def write_parquet(df, path, mode="overwrite"):
    df.write.format("parquet").mode(mode).save(path)

def write_delta(df, path, mode="overwrite"):
    df.write.format("delta").mode(mode).save(path)