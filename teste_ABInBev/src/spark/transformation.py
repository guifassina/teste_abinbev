from pyspark.sql import SparkSession
import argparse


particao = "country","state","city"

def json_to_parquet(spark, arq_json, arq_parquet, particao):
    df = spark.read.json(arq_json)
    df.write.partitionBy(particao).format("parquet").save(arq_parquet)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Spark Brewery Transformation"
    )
    parser.add_argument("--arq_json", required=True)
    parser.add_argument("--arq_parquet", required=True)

    args = parser.parse_args()
    
    spark = SparkSession\
        .builder\
        .appName("transforma_dados")\
        .getOrCreate()
    
    json_to_parquet(spark, args.arq_json, args.arq_parquet, particao)