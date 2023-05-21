from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql import SparkSession
import argparse

def get_stores_type_location(df_brewery):
    return df_brewery.alias("brewery")\
                .groupBy("country","state","city","brewery_type")\
                .agg(f.count("brewery_type").alias("n_stores"))

def export_csv(df, dest):    
    df.write.csv(dest + "/view_stores_type_location", header = True, mode = "overwrite")

def create_view(spark, src, dest):

    df_brewery = spark.read.parquet(src)

    stores_type_location = get_stores_type_location(df_brewery)

    export_csv(stores_type_location, dest)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create Aggregated View"
    )
    parser.add_argument("--src", required=True)
    parser.add_argument("--dest", required=True)

    args = parser.parse_args()
    
    spark = SparkSession\
        .builder\
        .appName("create_view")\
        .getOrCreate()
    
    create_view(spark, args.src, args.dest)
