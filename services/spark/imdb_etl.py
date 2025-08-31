import requests
import gzip
import io
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, split, regexp_replace, trim
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType

def download_imdb_data(url, local_path):
    """Download and decompress IMDB data"""
    print(f"Downloading {url}...")
    response = requests.get(url)
    response.raise_for_status()
    
    # Decompress gzipped content
    with gzip.open(io.BytesIO(response.content), 'rt', encoding='utf-8') as f:
        content = f.read()
    
    # Write to local file
    with open(local_path, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print(f"Downloaded and decompressed to {local_path}")
    return local_path

def create_iceberg_tables(spark):
    """Create Iceberg tables for IMDB data"""
    
    # Configure Iceberg catalog
    spark.conf.set("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set("spark.sql.catalog.iceberg.type", "hive")
    spark.conf.set("spark.sql.catalog.iceberg.uri", "thrift://hive-metastore:9083")
    spark.conf.set("spark.sql.catalog.iceberg.warehouse", "/opt/data/warehouse")
    
    # Enable Iceberg features
    spark.conf.set("spark.sql.catalog.iceberg.cache-enabled", "false")
    
    print("Iceberg catalog configured")

def load_name_basics(spark, data_path):
    """Load name.basics.tsv into Iceberg table"""
    
    # Define schema for name.basics
    schema = StructType([
        StructField("nconst", StringType(), True),
        StructField("primaryName", StringType(), True),
        StructField("birthYear", StringType(), True),
        StructField("deathYear", StringType(), True),
        StructField("primaryProfession", StringType(), True),
        StructField("knownForTitles", StringType(), True)
    ])
    
    # Read TSV data
    df = spark.read.option("sep", "\t").option("header", "true").schema(schema).csv(data_path)
    
    # Clean and transform data
    df_clean = df.select(
        col("nconst"),
        col("primaryName"),
        when(col("birthYear") == "\\N", None).otherwise(col("birthYear").cast(IntegerType())).alias("birthYear"),
        when(col("deathYear") == "\\N", None).otherwise(col("deathYear").cast(IntegerType())).alias("deathYear"),
        col("primaryProfession"),
        col("knownForTitles")
    )
    
    # Write to Iceberg table
    df_clean.writeTo("iceberg.default.name_basics").using("iceberg").createOrReplace()
    
    print(f"Loaded {df_clean.count()} records into name_basics table")
    return df_clean

def load_title_basics(spark, data_path):
    """Load title.basics.tsv into Iceberg table"""
    
    # Define schema for title.basics
    schema = StructType([
        StructField("tconst", StringType(), True),
        StructField("titleType", StringType(), True),
        StructField("primaryTitle", StringType(), True),
        StructField("originalTitle", StringType(), True),
        StructField("isAdult", StringType(), True),
        StructField("startYear", StringType(), True),
        StructField("endYear", StringType(), True),
        StructField("runtimeMinutes", StringType(), True),
        StructField("genres", StringType(), True)
    ])
    
    # Read TSV data
    df = spark.read.option("sep", "\t").option("header", "true").schema(schema).csv(data_path)
    
    # Clean and transform data
    df_clean = df.select(
        col("tconst"),
        col("titleType"),
        col("primaryTitle"),
        col("originalTitle"),
        when(col("isAdult") == "1", True).otherwise(False).alias("isAdult"),
        when(col("startYear") == "\\N", None).otherwise(col("startYear").cast(IntegerType())).alias("startYear"),
        when(col("endYear") == "\\N", None).otherwise(col("endYear").cast(IntegerType())).alias("endYear"),
        when(col("runtimeMinutes") == "\\N", None).otherwise(col("runtimeMinutes").cast(IntegerType())).alias("runtimeMinutes"),
        col("genres")
    )
    
    # Write to Iceberg table
    df_clean.writeTo("iceberg.default.title_basics").using("iceberg").createOrReplace()
    
    print(f"Loaded {df_clean.count()} records into title_basics table")
    return df_clean

def main():
    """Main ETL process"""
    
    # Initialize SparkSession with Iceberg support
    spark = SparkSession.builder \
        .appName("IMDB-ETL") \
        .master("spark://spark:7077") \
        .config("spark.driver.host", "spark") \
        .config("spark.driver.bindAddress", "0.0.0.0") \
        .config("spark.sql.warehouse.dir", "/opt/data/warehouse") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .getOrCreate()
    
    print("Spark session initialized")
    
    # Create data directory
    data_dir = "/opt/data/raw"
    os.makedirs(data_dir, exist_ok=True)
    
    # Configure Iceberg
    create_iceberg_tables(spark)
    
    # Download and load IMDB datasets
    datasets = [
        {
            "url": "https://datasets.imdbws.com/name.basics.tsv.gz",
            "filename": "name.basics.tsv",
            "loader": load_name_basics
        },
        {
            "url": "https://datasets.imdbws.com/title.basics.tsv.gz",
            "filename": "title.basics.tsv",
            "loader": load_title_basics
        }
    ]
    
    for dataset in datasets:
        try:
            local_path = os.path.join(data_dir, dataset["filename"])
            download_imdb_data(dataset["url"], local_path)
            dataset["loader"](spark, local_path)
        except Exception as e:
            print(f"Error processing {dataset['filename']}: {e}")
    
    print("IMDB ETL process completed!")
    spark.stop()

if __name__ == "__main__":
    main()
