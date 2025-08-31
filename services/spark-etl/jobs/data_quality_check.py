#!/usr/bin/env python3
"""
Data Quality Check Job

This script performs data quality checks on the ingested IMDb data
to ensure data integrity and completeness.
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create and configure Spark session with Iceberg support."""
    return (SparkSession.builder
            .appName("Data-Quality-Check")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.iceberg.type", "hive")
            .config("spark.sql.catalog.iceberg.uri", "thrift://hive-metastore:9083")
            .config("spark.sql.catalog.iceberg.warehouse", "/opt/data/warehouse")
            .getOrCreate())

def check_bronze_layer(spark):
    """Check data quality in bronze layer."""
    logger.info("Checking bronze layer data quality...")
    
    # Check title_basics
    title_basics_count = spark.sql("SELECT COUNT(*) as count FROM iceberg.bronze.title_basics").collect()[0]['count']
    title_basics_null_titles = spark.sql("SELECT COUNT(*) as count FROM iceberg.bronze.title_basics WHERE primaryTitle IS NULL").collect()[0]['count']
    
    logger.info(f"Bronze title_basics: {title_basics_count} records, {title_basics_null_titles} null titles")
    
    # Check title_ratings
    title_ratings_count = spark.sql("SELECT COUNT(*) as count FROM iceberg.bronze.title_ratings").collect()[0]['count']
    title_ratings_invalid_ratings = spark.sql("SELECT COUNT(*) as count FROM iceberg.bronze.title_ratings WHERE averageRating < 0 OR averageRating > 10").collect()[0]['count']
    
    logger.info(f"Bronze title_ratings: {title_ratings_count} records, {title_ratings_invalid_ratings} invalid ratings")
    
    return {
        'bronze_title_basics_count': title_basics_count,
        'bronze_title_ratings_count': title_ratings_count,
        'bronze_null_titles': title_basics_null_titles,
        'bronze_invalid_ratings': title_ratings_invalid_ratings
    }

def check_silver_layer(spark):
    """Check data quality in silver layer."""
    logger.info("Checking silver layer data quality...")
    
    # Check silver title_basics (should be filtered)
    silver_title_basics_count = spark.sql("SELECT COUNT(*) as count FROM iceberg.silver.title_basics").collect()[0]['count']
    silver_adult_content = spark.sql("SELECT COUNT(*) as count FROM iceberg.silver.title_basics WHERE isAdult = true").collect()[0]['count']
    silver_no_year = spark.sql("SELECT COUNT(*) as count FROM iceberg.silver.title_basics WHERE startYear IS NULL").collect()[0]['count']
    
    logger.info(f"Silver title_basics: {silver_title_basics_count} records, {silver_adult_content} adult content, {silver_no_year} no year")
    
    # Check silver title_ratings (should have minimum votes)
    silver_title_ratings_count = spark.sql("SELECT COUNT(*) as count FROM iceberg.silver.title_ratings").collect()[0]['count']
    silver_low_votes = spark.sql("SELECT COUNT(*) as count FROM iceberg.silver.title_ratings WHERE numVotes < 10").collect()[0]['count']
    
    logger.info(f"Silver title_ratings: {silver_title_ratings_count} records, {silver_low_votes} low votes")
    
    return {
        'silver_title_basics_count': silver_title_basics_count,
        'silver_title_ratings_count': silver_title_ratings_count,
        'silver_adult_content': silver_adult_content,
        'silver_no_year': silver_no_year,
        'silver_low_votes': silver_low_votes
    }

def check_gold_layer(spark):
    """Check data quality in gold layer."""
    logger.info("Checking gold layer data quality...")
    
    # Check gold movies
    gold_movies_count = spark.sql("SELECT COUNT(*) as count FROM iceberg.gold.movies").collect()[0]['count']
    gold_movies_excellent = spark.sql("SELECT COUNT(*) as count FROM iceberg.gold.movies WHERE rating_category = 'Excellent'").collect()[0]['count']
    
    logger.info(f"Gold movies: {gold_movies_count} records, {gold_movies_excellent} excellent")
    
    # Check top actors
    top_actors_count = spark.sql("SELECT COUNT(*) as count FROM iceberg.gold.top_actors").collect()[0]['count']
    
    logger.info(f"Top actors: {top_actors_count} records")
    
    return {
        'gold_movies_count': gold_movies_count,
        'gold_movies_excellent': gold_movies_excellent,
        'top_actors_count': top_actors_count
    }

def run_data_quality_checks(spark):
    """Run all data quality checks."""
    logger.info("Starting comprehensive data quality checks...")
    
    bronze_metrics = check_bronze_layer(spark)
    silver_metrics = check_silver_layer(spark)
    gold_metrics = check_gold_layer(spark)
    
    # Combine all metrics
    all_metrics = {**bronze_metrics, **silver_metrics, **gold_metrics}
    
    # Log summary
    logger.info("=== DATA QUALITY SUMMARY ===")
    for metric, value in all_metrics.items():
        logger.info(f"{metric}: {value}")
    
    # Check for potential issues
    issues = []
    if bronze_metrics['bronze_null_titles'] > 0:
        issues.append(f"Found {bronze_metrics['bronze_null_titles']} null titles in bronze layer")
    
    if silver_metrics['silver_adult_content'] > 0:
        issues.append(f"Found {silver_metrics['silver_adult_content']} adult content in silver layer (should be filtered)")
    
    if silver_metrics['silver_low_votes'] > 0:
        issues.append(f"Found {silver_metrics['silver_low_votes']} low vote counts in silver layer (should be filtered)")
    
    if issues:
        logger.warning("Data quality issues found:")
        for issue in issues:
            logger.warning(f"  - {issue}")
    else:
        logger.info("No data quality issues found!")
    
    return all_metrics

def main():
    """Main execution function."""
    try:
        # Create Spark session
        spark = create_spark_session()
        logger.info("Spark session created successfully")
        
        # Run data quality checks
        metrics = run_data_quality_checks(spark)
        
        logger.info("Data quality checks completed successfully!")
        
    except Exception as e:
        logger.error(f"Error during data quality checks: {str(e)}")
        sys.exit(1)
    finally:
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    main()
