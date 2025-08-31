# 🚀 Spark ETL Service

The Spark ETL service for PopcornDB processes IMDb TSV data files and loads them into Iceberg tables following a bronze/silver/gold data architecture.

## 🏗 Architecture

This service implements a **medallion architecture** with three data layers:

- **Bronze Layer**: Raw data as-is from IMDb TSV files
- **Silver Layer**: Cleaned and validated data with business rules applied
- **Gold Layer**: Business-ready aggregated data for analytics

## 📊 Data Processing

### Input Data

- `title.basics.tsv` - Movie/TV show metadata
- `title.ratings.tsv` - User ratings and vote counts
- `name.basics.tsv` - Person/actor information
- `title.principals.tsv` - Cast and crew relationships

### Output Tables

#### Bronze Layer

- `iceberg.bronze.title_basics`
- `iceberg.bronze.title_ratings`
- `iceberg.bronze.name_basics`
- `iceberg.bronze.title_principals`

#### Silver Layer

- `iceberg.silver.title_basics` - Filtered for movies/TV, no adult content, valid years
- `iceberg.silver.title_ratings` - Minimum vote threshold applied
- `iceberg.silver.name_basics` - Valid names with profession arrays
- `iceberg.silver.title_principals` - Key roles only (actor, actress, director, writer, producer)

#### Gold Layer

- `iceberg.gold.movies` - Movies with ratings, metadata, and rating categories
- `iceberg.gold.top_actors` - Top actors by movie count and average rating
- `iceberg.gold.genre_popularity` - Genre trends over time

## 🛠 Usage

### Running the ETL Job

```bash
# Run the main ingestion job
docker exec spark-etl spark-submit jobs/imdb_ingest.py

# Run data quality checks
docker exec spark-etl spark-submit jobs/data_quality_check.py

# Download IMDb data (if needed)
docker exec spark-etl python scripts/download_imdb_data.py
```

### Environment Variables

- `IMDB_DATA_PATH` - Path to IMDb TSV files (default: `/opt/data/raw`)
- `SPARK_DRIVER_MEMORY` - Spark driver memory (default: 2g)
- `SPARK_EXECUTOR_MEMORY` - Spark executor memory (default: 2g)

### Configuration

The service uses `config/spark-defaults.conf` for Spark configuration including:

- Iceberg catalog settings
- Performance optimizations
- Memory and shuffle configurations

## 🔧 Development

### Local Development

```bash
# Build the container
docker build -t spark-etl .

# Run with volume mounts
docker run -it --rm \
  -v $(pwd)/data:/opt/data \
  -v $(pwd)/jobs:/opt/spark-apps/jobs \
  spark-etl bash
```

### Testing

```bash
# Run with sample data
IMDB_DATA_PATH=/opt/data/sample spark-submit jobs/imdb_ingest.py

# Validate data quality
spark-submit jobs/data_quality_check.py
```

## 📈 Performance

### Optimizations

- Adaptive query execution enabled
- Partition coalescing for optimal file sizes
- Skew join handling for large datasets
- Kryo serialization for better performance

### Monitoring

- Spark event logs enabled
- Structured logging with metrics
- Data quality validation reports

## 🚨 Troubleshooting

### Common Issues

1. **Hive Metastore Connection**

   - Ensure Hive metastore is running on port 9083
   - Check network connectivity between containers

2. **Memory Issues**

   - Increase `SPARK_DRIVER_MEMORY` and `SPARK_EXECUTOR_MEMORY`
   - Monitor container memory usage

3. **Data Quality Issues**
   - Run `data_quality_check.py` to identify problems
   - Check input data format and completeness

### Logs

- Spark logs: `/opt/spark/logs/`
- Application logs: Console output with structured logging

## 🔗 Integration

This service integrates with:

- **Apache Airflow** - Orchestration and scheduling
- **Apache Iceberg** - Lakehouse storage
- **Hive Metastore** - Table metadata management
- **Docker Compose** - Local development environment

## 📚 Resources

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Apache Iceberg Documentation](https://iceberg.apache.org/docs/latest/)
- [PySpark API Reference](https://spark.apache.org/docs/latest/api/python/)
