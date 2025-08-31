# 🚨 Troubleshooting Guide

## Docker Build Issues

### Docker Image Not Found

**Problem**: `failed to solve: apache/spark:3.5.0-python3.11: not found`

**Solution**: This specific image tag doesn't exist. Try these alternatives:

1. **Use available Spark images**:

   ```bash
   # Check what's available
   docker search apache/spark

   # Use the main Dockerfile (Python 3.8)
   docker build -t spark-etl:latest .
   ```

2. **Use the alternative Dockerfile** (Python 3.9):

   ```bash
   docker build -f Dockerfile.alternative -t spark-etl:latest .
   ```

3. **Use the build script** (tries both automatically):
   ```bash
   chmod +x build.sh
   ./build.sh
   ```

### Package Version Compatibility

**Problem**: `ERROR: Could not find a version that satisfies the requirement pandas==2.1.4`

**Solution**: The Docker image now uses Python 3.8 which supports pandas 1.5.3. If you still encounter issues:

1. Check Python version in container: `python --version`
2. Update requirements.txt with compatible versions:

   ```bash
   # For Python 3.8
   pandas==1.5.3
   numpy==1.24.3

   # For Python 3.9+
   pandas==2.0.3
   numpy==1.24.3
   ```

### Build Context Issues

**Problem**: Build takes too long or fails due to large context

**Solution**: Use the `.dockerignore` file to exclude unnecessary files:

```bash
# Rebuild with clean context
docker build --no-cache -t spark-etl:latest .
```

## Runtime Issues

### Hive Metastore Connection

**Problem**: `Connection refused` to hive-metastore:9083

**Solution**:

1. Ensure all services are running: `docker-compose ps`
2. Check network connectivity: `docker network ls`
3. Verify service dependencies in docker-compose.yml
4. Wait for services to fully start up

### Memory Issues

**Problem**: Spark jobs fail with out-of-memory errors

**Solution**: Adjust memory settings in docker-compose.yml:

```yaml
environment:
  - SPARK_DRIVER_MEMORY=4g
  - SPARK_EXECUTOR_MEMORY=4g
```

### Port Conflicts

**Problem**: Ports already in use (4040, 7077, 8080, 9083, 3306)

**Solution**:

1. Check what's using the ports: `netstat -tulpn | grep :4040`
2. Stop conflicting services
3. Or change ports in docker-compose.yml

## Testing and Validation

### Test Installation

```bash
# Test package imports and Spark functionality
docker exec spark-etl python test_installation.py
```

### Test ETL Pipeline

```bash
# Run a small test
docker exec spark-etl spark-submit jobs/imdb_ingest.py
```

### Check Logs

```bash
# View container logs
docker logs spark-etl

# View specific service logs
docker logs hive-metastore
docker logs mysql
```

## Common Commands

### Rebuild and Restart

```bash
# Full rebuild
cd infra
docker-compose down
docker-compose up --build

# Rebuild specific service
docker-compose build spark-etl
docker-compose up spark-etl
```

### Debug Container

```bash
# Enter container for debugging
docker exec -it spark-etl bash

# Check environment
env | grep SPARK
python --version
pip list
```

### Clean Up

```bash
# Remove all containers and volumes
docker-compose down -v

# Remove images
docker rmi spark-etl:latest
```

## Performance Tuning

### Spark Configuration

Adjust `config/spark-defaults.conf` for your environment:

```conf
# For development
spark.driver.memory=2g
spark.executor.memory=2g

# For production
spark.driver.memory=8g
spark.executor.memory=8g
```

### Container Resources

Limit container resources in docker-compose.yml:

```yaml
deploy:
  resources:
    limits:
      memory: 4G
      cpus: "2.0"
```

## Getting Help

1. **Check logs first**: Most issues are visible in container logs
2. **Verify dependencies**: Ensure all required services are running
3. **Test incrementally**: Start with basic functionality before running full ETL
4. **Check versions**: Ensure package versions are compatible with Python version
5. **Network issues**: Verify container networking and service discovery
