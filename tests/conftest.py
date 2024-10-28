from pyspark.sql import SparkSession
import pytest

@pytest.fixture(scope="module")
def spark():
    """Create a SparkSession for testing."""
    spark_session = SparkSession.builder \
        .master("local[1]") \
        .appName("Testing PySpark Example") \
        .getOrCreate()
    yield spark_session
    spark_session.stop()