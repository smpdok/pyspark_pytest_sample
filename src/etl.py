# src/etl.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace

# Initialize Spark session
spark = SparkSession.builder \
    .master("local[1]") \
    .appName("Testing PySpark Example") \
    .getOrCreate()

def remove_extra_spaces(df, column_name):
    """
    Remove additional spaces from the specified column in the DataFrame.

    Args:
        df (DataFrame): Input DataFrame.
        column_name (str): Name of the column from which to remove extra spaces.

    Returns:
        DataFrame: Transformed DataFrame with extra spaces removed.
    """
    # Remove extra spaces using regular expression
    df_transformed = df.withColumn(
        column_name, regexp_replace(col(column_name), "\\s+", " ")
    )
    return df_transformed

# Sample data
sample_data = [
    {"name": "John    D.", "age": 30},
    {"name": "Alice   G.", "age": 25},
    {"name": "Bob  T.", "age": 35},
    {"name": "Eve   A.", "age": 28},
]

# Create a DataFrame from the sample data
df = spark.createDataFrame(sample_data)

# Apply the transformation
transformed_df = remove_extra_spaces(df, "name")

# Show the transformed DataFrame
transformed_df.show()

# Stop the Spark session
spark.stop()
