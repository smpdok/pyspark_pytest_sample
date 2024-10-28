# tests/test_etl.py

from pyspark.testing.utils import assertDataFrameEqual
from src.etl import remove_extra_spaces


def test_remove_extra_spaces(spark):
    """Test the remove_extra_spaces function."""
    # Sample input data
    sample_data = [
        {"name": "John    D.", "age": 30},
        {"name": "Alice   G.", "age": 25},
        {"name": "Bob  T.", "age": 35},
        {"name": "Eve   A.", "age": 28}
    ]

    # Create a Spark DataFrame from the sample data
    original_df = spark.createDataFrame(sample_data)

    # Apply the transformation function
    transformed_df = remove_extra_spaces(original_df, "name")

    # Expected output data
    expected_data = [
        {"name": "John D.", "age": 30},
        {"name": "Alice G.", "age": 25},
        {"name": "Bob T.", "age": 35},
        {"name": "Eve A.", "age": 28}
    ]

    # Create a Spark DataFrame for expected results
    expected_df = spark.createDataFrame(expected_data)

    # Assert that the transformed DataFrame matches the expected DataFrame
    assertDataFrameEqual(transformed_df, expected_df)
