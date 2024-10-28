import datetime
import pytest
from pyspark.testing.utils import assertDataFrameEqual


class TestGroup:

    @pytest.fixture
    def input_df(self, spark):
        df = spark.createDataFrame(
            [
                ("socks", 7.55, datetime.date(2022, 5, 15)),
                ("handbag", 49.99, datetime.date(2022, 5, 16)),
                ("shorts", 35.00, datetime.date(2023, 1, 5)),
                ("socks", 25.00, datetime.date(2023, 12, 23)),
            ],
            ["item", "amount", "purchase_date"],
        )
        return df

    def test_amount(self, spark, input_df):
        actual_df = input_df.where("amount > 30.0")

        expected_df = spark.createDataFrame(
            [
                ("handbag", 49.99, datetime.date(2022, 5, 16)),
                ("shorts", 35.00, datetime.date(2023, 1, 5)),
            ],
            ["item", "amount", "purchase_date"],
        )
        assertDataFrameEqual(actual_df, expected_df)

    def test_purchase_date(self, spark, input_df):
        actual_df = input_df.where("purchase_date between '2022-05-15' and '2023-06-01'")

        expected_df = spark.createDataFrame(
            [
                ("socks", 7.55, datetime.date(2022, 5, 15)),
                ("handbag", 49.99, datetime.date(2022, 5, 16)),
                ("shorts", 35.00, datetime.date(2023, 1, 5)),
            ],
            ["item", "amount", "purchase_date"],
        )
        assertDataFrameEqual(actual_df, expected_df)
