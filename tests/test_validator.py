import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType
from validator import Validator


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-validator")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


class TestValidator:
    def _make_df(self, spark, rows: list[dict]):
        return spark.createDataFrame(rows)

    def test_all_ok(self, spark):
        df = self._make_df(spark, [
            {"policy_number": "123", "driver_age": 30, "plate_number": "ABC-123"},
        ])
        config = [
            {"field": "plate_number", "validations": ["notEmpty"]},
            {"field": "driver_age",   "validations": ["notNull"]},
        ]
        ok, ko = Validator(config).apply(df)
        assert ok.count() == 1
        assert ko.count() == 0

    def test_empty_plate_goes_to_ko(self, spark):
        df = self._make_df(spark, [
            {"policy_number": "12345", "driver_age": 45, "plate_number": ""},
        ])
        config = [{"field": "plate_number", "validations": ["notEmpty"]}]
        ok, ko = Validator(config).apply(df)
        assert ok.count() == 0
        assert ko.count() == 1

    def test_null_driver_age_goes_to_ko(self, spark):
        # explicit schema needed so Spark treats driver_age as LongType, not StringType
        schema = StructType([
            StructField("policy_number", StringType(), True),
            StructField("driver_age",    LongType(),   True),
            StructField("plate_number",  StringType(), True),
        ])
        df = spark.createDataFrame([("67890", None, "ABC-123")], schema)
        config = [{"field": "driver_age", "validations": ["notNull"]}]
        ok, ko = Validator(config).apply(df)
        assert ok.count() == 0
        assert ko.count() == 1

    def test_ko_contains_validation_errors_column(self, spark):
        df = self._make_df(spark, [
            {"policy_number": "12345", "driver_age": 45, "plate_number": ""},
        ])
        config = [{"field": "plate_number", "validations": ["notEmpty"]}]
        _, ko = Validator(config).apply(df)
        assert "validation_errors" in ko.columns

    def test_multiple_failures_all_captured(self, spark):
        schema = StructType([
            StructField("policy_number", StringType(), True),
            StructField("driver_age",    LongType(),   True),
            StructField("plate_number",  StringType(), True),
        ])
        df = spark.createDataFrame([("", None, "")], schema)
        config = [
            {"field": "plate_number",  "validations": ["notEmpty"]},
            {"field": "driver_age",    "validations": ["notNull"]},
            {"field": "policy_number", "validations": ["notEmpty"]},
        ]
        _, ko = Validator(config).apply(df)
        assert ko.count() == 1
        errors = ko.collect()[0]["validation_errors"]
        assert len(errors) == 3

    def test_mixed_records_split_correctly(self, spark):
        schema = StructType([
            StructField("policy_number", StringType(), True),
            StructField("driver_age",    LongType(),   True),
            StructField("plate_number",  StringType(), True),
        ])
        df = spark.createDataFrame(
            [
                ("12345", 45,   ""),        # KO - empty plate
                ("67890", None, "ABC-123"), # KO - null age
                ("54321", 30,   "XYZ-789"), # OK
            ],
            schema,
        )
        config = [
            {"field": "plate_number", "validations": ["notEmpty"]},
            {"field": "driver_age",   "validations": ["notNull"]},
        ]
        ok, ko = Validator(config).apply(df)
        assert ok.count() == 1
        assert ko.count() == 2

    def test_unknown_rule_raises(self, spark):
        with pytest.raises(ValueError, match="Unknown validation rule"):
            Validator([{"field": "x", "validations": ["doesNotExist"]}])

    def test_no_validations_all_ok(self, spark):
        df = self._make_df(spark, [{"a": 1}, {"a": 2}])
        ok, ko = Validator([]).apply(df)
        assert ok.count() == 2
        assert ko.count() == 0
