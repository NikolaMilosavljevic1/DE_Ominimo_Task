"""
Validator — applies field-level validation rules driven entirely by metadata.

Rules are registered in VALIDATION_REGISTRY. Adding a new rule key here
automatically makes it available to any pipeline config without code changes.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, ArrayType


# ---------------------------------------------------------------------------
# Validation rule registry
# Each entry is a function: (field_name: str) -> Column (boolean expression)
# ---------------------------------------------------------------------------

VALIDATION_REGISTRY: dict[str, callable] = {
    # Field must be present and non-null
    "notNull": lambda field: F.col(field).isNotNull(),
    # Field must be present, non-null, and non-empty string
    "notEmpty": lambda field: F.col(field).isNotNull() & (F.trim(F.col(field).cast(StringType())) != ""),
    # Field must be a positive number (> 0)
    "positive": lambda field: F.col(field).isNotNull() & (F.col(field) > 0),
    # Field must be >= 18 (legal driving age)
    "minAge18": lambda field: F.col(field).isNotNull() & (F.col(field) >= 18),
    # Field must be <= 100
    "maxAge100": lambda field: F.col(field).isNotNull() & (F.col(field) <= 100),
}


class Validator:
    """
    Splits a DataFrame into (ok_df, ko_df) based on validation rules
    defined in the metadata config.

    The validation config looks like:
        [
          {"field": "plate_number", "validations": ["notEmpty"]},
          {"field": "driver_age",   "validations": ["notNull"]}
        ]

    For each record, all failing rules are collected into a
    `validation_errors` map column so the KO side is self-documenting.
    """

    ERROR_FIELD = "validation_errors"

    def __init__(self, validations_config: list[dict]):
        self.validations_config = validations_config
        self._validate_config()

    def _validate_config(self):
        for entry in self.validations_config:
            for rule in entry.get("validations", []):
                if rule not in VALIDATION_REGISTRY:
                    raise ValueError(
                        f"Unknown validation rule '{rule}'. "
                        f"Available rules: {sorted(VALIDATION_REGISTRY.keys())}"
                    )

    def apply(self, df: DataFrame) -> tuple[DataFrame, DataFrame]:
        """
        Returns (ok_df, ko_df).

        ok_df  — records that passed all validations (no validation_errors column).
        ko_df  — records that failed at least one validation, with a
                 validation_errors map<string, string> column.
        """
        # Build a column for each (field, rule) pair that captures the error message
        # when the rule fails, or null when it passes.
        error_columns: list[tuple[str, any]] = []

        for entry in self.validations_config:
            field_name = entry["field"]
            for rule_name in entry.get("validations", []):
                rule_fn = VALIDATION_REGISTRY[rule_name]
                error_key = f"{field_name}.{rule_name}"
                error_msg = f"Field '{field_name}' failed rule '{rule_name}'"

                # Pass → null; Fail → error message string
                error_col = F.when(~rule_fn(field_name), F.lit(error_msg)).otherwise(F.lit(None).cast(StringType()))
                error_columns.append((error_key, error_col))

        # Combine all individual error columns into a map<string, string>
        # by building pairs [key, value, key, value, …] then calling map_from_arrays
        if not error_columns:
            # No validations configured — everything is OK
            return df, df.limit(0)

        keys_array = F.array(*[F.lit(k) for k, _ in error_columns])
        vals_array = F.array(*[col for _, col in error_columns])

        df_with_errors = df.withColumn(
            self.ERROR_FIELD,
            F.map_from_arrays(keys_array, vals_array),
        )

        # Filter out null values from the map (only keep entries where a rule fired)
        df_with_errors = df_with_errors.withColumn(
            self.ERROR_FIELD,
            F.map_filter(F.col(self.ERROR_FIELD), lambda k, v: v.isNotNull()),
        )

        # Records with an empty error map → OK; non-empty → KO
        has_errors = F.size(F.col(self.ERROR_FIELD)) > 0

        ok_df = df_with_errors.filter(~has_errors).drop(self.ERROR_FIELD)
        ko_df = df_with_errors.filter(has_errors)

        return ok_df, ko_df
