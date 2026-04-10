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
    # Field must be a positive number
    "positive": lambda field: F.col(field).isNotNull() & (F.col(field) > 0),
    # Driver must be at least 18 years old
    "minAge18": lambda field: F.col(field).isNotNull() & (F.col(field) >= 18),
    # Driver must be below 100 years old
    "maxAge100": lambda field: F.col(field).isNotNull() & (F.col(field) <= 100),
    # Plate must match format: 2-3 uppercase letters, hyphen, 3 digits (ABC-123)
    "validPlate": lambda field: F.col(field).isNotNull() & F.col(field).rlike(r"^[A-Z]{2,3}-\d{3}$"),
    # Vehicle year must be between 1886 (first car ever built) and current year
    "validYear": lambda field: (
        F.col(field).isNotNull() &
        (F.col(field) >= 1886) &
        (F.col(field) <= F.year(F.current_date()))
    ),
    # Price must be at least 1.0
    "validPrice": lambda field: (
        F.col(field).isNotNull() &
        F.col(field) >= 1.0
    ),
    # Coverage type must be one of Ominimo's actual motor insurance products
    "validCoverage": lambda field: (
        F.col(field).isNotNull() &
        F.col(field).isin("MTPL", "Limited Casco", "Casco")
    ),
    # Variant is only applicable for Limited Casco and Casco products
    "validVariant": lambda field: (
        F.col(field).isNotNull() &
        F.col(field).isin("Compact", "Basic", "Comfort", "Premium")
    ),
    # Deductible must be one of the three allowed values (100, 200, 500)
    "validDeductible": lambda field: (
        F.col(field).isNotNull() &
        F.col(field).isin(100, 200, 500)
    ),
    # Vehicle make must not be empty and must contain only letters, spaces, or hyphens
    "validMake": lambda field: (
        F.col(field).isNotNull() &
        (F.trim(F.col(field).cast(StringType())) != "") &
        F.col(field).rlike(r"^[A-Za-z\s\-]+$")
    ),
    # Date must be in yyyy-MM-dd HH:mm:ss format
    "validDateFormat": lambda field: (
        F.col(field).isNotNull() &
        F.to_timestamp(F.col(field), "yyyy-MM-dd HH:mm:ss").isNotNull()
    ),
    # Policy start date must be before policy end date (cross-field rule)
    # Used on policy_start_date field, reads policy_end_date from same row
    "startBeforeEnd": lambda field: (
        F.col(field).isNotNull() &
        F.col("policy_end_date").isNotNull() &
        (
            F.to_timestamp(F.col(field), "yyyy-MM-dd HH:mm:ss") <
            F.to_timestamp(F.col("policy_end_date"), "yyyy-MM-dd HH:mm:ss")
        )
    ),
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
