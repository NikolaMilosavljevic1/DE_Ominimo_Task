from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


VALIDATION_REGISTRY: dict[str, callable] = {
    "notNull": lambda field: F.col(field).isNotNull(),
    "notEmpty": lambda field: F.col(field).isNotNull() & (F.trim(F.col(field).cast(StringType())) != ""),
    "positive": lambda field: F.col(field).isNotNull() & (F.col(field) > 0),
    "minAge18": lambda field: F.col(field).isNotNull() & (F.col(field) >= 18),
    "maxAge100": lambda field: F.col(field).isNotNull() & (F.col(field) <= 100),
    # expects format like ABC-123 (2-3 uppercase letters, hyphen, 3 digits)
    "validPlate": lambda field: F.col(field).isNotNull() & F.col(field).rlike(r"^[A-Z]{2,3}-\d{3}$"),
    # 1886 is when the first car was built
    "validYear": lambda field: (
        F.col(field).isNotNull() &
        (F.col(field) >= 1886) &
        (F.col(field) <= F.year(F.current_date()))
    ),
    "validPrice": lambda field: (
        F.col(field).isNotNull() &
        (F.col(field) >= 1.0)
    ),
    "validCoverage": lambda field: (
        F.col(field).isNotNull() &
        F.col(field).isin("MTPL", "Limited Casco", "Casco")
    ),
    "validVariant": lambda field: (
        F.col(field).isNotNull() &
        F.col(field).isin("Compact", "Basic", "Comfort", "Premium")
    ),
    "validDeductible": lambda field: (
        F.col(field).isNotNull() &
        F.col(field).isin(100, 200, 500)
    ),
    # must be not null, and consist of characters, this is the brand of the car
    "validMake": lambda field: (
        F.col(field).isNotNull() &
        (F.trim(F.col(field).cast(StringType())) != "") &
        F.col(field).rlike(r"^[A-Za-z\s\-]+$")
    ),
    "validDateFormat": lambda field: (
        F.col(field).isNotNull() &
        F.to_timestamp(F.col(field), "yyyy-MM-dd HH:mm:ss").isNotNull()
    ),
    # hecks that start is before end on the same row
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
        if not self.validations_config:
            return df, df.limit(0)

        error_columns: list[tuple[str, any]] = []

        for entry in self.validations_config:
            field_name = entry["field"]
            for rule_name in entry.get("validations", []):
                rule_fn = VALIDATION_REGISTRY[rule_name]
                error_key = f"{field_name}.{rule_name}"
                error_msg = f"Field '{field_name}' failed rule '{rule_name}'"
                # if rule passes the column is null, if it fails we store the error message
                error_col = F.when(~rule_fn(field_name), F.lit(error_msg)).otherwise(F.lit(None).cast(StringType()))
                error_columns.append((error_key, error_col))

        keys_array = F.array(*[F.lit(k) for k, _ in error_columns])
        vals_array = F.array(*[col for _, col in error_columns])

        df_with_errors = df.withColumn(
            self.ERROR_FIELD,
            F.map_from_arrays(keys_array, vals_array),
        )

        # drop nulls from the map so only actual failures remain
        df_with_errors = df_with_errors.withColumn(
            self.ERROR_FIELD,
            F.map_filter(F.col(self.ERROR_FIELD), lambda k, v: v.isNotNull()),
        )

        has_errors = F.size(F.col(self.ERROR_FIELD)) > 0

        ok_df = df_with_errors.filter(~has_errors).drop(self.ERROR_FIELD)
        ko_df = df_with_errors.filter(has_errors)

        return ok_df, ko_df
