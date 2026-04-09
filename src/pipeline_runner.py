"""
PipelineRunner — executes a DataflowConfig end-to-end.

Execution model:
  1. Sources are read into named DataFrames.
  2. Transformations are applied in order; each step may produce named outputs
     (e.g. "validation" produces both "validation_ok" and "validation_ko").
  3. Sinks write the named DataFrames to their configured paths.

All behaviour is driven by the DataflowConfig object — no field names,
paths, or rules are hardcoded here.
"""

import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from metadata_parser import DataflowConfig, TransformationConfig, SinkConfig
from validator import Validator

logger = logging.getLogger(__name__)


class PipelineRunner:
    """Executes a single DataflowConfig using PySpark."""

    # Maps add_fields function names from metadata → Spark Column expressions
    FIELD_FUNCTION_REGISTRY: dict[str, callable] = {
        "current_timestamp": lambda: F.current_timestamp(),
        "current_date": lambda: F.current_date(),
    }

    def __init__(self, spark: SparkSession, dataflow: DataflowConfig):
        self.spark = spark
        self.dataflow = dataflow
        # Namespace for intermediate DataFrames, keyed by their logical name
        self._namespace: dict[str, DataFrame] = {}

    def run(self):
        logger.info("Starting dataflow: %s", self.dataflow.name)
        self._load_sources()
        self._apply_transformations()
        self._write_sinks()
        logger.info("Dataflow '%s' completed successfully.", self.dataflow.name)

    # ------------------------------------------------------------------
    # Sources
    # ------------------------------------------------------------------

    def _load_sources(self):
        for source in self.dataflow.sources:
            logger.info("Reading source '%s' from '%s' (format=%s)", source.name, source.path, source.format)
            reader = self.spark.read.options(**source.options)

            if source.format == "JSON":
                df = reader.json(source.path)
            elif source.format == "CSV":
                df = reader.option("header", "true").csv(source.path)
            elif source.format == "PARQUET":
                df = reader.parquet(source.path)
            elif source.format == "DELTA":
                df = reader.format("delta").load(source.path)
            else:
                raise ValueError(f"Unsupported source format: {source.format}")

            self._namespace[source.name] = df
            logger.info("Source '%s' loaded with %d columns.", source.name, len(df.columns))

    # ------------------------------------------------------------------
    # Transformations
    # ------------------------------------------------------------------

    def _apply_transformations(self):
        for transformation in self.dataflow.transformations:
            logger.info("Applying transformation '%s' (type=%s)", transformation.name, transformation.type)
            if transformation.type == "validate_fields":
                self._apply_validate_fields(transformation)
            elif transformation.type == "add_fields":
                self._apply_add_fields(transformation)
            else:
                raise ValueError(f"Unsupported transformation type: {transformation.type}")

    def _apply_validate_fields(self, t: TransformationConfig):
        input_name = t.params.get("input")
        if input_name not in self._namespace:
            raise KeyError(f"Transformation '{t.name}' references unknown input '{input_name}'.")

        input_df = self._namespace[input_name]
        validations = t.params.get("validations", [])

        validator = Validator(validations)
        ok_df, ko_df = validator.apply(input_df)

        # Publish both output streams so downstream steps can reference them by name
        self._namespace[f"{t.name}_ok"] = ok_df
        self._namespace[f"{t.name}_ko"] = ko_df

        logger.info(
            "Validation '%s': ok=%d, ko=%d",
            t.name,
            ok_df.count(),
            ko_df.count(),
        )

    def _apply_add_fields(self, t: TransformationConfig):
        input_name = t.params.get("input")
        if input_name not in self._namespace:
            raise KeyError(f"Transformation '{t.name}' references unknown input '{input_name}'.")

        df = self._namespace[input_name]

        for field_spec in t.params.get("addFields", []):
            field_name = field_spec["name"]
            function_name = field_spec["function"]

            if function_name not in self.FIELD_FUNCTION_REGISTRY:
                raise ValueError(
                    f"Unknown field function '{function_name}'. "
                    f"Available: {sorted(self.FIELD_FUNCTION_REGISTRY.keys())}"
                )

            df = df.withColumn(field_name, self.FIELD_FUNCTION_REGISTRY[function_name]())
            logger.info("Added field '%s' using function '%s'.", field_name, function_name)

        self._namespace[t.name] = df

    # ------------------------------------------------------------------
    # Sinks
    # ------------------------------------------------------------------

    def _write_sinks(self):
        for sink in self.dataflow.sinks:
            self._write_sink(sink)

    def _write_sink(self, sink: SinkConfig):
        if sink.input not in self._namespace:
            raise KeyError(f"Sink '{sink.name}' references unknown input '{sink.input}'.")

        df = self._namespace[sink.input]

        for path in sink.paths:
            logger.info(
                "Writing sink '%s' → '%s' (format=%s, saveMode=%s)",
                sink.name, path, sink.format, sink.save_mode,
            )
            writer = (
                df.coalesce(1)
                .write.mode(sink.save_mode.lower())
                .options(**sink.options)
            )

            if sink.format == "JSON":
                writer.json(path)
            elif sink.format == "CSV":
                writer.option("header", "true").csv(path)
            elif sink.format == "PARQUET":
                writer.parquet(path)
            elif sink.format == "DELTA":
                writer.format("delta").save(path)
            else:
                raise ValueError(f"Unsupported sink format: {sink.format}")

            logger.info("Sink '%s' written to '%s'.", sink.name, path)
