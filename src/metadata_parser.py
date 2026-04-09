"""
MetadataParser — loads and validates a pipeline metadata JSON config.

The config drives all pipeline behaviour: sources, transformations, and sinks
are expressed as data, not code. Changing the JSON changes what the pipeline does.
"""

import json
from dataclasses import dataclass, field
from typing import Any

@dataclass
class SourceConfig:
    name: str
    path: str
    format: str
    options: dict = field(default_factory=dict)


@dataclass
class TransformationConfig:
    name: str
    type: str
    params: dict


@dataclass
class SinkConfig:
    input: str
    name: str
    paths: list[str]
    format: str
    save_mode: str
    options: dict = field(default_factory=dict)


@dataclass
class DataflowConfig:
    name: str
    sources: list[SourceConfig]
    transformations: list[TransformationConfig]
    sinks: list[SinkConfig]


class MetadataParser:
    """Parses a pipeline metadata JSON file into typed config objects."""

    SUPPORTED_SOURCE_FORMATS = {"JSON", "CSV", "PARQUET", "DELTA"}
    SUPPORTED_SINK_FORMATS = {"JSON", "CSV", "PARQUET", "DELTA"}
    SUPPORTED_TRANSFORMATION_TYPES = {"validate_fields", "add_fields"}

    def __init__(self, config_path: str):
        self.config_path = config_path
        self._raw: dict[str, Any] = {}

    def load(self) -> list[DataflowConfig]:
        with open(self.config_path, "r", encoding="utf-8") as f:
            self._raw = json.load(f)
        return [self._parse_dataflow(df) for df in self._raw.get("dataflows", [])]

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _parse_dataflow(self, raw: dict) -> DataflowConfig:
        name = self._require(raw, "name", context="dataflow")
        sources = [self._parse_source(s) for s in raw.get("sources", [])]
        transformations = [self._parse_transformation(t) for t in raw.get("transformations", [])]
        sinks = [self._parse_sink(sk) for sk in raw.get("sinks", [])]
        return DataflowConfig(name=name, sources=sources, transformations=transformations, sinks=sinks)

    def _parse_source(self, raw: dict) -> SourceConfig:
        fmt = raw.get("format", "JSON").upper()
        if fmt not in self.SUPPORTED_SOURCE_FORMATS:
            raise ValueError(f"Unsupported source format '{fmt}'. Supported: {self.SUPPORTED_SOURCE_FORMATS}")
        return SourceConfig(
            name=self._require(raw, "name", "source"),
            path=self._require(raw, "path", "source"),
            format=fmt,
            options=raw.get("options", {}),
        )

    def _parse_transformation(self, raw: dict) -> TransformationConfig:
        t_type = self._require(raw, "type", "transformation")
        if t_type not in self.SUPPORTED_TRANSFORMATION_TYPES:
            raise ValueError(
                f"Unsupported transformation type '{t_type}'. Supported: {self.SUPPORTED_TRANSFORMATION_TYPES}"
            )
        return TransformationConfig(
            name=self._require(raw, "name", "transformation"),
            type=t_type,
            params=raw.get("params", {}),
        )

    def _parse_sink(self, raw: dict) -> SinkConfig:
        fmt = raw.get("format", "JSON").upper()
        if fmt not in self.SUPPORTED_SINK_FORMATS:
            raise ValueError(f"Unsupported sink format '{fmt}'. Supported: {self.SUPPORTED_SINK_FORMATS}")
        return SinkConfig(
            input=self._require(raw, "input", "sink"),
            name=self._require(raw, "name", "sink"),
            paths=self._require(raw, "paths", "sink"),
            format=fmt,
            save_mode=raw.get("saveMode", "OVERWRITE").upper(),
            options=raw.get("options", {}),
        )

    @staticmethod
    def _require(raw: dict, key: str, context: str) -> Any:
        if key not in raw or raw[key] is None:
            raise ValueError(f"Missing required field '{key}' in {context} config: {raw}")
        return raw[key]