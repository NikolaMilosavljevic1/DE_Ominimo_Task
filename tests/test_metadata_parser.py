import json
import os
import tempfile

import pytest

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from metadata_parser import MetadataParser, DataflowConfig, SourceConfig, SinkConfig


VALID_CONFIG = {
    "dataflows": [
        {
            "name": "test-flow",
            "sources": [
                {"name": "src", "path": "/data/input", "format": "JSON"}
            ],
            "transformations": [
                {
                    "name": "val",
                    "type": "validate_fields",
                    "params": {
                        "input": "src",
                        "validations": [
                            {"field": "plate_number", "validations": ["notEmpty"]}
                        ],
                    },
                }
            ],
            "sinks": [
                {
                    "input": "val_ok",
                    "name": "ok-sink",
                    "paths": ["/data/output/ok"],
                    "format": "JSON",
                    "saveMode": "OVERWRITE",
                }
            ],
        }
    ]
}


def write_config(config: dict) -> str:
    f = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
    json.dump(config, f)
    f.close()
    return f.name


class TestMetadataParser:
    def test_load_valid_config(self):
        path = write_config(VALID_CONFIG)
        try:
            dataflows = MetadataParser(path).load()
            assert len(dataflows) == 1
            df = dataflows[0]
            assert isinstance(df, DataflowConfig)
            assert df.name == "test-flow"
            assert len(df.sources) == 1
            assert len(df.transformations) == 1
            assert len(df.sinks) == 1
        finally:
            os.unlink(path)

    def test_source_parsed_correctly(self):
        path = write_config(VALID_CONFIG)
        try:
            source: SourceConfig = MetadataParser(path).load()[0].sources[0]
            assert source.name == "src"
            assert source.path == "/data/input"
            assert source.format == "JSON"
        finally:
            os.unlink(path)

    def test_sink_save_mode_uppercased(self):
        config = json.loads(json.dumps(VALID_CONFIG))
        config["dataflows"][0]["sinks"][0]["saveMode"] = "overwrite"
        path = write_config(config)
        try:
            sink: SinkConfig = MetadataParser(path).load()[0].sinks[0]
            assert sink.save_mode == "OVERWRITE"
        finally:
            os.unlink(path)

    def test_missing_required_field_raises(self):
        config = json.loads(json.dumps(VALID_CONFIG))
        del config["dataflows"][0]["sources"][0]["name"]
        path = write_config(config)
        try:
            with pytest.raises(ValueError, match="Missing required field 'name'"):
                MetadataParser(path).load()
        finally:
            os.unlink(path)

    def test_unsupported_format_raises(self):
        config = json.loads(json.dumps(VALID_CONFIG))
        config["dataflows"][0]["sources"][0]["format"] = "XML"
        path = write_config(config)
        try:
            with pytest.raises(ValueError, match="Unsupported source format"):
                MetadataParser(path).load()
        finally:
            os.unlink(path)

    def test_unsupported_transformation_type_raises(self):
        config = json.loads(json.dumps(VALID_CONFIG))
        config["dataflows"][0]["transformations"][0]["type"] = "unknown_type"
        path = write_config(config)
        try:
            with pytest.raises(ValueError, match="Unsupported transformation type"):
                MetadataParser(path).load()
        finally:
            os.unlink(path)

    def test_empty_dataflows_returns_empty_list(self):
        path = write_config({"dataflows": []})
        try:
            assert MetadataParser(path).load() == []
        finally:
            os.unlink(path)
