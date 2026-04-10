import argparse
import logging
import sys

from pyspark.sql import SparkSession

from metadata_parser import MetadataParser
from pipeline_runner import PipelineRunner


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Ominimo metadata-driven motor insurance policy pipeline"
    )
    parser.add_argument(
        "--config",
        required=True,
        help="Path to the pipeline metadata JSON file.",
    )
    parser.add_argument(
        "--dataflow",
        default=None,
        help="Name of a specific dataflow to run. If omitted, all dataflows are run.",
    )
    return parser


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("ominimo-motor-ingestion")
        # required for map_filter to work on Spark 3.x
        .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
        .getOrCreate()
    )


def configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )
    # keeping only warnings
    for noisy in ("py4j", "pyspark", "org.apache.spark"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def main():
    configure_logging()
    logger = logging.getLogger(__name__)

    args = build_arg_parser().parse_args()

    logger.info("Loading pipeline metadata from: %s", args.config)
    parser = MetadataParser(args.config)
    dataflows = parser.load()

    if not dataflows:
        logger.error("No dataflows found in config. Exiting.")
        sys.exit(1)

    if args.dataflow:
        dataflows = [df for df in dataflows if df.name == args.dataflow]
        if not dataflows:
            logger.error("No dataflow named '%s' found in config.", args.dataflow)
            sys.exit(1)

    spark = build_spark()

    try:
        for dataflow in dataflows:
            logger.info("Running dataflow: %s", dataflow.name)
            runner = PipelineRunner(spark=spark, dataflow=dataflow)
            runner.run()
    finally:
        spark.stop()

    logger.info("All dataflows completed.")


if __name__ == "__main__":
    main()
