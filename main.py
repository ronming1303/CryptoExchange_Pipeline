#!/usr/bin/env python3
"""Main entry point for the Crypto Exchange Price Pipeline."""

import argparse
import logging
import sys
from pathlib import Path

from src.config import load_config
from src.scheduler import PipelineScheduler


def setup_logging(log_level: str = "INFO", log_file: str = None) -> None:
    """Setup logging configuration.

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR).
        log_file: Optional path to log file.
    """
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    handlers = [logging.StreamHandler(sys.stdout)]

    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file))

    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format=log_format,
        handlers=handlers
    )


def parse_args() -> argparse.Namespace:
    """Parse command line arguments.

    Returns:
        Parsed arguments namespace.
    """
    parser = argparse.ArgumentParser(
        description="Crypto Exchange Price Pipeline - Fetch and store cryptocurrency prices"
    )

    parser.add_argument(
        "--once",
        action="store_true",
        help="Run the pipeline once and exit (no scheduling)"
    )

    parser.add_argument(
        "--config",
        type=str,
        default="config.yaml",
        help="Path to configuration file (default: config.yaml)"
    )

    parser.add_argument(
        "--log-level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level (default: INFO)"
    )

    parser.add_argument(
        "--log-file",
        type=str,
        default=None,
        help="Path to log file (optional)"
    )

    return parser.parse_args()


def main() -> int:
    """Main function.

    Returns:
        Exit code (0 for success, 1 for failure).
    """
    args = parse_args()

    # Setup logging
    setup_logging(args.log_level, args.log_file)
    logger = logging.getLogger(__name__)

    try:
        # Load configuration
        logger.info(f"Loading configuration from {args.config}")
        config = load_config(args.config)

        logger.info(f"Configured exchanges: {config.exchanges}")
        logger.info(f"Configured pairs: {[p.symbol for p in config.pairs]}")
        logger.info(f"Storage format: {config.storage.format}")
        logger.info(f"Storage path: {config.storage.path}")

        # Create scheduler
        scheduler = PipelineScheduler(config)

        if args.once:
            # Run once and exit
            logger.info("Running pipeline once...")
            stats = scheduler.run_once()
            logger.info(f"Pipeline completed: {stats}")
            return 0
        else:
            # Start scheduled runs
            logger.info(f"Starting scheduled pipeline (every {config.scheduler.interval_minutes} minutes)")
            scheduler.start()
            return 0

    except FileNotFoundError as e:
        logger.error(f"Configuration file not found: {e}")
        return 1
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        return 1
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        return 0
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
