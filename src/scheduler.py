"""Scheduler module for periodic data fetching using APScheduler."""

import logging
import signal
import sys
from datetime import datetime
from typing import Callable, Optional

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger

from .config import Config
from .data_fetcher import DataFetcher, get_quarter_hour_window
from .data_storage import DataStorage

logger = logging.getLogger(__name__)


class PipelineScheduler:
    """Scheduler for running the data pipeline at regular intervals."""

    def __init__(self, config: Config):
        """Initialize the scheduler.

        Args:
            config: Configuration object.
        """
        self.config = config
        self.fetcher = DataFetcher(config)
        self.storage = DataStorage(config.storage)
        self.scheduler = BlockingScheduler()
        self._setup_signal_handlers()

    def _setup_signal_handlers(self) -> None:
        """Setup signal handlers for graceful shutdown."""
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

    def _handle_shutdown(self, signum, frame) -> None:
        """Handle shutdown signals gracefully."""
        logger.info("Shutdown signal received, stopping scheduler...")
        self.stop()
        sys.exit(0)

    def _find_missing_timestamps(self, lookback_hours: int = 2) -> list:
        """Find 15-minute timestamps in the lookback window that are missing from storage.

        Checks each (exchange, pair) combo and collects timestamps not yet stored.
        Returns the union of all missing timestamps (deduplicated).
        """
        window = get_quarter_hour_window(lookback_hours)
        missing = set()

        for pair in self.config.pairs:
            for exchange in self.config.exchanges:
                # Collect existing timestamps across all relevant dates in the window
                dates = set(dt.date() for dt, _ in window)
                existing = set()
                for d in dates:
                    existing |= self.storage.get_timestamps(
                        exchange, pair.symbol, datetime(d.year, d.month, d.day)
                    )

                for target_dt, target_ts in window:
                    if target_dt not in existing:
                        missing.add((target_dt, target_ts))

        return sorted(missing, key=lambda x: x[0])

    def run_once(self) -> dict:
        """Run the pipeline once, backfilling any missing 15-minute slots in the last 2 hours.

        Returns:
            Dictionary with run statistics.
        """
        start_time = datetime.utcnow()
        logger.info(f"Starting data fetch at {start_time}")

        # Find timestamps not yet in storage
        missing_timestamps = self._find_missing_timestamps(lookback_hours=2)

        if not missing_timestamps:
            logger.info("No missing timestamps found in the last 2 hours, skipping fetch")
            return {
                "start_time": start_time.isoformat(),
                "end_time": datetime.utcnow().isoformat(),
                "duration_seconds": 0,
                "total_fetches": 0,
                "successful": 0,
                "failed": 0,
                "total_rows": 0,
                "files_saved": 0,
            }

        logger.info(f"Found {len(missing_timestamps)} missing timestamp(s): {[str(dt) for dt, _ in missing_timestamps]}")

        # Fetch only the missing timestamps
        results = self.fetcher.fetch_all_configured(timestamps=missing_timestamps)

        # Save results
        saved_paths = self.storage.save_batch(results)

        # Calculate statistics
        successful = sum(1 for r in results if r.success)
        failed = len(results) - successful
        total_rows = sum(len(r.data) for r in results if r.success and r.data is not None)

        end_time = datetime.utcnow()
        duration = (end_time - start_time).total_seconds()

        stats = {
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_seconds": duration,
            "total_fetches": len(results),
            "successful": successful,
            "failed": failed,
            "total_rows": total_rows,
            "files_saved": len(saved_paths)
        }

        logger.info(
            f"Fetch completed: {successful}/{len(results)} successful, "
            f"{total_rows} rows, {len(saved_paths)} files saved in {duration:.2f}s"
        )

        return stats

    def _scheduled_job(self) -> None:
        """Job that runs on schedule."""
        try:
            self.run_once()
        except Exception as e:
            logger.error(f"Scheduled job failed: {e}", exc_info=True)

    def start(self) -> None:
        """Start the scheduler."""
        interval_minutes = self.config.scheduler.interval_minutes

        logger.info(f"Starting scheduler with {interval_minutes} minute interval")

        # Add the job
        self.scheduler.add_job(
            self._scheduled_job,
            trigger=IntervalTrigger(minutes=interval_minutes),
            id="data_fetch_job",
            name="Crypto Exchange Data Fetch",
            replace_existing=True,
            next_run_time=datetime.now()  # Run immediately on start
        )

        # Start the scheduler
        logger.info("Scheduler started. Press Ctrl+C to stop.")
        self.scheduler.start()

    def stop(self) -> None:
        """Stop the scheduler."""
        if self.scheduler.running:
            self.scheduler.shutdown(wait=False)
            logger.info("Scheduler stopped")
