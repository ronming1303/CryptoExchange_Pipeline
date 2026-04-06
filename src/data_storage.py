"""Data storage module for saving cryptocurrency data to CSV/Parquet files."""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Literal, Optional

import pandas as pd

from .config import StorageConfig

logger = logging.getLogger(__name__)


class DataStorage:
    """Handles saving data to CSV or Parquet files with date partitioning."""

    def __init__(self, config: StorageConfig):
        """Initialize the data storage.

        Args:
            config: Storage configuration.
        """
        self.config = config
        self.base_path = Path(config.path)
        self.format = config.format.lower()

        # Create base directory if it doesn't exist
        self.base_path.mkdir(parents=True, exist_ok=True)

    def save(
        self,
        df: pd.DataFrame,
        exchange: str,
        pair: str,
        date: Optional[datetime] = None
    ) -> Path:
        """Save data to file with date partitioning.

        Args:
            df: DataFrame to save.
            exchange: Exchange name.
            pair: Trading pair symbol.
            date: Date for partitioning. If None, uses current date.

        Returns:
            Path to the saved file.
        """
        if df.empty:
            logger.warning(f"Empty DataFrame for {exchange}/{pair}, skipping save")
            return None

        if date is None:
            date = datetime.utcnow()

        # Create date-partitioned directory
        date_str = date.strftime("%Y-%m-%d")
        partition_path = self.base_path / date_str
        partition_path.mkdir(parents=True, exist_ok=True)

        # Generate filename
        filename = f"{exchange}_{pair.replace('-', '_')}"
        if self.format == "parquet":
            filepath = partition_path / f"{filename}.parquet"
        else:
            filepath = partition_path / f"{filename}.csv"

        # Check if file exists and merge data
        if filepath.exists():
            existing_df = self._read_file(filepath)
            df = self._merge_and_dedupe(existing_df, df)

        # Save file
        self._write_file(df, filepath)
        logger.info(f"Saved {len(df)} rows to {filepath}")

        return filepath

    def save_batch(
        self,
        results: list,
        date: Optional[datetime] = None
    ) -> list[Path]:
        """Save multiple fetch results.

        Args:
            results: List of FetchResult objects.
            date: Date for partitioning.

        Returns:
            List of saved file paths.
        """
        saved_paths = []

        for result in results:
            if not result.success or result.data is None or result.data.empty:
                continue

            exchange = result.exchange or "aggregated"
            pair = result.pair

            path = self.save(result.data, exchange, pair, date)
            if path:
                saved_paths.append(path)

        return saved_paths

    def _read_file(self, filepath: Path) -> pd.DataFrame:
        """Read existing file.

        Args:
            filepath: Path to the file.

        Returns:
            DataFrame from the file.
        """
        try:
            if filepath.suffix == ".parquet":
                return pd.read_parquet(filepath)
            else:
                return pd.read_csv(filepath, parse_dates=["timestamp", "fetched_at"])
        except Exception as e:
            logger.warning(f"Failed to read existing file {filepath}: {e}")
            return pd.DataFrame()

    def _write_file(self, df: pd.DataFrame, filepath: Path) -> None:
        """Write DataFrame to file.

        Args:
            df: DataFrame to write.
            filepath: Path to save to.
        """
        if filepath.suffix == ".parquet":
            df.to_parquet(filepath, index=False, engine="pyarrow")
        else:
            df.to_csv(filepath, index=False)

    def _merge_and_dedupe(
        self,
        existing_df: pd.DataFrame,
        new_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Merge existing and new data, removing duplicates.

        Args:
            existing_df: Existing DataFrame.
            new_df: New DataFrame to merge.

        Returns:
            Merged and deduplicated DataFrame.
        """
        if existing_df.empty:
            return new_df
        if new_df.empty:
            return existing_df

        # Concatenate
        combined = pd.concat([existing_df, new_df], ignore_index=True)

        # Deduplicate based on timestamp and exchange
        dedup_columns = ["timestamp", "exchange", "pair"]
        available_dedup_cols = [c for c in dedup_columns if c in combined.columns]

        if available_dedup_cols:
            combined = combined.drop_duplicates(subset=available_dedup_cols, keep="last")

        # Sort by timestamp
        if "timestamp" in combined.columns:
            combined = combined.sort_values("timestamp", ascending=False)

        return combined.reset_index(drop=True)

    def list_files(
        self,
        date: Optional[datetime] = None,
        exchange: Optional[str] = None,
        pair: Optional[str] = None
    ) -> list[Path]:
        """List saved data files.

        Args:
            date: Filter by date.
            exchange: Filter by exchange.
            pair: Filter by trading pair.

        Returns:
            List of matching file paths.
        """
        pattern = f"**/*.{self.format}" if self.format == "parquet" else "**/*.csv"
        files = list(self.base_path.glob(pattern))

        if date:
            date_str = date.strftime("%Y-%m-%d")
            files = [f for f in files if date_str in str(f)]

        if exchange:
            files = [f for f in files if exchange in f.name]

        if pair:
            pair_normalized = pair.replace("-", "_")
            files = [f for f in files if pair_normalized in f.name]

        return sorted(files)

    def read_data(
        self,
        date: Optional[datetime] = None,
        exchange: Optional[str] = None,
        pair: Optional[str] = None
    ) -> pd.DataFrame:
        """Read and combine data from files.

        Args:
            date: Filter by date.
            exchange: Filter by exchange.
            pair: Filter by trading pair.

        Returns:
            Combined DataFrame from matching files.
        """
        files = self.list_files(date, exchange, pair)

        if not files:
            return pd.DataFrame()

        dfs = []
        for f in files:
            try:
                df = self._read_file(f)
                dfs.append(df)
            except Exception as e:
                logger.warning(f"Failed to read {f}: {e}")

        if not dfs:
            return pd.DataFrame()

        return pd.concat(dfs, ignore_index=True)
