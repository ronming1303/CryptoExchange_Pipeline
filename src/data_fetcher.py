"""Data fetching logic for cryptocurrency exchange data."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Optional

import pandas as pd

from .config import Config, TradingPair
from .cryptocompare_client import CryptoCompareClient, CryptoCompareAPIError

logger = logging.getLogger(__name__)


def get_quarter_hour_window(lookback_hours: int = 2) -> list[tuple[datetime, int]]:
    """Get all 15-minute timestamps in the lookback window (oldest first).

    Args:
        lookback_hours: How many hours back to look.

    Returns:
        List of (datetime, unix_timestamp) tuples, oldest first.
    """
    now = datetime.now(timezone.utc)
    current_minute = (now.minute // 15) * 15
    end = now.replace(minute=current_minute, second=0, microsecond=0)

    count = lookback_hours * 4  # 4 quarter-hours per hour
    result = []
    for i in range(count - 1, -1, -1):
        ts = end - timedelta(minutes=15 * i)
        result.append((ts, int(ts.timestamp())))
    return result


def get_last_quarter_hour_timestamp() -> tuple[datetime, int]:
    """Get the timestamp of the last quarter hour (00, 15, 30, 45 minutes).

    Returns:
        Tuple of (datetime object, unix timestamp).
    """
    now = datetime.now(timezone.utc)
    # Round down to last 15-minute mark
    minute = (now.minute // 15) * 15
    target = now.replace(minute=minute, second=0, microsecond=0)
    return target, int(target.timestamp())


@dataclass
class FetchResult:
    """Result of a data fetch operation."""
    success: bool
    data: Optional[pd.DataFrame] = None
    error: Optional[str] = None
    exchange: Optional[str] = None
    pair: Optional[str] = None
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now(timezone.utc)


class DataFetcher:
    """Fetches cryptocurrency price data from multiple exchanges."""

    def __init__(self, config: Config):
        """Initialize the data fetcher.

        Args:
            config: Configuration object.
        """
        self.config = config
        self.client = CryptoCompareClient(api_key=config.api_key)

    def fetch_historical_price(
        self,
        pair: TradingPair,
        exchange: str,
        target_ts: int
    ) -> FetchResult:
        """Fetch historical price for a specific timestamp.

        Args:
            pair: Trading pair to fetch.
            exchange: Exchange name.
            target_ts: Unix timestamp for target time.

        Returns:
            FetchResult with the data or error.
        """
        try:
            data = self.client.get_historical_price_at_time(
                base=pair.base,
                quote=pair.quote,
                exchange=exchange,
                target_ts=target_ts
            )

            if not data:
                logger.warning(f"No data returned for {pair.symbol} on {exchange}")
                return FetchResult(
                    success=True,
                    data=pd.DataFrame(),
                    exchange=exchange,
                    pair=pair.symbol
                )

            df = self._normalize_historical_data(data, pair, exchange)
            return FetchResult(
                success=True,
                data=df,
                exchange=exchange,
                pair=pair.symbol
            )

        except CryptoCompareAPIError as e:
            logger.error(f"Failed to fetch {pair.symbol} from {exchange}: {e}")
            return FetchResult(
                success=False,
                error=str(e),
                exchange=exchange,
                pair=pair.symbol
            )

    def fetch_all_configured(
        self,
        timestamps: Optional[list[tuple[datetime, int]]] = None
    ) -> list[FetchResult]:
        """Fetch data for all configured exchanges and pairs.

        Args:
            timestamps: List of (datetime, unix_timestamp) tuples to fetch.
                        If None, fetches only the last quarter-hour.

        Returns:
            List of FetchResult objects.
        """
        if timestamps is None:
            target_dt, target_ts = get_last_quarter_hour_timestamp()
            timestamps = [(target_dt, target_ts)]

        results = []
        logger.info(f"Fetching {len(timestamps)} timestamp(s) across {len(self.config.pairs)} pairs / {len(self.config.exchanges)} exchanges")

        for target_dt, target_ts in timestamps:
            logger.info(f"Fetching data for timestamp: {target_dt.isoformat()}")
            for pair in self.config.pairs:
                for exchange in self.config.exchanges:
                    logger.info(f"Fetching {pair.symbol} from {exchange}...")
                    result = self.fetch_historical_price(pair, exchange, target_ts)
                    results.append(result)

                    if result.success:
                        row_count = len(result.data) if result.data is not None else 0
                        logger.info(f"Successfully fetched {row_count} rows for {pair.symbol} from {exchange}")
                    else:
                        logger.warning(f"Failed to fetch {pair.symbol} from {exchange}: {result.error}")

                    # Rate limit: wait 0.5 seconds between requests
                    time.sleep(0.5)

        return results

    def _normalize_historical_data(
        self,
        data: dict,
        pair: TradingPair,
        exchange: str
    ) -> pd.DataFrame:
        """Normalize historical price data into a DataFrame.

        Args:
            data: Raw data from the API.
            pair: Trading pair.
            exchange: Exchange name.

        Returns:
            Normalized DataFrame.
        """
        if not data:
            return pd.DataFrame()

        row = {
            "timestamp": data.get("timestamp", datetime.now(timezone.utc)),
            "exchange": exchange,
            "pair": pair.symbol,
            "base": pair.base,
            "quote": pair.quote,
            "price": data.get("price"),
            "open": data.get("open"),
            "high": data.get("high"),
            "low": data.get("low"),
            "close": data.get("close"),
            "volume_from": data.get("volume_from"),
            "volume_to": data.get("volume_to"),
            "fetched_at": datetime.now(timezone.utc),
        }

        df = pd.DataFrame([row])

        # Ensure timestamp is datetime with UTC
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

        return df
