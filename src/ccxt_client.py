"""CCXT API client for fetching cryptocurrency market data from exchanges."""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any, Optional

import ccxt

logger = logging.getLogger(__name__)


class CCXTAPIError(Exception):
    """Custom exception for CCXT API errors."""
    pass


class CCXTClient:
    """Client for fetching data from multiple exchanges using CCXT."""

    # Map exchange names to CCXT exchange classes
    EXCHANGE_MAP = {
        "binance": ccxt.binance,
        "coinbase": ccxt.coinbase,
        "kraken": ccxt.kraken,
        "okx": ccxt.okx,
        "bybit": ccxt.bybit,
        "bitfinex": ccxt.bitfinex,
    }

    def __init__(self, api_key: Optional[str] = None):
        """Initialize the CCXT client.

        Args:
            api_key: Optional API key (not needed for public endpoints).
        """
        self.api_key = api_key
        self._exchange_instances = {}

    def _get_exchange(self, exchange_name: str) -> ccxt.Exchange:
        """Get or create an exchange instance.

        Args:
            exchange_name: Exchange name (e.g., 'binance').

        Returns:
            CCXT exchange instance.

        Raises:
            CCXTAPIError: If exchange is not supported.
        """
        exchange_name_lower = exchange_name.lower()

        if exchange_name_lower not in self._exchange_instances:
            if exchange_name_lower not in self.EXCHANGE_MAP:
                raise CCXTAPIError(f"Exchange '{exchange_name}' is not supported")

            try:
                exchange_class = self.EXCHANGE_MAP[exchange_name_lower]
                # Initialize exchange with minimal config
                self._exchange_instances[exchange_name_lower] = exchange_class({
                    'enableRateLimit': True,  # Respect rate limits
                    'timeout': 30000,  # 30 seconds timeout
                })
            except Exception as e:
                raise CCXTAPIError(f"Failed to initialize {exchange_name}: {e}") from e

        return self._exchange_instances[exchange_name_lower]

    def _normalize_symbol(self, base: str, quote: str) -> str:
        """Normalize trading pair to CCXT format.

        Args:
            base: Base currency (e.g., 'btc').
            quote: Quote currency (e.g., 'usdt').

        Returns:
            Normalized symbol (e.g., 'BTC/USDT').
        """
        return f"{base.upper()}/{quote.upper()}"

    def get_historical_price_at_time(
        self,
        base: str,
        quote: str,
        exchange: str,
        target_ts: int
    ) -> Optional[dict[str, Any]]:
        """Get historical price at a specific timestamp.

        Args:
            base: Base currency (e.g., 'btc').
            quote: Quote currency (e.g., 'usdt').
            exchange: Exchange name (e.g., 'binance').
            target_ts: Unix timestamp for the target time (seconds).

        Returns:
            Dictionary with price data or None if not available.
        """
        try:
            exchange_instance = self._get_exchange(exchange)
            symbol = self._normalize_symbol(base, quote)

            # Convert timestamp to milliseconds and fetch 1 minute of data
            # We fetch 2 candles to ensure we get the one closest to target time
            since_ms = (target_ts - 60) * 1000  # 1 minute before target

            # Fetch OHLCV data (1m timeframe)
            # Returns: [[timestamp, open, high, low, close, volume], ...]
            ohlcv = exchange_instance.fetch_ohlcv(
                symbol=symbol,
                timeframe='1m',
                since=since_ms,
                limit=2
            )

            if not ohlcv:
                logger.warning(f"No OHLCV data returned for {symbol} on {exchange}")
                return None

            # Find the candle closest to target timestamp
            target_ms = target_ts * 1000
            closest_candle = None
            min_diff = float('inf')

            for candle in ohlcv:
                candle_ts = candle[0]
                diff = abs(candle_ts - target_ms)
                if diff < min_diff:
                    min_diff = diff
                    closest_candle = candle

            if closest_candle is None:
                return None

            # Parse OHLCV data: [timestamp, open, high, low, close, volume]
            timestamp_ms, open_price, high, low, close, volume = closest_candle

            return {
                "timestamp": datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc),
                "exchange": exchange,
                "base": base.lower(),
                "quote": quote.lower(),
                "price": close,  # Use close price as the main price
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
                "volume_from": volume,  # In CCXT, volume is typically in base currency
                "volume_to": volume * close if close and volume else None,  # Approximate
            }

        except ccxt.NetworkError as e:
            logger.error(f"Network error fetching {base}/{quote} from {exchange}: {e}")
            raise CCXTAPIError(f"Network error: {e}") from e
        except ccxt.ExchangeError as e:
            logger.error(f"Exchange error fetching {base}/{quote} from {exchange}: {e}")
            raise CCXTAPIError(f"Exchange error: {e}") from e
        except Exception as e:
            logger.error(f"Unexpected error fetching {base}/{quote} from {exchange}: {e}")
            raise CCXTAPIError(f"Unexpected error: {e}") from e

    def health_check(self) -> bool:
        """Check if the client can connect to exchanges.

        Returns:
            True if at least one exchange is accessible.
        """
        try:
            # Try to fetch BTC/USDT from Binance as a health check
            exchange = self._get_exchange('binance')
            ticker = exchange.fetch_ticker('BTC/USDT')
            return ticker is not None
        except Exception as e:
            logger.warning(f"Health check failed: {e}")
            return False
