"""CryptoCompare API client for fetching cryptocurrency market data."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class CryptoCompareAPIError(Exception):
    """Custom exception for CryptoCompare API errors."""
    pass


class CryptoCompareClient:
    """Client for interacting with the CryptoCompare REST API."""

    BASE_URL = "https://min-api.cryptocompare.com/data"

    # Map exchange names to CryptoCompare format
    EXCHANGE_MAP = {
        "binance": "Binance",
        "coinbase": "Coinbase",
        "kraken": "Kraken",
        "okx": "OKX",
        "bybit": "Bybit",
        "bitfinex": "Bitfinex",
        "huobi": "Huobi",
        "kucoin": "KuCoin",
    }

    def __init__(self, api_key: str):
        """Initialize the CryptoCompare API client.

        Args:
            api_key: CryptoCompare API key for authentication.
        """
        self.api_key = api_key
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """Create a requests session with retry logic."""
        session = requests.Session()

        # Configure retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # Set default headers
        session.headers.update({
            "Accept": "application/json",
            "authorization": f"Apikey {self.api_key}"
        })

        return session

    def _make_request(self, endpoint: str, params: Optional[dict] = None) -> dict[str, Any]:
        """Make a request to the CryptoCompare API.

        Args:
            endpoint: API endpoint path.
            params: Query parameters.

        Returns:
            JSON response as dictionary.

        Raises:
            CryptoCompareAPIError: If the API request fails.
        """
        url = f"{self.BASE_URL}/{endpoint.lstrip('/')}"

        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            # CryptoCompare returns Response: "Error" for some errors
            if data.get("Response") == "Error":
                raise CryptoCompareAPIError(data.get("Message", "Unknown error"))

            return data

        except requests.exceptions.HTTPError as e:
            if response.status_code == 401:
                raise CryptoCompareAPIError("Invalid API key") from e
            elif response.status_code == 429:
                raise CryptoCompareAPIError("Rate limit exceeded") from e
            else:
                raise CryptoCompareAPIError(f"HTTP error {response.status_code}: {e}") from e
        except requests.exceptions.ConnectionError as e:
            raise CryptoCompareAPIError(f"Connection error: {e}") from e
        except requests.exceptions.Timeout as e:
            raise CryptoCompareAPIError(f"Request timeout: {e}") from e
        except requests.exceptions.RequestException as e:
            raise CryptoCompareAPIError(f"Request failed: {e}") from e

    def _normalize_exchange(self, exchange: str) -> str:
        """Normalize exchange name to CryptoCompare format."""
        return self.EXCHANGE_MAP.get(exchange.lower(), exchange)

    def get_price_multi_full(
        self,
        fsyms: list[str],
        tsyms: list[str],
        exchange: Optional[str] = None
    ) -> dict[str, Any]:
        """Get full price data for multiple symbols.

        Args:
            fsyms: List of from symbols (e.g., ['BTC', 'ETH']).
            tsyms: List of to symbols (e.g., ['USD']).
            exchange: Optional exchange name.

        Returns:
            Full price data including price, volume, etc.
        """
        params = {
            "fsyms": ",".join([s.upper() for s in fsyms]),
            "tsyms": ",".join([s.upper() for s in tsyms]),
        }

        if exchange:
            params["e"] = self._normalize_exchange(exchange)

        return self._make_request("pricemultifull", params)

    def get_price_by_exchange(
        self,
        base: str,
        quote: str,
        exchange: str
    ) -> dict[str, Any]:
        """Get price for a specific exchange.

        Args:
            base: Base currency (e.g., 'BTC').
            quote: Quote currency (e.g., 'USD').
            exchange: Exchange name.

        Returns:
            Price data for the specific exchange.
        """
        params = {
            "fsym": base.upper(),
            "tsym": quote.upper(),
            "e": self._normalize_exchange(exchange),
        }

        return self._make_request("generateAvg", params)

    def get_top_exchanges(
        self,
        base: str,
        quote: str,
        limit: int = 10
    ) -> list[dict[str, Any]]:
        """Get top exchanges by volume for a trading pair.

        Args:
            base: Base currency.
            quote: Quote currency.
            limit: Number of exchanges to return.

        Returns:
            List of exchange data sorted by volume.
        """
        params = {
            "fsym": base.upper(),
            "tsym": quote.upper(),
            "limit": limit
        }

        response = self._make_request("top/exchanges", params)
        return response.get("Data", [])

    def get_exchange_price(
        self,
        base: str,
        quote: str,
        exchange: str
    ) -> Optional[dict[str, Any]]:
        """Get current price from a specific exchange.

        Args:
            base: Base currency (e.g., 'btc').
            quote: Quote currency (e.g., 'usd').
            exchange: Exchange name (e.g., 'binance').

        Returns:
            Dictionary with price data or None if not available.
        """
        try:
            # Use pricemultifull with exchange filter
            response = self.get_price_multi_full(
                fsyms=[base],
                tsyms=[quote],
                exchange=exchange
            )

            raw_data = response.get("RAW", {})
            base_data = raw_data.get(base.upper(), {})
            quote_data = base_data.get(quote.upper(), {})

            if not quote_data:
                return None

            return {
                "timestamp": datetime.utcfromtimestamp(quote_data.get("LASTUPDATE", 0)),
                "exchange": exchange,
                "base": base.lower(),
                "quote": quote.lower(),
                "price": quote_data.get("PRICE"),
                "volume_24h": quote_data.get("VOLUME24HOUR"),
                "volume_24h_to": quote_data.get("VOLUME24HOURTO"),
                "open_24h": quote_data.get("OPEN24HOUR"),
                "high_24h": quote_data.get("HIGH24HOUR"),
                "low_24h": quote_data.get("LOW24HOUR"),
                "change_24h": quote_data.get("CHANGE24HOUR"),
                "change_pct_24h": quote_data.get("CHANGEPCT24HOUR"),
                "market": quote_data.get("MARKET"),
            }

        except CryptoCompareAPIError as e:
            logger.warning(f"Failed to get price for {base}/{quote} on {exchange}: {e}")
            return None

    def get_histominute(
        self,
        base: str,
        quote: str,
        exchange: Optional[str] = None,
        limit: int = 60,
        to_ts: Optional[int] = None
    ) -> list[dict[str, Any]]:
        """Get historical minute data.

        Args:
            base: Base currency.
            quote: Quote currency.
            exchange: Optional exchange name.
            limit: Number of data points (max 2000).
            to_ts: Unix timestamp for end time.

        Returns:
            List of OHLCV data points.
        """
        params = {
            "fsym": base.upper(),
            "tsym": quote.upper(),
            "limit": min(limit, 2000)
        }

        if exchange:
            params["e"] = self._normalize_exchange(exchange)

        if to_ts:
            params["toTs"] = to_ts

        response = self._make_request("v2/histominute", params)
        return response.get("Data", {}).get("Data", [])

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
            target_ts: Unix timestamp for the target time.

        Returns:
            Dictionary with price data or None if not available.
        """
        try:
            # Get 1 data point ending at target_ts
            data = self.get_histominute(
                base=base,
                quote=quote,
                exchange=exchange,
                limit=1,
                to_ts=target_ts
            )

            if not data:
                return None

            # Get the data point closest to target timestamp
            point = data[-1]

            return {
                "timestamp": datetime.utcfromtimestamp(point.get("time", 0)),
                "exchange": exchange,
                "base": base.lower(),
                "quote": quote.lower(),
                "price": point.get("close"),  # Use close price
                "open": point.get("open"),
                "high": point.get("high"),
                "low": point.get("low"),
                "close": point.get("close"),
                "volume_from": point.get("volumefrom"),
                "volume_to": point.get("volumeto"),
            }

        except CryptoCompareAPIError as e:
            logger.warning(f"Failed to get historical price for {base}/{quote} on {exchange}: {e}")
            return None

    def health_check(self) -> bool:
        """Check if the API is accessible and the key is valid.

        Returns:
            True if the API is accessible.
        """
        try:
            self.get_price_multi_full(["BTC"], ["USD"])
            return True
        except CryptoCompareAPIError:
            return False
