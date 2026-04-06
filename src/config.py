"""Configuration management for the Crypto Exchange Pipeline."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml
from dotenv import load_dotenv


@dataclass
class TradingPair:
    """Represents a trading pair."""
    base: str
    quote: str

    @property
    def symbol(self) -> str:
        """Return the symbol in format 'base-quote'."""
        return f"{self.base}-{self.quote}"


@dataclass
class SchedulerConfig:
    """Scheduler configuration."""
    interval_minutes: int = 15


@dataclass
class StorageConfig:
    """Storage configuration."""
    format: str = "parquet"
    path: str = "./data/raw"


@dataclass
class Config:
    """Main configuration class."""
    api_key: str
    exchanges: list[str] = field(default_factory=list)
    pairs: list[TradingPair] = field(default_factory=list)
    scheduler: SchedulerConfig = field(default_factory=SchedulerConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)


def load_config(config_path: Optional[str] = None) -> Config:
    """Load configuration from YAML file and environment variables.

    Args:
        config_path: Path to the config.yaml file. If None, uses default location.

    Returns:
        Config object with all settings.

    Raises:
        ValueError: If CRYPTOCOMPARE_API_KEY is not set.
        FileNotFoundError: If config file doesn't exist.
    """
    # Load environment variables from .env file
    load_dotenv()

    # Get API key from environment
    api_key = os.getenv("CRYPTOCOMPARE_API_KEY")
    if not api_key:
        raise ValueError("CRYPTOCOMPARE_API_KEY environment variable is not set")

    # Determine config file path
    if config_path is None:
        config_path = Path(__file__).parent.parent / "config.yaml"
    else:
        config_path = Path(config_path)

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    # Load YAML config
    with open(config_path, "r") as f:
        yaml_config = yaml.safe_load(f)

    # Parse exchanges
    exchanges = yaml_config.get("exchanges", [])

    # Parse trading pairs
    pairs = [
        TradingPair(base=p["base"], quote=p["quote"])
        for p in yaml_config.get("pairs", [])
    ]

    # Parse scheduler config
    scheduler_yaml = yaml_config.get("scheduler", {})
    scheduler_config = SchedulerConfig(
        interval_minutes=scheduler_yaml.get("interval_minutes", 15)
    )

    # Parse storage config
    storage_yaml = yaml_config.get("storage", {})
    storage_config = StorageConfig(
        format=storage_yaml.get("format", "parquet"),
        path=storage_yaml.get("path", "./data/raw")
    )

    return Config(
        api_key=api_key,
        exchanges=exchanges,
        pairs=pairs,
        scheduler=scheduler_config,
        storage=storage_config
    )
