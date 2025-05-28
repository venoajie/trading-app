"""
Combined utilities for streaming operations
"""

import os
import logging
from pathlib import Path

def extract_currency(instrument: str) -> str:
    """Extract currency from instrument name"""
    return instrument.split("-")[0].upper()

def get_data_path(filename: str) -> str:
    """Generate standardized data path"""
    data_dir = os.getenv("DATA_DIR", "/data")
    return str(Path(data_dir) / f"{filename}.db")

def log_error(error: Exception):
    """Centralized error logging"""
    logging.error(f"Error: {error}", exc_info=True)