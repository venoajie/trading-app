"""
System-level utilities and path management
"""

import os
import logging
from pathlib import Path

def extract_currency_from_text(label: str) -> str:
    """
    Extract currency from instrument name
    Example: 'chart.trades.BTC-PERPETUAL.1' -> 'BTC'
    """
    parts = label.split(".")
    if len(parts) >= 3:
        instrument = parts[2]
    else:
        instrument = label
    
    return instrument.split("-")[0].upper()

def provide_path_for_file(
    filename: str, 
    currency: str = None, 
    status: str = None
) -> str:
    """Generate file paths with consistent structure"""
    base_dir = os.getenv("DATA_DIR", "/data")
    path = Path(base_dir)
    
    if currency and status:
        path = path / f"{filename}_{currency}_{status}.pkl"
    elif currency:
        path = path / f"{filename}_{currency}.pkl"
    else:
        path = path / f"{filename}.pkl"
    
    return str(path)