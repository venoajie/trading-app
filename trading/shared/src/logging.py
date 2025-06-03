import logging

def setup_logging(name: str, log_config: dict) -> logging.Logger:
    """Configure logging system"""
    logger = logging.getLogger(name)
    logger.setLevel(log_config.get("level", "INFO"))
    
    formatter = logging.Formatter(
        log_config.get("format", "%(asctime)s | %(levelname)s | %(name)s | %(message)s"),
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    return logger