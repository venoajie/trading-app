# trading_app/shared/error_handling.py
"""Enhanced error handling with verbose logging and Redis reporting"""

import asyncio
import orjson
import traceback
import logging
import sys
from datetime import datetime
from typing import Optional, Any

# Third-party imports
from loguru import logger as log

# Application imports
from shared import template

def parse_error_message(
    error: Exception,
    context: Optional[str] = None,
    additional_info: Optional[dict] = None,
) -> str:
    """
    Capture and format detailed error information including:
    - Full stack trace
    - Error type and message
    - Timestamp
    - Contextual information
    - Additional metadata
    
    Args:
        error: Exception instance
        context: Human-readable context description
        additional_info: Additional debugging information
        
    Returns:
        Formatted error string
    """
    # Capture full traceback
    exc_type, exc_value, exc_traceback = sys.exc_info()
    full_traceback = "".join(traceback.format_exception(exc_type, exc_value, exc_traceback))
    
    # Basic error information
    error_info = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "error_type": type(error).__name__,
        "error_message": str(error),
        "context": context or "Unspecified error context",
        "stack_trace": full_traceback,
    }
    
    # Add additional metadata if provided
    if additional_info:
        error_info["additional_info"] = additional_info
    
    # Format as JSON string for readability
    formatted_error = orjson.dumps(
        error_info, 
        option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS
    ).decode("utf-8")
    
    # Log critical error with traceback
    log.critical(f"ERROR DETAILS:\n{formatted_error}")
    
    return formatted_error

async def parse_error_message_with_redis(
    client_redis: Any,
    error: Exception,
    context: Optional[str] = None,
    additional_info: Optional[dict] = None,
) -> None:
    """
    Report detailed error information to Redis pub/sub channel
    Includes stack trace, context, and additional metadata
    
    Args:
        client_redis: Redis client instance
        error: Exception instance
        context: Human-readable context description
        additional_info: Additional debugging information
    """
    channel = "error"
    
    try:
        # Create message template
        result = template.redis_message_template()
        
        # Get detailed error information
        error_details = parse_error_message(
            error,
            context,
            additional_info
        )
        
        # Prepare publication message
        pub_message = {
            "error_details": orjson.loads(error_details),  # Parse back to dict
            "service": "receiver",
            "severity": "CRITICAL"
        }
        
        result["params"].update({
            "channel": channel,
            "data": pub_message
        })
        
        # Publish to Redis
        await client_redis.publish(
            channel,
            orjson.dumps(result)
        )
        
        log.info(f"Error reported to Redis channel '{channel}'")
        
    except Exception as inner_error:
        # Fallback to basic logging if Redis fails
        log.critical("FAILED TO REPORT ERROR TO REDIS")
        parse_error_message(
            inner_error,
            "Error handling failure",
            {"original_error": str(error)}
        )