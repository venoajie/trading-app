"""
shared/security.py
Security middleware integrated with centralized configuration
"""

import logging
from typing import Callable, Awaitable, Dict, Any
from aiohttp import web

# Configure logger
log = logging.getLogger(__name__)

def security_middleware_factory(config: Dict[str, Any]) -> Callable:
    """
    Factory function that creates security middleware using provided config
    
    Args:
        config: Dictionary with security configuration
            - blocked_scanners: List of scanner user agents to block
            - rate_limit: Requests per second limit
            - security_headers: Dictionary of security headers
    """
    # Extract configuration with defaults
    blocked_scanners = config.get("blocked_scanners", [])
    rate_limit = config.get("rate_limit", 100)
    security_headers = config.get("security_headers", {})
    
    async def security_middleware(
        app: web.Application, 
        handler: Callable
    ) -> Callable[[web.Request], Awaitable[web.StreamResponse]]:
        """
        Core security middleware implementation with:
        - Scanner blocking
        - Path restrictions
        - Security headers
        - Rate limiting placeholder
        
        Args:
            app: aiohttp Application instance
            handler: Next handler in middleware chain
            
        Returns:
            Response handler with security enhancements
        """
        async def middleware_handler(request: web.Request) -> web.StreamResponse:
            """
            Handles incoming requests with security checks
            """
            # 1. Block requests to root path
            if request.path == '/':
                log.warning(f"Blocked root path access from {request.remote}")
                return web.Response(status=404, text="Not Found")
            
            # 2. Block known security scanners
            user_agent = request.headers.get('User-Agent', '').lower()
            if any(scanner in user_agent for scanner in blocked_scanners):
                log.warning(f"Blocked security scanner: {user_agent}")
                return web.Response(status=403, text="Forbidden")
            
            # 3. Rate limiting (implemented in next version)
            if await should_rate_limit(request, rate_limit):
                log.warning(f"Rate limit exceeded by {request.remote}")
                return web.Response(status=429, text="Too Many Requests")
            
            # 4. Process request with next handler
            response = await handler(request)
            
            # 5. Add security headers to all responses
            response.headers.update(security_headers)
            
            return response
            
        return middleware_handler
        
    return security_middleware

async def should_rate_limit(request: web.Request, limit: int) -> bool:
    """
    Placeholder for rate limiting functionality
    
    Note: Production implementation will use Redis-based rate limiting
    
    Args:
        request: Incoming request
        limit: Allowed requests per second
        
    Returns:
        True if request should be rate limited
    """
    # TODO: Implement Redis-based rate limiting
    return False