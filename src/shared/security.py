"""
shared/security.py
Enhanced security middleware with configurable threat protection
"""

import logging
from typing import Callable, Awaitable
from aiohttp import web

# Configure logger
log = logging.getLogger(__name__)

# Default security configuration (can be overridden via environment/config)
DEFAULT_SECURITY_CONFIG = {
    "blocked_scanners": ["censysinspect", "nmap", "nessus", "acunetix", "sqlmap"],
    "rate_limit": 10,  # Requests per second
    "security_headers": {
        "X-Content-Type-Options": "nosniff",
        "X-Frame-Options": "DENY",
        "Content-Security-Policy": "default-src 'self'",
        "Strict-Transport-Security": "max-age=63072000; includeSubDomains"
    }
}

def security_middleware_factory(config: dict = None) -> Callable:
    """
    Factory function that creates security middleware with configurable settings
    
    Args:
        config: Security configuration dictionary. Uses DEFAULT_SECURITY_CONFIG if None.
        
    Returns:
        Middleware function ready for use in aiohttp application
    """
    # Merge default config with any provided overrides
    security_config = {**DEFAULT_SECURITY_CONFIG, **(config or {})}
    
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
            if any(scanner in user_agent for scanner in security_config["blocked_scanners"]):
                log.warning(f"Blocked security scanner: {user_agent}")
                return web.Response(status=403, text="Forbidden")
            
            # 3. Rate limiting (placeholder - implement Redis-based solution later)
            if await should_rate_limit(request, security_config["rate_limit"]):
                log.warning(f"Rate limit exceeded by {request.remote}")
                return web.Response(status=429, text="Too Many Requests")
            
            # 4. Process request with next handler
            response = await handler(request)
            
            # 5. Add security headers to all responses
            response.headers.update(security_config["security_headers"])
            
            return response
            
        return middleware_handler
        
    return security_middleware

async def should_rate_limit(request: web.Request, limit: int) -> bool:
    """
    Placeholder for rate limiting functionality
    
    Note: For production, implement Redis-based rate limiting with:
    - IP-based tracking
    - Token bucket algorithm
    - Distributed counting
    
    Args:
        request: Incoming request
        limit: Allowed requests per second
        
    Returns:
        True if request should be rate limited
    """
    # TODO: Implement proper rate limiting with Redis
    return False