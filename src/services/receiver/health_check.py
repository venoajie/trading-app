from aiohttp import web

async def health_handler(request):
    return web.json_response({
        "status": "ok",
        "service": "receiver",
        "maintenance_mode": False
    })

def setup_routes(app):
    app.router.add_get("/health", health_handler)