from aiohttp import web


async def home(_: web.Request):
    return web.Response(text="Welcome to jticker-controller (TBD)")
