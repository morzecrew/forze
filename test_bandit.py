import contextlib

async def test():
    with contextlib.suppress(Exception):
        await asyncio.sleep(1)
