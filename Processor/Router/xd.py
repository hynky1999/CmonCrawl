import asyncio
from multiprocessing.sharedctypes import Value


async def locked():
    lock = asyncio.Lock()
    async with lock:
        raise ValueError("Locked")



async def main():
    await locked()

asyncio.run(main())

