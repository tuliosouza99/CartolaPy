import asyncio
import weakref

import aiohttp
from aiolimiter import AsyncLimiter


class RequestHandler:
    DEFAULT_TIMEOUT = aiohttp.ClientTimeout(total=30, connect=10)

    def __init__(self):
        self.session = aiohttp.ClientSession(timeout=self.DEFAULT_TIMEOUT)
        self.rate_limiter = AsyncLimiter(10, 1)
        self._finalizer = weakref.finalize(self, self._cleanup_session, self.session)

    @staticmethod
    def _cleanup_session(session: aiohttp.ClientSession):
        """Static method to clean up session - called by finalizer"""
        if session and not session.closed:
            try:
                loop = asyncio.get_event_loop()
                if loop.is_closed():
                    raise RuntimeError("Event loop is closed")
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

            if not loop.is_running():
                loop.run_until_complete(session.close())
            else:
                asyncio.create_task(session.close())

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

    async def make_get_request(self, url: str):
        async with self.rate_limiter:
            async with self.session.get(url) as response:
                page_json = await response.json()

        return page_json
