import asyncio
import weakref

import aiohttp
from aiolimiter import AsyncLimiter


class RequestHandler:
    def __init__(self):
        self.session = aiohttp.ClientSession()
        self.rate_limiter = AsyncLimiter(10, 1)
        self._finalizer = weakref.finalize(self, self._cleanup_session, self.session)

    @staticmethod
    def _cleanup_session(session: aiohttp.ClientSession):
        """Static method to clean up session - called by finalizer"""
        if session and not session.closed:
            # Create a new event loop if none exists
            try:
                loop = asyncio.get_event_loop()
                if loop.is_closed():
                    raise RuntimeError("Event loop is closed")
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

            # Close the session
            if not loop.is_running():
                loop.run_until_complete(session.close())
            else:
                # If loop is running, schedule the close operation
                asyncio.create_task(session.close())

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

    async def make_get_request(self, url: str):
        async with self.rate_limiter:
            async with self.session.get(url) as response:
                page_json = await response.json()

        return page_json
