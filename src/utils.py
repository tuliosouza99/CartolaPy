import aiohttp


async def get_page_json(url: str) -> dict:
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            page_json = await response.json()

    return page_json
