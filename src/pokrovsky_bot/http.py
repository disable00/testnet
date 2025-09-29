from typing import Optional
import aiohttp
import asyncio
from .config import HEADERS

async def fetch_text(url: str, session: Optional[aiohttp.ClientSession] = None, retries: int = 3) -> str:
    timeout = aiohttp.ClientTimeout(total=60, connect=30)  # Увеличили таймауты
    
    for attempt in range(retries):
        try:
            if session is None:
                connector = aiohttp.TCPConnector(limit=100, limit_per_host=30)
                async with aiohttp.ClientSession(
                    timeout=timeout, 
                    headers=HEADERS,
                    connector=connector
                ) as s:
                    async with s.get(url) as r:
                        r.raise_for_status()
                        return await r.text()
            else:
                async with session.get(url) as r:
                    r.raise_for_status()
                    return await r.text()
                    
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt == retries - 1:
                raise e
            # Ждем перед повторной попыткой
            await asyncio.sleep(2 ** attempt)
    
    raise Exception(f"Failed to fetch {url} after {retries} attempts")
