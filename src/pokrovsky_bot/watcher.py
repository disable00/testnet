import hashlib
import random
import asyncio
from aiogram import Bot

from .db import sched_get_all, sched_upsert, hash_get, hash_set
from .sheets import resolve_google_url, sheets_meta, csv_url
from .site import get_links_from_site
from .http import fetch_text
from .utils import fmt_msk
from . import state
from .notify import notify_new_schedule, notify_schedule_changes


async def broadcast(bot: Bot, text: str):
    """
    Оставил утилиту на всякий случай (не используется для расписаний).
    Рассылает сообщение всем пользователям из таблицы users.
    """
    from .db import DB
    users = [row[0] for row in DB.execute("SELECT user_id FROM users").fetchall()]
    sem = asyncio.Semaphore(20)

    async def send(uid):
        async with sem:
            try:
                await bot.send_message(uid, text, disable_notification=True)
            except Exception:
                pass

    await asyncio.gather(*(send(uid) for uid in users))


async def check_once(bot: Bot):
    try:
        links = await get_links_from_site()
    except Exception:
        return

    known = sched_get_all()

    # 1) Новые даты расписаний
    for l in links:
        if l.date not in known:
            try:
                g_url = await resolve_google_url(l.url)
            except Exception:
                g_url = None
            sched_upsert(l.date, l.url, g_url)
            state.DOC_URL[l.date] = g_url or state.DOC_URL.get(l.date)

            # Точечная рассылка нового расписания по выбранным пользователем классам
            try:
                await notify_new_schedule(bot, l.date)
            except Exception:
                pass

    # 2) Проверка изменений по уже известным датам (хэши листов)
    for date, (link_url, g_url) in sched_get_all().items():
        if not g_url:
            try:
                g_url = await resolve_google_url(link_url)
                sched_upsert(date, link_url, g_url)
            except Exception:
                continue
        try:
            gid2title, gids = await sheets_meta(g_url)
        except Exception:
            continue

        for gid in (gid2title.keys() or gids):
            try:
                csv_text = await fetch_text(csv_url(g_url, gid))
            except Exception:
                continue

            h = hashlib.sha256(csv_text.encode("utf-8")).hexdigest()
            old = hash_get(date, gid)
            if old is None:
                hash_set(date, gid, gid2title.get(gid, ""), h)
            elif old != h:
                # зафиксировали изменение
                hash_set(date, gid, gid2title.get(gid, ""), h)
                tnow = fmt_msk(None)
                title = gid2title.get(gid, f"лист {gid}")
                diff_text = f"Изменения в листе «{title}»\n{tnow}"

                # Отправляем только тем, кто включил уведомления об изменениях
                try:
                    await notify_schedule_changes(bot, date, diff_text)
                except Exception:
                    pass

    # 3) Обновляем кеш ссылок в state
    state.LINKS.clear()
    state.LINKS.extend(links or [])


async def watch_loop(bot: Bot):
    await check_once(bot)
    while True:
        await asyncio.sleep(random.randint(300, 600))
        await check_once(bot)
