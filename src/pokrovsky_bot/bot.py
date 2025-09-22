from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import BotCommand

from .config import settings
from .db import ensure_db
from .subscription import SubscriptionMiddleware
from .handlers import (
    # Основной поток
    cmd_start, on_main, on_back, on_news,
    on_pick_date, on_pick_grade, on_pick_label, cmd_admin,
    on_check_subscription,

    # Личный кабинет
    on_profile_open, on_profile_cmd, on_profile_close, on_profile_back,
    on_profile_toggle_new, on_profile_toggle_chg,
    on_profile_choose_class, on_profile_choose_grade_letters, on_profile_pick_class,
    on_profile_my_open, on_profile_my_page, on_profile_my_pick_date,

    # Расписание кабинетов
    on_rooms, on_rooms_pick_date, on_rooms_pick_group, on_rooms_pick_cabinet, on_rooms_back_to_cabs, on_rooms_back_to_dates, on_donate
)


def build_bot_dp():
    if not settings.BOT_TOKEN or settings.BOT_TOKEN == "PUT_YOUR_TELEGRAM_BOT_TOKEN_HERE":
        raise SystemExit("Вставьте токен бота в переменную окружения BOT_TOKEN (см. .env).")

    ensure_db()

    bot = Bot(
        settings.BOT_TOKEN,
        default=DefaultBotProperties(
            parse_mode=ParseMode.HTML,
            link_preview_is_disabled=True,
        ),
    )

    dp = Dispatcher()

    # Проверка подписки
    sub_mw = SubscriptionMiddleware(
        settings.NEWS_CHANNEL_ID,
        settings.NEWS_CHANNEL_URL,
        settings.ADMIN_ID,
    )
    dp.message.middleware(sub_mw)
    dp.callback_query.middleware(sub_mw)

    from aiogram.filters import Command

    dp.message.register(on_donate, F.text.casefold() == "💳 донаты".casefold())
    dp.message.register(on_donate, Command("donate"))


    # Основной поток
    dp.message.register(cmd_start, Command("start"))
    dp.message.register(on_main, F.text.casefold() == "📅 посмотреть расписание".casefold())
    dp.message.register(on_back, F.text.casefold() == "⬅️ назад".casefold())
    dp.message.register(on_news, F.text.casefold() == "🔔 новостной канал".casefold())
    

    dp.callback_query.register(on_check_subscription, F.data == "check_sub")
    dp.callback_query.register(on_pick_date, F.data.startswith("d:"))
    dp.callback_query.register(on_pick_grade, F.data.startswith("g:"))
    dp.callback_query.register(on_pick_label, F.data.startswith("c:"))

    dp.message.register(cmd_admin, Command("admin"))

    # Личный кабинет
    dp.message.register(on_profile_open, F.text.casefold() == "👤 личный кабинет".casefold())
    dp.message.register(on_profile_cmd, Command("profile"))
    dp.callback_query.register(on_profile_close, F.data == "prof:close")
    dp.callback_query.register(on_profile_back, F.data == "prof:back")
    dp.callback_query.register(on_profile_toggle_new, F.data == "prof:toggle:new")
    dp.callback_query.register(on_profile_toggle_chg, F.data == "prof:toggle:chg")
    dp.callback_query.register(on_profile_choose_class, F.data == "prof:klass")
    dp.callback_query.register(on_profile_choose_grade_letters, F.data.startswith("prof:g:"))
    dp.callback_query.register(on_profile_pick_class, F.data.startswith("prof:pick:"))
    dp.callback_query.register(on_profile_my_open, F.data == "prof:my")
    dp.callback_query.register(on_profile_my_page, F.data.startswith("prof:my:page:"))
    dp.callback_query.register(on_profile_my_pick_date, F.data.startswith("prof:my:"))

    # 🏫 Расписание кабинетов
    dp.message.register(on_rooms, F.text.casefold() == "🏫 расписание кабинетов".casefold())
    dp.callback_query.register(on_rooms_pick_date, F.data.startswith("rd:"))
    dp.callback_query.register(on_rooms_pick_group, F.data.startswith("rcgrp:"))
    dp.callback_query.register(on_rooms_pick_cabinet, F.data.startswith("rc:"))
    dp.callback_query.register(on_rooms_back_to_cabs, F.data.startswith("rcb:"))
    dp.callback_query.register(on_rooms_back_to_dates, F.data == "rr:back")  # ← было on_rooms

    async def on_startup():
        await bot.set_my_commands([
            BotCommand(command="start", description="Посмотреть расписание"),
            BotCommand(command="profile", description="Личный кабинет"),
        ])

    dp.startup.register(on_startup)
    return bot, dp
