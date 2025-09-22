# src/pokrovsky_bot/config.py
import os
from dataclasses import dataclass
from pathlib import Path
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

# Всегда берём .env из корня проекта
PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENV_FILE = PROJECT_ROOT / ".env"
load_dotenv(dotenv_path=ENV_FILE)

@dataclass(frozen=True)
class Settings:
    # Бот
    BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
    ADMIN_ID: int = int(os.getenv("ADMIN_ID", "0"))

    # Канал: username (@channel) ИЛИ числовой id (-100…)
    NEWS_CHANNEL_URL: str = os.getenv("NEWS_CHANNEL_URL", "https://t.me/YourNewsChannel")
    NEWS_CHANNEL_ID: str = os.getenv("NEWS_CHANNEL_ID", "").strip()  # <-- строка, без int()

    # Источник расписания и база
    PAGE_URL: str = os.getenv("PAGE_URL", "https://pokrovsky.gosuslugi.ru/glavnoe/raspisanie/")
    DB_PATH: str = os.getenv("DB_PATH") or str(PROJECT_ROOT / "data" / "bot.db")

    # Прочее
    TZ: str = os.getenv("TZ", "Europe/Moscow")
    USER_AGENT: str = os.getenv("USER_AGENT", "ScheduleBot/1.0")

    # Донаты
    DONATE_CRYPTOBOT_URL: str = os.getenv("DONATE_CRYPTOBOT_URL", "")
    DONATE_HELEKET_URL: str = os.getenv("DONATE_HELEKET_URL", "")
    DONATE_DONATIONALERTS_URL: str = os.getenv("DONATIONALERTS_URL", "") or os.getenv("DONATE_DONATIONALERTS_URL", "")

settings = Settings()

MSK = ZoneInfo(settings.TZ or "Europe/Moscow")
HEADERS = {"User-Agent": settings.USER_AGENT}
