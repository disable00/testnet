# Покровский Schedule Bot (Telegram, aiogram v3)

Проект Telegram‑бота для школы «Комплекс Покровский». Возможности:

- Автопоиск дат расписания (площадка №1) со страницы школы
- Парсинг Google Sheets (CSV, без авторизации)
- Корректное извлечение кабинетов
- Удобные клавиатуры и лоадер «⚙️ Загружаю…»
- Кнопка «🔔 Новостной канал»
- Админка `/admin` (не в меню), статистика в SQLite
- **Автонаблюдатель**: каждые 5–10 минут ищет новые даты и правки в таблицах и уведомляет пользователей

> ⚠️ **Безопасность токена**: Никогда не храните токен в коде/репозитории. Используйте `.env`.
> Если вы случайно засветили токен, немедленно **пересоздайте его** в `@BotFather`.

## Быстрый старт

1) Установите зависимости:

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

2) Создайте файл `.env` (см. пример `.env.example`) и заполните значения.

3) Запуск:

```bash
python -m pokrovsky_bot
```

или

```bash
python run.py
```

## Переменные окружения

См. `.env.example` и `src/pokrovsky_bot/config.py`:

- `BOT_TOKEN` — токен бота от `@BotFather` (обязательно)
- `ADMIN_ID` — Telegram ID администратора (число)
- `NEWS_CHANNEL_URL` — ссылка на новостной канал
- `PAGE_URL` — страница расписаний
- `DB_PATH` — путь к SQLite базе (по умолчанию `bot_stats.sqlite3`)
- `TZ` — таймзона для форматирования (по умолчанию `Europe/Moscow`)

## Запуск в Docker

```bash
docker build -t pokrovsky-bot .
docker run --env-file .env --name pokrovsky-bot --restart unless-stopped pokrovsky-bot
```

## Структура проекта

```
.
├─ .env.example
├─ requirements.txt
├─ Dockerfile
├─ run.py
└─ src/pokrovsky_bot
   ├─ __init__.py
   ├─ bot.py            # создание Bot/Dispatcher, регистрация хэндлеров
   ├─ config.py         # конфиг + загрузка .env
   ├─ db.py             # SQLite и логирование событий
   ├─ handlers.py       # команды и колбэки
   ├─ http.py           # HTTP-запросы
   ├─ keyboard.py       # клавиатуры
   ├─ models.py         # dataclass SLink
   ├─ parser.py         # парсинг CSV/времени/кабинетов/расписания
   ├─ sheets.py         # работа с Google Sheets
   ├─ site.py           # парсинг сайта с датами
   ├─ state.py          # оперативный кэш и константы/регулярки
   ├─ utils.py          # хелперы форматирования
   ├─ watcher.py        # автонаблюдатель изменений
   └─ main.py           # entrypoint
```

## Лицензия

MIT (или задайте свою).
# moyproject
# testnet
