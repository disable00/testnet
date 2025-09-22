import csv
import io
import asyncio
import html
import re
import time
from typing import List, Tuple, Dict, Any, Optional
from .parser import (
    collapse_by_time, extract_schedule, pretty, grade_from_label,
    extract_schedule_anycab, parse_headers
)
from .donations import make_donate_keyboard

from aiogram import F, Bot
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder

from .config import settings
from .db import (
    upsert_user,
    log_event,
    sched_upsert,
    prefs_get,
    prefs_toggle,
    prefs_set,
    prefs_get_user_class,
)
from .keyboard import (
    MAIN_KB,
    profile_kb,
    profile_grades_kb,
    profile_labels_kb,
    classes_kb,
    profile_dates_kb,
)
from .parser import collapse_by_time, extract_schedule, pretty, grade_from_label
from .sheets import resolve_google_url, sheets_meta, csv_url
from .site import get_links_from_site
from .http import fetch_text
from .state import (
    LINKS,
    DOC_URL,
    GID_BY_GRADE,
    MATRIX,
    STATE,
    kb_dates,
    kb_grades,
    kb_labels,
)

# =========================
# Вспомогательные
# =========================

from aiogram.types import Message  # если ещё не импортирован

async def on_donate(m):
    from .config import settings
    upsert_user(m.from_user); log_event(m.from_user.id, "donate_open")

    url_cb = (settings.DONATE_CRYPTOBOT_URL or "").strip()
    url_he = (settings.DONATE_HELEKET_URL or "").strip()
    url_da = (settings.DONATE_DONATIONALERTS_URL or "").strip()

    if not (url_cb or url_he or url_da):
        await m.answer("Способы доната пока не настроены. Добавьте ссылки в .env и перезапустите бота.")
        return

    kb = make_donate_keyboard(
        cryptobot_url=url_cb,
        heleket_url=url_he,
        donationalerts_url=url_da,
    )
    await m.answer(
        "Поддержите проект 🙏\nВыберите удобный способ ниже. Откроется платёжная страница.",
        reply_markup=kb, disable_web_page_preview=True
    )




_LINKS_LAST_FETCH_AT = 0.0
_LINKS_TTL_SECONDS = 60.0

def _csv_to_rows(csv_text: str) -> List[List[str]]:
    f = io.StringIO(csv_text)
    reader = csv.reader(f, delimiter=",", quotechar='"')
    return [list(row) for row in reader]

def _split_cab_keys(cab: str) -> List[str]:
    """
    Делит "Г3-04/Г4-03" на ["Г3-04", "Г4-03"] и убирает мусорные статусы.
    """
    if not cab:
        return []
    raw = re.split(r"[\/,;|]+", cab)
    out = []
    for part in raw:
        p = (part or "").strip().upper()
        if not p:
            continue
        if p.lower() in {"онлайн", "офлайн", "дистант"}:
            continue
        out.append(p)
    return out


async def show_loader(cb_or_msg, toast="Загружаю…", text="⚙️ Загружаю…") -> Message:
    if isinstance(cb_or_msg, CallbackQuery):
        try:
            await cb_or_msg.answer(toast, show_alert=False)
        except Exception:
            pass
        return await cb_or_msg.message.answer(text)
    return await cb_or_msg.answer(text)


async def replace_loader(loader: Message, text: str, **kw):
    try:
        await loader.edit_text(text, **kw)
    except Exception:
        try:
            await loader.answer(text, **kw)
        finally:
            try:
                await loader.delete()
            except Exception:
                pass


async def ensure_links(force: bool = False):
    """Подтягиваем список дат с сайта. force=True — игнор кэша (фикс «новая дата не появляется без перезапуска»)."""
    global LINKS, _LINKS_LAST_FETCH_AT
    now = time.monotonic()
    if force or not LINKS or (now - _LINKS_LAST_FETCH_AT) >= _LINKS_TTL_SECONDS:
        LINKS[:] = await get_links_from_site()
        _LINKS_LAST_FETCH_AT = now


def _klass_sort_key(k: str) -> tuple[int, str]:
    # номер + буква (латиница/кириллица)
    m = re.match(r"^\s*(\d{1,2})\s*([A-Za-zА-Яа-яЁё]+)?\s*$", k)
    if m:
        num = int(m.group(1))
        suf = (m.group(2) or "").upper()
        return (num, suf)
    return (999, (k or "").upper())


def _known_classes_from_matrix() -> List[str]:
    labels: set[str] = set()
    for (_date, _gid), payload in MATRIX.items():
        try:
            _rows, lbls, _hr, _cab = payload
        except Exception:
            continue
        for L in (lbls or []):
            s = str(L or "").strip().upper()
            if s:
                labels.add(s)
    return sorted(labels, key=_klass_sort_key)


async def _best_date_label() -> Optional[str]:
    await ensure_links(force=True)
    if LINKS:
        return LINKS[0].date
    if DOC_URL:
        return next(iter(DOC_URL.keys()))
    return None


async def _prepare_all_classes() -> List[str]:
    """
    Делаем список классов доступным: тянем свежую дату, загружаем параллели (заполняет MATRIX),
    чтобы получить полный набор меток классов.
    """
    classes = _known_classes_from_matrix()
    if classes:
        return classes

    date = await _best_date_label()
    if not date:
        return classes

    g_url = DOC_URL.get(date)
    if not g_url:
        link = next((l for l in LINKS if l.date == date), None)
        if link:
            try:
                g_url = await resolve_google_url(link.url)
                DOC_URL[date] = g_url
                sched_upsert(date, link.url, g_url)
            except Exception:
                g_url = None
    if not g_url:
        return classes

    try:
        gid2title, _ = await sheets_meta(g_url)
    except Exception:
        gid2title = {}

    if gid2title:
        from .state import parse_class_label
        quick = {grade_from_label(parse_class_label(t) or ""): gid for gid, t in gid2title.items()}
        grades = [g for g in quick.keys() if g]
        if grades:
            GID_BY_GRADE[date] = {g: quick[g] for g in grades}
            from .ensure import ensure_sheet_for_grade
            for g in grades:
                try:
                    await ensure_sheet_for_grade(date, g)  # заполняет MATRIX → labels
                except Exception:
                    pass

    return _known_classes_from_matrix()


def _recent_dates(limit: int = 12) -> List[str]:
    try:
        from .db import DB
        rows = DB.execute(
            "SELECT date_label FROM schedules ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
        dates = [r[0] for r in rows]
    except Exception:
        dates = []
    if not dates:
        dates = [l.date for l in LINKS[:limit]]
    return dates


# =========================
# Основной пользовательский поток
# =========================

async def cmd_start(m: Message):
    upsert_user(m.from_user)
    log_event(m.from_user.id, "cmd_start")
    await m.answer("Ищу расписания (площадка №1)...", reply_markup=MAIN_KB)
    await show_dates(m)


async def show_dates(m: Message):
    await ensure_links(force=True)
    if not LINKS:
        return await m.answer("Не нашёл ссылки в секции №1.", reply_markup=MAIN_KB)
    STATE[m.chat.id] = {"step": "dates"}
    await m.answer("Выбери дату:", reply_markup=kb_dates(LINKS))


async def on_main(m: Message):
    upsert_user(m.from_user)
    log_event(m.from_user.id, "click_main")
    await show_dates(m)


async def on_back(m: Message):
    upsert_user(m.from_user)
    log_event(m.from_user.id, "click_back")
    st = STATE.get(m.chat.id) or {}
    if st.get("step") in (None, "dates"):
        return await show_dates(m)
    if st.get("step") == "grades":
        return await show_dates(m)
    if st.get("step") == "classes":
        date = st.get("date")
        if not date:
            return await show_dates(m)
        return await ask_grades(m, date)
    if st.get("step") == "shown":
        date, gid, grade = st.get("date"), st.get("gid"), st.get("grade")
        if not (date and gid and grade is not None):
            return await show_dates(m)
        rows, labels, _hr, _cab = MATRIX.get((date, gid), (None, None, None, None))
        if rows is None:
            from .ensure import ensure_sheet_for_grade
            rows, labels, hr, cab = (await ensure_sheet_for_grade(date, grade))[2]
            MATRIX[(date, gid)] = (rows, labels, hr, cab)
        ks = [L for L in labels if grade_from_label(L) == grade]
        await m.answer("Выбери класс:", reply_markup=kb_labels(date, gid, ks))
        STATE[m.chat.id] = {"step": "classes", "date": date, "gid": gid, "grade": grade}


async def on_news(m: Message):
    upsert_user(m.from_user)
    log_event(m.from_user.id, "click_news")
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Открыть новостной канал", url=settings.NEWS_CHANNEL_URL)]
        ]
    )
    await m.answer("Наш новостной канал:", reply_markup=kb)


async def on_pick_date(c: CallbackQuery):
    upsert_user(c.from_user)
    await ensure_links(force=True)
    idx = int(c.data.split(":", 1)[1])
    if idx < 0 or idx >= len(LINKS):
        return await c.answer()
    link = LINKS[idx]
    log_event(c.from_user.id, "pick_date", link.date)
    loader = await show_loader(c, "Загружаю…", "⚙️ Загружаю список классов…")
    try:
        g_url = await resolve_google_url(link.url)
    except Exception as e:
        return await replace_loader(loader, f"Не удалось найти Google Sheets: {e}")
    DOC_URL[link.date] = g_url
    sched_upsert(link.date, link.url, g_url)
    gid2title, _ = await sheets_meta(g_url)
    from .state import parse_class_label
    quick = {grade_from_label(parse_class_label(t) or ""): gid for gid, t in gid2title.items()}
    grades = [g for g in quick.keys() if g]
    if grades:
        GID_BY_GRADE[link.date] = {g: quick[g] for g in grades}
        await replace_loader(
            loader,
            f"Выбери номер класса ({link.date}):",
            reply_markup=kb_grades(link.date, grades),
        )
    else:
        await replace_loader(
            loader,
            f"Выбери номер класса ({link.date}):",
            reply_markup=kb_grades(link.date, list(range(1, 12))),
        )
    STATE[c.message.chat.id] = {"step": "grades", "date": link.date}


async def ask_grades(msg_target: Message, date: str):
    await ensure_links(force=True)
    g_url = DOC_URL.get(date)
    if not g_url:
        link = next((l for l in LINKS if l.date == date), None)
        if not link:
            return await msg_target.answer("Не нашёл такую дату.", reply_markup=MAIN_KB)
        g_url = await resolve_google_url(link.url)
        DOC_URL[date] = g_url
        sched_upsert(date, link.url, g_url)
    gid2title, _ = await sheets_meta(g_url)
    from .state import parse_class_label
    quick = {grade_from_label(parse_class_label(t) or ""): gid for gid, t in gid2title.items()}
    grades = [g for g in quick.keys() if g]
    if grades:
        GID_BY_GRADE[date] = {g: quick[g] for g in grades}
        await msg_target.answer(
            f"Выбери номер класса ({date}):", reply_markup=kb_grades(date, grades)
        )
    else:
        await msg_target.answer(
            f"Выбери номер класса ({date}):",
            reply_markup=kb_grades(date, list(range(1, 12))),
        )


async def on_pick_grade(c: CallbackQuery):
    upsert_user(c.from_user)
    _, date, gs = c.data.split(":", 2)
    grade = int(gs)
    log_event(c.from_user.id, "pick_grade", f"{date}|{grade}")
    loader = await show_loader(c, "Загружаю…", "⚙️ Загружаю расписание…")
    try:
        from .ensure import ensure_sheet_for_grade
        _g_url, gid, payload = await ensure_sheet_for_grade(date, grade)
    except Exception as e:
        return await replace_loader(loader, f"Не нашёл вкладку: {e}")
    rows, labels, _hr, _cab = payload
    ks = [L for L in labels if grade_from_label(L) == grade]
    await replace_loader(loader, "Выбери класс:", reply_markup=kb_labels(date, gid, ks))
    STATE[c.message.chat.id] = {"step": "classes", "date": date, "gid": gid, "grade": grade}


async def on_pick_label(c: CallbackQuery):
    upsert_user(c.from_user)
    _, date, gid, klass = c.data.split(":", 3)
    key = (klass or "").upper()
    log_event(c.from_user.id, "pick_class", f"{date}|{key}")
    loader = await show_loader(c, "Загружаю…", "⚙️ Загружаю расписание…")

    if (date, gid) not in MATRIX:
        try:
            grade = int(re.match(r"(\d{1,2})", key).group(1))
            from .ensure import ensure_sheet_for_grade
            _g_url, _gid, _payload = await ensure_sheet_for_grade(date, grade)
        except Exception as e:
            return await replace_loader(loader, f"Ошибка доступа к листу: {e}")

    rows, labels, headers, cab_map = MATRIX[(date, gid)]
    if key not in labels:
        return await replace_loader(loader, "Такой класс не нашёлся на листе.")
    items = collapse_by_time(
        extract_schedule(rows, labels, headers, key, cab_map.get(key, (None, 0)))
    )
    await replace_loader(loader, pretty(date, key, items), parse_mode="HTML")
    STATE[c.message.chat.id] = {
        "step": "shown",
        "date": date,
        "gid": gid,
        "grade": grade_from_label(key),
        "klass": key,
    }
    log_event(c.from_user.id, "show_schedule", f"{date}|{key}")
    # Важно: НИКАКОГО автосохранения! (класс в ЛК меняется только вручную)


# =========================
# Проверка подписки на канал
# =========================

from .subscription import ALLOWED_STATUSES, make_sub_keyboard

async def on_check_subscription(cb: CallbackQuery, bot: Bot):
    try:
        await cb.answer()
    except Exception:
        pass

    user_id = cb.from_user.id
    ok = False
    try:
        member = await bot.get_chat_member(settings.NEWS_CHANNEL_ID, user_id)
        ok = getattr(member, "status", None) in ALLOWED_STATUSES
    except TelegramBadRequest:
        ok = False

    if ok:
        if cb.message:
            try:
                await cb.message.edit_reply_markup(reply_markup=None)
            except TelegramBadRequest:
                pass
            await cb.message.answer("Готово! Подписка найдена ✅\nНажмите /start, чтобы продолжить.")
    else:
        if cb.message:
            try:
                await cb.message.edit_reply_markup(
                    reply_markup=make_sub_keyboard(settings.NEWS_CHANNEL_URL)
                )
            except TelegramBadRequest:
                pass
            await cb.message.answer(
                "Пока не вижу подписки. Подпишитесь и нажмите «Проверить подписку» ещё раз."
            )


# =========================
# ЛИЧНЫЙ КАБИНЕТ
# =========================

async def on_profile_open(m: Message):
    """Открыть личный кабинет (кнопка или /profile)."""
    upsert_user(m.from_user)
    log_event(m.from_user.id, "profile_open")
    prefs = prefs_get(m.from_user.id)
    await m.answer(
        "Личный кабинет:\n• Включайте/отключайте уведомления\n• Выберите свой класс (номер → буква)\n• Или откройте 📘 Расписание класса",
        reply_markup=profile_kb(prefs),
    )


async def on_profile_cmd(m: Message):
    await on_profile_open(m)


async def on_profile_close(cb: CallbackQuery):
    try:
        await cb.message.edit_text("Личный кабинет закрыт. Вернитесь через кнопку в меню.")
    except Exception:
        await cb.message.answer("Личный кабинет закрыт.")
    await cb.answer()


async def on_profile_back(cb: CallbackQuery):
    prefs = prefs_get(cb.from_user.id)
    try:
        await cb.message.edit_text("Личный кабинет:", reply_markup=profile_kb(prefs))
    except Exception:
        await cb.message.answer("Личный кабинет:", reply_markup=profile_kb(prefs))
    await cb.answer()


async def on_profile_toggle_new(cb: CallbackQuery):
    val = prefs_toggle(cb.from_user.id, "notify_new")
    prefs = prefs_get(cb.from_user.id)
    try:
        await cb.message.edit_reply_markup(reply_markup=profile_kb(prefs))
    except Exception:
        pass
    await cb.answer(f"Новые расписания: {'ON' if val else 'OFF'}")
    log_event(cb.from_user.id, "profile_toggle_new", str(val))


async def on_profile_toggle_chg(cb: CallbackQuery):
    val = prefs_toggle(cb.from_user.id, "notify_change")
    prefs = prefs_get(cb.from_user.id)
    try:
        await cb.message.edit_reply_markup(reply_markup=profile_kb(prefs))
    except Exception:
        pass
    await cb.answer(f"Изменения: {'ON' if val else 'OFF'}")
    log_event(cb.from_user.id, "profile_toggle_chg", str(val))


async def on_profile_choose_class(cb: CallbackQuery):
    """В ЛК: сначала выбор номера (только 5–11), без дат, без расписания."""
    await ensure_links(force=True)
    grades = list(range(5, 12))  # 5–11
    try:
        await cb.message.edit_text("Выберите номер класса (5–11):", reply_markup=profile_grades_kb(grades))
    except Exception:
        await cb.message.answer("Выберите номер класса (5–11):", reply_markup=profile_grades_kb(grades))
    await cb.answer()


async def on_profile_choose_grade_letters(cb: CallbackQuery):
    """После 'prof:g:<grade>': показать буквы для выбранного номера из самой свежей даты."""
    try:
        grade = int(cb.data.split(":")[-1])
    except Exception:
        await cb.answer()
        return

    date = await _best_date_label()
    if not date:
        await cb.answer("Нет доступных дат, попробуйте позже.", show_alert=True)
        return

    try:
        from .ensure import ensure_sheet_for_grade
        _g_url, _gid, payload = await ensure_sheet_for_grade(date, grade)
    except Exception:
        await cb.answer("Для этой параллели пока нет расписания.", show_alert=True)
        return

    _rows, labels, _hr, _cab = payload
    letters = [L for L in labels if grade_from_label(L) == grade]
    if not letters:
        await cb.answer("Букв для этой параллели не найдено.", show_alert=True)
        return

    try:
        await cb.message.edit_text(
            f"Выберите букву для {grade} класса:",
            reply_markup=profile_labels_kb(grade, letters),
        )
    except Exception:
        await cb.message.answer(
            f"Выберите букву для {grade} класса:",
            reply_markup=profile_labels_kb(grade, letters),
        )
    await cb.answer()


async def on_profile_pick_class(cb: CallbackQuery):
    """Сохраняем выбранный класс и не показываем расписание."""
    klass = cb.data.split(":")[-1].upper()
    prefs_set(cb.from_user.id, klass=klass)
    prefs = prefs_get(cb.from_user.id)
    txt = f"Класс установлен: <b>{html.escape(klass)}</b>\n\nЛичный кабинет:"
    try:
        await cb.message.edit_text(txt, reply_markup=profile_kb(prefs), parse_mode="HTML")
    except Exception:
        await cb.message.answer(txt, reply_markup=profile_kb(prefs), parse_mode="HTML")
    await cb.answer("Сохранено ✅")
    log_event(cb.from_user.id, "profile_set_class", klass)


# «📘 Расписание класса»
async def on_profile_my_open(cb: CallbackQuery):
    await ensure_links(force=True)
    dates = _recent_dates(limit=12)
    if not dates:
        await cb.answer("Пока нет доступных дат.", show_alert=True)
        return
    try:
        await cb.message.edit_text("Выберите дату:", reply_markup=profile_dates_kb(dates))
    except Exception:
        await cb.message.answer("Выберите дату:", reply_markup=profile_dates_kb(dates))
    await cb.answer()


async def on_profile_my_page(cb: CallbackQuery):
    try:
        page = int(cb.data.split(":")[-1])
    except Exception:
        page = 0
    dates = _recent_dates(limit=50)
    try:
        await cb.message.edit_reply_markup(reply_markup=profile_dates_kb(dates, page=page))
    except Exception:
        pass
    await cb.answer()


async def on_profile_my_pick_date(cb: CallbackQuery):
    date = cb.data.split(":", 2)[-1]
    klass = (prefs_get_user_class(cb.from_user.id) or "").upper().strip()
    if not klass:
        await cb.answer("Сначала выберите класс в профиле.", show_alert=True)
        return
    grade = grade_from_label(klass)
    if grade is None:
        await cb.answer("Не удалось распознать номер класса. Выберите класс заново.", show_alert=True)
        return
    try:
        from .ensure import ensure_sheet_for_grade
        _g_url, gid, payload = await ensure_sheet_for_grade(date, grade)
    except Exception as e:
        await cb.answer(f"Лист не найден: {e}", show_alert=True)
        return
    rows, labels, headers, cab_map = payload
    key = klass if klass in labels else next((l for l in labels if l.upper() == klass), None)
    if not key:
        await cb.answer("Ваш класс не найден в листе на эту дату.", show_alert=True)
        return
    items = collapse_by_time(extract_schedule(rows, labels, headers, key, cab_map.get(key, (None, 0))))
    text = f"📘 Расписание для {html.escape(key)} на {html.escape(date)}\n\n" + pretty(date, key, items)
    try:
        await cb.message.edit_text(text, parse_mode="HTML", disable_web_page_preview=True)
    except Exception:
        await cb.message.answer(text, parse_mode="HTML", disable_web_page_preview=True)
    await cb.answer()
    log_event(cb.from_user.id, "profile_my_show", f"{date}|{key}")


# =========================
# РАСПИСАНИЕ КАБИНЕТОВ (группы по этажам)
# =========================

from aiogram.utils.keyboard import InlineKeyboardBuilder

# Кеш: {date: {cab: [(time, klass, subj), ...]}}
CAB_INDEX: Dict[str, Dict[str, List[Tuple[str, str, Optional[str]]]]] = {}

def _is_physical_cab(cab: Optional[str]) -> bool:
    if not cab:
        return False
    u = cab.upper()
    if any(x in u for x in ("ОНЛАЙН", "ОФЛАЙН", "ДИСТАНТ")):
        return False
    return True

# --- группировка по этажам/спортзалам ---
def _cab_group_key(cab: str) -> str:
    """
    Возвращает одну из групп: F1/F2/F3/F4/SPORT/OTHER.
    Правило: первая буква — корпус, первая цифра после неё — этаж.
    Примеры: Г3-08 -> F3, Б4-08 -> F4, Г2-21 -> F2, спортзал2 -> SPORT
    """
    u = (cab or "").upper()
    if "СПОРТЗАЛ" in u:
        return "SPORT"
    m = re.match(r"^[A-ZА-Я]\s*(\d)", u)
    if m:
        f = int(m.group(1))
        if 1 <= f <= 4:
            return f"F{f}"
    return "OTHER"

def _group_cabs(cabmap: Dict[str, List[Tuple[str, str, Optional[str]]]]) -> Dict[str, List[Tuple[str, int]]]:
    """
    -> {"F1":[(cab, count), ...], "F2":..., "SPORT":[...], "OTHER":[...]}
    """
    groups: Dict[str, List[Tuple[str, int]]] = {}
    for cab, entries in cabmap.items():
        g = _cab_group_key(cab)
        groups.setdefault(g, []).append((cab, len(entries)))
    for g in groups:
        groups[g].sort(key=lambda x: x[0])  # сортировка по названию кабинета
    return groups

def _kb_rooms_dates(links: List) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    for i, l in enumerate(links):
        b.button(text=l.date, callback_data=f"rd:{i}")
    b.adjust(3)
    return b.as_markup()

def _kb_room_groups(date: str, groups: Dict[str, List[Tuple[str, int]]]) -> InlineKeyboardMarkup:
    """
    Клавиатура выбора этажа/спортзалов.
    Показываем только те группы, где есть кабинеты.
    """
    titles = [
        ("F1", "1 этаж"),
        ("F2", "2 этаж"),
        ("F3", "3 этаж"),
        ("F4", "4 этаж"),
        ("SPORT", "Спортзалы"),
        ("OTHER", "Прочее"),
    ]
    b = InlineKeyboardBuilder()
    for key, label in titles:
        if groups.get(key):
            total = sum(n for _cab, n in groups[key])
            b.button(text=f"{label} ({total})", callback_data=f"rcgrp:{date}:{key}")
    b.button(text="⬅️ К датам", callback_data="rr:back")
    # 2-2-2-...-1 раскладка
    rows: List[int] = []
    cnt = len([1 for key, _ in titles if groups.get(key)])
    if cnt:
        rows.extend([2] * (cnt // 2))
        if cnt % 2:
            rows.append(1)
    rows.append(1)  # «К датам»
    b.adjust(*rows)
    return b.as_markup()

def _kb_cabinets_for_group(date: str, group_key: str, items: List[Tuple[str, int]]) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    for cab, n in items:
        b.button(text=f"{cab} ({n})", callback_data=f"rc:{date}:{cab}")
    b.button(text="⬅️ К этажам", callback_data=f"rcb:{date}")
    # 3 в ряд + назад
    rows: List[int] = []
    cnt = len(items)
    if cnt:
        rows.extend([3] * (cnt // 3))
        if cnt % 3:
            rows.append(cnt % 3)
    rows.append(1)
    b.adjust(*rows)
    return b.as_markup()

def _time_sort_key(t: str) -> Tuple[int, int]:
    s = (t or "").replace(".", ":")
    m = re.search(r"(\d{1,2}):(\d{2})", s)
    if not m:
        return (99, 99)
    return (int(m.group(1)), int(m.group(2)))

# --- построение индекса кабинетов с нуля (как сделали ранее) ---
async def _build_cabinet_index(date: str) -> Dict[str, List[Tuple[str, str, Optional[str]]]]:
    cached = CAB_INDEX.get(date)
    if cached:
        return cached

    g_url = DOC_URL.get(date)
    if not g_url:
        link = next((l for l in LINKS if l.date == date), None)
        if link:
            try:
                g_url = await resolve_google_url(link.url)
                DOC_URL[date] = g_url
                sched_upsert(date, link.url, g_url)
            except Exception:
                g_url = None
    if not g_url:
        return {}

    try:
        gid2title, gids = await sheets_meta(g_url)
        gid_list = list(gid2title.keys() or gids or [])
    except Exception:
        gid_list = []

    out: Dict[str, List[Tuple[str, str, Optional[str]]]] = {}

    # Вытаскиваем пары по всем классам всех вкладок (кабинет ищем «в своей зоне»)
    from .ensure import ensure_sheet_for_grade
    from .state import parse_class_label
    for gid in gid_list:
        # узнаём номер класса (grade) для вкладки через заголовок
        title = (gid2title.get(gid) or "").strip()
        grade = grade_from_label(parse_class_label(title) or "") if title else None
        if not grade:
            # fallback: переберём 5..11
            for g in range(5, 12):
                try:
                    _g_url, _gid, payload = await ensure_sheet_for_grade(date, g)
                except Exception:
                    continue
                if _gid == gid:
                    grade = g
                    rows, labels, headers, _cab_map_unused = payload
                    break
            else:
                # не нашли — просто пропускаем вкладку
                continue
        else:
            try:
                _g_url, _gid, payload = await ensure_sheet_for_grade(date, grade)
            except Exception:
                continue
            rows, labels, headers, _cab_map_unused = payload

        for klass in labels:
            items = extract_schedule_anycab(rows, labels, headers, klass)
            for t, subj, cab in items:
                if not _is_physical_cab(cab):
                    continue
                # делим "Г3-04/Г4-03" на отдельные кабинеты
                for cab_one in re.split(r"[\/,;|]+", (cab or "").upper()):
                    key = cab_one.strip()
                    if not key or not _is_physical_cab(key):
                        continue
                    out.setdefault(key, []).append((t, klass, subj))

    for cab, lst in out.items():
        lst.sort(key=lambda x: _time_sort_key(x[0]))

    CAB_INDEX[date] = out
    return out

# --- хендлеры ---

async def on_rooms(m: Message):
    """Кнопка '🏫 Расписание кабинетов' — сначала даты."""
    upsert_user(m.from_user); log_event(m.from_user.id, "rooms_open")
    await ensure_links(force=True)
    if not LINKS:
        return await m.answer("Пока нет дат с расписанием.", reply_markup=MAIN_KB)
    await m.answer("Выберите дату для просмотра расписания кабинетов:", reply_markup=_kb_rooms_dates(LINKS))

async def on_rooms_pick_date(c: CallbackQuery):
    upsert_user(c.from_user); await ensure_links(force=True)
    try:
        idx = int(c.data.split(":", 1)[1])
    except Exception:
        return await c.answer()
    if idx < 0 or idx >= len(LINKS):
        return await c.answer()

    link = LINKS[idx]
    log_event(c.from_user.id, "rooms_pick_date", link.date)
    loader = await show_loader(c, "Собираю кабинеты…", "⚙️ Строю расписание по кабинетам…")

    try:
        cabmap = await _build_cabinet_index(link.date)
    except Exception as e:
        return await replace_loader(loader, f"Не удалось построить список кабинетов: {e}")

    if not cabmap:
        return await replace_loader(loader, "Для выбранной даты кабинетная сетка не найдена.")

    groups = _group_cabs(cabmap)
    await replace_loader(loader, f"Этажи на {link.date}:", reply_markup=_kb_room_groups(link.date, groups))
    STATE[c.message.chat.id] = {"step": "rooms_groups", "date": link.date}

async def on_rooms_pick_group(c: CallbackQuery):
    """Выбор группы (этаж/спортзалы)."""
    upsert_user(c.from_user)
    try:
        _, date, group_key = c.data.split(":", 2)
    except Exception:
        return await c.answer()

    cabmap = CAB_INDEX.get(date) or {}
    if not cabmap:
        cabmap = await _build_cabinet_index(date)
    groups = _group_cabs(cabmap)
    items = groups.get(group_key) or []
    if not items:
        return await c.answer("Пока пусто.", show_alert=True)

    label = {"F1": "1 этаж", "F2": "2 этаж", "F3": "3 этаж", "F4": "4 этаж",
             "SPORT": "Спортзалы", "OTHER": "Прочее"}.get(group_key, group_key)
    await c.message.edit_text(f"{label} — {date}:", reply_markup=_kb_cabinets_for_group(date, group_key, items))
    await c.answer()

async def on_rooms_pick_cabinet(c: CallbackQuery):
    upsert_user(c.from_user)
    try:
        _, date, cab = c.data.split(":", 2)
    except Exception:
        return await c.answer()

    cabmap = CAB_INDEX.get(date) or {}
    if not cabmap:
        cabmap = await _build_cabinet_index(date)

    items = cabmap.get(cab)
    if not items:
        return await c.answer("Для этого кабинета нет записей.", show_alert=True)

    lines = [f"<b>Кабинет {html.escape(cab)}</b> — {html.escape(date)}", ""]
    for i, (t, klass, subj) in enumerate(items, 1):
        subj_html = f" — <i>{html.escape(subj)}</i>" if (subj and subj.strip()) else ""
        lines.append(f"{i}. {html.escape(t)} — <b>{html.escape(klass)}</b>{subj_html}")

    b = InlineKeyboardBuilder()
    b.button(text="⬅️ К этажам", callback_data=f"rcb:{date}")
    kb = b.as_markup()

    await c.message.edit_text("\n".join(lines), parse_mode="HTML", reply_markup=kb)
    await c.answer()
    log_event(c.from_user.id, "rooms_show_cab", f"{date}|{cab}")

async def on_rooms_back_to_cabs(c: CallbackQuery):
    """Назад со списка кабинетов к этажам."""
    upsert_user(c.from_user)
    date = c.data.split(":", 1)[1] if ":" in c.data else None
    if not date:
        await ensure_links(force=True)
        return await c.message.edit_text(
            "Выберите дату для просмотра расписания кабинетов:",
            reply_markup=_kb_rooms_dates(LINKS),
        )
    cabmap = CAB_INDEX.get(date) or {}
    if not cabmap:
        cabmap = await _build_cabinet_index(date)
    groups = _group_cabs(cabmap)
    await c.message.edit_text(f"Этажи на {date}:", reply_markup=_kb_room_groups(date, groups))
    await c.answer()
async def on_rooms_back_to_dates(c: CallbackQuery):
    """Назад с этажей к выбору дат (чтобы не показывать toast)."""
    upsert_user(c.from_user)
    await ensure_links(force=True)
    try:
        await c.message.edit_text(
            "Выберите дату для просмотра расписания кабинетов:",
            reply_markup=_kb_rooms_dates(LINKS),
        )
    except Exception:
        await c.message.answer(
            "Выберите дату для просмотра расписания кабинетов:",
            reply_markup=_kb_rooms_dates(LINKS),
        )
    await c.answer()


# =========================
# ADMIN
# =========================

def is_admin(uid: int) -> bool:
    return uid == settings.ADMIN_ID


async def cmd_admin(m: Message):
    if not is_admin(m.from_user.id):
        return await m.answer("⛔ Доступ запрещён.")
    upsert_user(m.from_user)
    try:
        from .db import DB
        from .utils import fmt_msk
    except Exception:
        return await m.answer("Админ-панель временно недоступна.")

    tu = DB.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    te = DB.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    a24 = DB.execute("SELECT COUNT(DISTINCT user_id) FROM events WHERE ts >= datetime('now','-1 day')").fetchone()[0]
    top = DB.execute(
        "SELECT user_id, first_name, username, msg_count, last_seen FROM users ORDER BY msg_count DESC, last_seen DESC LIMIT 10"
    ).fetchall()
    last = DB.execute(
        "SELECT e.ts, e.type, u.user_id, u.username, e.meta FROM events e JOIN users u USING(user_id) ORDER BY e.id DESC LIMIT 10"
    ).fetchall()

    def ulabel(r):
        uid, fn, un, cnt, ls = r
        tag = f"@{un}" if un else str(uid)
        return f"{tag} — {cnt} событий (посл.: {fmt_msk(ls)})"

    def eline(r):
        ts, et, uid, un, meta = r
        tag = f"@{un}" if un else str(uid)
        return f"{fmt_msk(ts)} · {et} · {tag} · {meta or ''}"

    msg = [
        "🛠 <b>Админ-панель</b>",
        f"\n👥 Пользователей: <b>{tu}</b>",
        f"\n📨 Событий: <b>{te}</b>",
        f"\n🟢 Активно за 24ч: <b>{a24}</b>",
        "\n\n🏆 <b>Топ 10 по активности</b>\n",
    ]
    msg += [f"• {ulabel(r)}" for r in top] or ["— нет данных —"]
    msg += ["\n\n📝 <b>Последние 10 событий</b>\n"]
    msg += [f"• {eline(r)}" for r in last] or ["— нет данных —"]
    await m.answer("".join(msg), parse_mode="HTML")
