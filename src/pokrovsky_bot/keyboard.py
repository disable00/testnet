from typing import List, Dict, Any
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder

# Главное меню
MAIN_KB = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📅 Посмотреть расписание"), KeyboardButton(text="🏫 Расписание кабинетов")],
        [KeyboardButton(text="👤 Личный кабинет")],
        [KeyboardButton(text="💳 Донаты")],
        [KeyboardButton(text="⬅️ Назад"), KeyboardButton(text="🔔 Новостной канал")]
    ],
    resize_keyboard=True,
    one_time_keyboard=False,
    input_field_placeholder="Выберите действие…"
)

def profile_kb(prefs: Dict[str, Any]) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text=f"🔔 Новые расписания: {'ON' if prefs.get('notify_new') else 'OFF'}", callback_data="prof:toggle:new")
    b.button(text=f"🛎 Изменения: {'ON' if prefs.get('notify_change') else 'OFF'}", callback_data="prof:toggle:chg")
    klass = prefs.get("klass")
    b.button(text=f"🎓 Класс: {klass if klass else 'не выбран'}", callback_data="prof:klass")
    b.button(text="📘 Расписание класса", callback_data="prof:my")
    b.button(text="⬅️ Закрыть", callback_data="prof:close")
    b.adjust(1, 1, 1, 1, 1)
    return b.as_markup()

# Выбор номера класса в ЛК
def profile_grades_kb(grades: List[int]) -> InlineKeyboardMarkup:
    g_sorted = sorted(set(int(g) for g in grades if g))
    b = InlineKeyboardBuilder()
    for g in g_sorted:
        b.button(text=str(g), callback_data=f"prof:g:{g}")
    b.button(text="⬅️ В профиль", callback_data="prof:back")
    rows: List[int] = []
    cnt = len(g_sorted)
    if cnt:
        rows.extend([4] * (cnt // 4))
        if cnt % 4:
            rows.append(cnt % 4)
    rows.append(1)
    b.adjust(*rows)
    return b.as_markup()

def profile_labels_kb(grade: int, labels: List[str]) -> InlineKeyboardMarkup:
    labs = sorted({str(x).strip().upper() for x in labels if str(x).strip()})
    b = InlineKeyboardBuilder()
    for k in labs:
        b.button(text=k, callback_data=f"prof:pick:{k}")
    b.button(text="« Номера", callback_data="prof:klass")
    b.button(text="⬅️ В профиль", callback_data="prof:back")
    rows: List[int] = []
    cnt = len(labs)
    if cnt:
        rows.extend([3] * (cnt // 3))
        if cnt % 3:
            rows.append(cnt % 3)
    rows.append(1)
    rows.append(1)
    b.adjust(*rows)
    return b.as_markup()

# Пагинация по произвольным классам (используется в других местах)
def classes_kb(classes: List[str], page: int = 0, per_page: int = 21) -> InlineKeyboardMarkup:
    classes_sorted = sorted({str(c).strip().upper() for c in classes if str(c).strip()})
    start = max(page, 0) * per_page
    end = start + per_page
    chunk = classes_sorted[start:end]
    b = InlineKeyboardBuilder()
    for k in chunk:
        b.button(text=k, callback_data=f"prof:pick:{k}")
    have_prev = start > 0
    have_next = end < len(classes_sorted)
    if have_prev:
        b.button(text="« Назад", callback_data=f"prof:page:{page-1}")
    if have_next:
        b.button(text="Вперёд »", callback_data=f"prof:page:{page+1}")
    b.button(text="⬅️ В профиль", callback_data="prof:back")
    rows: List[int] = []
    cnt = len(chunk)
    if cnt:
        rows.extend([3] * (cnt // 3))
        if cnt % 3:
            rows.append(cnt % 3)
    if have_prev or have_next:
        rows.append(2 if (have_prev and have_next) else 1)
        rows.append(1)
    else:
        rows.append(1)
    b.adjust(*rows)
    return b.as_markup()

def profile_dates_kb(dates: List[str], page: int = 0, per_page: int = 9) -> InlineKeyboardMarkup:
    dates = [d for d in dates if d]
    start = max(page, 0) * per_page
    end = start + per_page
    chunk = dates[start:end]
    b = InlineKeyboardBuilder()
    for d in chunk:
        b.button(text=d, callback_data=f"prof:my:{d}")
    have_prev = start > 0
    have_next = end < len(dates)
    if have_prev:
        b.button(text="« Назад", callback_data=f"prof:my:page:{page-1}")
    if have_next:
        b.button(text="Вперёд »", callback_data=f"prof:my:page:{page+1}")
    b.button(text="⬅️ В профиль", callback_data="prof:back")
    rows: List[int] = []
    cnt = len(chunk)
    if cnt:
        rows.extend([3] * (cnt // 3))
        if cnt % 3:
            rows.append(cnt % 3)
    if have_prev or have_next:
        rows.append(2 if (have_prev and have_next) else 1)
        rows.append(1)
    else:
        rows.append(1)
    b.adjust(*rows)
    return b.as_markup()
