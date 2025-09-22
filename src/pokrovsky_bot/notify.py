from typing import Optional
from aiogram import Bot

from .db import (
    prefs_users_for_new,
    prefs_users_for_changes,
    prefs_get_user_class,
    log_event,
)
from .parser import grade_from_label, collapse_by_time, extract_schedule, pretty
from .ensure import ensure_sheet_for_grade


async def notify_new_schedule(bot: Bot, date_label: str) -> None:
    """
    Рассылает пользователям новое расписание, учитывая их выбранный класс и настройку 'notify_new'.
    Вычисляет нужный лист через ensure_sheet_for_grade() по номеру класса.
    """
    user_ids = prefs_users_for_new()
    for uid in user_ids:
        klass = (prefs_get_user_class(uid) or "").upper().strip()
        if not klass:
            continue
        grade = grade_from_label(klass)
        if grade is None:
            continue

        try:
            # Получаем лист (payload) для указанного класса/параллели в выбранную дату
            _g_url, gid, payload = await ensure_sheet_for_grade(date_label, grade)
            rows, labels, headers, cab_map = payload

            # Если в листе нет точного ключа — пробуем кейс-инсенситив
            key = klass if klass in labels else next((l for l in labels if l.upper() == klass), None)
            if not key:
                # нет такого класса на листе — пропускаем
                log_event(uid, "notify_new_skip_no_class", f"{date_label}|{klass}")
                continue

            items = collapse_by_time(
                extract_schedule(rows, labels, headers, key, cab_map.get(key, (None, 0)))
            )
            text = f"🆕 Новое расписание на {date_label}\n\n" + pretty(date_label, key, items)
            await bot.send_message(uid, text, parse_mode="HTML", disable_web_page_preview=True)
            log_event(uid, "notify_new_sent", f"{date_label}|{key}")
        except Exception as e:
            # логируем и продолжаем рассылку остальным
            try:
                log_event(uid, "notify_new_error", f"{date_label}|{klass}|{e}")
            except Exception:
                pass


async def notify_schedule_changes(bot: Bot, date_label: str, diff_text: str) -> None:
    """
    Рассылает пользователям уведомление об изменениях расписания, учитывая 'notify_change'.
    Ожидает уже подготовленный текст diff_text (как у тебя в админ-уведомлении).
    """
    for uid in prefs_users_for_changes():
        try:
            await bot.send_message(
                uid,
                f"✏️ Обновления в расписании на {date_label}\n\n{diff_text}",
                disable_web_page_preview=True,
            )
            log_event(uid, "notify_change_sent", date_label)
        except Exception as e:
            try:
                log_event(uid, "notify_change_error", f"{date_label}|{e}")
            except Exception:
                pass
