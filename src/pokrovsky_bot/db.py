import sqlite3
from typing import Optional, Tuple, Dict, List, Any
from .config import settings
from .utils import fmt_msk

DB: Optional[sqlite3.Connection] = None
_NOARG = object()  # sentinel для отличия "не менять поле" от "установить None"


def ensure_db():
    global DB
    DB = sqlite3.connect(settings.DB_PATH, check_same_thread=False)
    DB.execute("PRAGMA journal_mode=WAL;")
    DB.executescript("""
    CREATE TABLE IF NOT EXISTS users(
      user_id INTEGER PRIMARY KEY, first_name TEXT, username TEXT,
      joined_at TEXT, last_seen TEXT, msg_count INTEGER DEFAULT 0);
    CREATE TABLE IF NOT EXISTS events(
      id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
      ts TEXT NOT NULL, type TEXT NOT NULL, meta TEXT,
      FOREIGN KEY(user_id) REFERENCES users(user_id));
    -- список известных расписаний (по датам)
    CREATE TABLE IF NOT EXISTS schedules(
      date_label TEXT PRIMARY KEY,         -- '08.09'
      link_url   TEXT NOT NULL,
      google_url TEXT,
      created_at TEXT NOT NULL
    );
    -- хэши листов (чтобы видеть правки)
    CREATE TABLE IF NOT EXISTS sheet_hashes(
      date_label TEXT NOT NULL,
      gid        TEXT NOT NULL,
      title      TEXT,
      hash       TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      PRIMARY KEY(date_label, gid)
    );
    -- настройки пользователей (личный кабинет)
    CREATE TABLE IF NOT EXISTS user_prefs(
      user_id       INTEGER PRIMARY KEY,
      notify_new    INTEGER NOT NULL DEFAULT 1,
      notify_change INTEGER NOT NULL DEFAULT 1,
      klass         TEXT,
      updated_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%S', 'now'))
    );
    """); DB.commit()



def now_utc() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def upsert_user(u):
    if DB is None:
        return
    uid, first, uname = u.id, (u.first_name or "").strip(), (u.username or "").strip()
    if DB.execute("SELECT 1 FROM users WHERE user_id=?", (uid,)).fetchone():
        DB.execute("UPDATE users SET first_name=?, username=?, last_seen=?, msg_count=msg_count+1 WHERE user_id=?",
                   (first, uname, now_utc(), uid))
    else:
        DB.execute("INSERT INTO users(user_id,first_name,username,joined_at,last_seen,msg_count) VALUES (?,?,?,?,?,1)",
                   (uid, first, uname, now_utc(), now_utc()))
    DB.commit()


def log_event(uid: int, t: str, meta: str = ""):
    if DB is None:
        return
    DB.execute("INSERT INTO events(user_id,ts,type,meta) VALUES (?,?,?,?)", (uid, now_utc(), t, meta)); DB.commit()


def sched_get_all() -> Dict[str, Tuple[str, Optional[str]]]:
    """return {date_label: (link_url, google_url)}"""
    cur = DB.execute("SELECT date_label, link_url, google_url FROM schedules")
    return {d: (lu, gu) for d, lu, gu in cur.fetchall()}


def sched_upsert(date_label: str, link_url: str, google_url: Optional[str]):
    if DB.execute("SELECT 1 FROM schedules WHERE date_label=?", (date_label,)).fetchone():
        DB.execute("UPDATE schedules SET link_url=?, google_url=? WHERE date_label=?", (link_url, google_url, date_label))
    else:
        DB.execute("INSERT INTO schedules(date_label, link_url, google_url, created_at) VALUES (?,?,?,?)",
                   (date_label, link_url, google_url, now_utc()))
    DB.commit()


def hash_get(date_label: str, gid: str) -> Optional[str]:
    row = DB.execute("SELECT hash FROM sheet_hashes WHERE date_label=? AND gid=?", (date_label, gid)).fetchone()
    return row[0] if row else None


def hash_set(date_label: str, gid: str, title: str, h: str):
    if DB.execute("SELECT 1 FROM sheet_hashes WHERE date_label=? AND gid=?", (date_label, gid)).fetchone():
        DB.execute("UPDATE sheet_hashes SET title=?, hash=?, updated_at=? WHERE date_label=? AND gid=?",
                   (title, h, now_utc(), date_label, gid))
    else:
        DB.execute("INSERT INTO sheet_hashes(date_label, gid, title, hash, updated_at) VALUES (?,?,?,?,?)",
                   (date_label, gid, title, h, now_utc()))
    DB.commit()


# =========================
# user_prefs (личный кабинет)
# =========================

def _ensure_prefs_row(user_id: int):
    """Гарантирует наличие строки в user_prefs с дефолтами."""
    if DB is None:
        return
    if not DB.execute("SELECT 1 FROM user_prefs WHERE user_id=?", (user_id,)).fetchone():
        DB.execute("INSERT INTO user_prefs(user_id) VALUES (?)", (user_id,))
        DB.commit()


def prefs_get(user_id: int) -> Dict[str, Any]:
    """Возвращает словарь настроек пользователя."""
    if DB is None:
        return {"user_id": user_id, "notify_new": 1, "notify_change": 1, "klass": None}
    _ensure_prefs_row(user_id)
    row = DB.execute(
        "SELECT user_id, notify_new, notify_change, klass FROM user_prefs WHERE user_id=?",
        (user_id,)
    ).fetchone()
    uid, n_new, n_chg, klass = row
    return {"user_id": uid, "notify_new": int(n_new), "notify_change": int(n_chg), "klass": klass}


def prefs_set(
    user_id: int,
    *,
    notify_new: Optional[bool] = None,
    notify_change: Optional[bool] = None,
    klass: Any = _NOARG,  # _NOARG -> не менять; None -> записать NULL; str -> записать значение
):
    """Частично обновляет настройки пользователя. Поля с None/_NOARG — см. сигнатуру."""
    if DB is None:
        return
    _ensure_prefs_row(user_id)
    fields, vals = [], []
    if notify_new is not None:
        fields.append("notify_new=?"); vals.append(int(bool(notify_new)))
    if notify_change is not None:
        fields.append("notify_change=?"); vals.append(int(bool(notify_change)))
    if klass is not _NOARG:
        fields.append("klass=?"); vals.append(klass)  # допускаем None -> NULL
    if not fields:
        return
    fields.append("updated_at=?"); vals.append(now_utc())
    vals.append(user_id)
    DB.execute(f"UPDATE user_prefs SET {', '.join(fields)} WHERE user_id=?", vals)
    DB.commit()


def prefs_toggle(user_id: int, field: str) -> int:
    """Переключает флаг notify_new|notify_change. Возвращает новое значение (0/1)."""
    assert field in {"notify_new", "notify_change"}
    if DB is None:
        return 0
    _ensure_prefs_row(user_id)
    cur = DB.execute(f"SELECT {field} FROM user_prefs WHERE user_id=?", (user_id,))
    row = cur.fetchone()
    current = int(row[0]) if row else 1
    new_val = 0 if current else 1
    DB.execute(f"UPDATE user_prefs SET {field}=?, updated_at=? WHERE user_id=?",
               (new_val, now_utc(), user_id))
    DB.commit()
    return new_val


def prefs_users_for_new() -> List[int]:
    """Пользователи, подписанные на новые расписания и указавшие класс."""
    if DB is None:
        return []
    return [r[0] for r in DB.execute("SELECT user_id FROM user_prefs WHERE notify_new=1 AND klass IS NOT NULL")]


def prefs_users_for_changes() -> List[int]:
    """Пользователи, подписанные на уведомления об изменениях."""
    if DB is None:
        return []
    return [r[0] for r in DB.execute("SELECT user_id FROM user_prefs WHERE notify_change=1")]


def prefs_get_user_class(user_id: int) -> Optional[str]:
    """Возвращает выбранный класс пользователя или None."""
    if DB is None:
        return None
    row = DB.execute("SELECT klass FROM user_prefs WHERE user_id=?", (user_id,)).fetchone()
    return row[0] if row else None
