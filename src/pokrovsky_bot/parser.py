import html
import re
from typing import Dict, List, Optional, Tuple
from .state import CLASS_PURE_RX, CLASS_LABEL_RX, TIME_RX
from .utils import norm, norm_soft, normalize_hyphens

HYPHENS = "-\u2010\u2011\u2012\u2013\u2014\u2212"
HCLASS = re.escape(HYPHENS)

# --- Режим проведения (онлайн/офлайн/дистант) ---
MODE_RX = re.compile(
    r"(?i)\b("
    r"онлайн|online|"
    r"оф+лайн|офлайн|offline|"
    r"дистанц\w*|дистант\w*|удал[её]нн\w*|"
    r"очно"
    r")\b"
)

def _normalize_mode(s: str) -> str:
    t = s.lower()
    if "онлайн" in t or "online" in t:
        return "онлайн"
    if "офлайн" in t or "offline" in t or "очно" in t:
        return "офлайн"
    if "дистан" in t or "дистант" in t or "удал" in t:
        return "дистант"
    return s.strip()

# --- Кабинеты/залы ---
CAB_CODE_RX = re.compile(
    rf"(?i)"
    rf"(?:\bкаб(?:инет)?\.?\s*[:\-]?\s*([A-Za-zА-Яа-я0-9_/ \t{HCLASS}]+))"
    rf"|(?:\b([А-ЯA-Z]\s*\d{{1,2}}\s*[{HCLASS}]\s*\d{{2}}(?:/\s*[А-ЯA-Z]\s*\d{{1,2}}\s*[{HCLASS}]\s*\d{{2}})*)\b)"
    rf"|(?:\b(спортзал(?:\s*\d*)?|актовый зал|спорт[ .-]?зал|ауд\.?\s*\d+)\b)"
)

def extract_cabinet(text: Optional[str]) -> Optional[str]:
    """Возвращает код кабинета/зал (в ВЕРХНЕМ_РЕГИСТРЕ без пробелов) или текстовый статус 'онлайн/офлайн/дистант'."""
    if not text:
        return None
    for raw_line in normalize_hyphens(norm_soft(text)).split("\n"):
        line = raw_line.strip()
        m_mode = MODE_RX.search(line)
        if m_mode:
            return _normalize_mode(m_mode.group(1))
        m = CAB_CODE_RX.search(line)
        if m:
            cab = next((g for g in m.groups() if g), None)
            if cab:
                import re as _re
                return _re.sub(r"\s+", "", normalize_hyphens(cab)).upper().replace("Ё", "Е")
    return None

# --- Лейблы классов ---
def parse_class_label(cell: str) -> Optional[str]:
    s = norm(cell).upper().replace("Ё", "Е")
    m = CLASS_LABEL_RX.search(s)
    if not m:
        return None
    suf = re.sub(r"[ \-\d]+", "", m.group(2))
    return f"{m.group(1)}{suf}" if suf else None

def grade_from_label(label: str) -> Optional[int]:
    m = re.match(r"(\d{1,2})", label)
    return int(m.group(1)) if m else None

# --- Заголовки и границы секций ---
def parse_headers(rows: List[List[str]]) -> Tuple[Dict[str, Tuple[int, int, int]], List[int]]:
    labels: Dict[str, Tuple[int, int, int]] = {}
    headers: List[int] = []
    for i, row in enumerate(rows[:400]):
        cells = [norm(c) for c in row]
        if not any("время" in (c or "").lower() for c in cells):
            continue
        time_cols = [j for j, c in enumerate(cells) if "время" in (c or "").lower()]
        if not time_cols:
            continue
        time_col = time_cols[0]
        headers.append(i)
        for j, cell in enumerate(cells):
            label = parse_class_label(cell)
            if label:
                labels[label] = (i, time_col, j)
    return labels, headers

def next_header(headers: List[int], idx: int, total_rows: int) -> int:
    for h in headers:
        if h > idx:
            return h
    return total_rows

def right_boundary(
    labels: Dict[str, Tuple[int, int, int]],
    headers: List[int],
    label: str,
    total_cols: int,
) -> int:
    hdr, _, subj_col = labels[label]
    same = sorted(col for (h, _t, col) in labels.values() if h == hdr)
    next_cols = [c for c in same if c > subj_col]
    return min(next_cols[0] if next_cols else total_cols - 1, total_cols - 1)

# --- Поиск колонки кабинетов (для обычного расписания классов; можем не использовать) ---
def detect_cab_col(rows: List[List[str]], hdr: int, subj_col: int, end_row: int, right_bound: int) -> Optional[int]:
    for cand in range(subj_col + 1, min(subj_col + 4, right_bound) + 1):
        if cand < len(rows[hdr]) and "каб" in norm(rows[hdr][cand]).lower():
            return cand
    best, hits = None, -1
    for cand in range(subj_col + 1, min(subj_col + 4, right_bound) + 1):
        cnt = 0
        for row in rows[hdr + 1 : min(end_row, hdr + 19)]:
            if cand < len(row) and extract_cabinet(row[cand]):
                cnt += 1
        if cnt > hits:
            best, hits = cand, cnt
    return best if hits > 0 else None

def build_cab_map(
    rows: List[List[str]],
    labels: Dict[str, Tuple[int, int, int]],
    headers: List[int]
) -> Dict[str, Tuple[Optional[int], int]]:
    total_cols, total_rows = max((len(r) for r in rows), default=0), len(rows)
    m: Dict[str, Tuple[Optional[int], int]] = {}
    for lb, (hdr, _t, subj_col) in labels.items():
        end = next_header(headers, hdr, total_rows)
        right = right_boundary(labels, headers, lb, total_cols)
        m[lb] = (detect_cab_col(rows, hdr, subj_col, end, right), right)
    return m

# --- Время ---
def _normalize_time(s: str) -> str:
    s = (s or "").replace(".", ":").strip()
    s = re.sub(r"\s*[-–—]\s*", " - ", s)
    return s

# --- Фильтр "линейки" ---
LINEYKA_TOKEN_RX = re.compile(r"(?i)\bлинейк[аи]\b")

def _strip_lineyka(subj: Optional[str]) -> Optional[str]:
    if not subj:
        return None
    s = normalize_hyphens(norm_soft(subj)).strip()
    parts = re.split(r"[;,/|\n]+", s)
    keep: List[str] = []
    for p in parts:
        t = p.strip()
        if not t:
            continue
        if LINEYKA_TOKEN_RX.search(t):
            continue
        keep.append(t)
    out = " / ".join(keep).strip()
    return out or None

# --- Извлечение расписания (классическое, с опорой на near-колонки) ---
def extract_schedule(
    rows: List[List[str]],
    labels: Dict[str, Tuple[int, int, int]],
    headers: List[int],
    label: str,
    cab_info: Tuple[Optional[int], int],
):
    hdr, time_col, subj_col = labels[label]
    end = next_header(headers, hdr, len(rows))
    total_cols = max((len(r) for r in rows), default=0)
    _cab_col, right_bound_in = cab_info
    right_bound = right_bound_in if (isinstance(right_bound_in, int) and right_bound_in > 0) else (total_cols - 1)

    out: List[Tuple[str, Optional[str], Optional[str]]] = []
    r = hdr + 1
    while r < end:
        row = rows[r]
        subj_cell = row[subj_col] if subj_col < len(row) else ""
        subj = norm_soft(subj_cell).strip() or None
        if subj and CLASS_PURE_RX.match(subj):
            break

        t_here = norm_soft(row[time_col]) if time_col < len(row) else ""
        t_next = norm_soft(rows[r + 1][time_col]) if (r + 1) < end and time_col < len(rows[r + 1]) else ""

        # вариант: предмет здесь, время на следующей строке
        if subj and t_next and not t_here and (r + 1) < end:
            time_range = _normalize_time(t_next)
            cab = None
            # пробуем найти кабинет рядом
            # 1) в следующей строке, справа от предмета
            cc = subj_col + 1
            if cc <= right_bound and cc < len(rows[r + 1]):
                cab = extract_cabinet(rows[r + 1][cc])
            # 2) в этой строке
            if not cab:
                cc0 = subj_col + 1
                if cc0 <= right_bound and cc0 < len(row):
                    cab = extract_cabinet(row[cc0])
            subj_clean = _strip_lineyka(subj)
            if subj_clean:
                out.append((time_range, subj_clean, cab))
            r += 2
            continue

        # обычный случай
        time_range = _normalize_time(t_here)
        cab = None
        if subj:
            # 1) в след. строке
            if (r + 1) < end:
                cc = subj_col + 1
                if cc <= right_bound and cc < len(rows[r + 1]):
                    cab = extract_cabinet(rows[r + 1][cc])
            # 2) в этой строке
            if not cab:
                cc0 = subj_col + 1
                if cc0 <= right_bound and cc0 < len(row):
                    cab = extract_cabinet(row[cc0])

        if not time_range and not subj:
            r += 1
            continue

        subj_clean = _strip_lineyka(subj) if subj else None
        if subj_clean:
            out.append((time_range, subj_clean, cab))
        r += 1

    return out

# --- НОВОЕ: извлечение расписания БЕЗ опоры на "сетку кабинетов" (ищем кабинет «где получится») ---
def extract_schedule_anycab(
    rows: List[List[str]],
    labels: Dict[str, Tuple[int, int, int]],
    headers: List[int],
    label: str,
) -> List[Tuple[str, Optional[str], Optional[str]]]:
    """
    Парсим пары для класса без явной колонки 'каб.'.
    Ищем кабинет только в своей зоне:
      1) в след. строке в колонке subj_col+1 (главный сценарий),
      2) в след. строке в subj_col,
      3) в этой строке в subj_col+1,
      4) в этой строке в subj_col,
      5) как крайний случай — subj_col+2 (эта и/или след. строка).
    ВЛЕВО НЕ СМОТРИМ, чтобы не цеплять соседний класс.
    """
    hdr, time_col, subj_col = labels[label]
    end = next_header(headers, hdr, len(rows))
    total_cols = max((len(r) for r in rows), default=0)

    # правая граница зоны класса — не дальше следующего класса и не дальше subj_col+3
    rb = right_boundary(labels, headers, label, total_cols - 1)
    right_bound = min(rb, subj_col + 3)

    def grab(r_idx: int, c_idx: int) -> Optional[str]:
        if r_idx < 0 or r_idx >= len(rows):
            return None
        row = rows[r_idx]
        if c_idx < 0 or c_idx >= len(row) or c_idx > right_bound:
            return None
        return extract_cabinet(row[c_idx])

    out: List[Tuple[str, Optional[str], Optional[str]]] = []
    r = hdr + 1

    while r < end:
        row = rows[r]
        subj_cell = row[subj_col] if subj_col < len(row) else ""
        subj_raw = norm_soft(subj_cell).strip() or None
        if subj_raw and CLASS_PURE_RX.match(subj_raw):
            break

        # время
        t_here = norm_soft(row[time_col]) if time_col < len(row) else ""
        t_next = norm_soft(rows[r + 1][time_col]) if (r + 1) < end and time_col < len(rows[r + 1]) else ""
        if subj_raw and t_next and not t_here and (r + 1) < end:
            time_range = _normalize_time(t_next)
            jump = 2
        else:
            time_range = _normalize_time(t_here)
            jump = 1

        # поиск кабинета — узкий приоритетный список
        cab = None
        if (r + 1) < end:
            cab = grab(r + 1, subj_col + 1) or grab(r + 1, subj_col)
        if cab is None:
            cab = grab(r, subj_col + 1) or grab(r, subj_col)
        if cab is None:
            # последний шанс в своей зоне — +2
            if (r + 1) < end:
                cab = grab(r + 1, subj_col + 2)
            if cab is None:
                cab = grab(r, subj_col + 2)

        # пропуск пустых
        if not time_range and not subj_raw:
            r += jump
            continue

        subj_clean = _strip_lineyka(subj_raw) if subj_raw else None
        if subj_clean:
            out.append((time_range, subj_clean, cab))

        r += jump

    return out


# --- Схлопывание и вывод ---
def _time_key(t: str) -> str:
    s = (t or "").replace(".", ":").strip()
    m = re.search(r'(\d{1,2}):(\d{2}).*?(\d{1,2}):(\d{2})', s)
    return f"{m.group(1).zfill(2)}:{m.group(2)}-{m.group(3).zfill(2)}:{m.group(4)}" if m else s

def collapse_by_time(items: List[tuple]) -> List[tuple]:
    order, subj_map, cab_map = [], {}, {}
    for t, subj, cab in items:
        k = _time_key(t)
        if k not in order:
            order.append(k); subj_map[k] = []; cab_map[k] = []
        s = (subj or "").strip()
        if s and s not in subj_map[k]:
            subj_map[k].append(s)
        if s and cab and cab.strip() and cab not in cab_map[k]:
            cab_map[k].append(cab.strip())
    out = []
    for k in order:
        if not k:
            continue
        sj = " / ".join(subj_map.get(k, [])) or "—"
        cb = "/".join(cab_map.get(k, [])) or None
        t = f"{k.split('-', 1)[0]} - {k.split('-', 1)[1]}" if "-" in k and ":" in k else k
        out.append((t, sj, cb))
    return out

def pretty(date_label: str, klass: str, items: List[tuple]) -> str:
    lines = [f"<b>РАСПИСАНИЕ НА {html.escape(date_label)}</b>", f"Класс: <b>{html.escape(klass)}</b>", ""]
    for i, (t, subj, cab) in enumerate(items, 1):
        subj_html = f"<b>{html.escape(subj)}</b>" if subj else "—"
        show_time = bool(re.search(r"\d{1,2}[:.]\d{2}.*\d{1,2}[:.]\d{2}", t))
        lines.append(f"{i} — ({html.escape(t)}) {subj_html}" if show_time else f"{i} — {subj_html}")
        lines.append(f"Кабинет: <b>{html.escape(cab)}</b>" if cab else "Кабинет: <b>—</b>")
    return "\n".join(lines) if items else "Пусто."
