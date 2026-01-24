import re
from typing import Iterable

_SPACE_RE = re.compile(r"\s+")


def normalize_spaces(s: str) -> str:
    return _SPACE_RE.sub(" ", s.replace("\xa0", " ")).strip()


def safe_lower(s: object) -> str:
    if not isinstance(s, str):
        return ""
    return normalize_spaces(s).lower()


def split_multi_categories(text: object) -> list[str]:
    """Split a multi-value HH field like ' полная занятость, частичная занятость'."""
    if not isinstance(text, str):
        return []
    s = normalize_spaces(text)
    if not s:
        return []
    parts = re.split(r"[,;/]", s)
    out: list[str] = []
    for p in parts:
        p = normalize_spaces(p).lower()
        if p:
            out.append(p)
    return out


def contains_any(haystack: str, needles: Iterable[str]) -> bool:
    return any(n in haystack for n in needles)


def extract_first_int(text: str) -> int | None:
    m = re.search(r"(\d+)", text)
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def normalize_city_name(value: object) -> str:
    """Нормализовать название города для дальнейшей группировки.

    Преобразования:
    - приводит к нижнему регистру;
    - удаляет служебные префиксы вроде "г.", "город";
    - заменяет частые варианты написания (например, "spb" → "санкт-петербург").

    Аргументы:
        value: Исходное значение поля города.

    Возвращает:
        Нормализованная строка (может быть пустой, если город не распознан).
    """

    s = safe_lower(value)
    if not s:
        return ""

    # Убираем BOM и управляющие символы, которые иногда встречаются в CSV.
    s = s.replace("\ufeff", "")

    # Убираем распространённые префиксы.
    s = re.sub(r"^(г\.|город)\s+", "", s).strip()

    # Приводим к единому виду дефисы.
    s = s.replace("—", "-").replace("–", "-")

    # Нормализуем известные английские/сокращённые варианты.
    aliases = {
        "msk": "москва",
        "moscow": "москва",
        "spb": "санкт-петербург",
        "saint petersburg": "санкт-петербург",
        "st petersburg": "санкт-петербург",
        "st. petersburg": "санкт-петербург",
        "petersburg": "санкт-петербург",
        "saint-petersburg": "санкт-петербург",
        "санкт петербург": "санкт-петербург",
        "питер": "санкт-петербург",
    }

    s = aliases.get(s, s)
    return s
