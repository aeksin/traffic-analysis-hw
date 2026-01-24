import logging
import re

import numpy as np

from ..context import PipelineContext
from ..utils.currency import load_fx_rates
from ..utils.text import normalize_spaces, safe_lower
from .base import Handler

logger = logging.getLogger(__name__)


_SALARY_COL = "ЗП"


def _detect_currency(s: str) -> str | None:
    """Определить валюту по текстовому представлению зарплаты.

    По строке ищутся характерные маркеры валюты (например, `руб`, `usd`, `€`).
    Если валюта не распознана, возвращается `None`.

    Аргументы:
        s: Текстовое значение зарплаты.

    Возвращает:
        Код валюты (например, `RUB`, `USD`, `EUR`) или `None`, если определить не удалось.
    """
    t = s.lower()
    if "руб" in t or "rur" in t or "rub" in t or "₽" in t:
        return "RUB"
    if "usd" in t or "$" in t:
        return "USD"
    if "eur" in t or "€" in t:
        return "EUR"
    if "kzt" in t or "тенге" in t:
        return "KZT"
    if "byn" in t or "бел" in t:
        return "BYN"
    if "uah" in t or "грн" in t:
        return "UAH"
    if "uzs" in t or "сум" in t:
        return "UZS"
    if "gel" in t or "лари" in t:
        return "GEL"
    if "amd" in t or "драм" in t:
        return "AMD"
    if "azn" in t or "манат" in t:
        return "AZN"
    return None


_NUM_RE = re.compile(r"(\d[\d\s\u00a0]*)")


def _extract_numbers(s: str) -> list[int]:
    """Извлечь целые числа из строки.

    По регулярному выражению находятся группы цифр (в том числе с пробелами и
    неразрывными пробелами), после чего они нормализуются и преобразуются в `int`.

    Аргументы:
        s: Текст, в котором нужно найти числа.

    Возвращает:
        Список найденных чисел в порядке появления в строке.
    """
    nums: list[int] = []
    for m in _NUM_RE.finditer(s):
        raw = m.group(1)
        raw = raw.replace("\u00a0", " ").replace(" ", "")
        if raw.isdigit():
            nums.append(int(raw))
    return nums


class ParseSalaryHandler(Handler):
    """Преобразовать зарплату в рублях в колонку `target_salary_rub`."""

    def _handle(self, ctx: PipelineContext) -> PipelineContext:
        """Распарсить зарплату и вычислить целевую зарплату в рублях.

        Аргументы:
            ctx: Контекст пайплайна.

        Возвращает:
            Контекст пайплайна с обновлённым `ctx.df`, содержащим колонки
            `salary_currency` и `target_salary_rub`.

        Исключения:
            ValueError: Если DataFrame отсутствует в контексте.
            ValueError: Если в DataFrame отсутствует обязательная колонка `ЗП`.
        """
        if ctx.df is None:
            raise ValueError("DataFrame is not loaded")

        df = ctx.df.copy()

        if _SALARY_COL not in df.columns:
            raise ValueError(f"Required target column '{_SALARY_COL}' not found")

        fx = load_fx_rates(ctx.output_dir)
        ctx.diag["fx_rates_source"] = fx.source

        raw_series = df[_SALARY_COL].astype(object)

        target = np.full(shape=(len(df),), fill_value=np.nan, dtype=float)
        curr_out = np.array([""] * len(df), dtype=object)

        for i, val in enumerate(raw_series):
            if not isinstance(val, str):
                continue
            s = normalize_spaces(val)
            if not s:
                continue
            t = safe_lower(s)
            if "договор" in t or "по договор" in t or "negotiable" in t:
                continue

            cur = _detect_currency(s)
            if cur is None:
                cur = "RUB"

            nums = _extract_numbers(s)
            if not nums:
                continue

            amount = float(nums[0])
            if len(nums) >= 2:
                amount = (nums[0] + nums[1]) / 2.0

            if "тыс" in t or "k" in t:
                amount *= 1000.0

            rate = fx.rates.get(cur)
            if rate is None:
                continue

            rub = amount * float(rate)
            if rub <= 0:
                continue

            target[i] = rub
            curr_out[i] = cur

        df["salary_currency"] = curr_out
        df["target_salary_rub"] = target

        nan_mask = np.isnan(target)
        ctx.diag["rows_with_nan_target"] = int(nan_mask.sum())
        if nan_mask.any():
            ex = df.loc[nan_mask, [_SALARY_COL, "salary_currency"]].head(20).copy()
            ctx.diag["nan_target_examples"] = ex

        ctx.diag["target_zeros"] = int((target == 0).sum())

        ctx.df = df
        return ctx
