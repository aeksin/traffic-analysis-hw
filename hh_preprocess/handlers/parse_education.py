import logging
import re

import numpy as np

from ..context import PipelineContext
from ..utils.text import safe_lower
from .base import Handler

logger = logging.getLogger(__name__)


_EDU_COL = "Образование и ВУЗ"
_YEAR_RE = re.compile(r"(19\d{2}|20\d{2})")


class ParseEducationHandler(Handler):
    """Извлечь признаки образования в `education_level` и `education_year`."""

    def _handle(self, ctx: PipelineContext) -> PipelineContext:
        """Распарсить информацию об образовании и добавить признаки уровня и года.

        Из колонки `Образование и ВУЗ` извлекаются:
        - укрупнённый уровень образования (`education_level`) по ключевым словам;
        - год окончания (`education_year`)

        Если колонка отсутствует, устанавливаются значения по умолчанию:
        `education_level = "Не указано"`, `education_year = NaN`.

        Аргументы:
            ctx: Контекст пайплайна.

        Возвращает:
            Контекст пайплайна с обновлённым `ctx.df`, содержащим колонки
            `education_level` и `education_year`.

        Исключения:
            ValueError: Если DataFrame отсутствует в контексте.
        """
        if ctx.df is None:
            raise ValueError("DataFrame is not loaded")
        df = ctx.df.copy()

        if _EDU_COL not in df.columns:
            logger.warning("Column '%s' not found; using defaults.", _EDU_COL)
            df["education_level"] = "Не указано"
            df["education_year"] = np.nan
            ctx.df = df
            return ctx

        def parse_level(val: object) -> str:
            """Определить укрупнённый уровень образования по текстовому значению.

            Правила основаны на поиске ключевых слов в строке. Если уровень
            определить не удалось, возвращается «Не указано».

            Аргументы:
                val: Значение из колонки «Образование и ВУЗ».

            Возвращает:
                Строковая категория уровня образования.
            """
            t = safe_lower(val)
            if not t:
                return "Не указано"
            if "доктор" in t:
                return "Доктор наук"
            if "кандидат" in t:
                return "Кандидат наук"
            if "неокончен" in t or "incomplete higher" in t:
                return "Неоконченное высшее"
            if (
                "высшее" in t
                or "higher education" in t
                or "bachelor" in t
                or "master" in t
            ):
                return "Высшее"
            if "среднее специаль" in t or "college" in t or "vocational" in t:
                return "Среднее специальное"
            if "среднее" in t or "secondary" in t:
                return "Среднее"
            return "Не указано"

        def parse_year(val: object) -> float:
            """Извлечь год окончания из текстового значения.

            Из строки извлекается год формата YYYY (19xx или 20xx). Далее
            выполняется проверка на разумный диапазон значений.

            Аргументы:
                val: Значение из колонки «Образование и ВУЗ».

            Возвращает:
                Год окончания (float) или `NaN`, если год отсутствует или некорректен.
            """
            t = safe_lower(val)
            if not t:
                return np.nan
            m = _YEAR_RE.search(t)
            if not m:
                return np.nan
            year = int(m.group(1))
            if 1950 <= year <= 2035:
                return float(year)
            return np.nan

        df["education_level"] = df[_EDU_COL].map(parse_level)
        df["education_year"] = df[_EDU_COL].map(parse_year)

        ctx.df = df
        return ctx
