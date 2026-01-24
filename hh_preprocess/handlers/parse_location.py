import logging

from ..context import PipelineContext
from ..utils.text import normalize_spaces, safe_lower
from .base import Handler

logger = logging.getLogger(__name__)


_CITY_COL = "Город"


class ParseLocationHandler(Handler):
    """Извлечь признаки города, готовности к переезду и командировкам."""

    def _handle(self, ctx: PipelineContext) -> PipelineContext:
        """Распарсить колонку «Город» и сформировать признаки мобильности.

        Из исходной колонки формируются признаки:
        - `city` — город проживания (первая часть строки до запятой);
        - `relocate_ready` — готовность к переезду (bool);
        - `trips_ready` — готовность к командировкам (bool).

        Если исходная колонка отсутствует, создаются значения по умолчанию:
        - `city = "Не указано"`;
        - `relocate_ready = False`;
        - `trips_ready = False`.

        Аргументы:
            ctx: Контекст пайплайна.

        Возвращает:
            Контекст пайплайна с обновлённым `ctx.df`, содержащим колонки
            `city`, `relocate_ready`, `trips_ready`.

        Исключения:
            ValueError: Если DataFrame отсутствует в контексте.
        """
        if ctx.df is None:
            raise ValueError("DataFrame is not loaded")
        df = ctx.df.copy()

        if _CITY_COL not in df.columns:
            logger.warning("Column '%s' not found; creating defaults.", _CITY_COL)
            df["city"] = "Не указано"
            df["relocate_ready"] = False
            df["trips_ready"] = False
            ctx.df = df
            return ctx

        def parse_city(s: object) -> str:
            """Извлечь город проживания из исходного значения.

            Аргументы:
                s: Значение из колонки «Город».

            Возвращает:
                Название города или «Не указано», если значение отсутствует
                либо не является строкой.
            """
            if not isinstance(s, str):
                return "Не указано"
            t = normalize_spaces(s)
            if not t:
                return "Не указано"
            # first part before comma
            return normalize_spaces(t.split(",")[0])

        def parse_relocate(s: object) -> bool:
            """Определить готовность к переезду по текстовому описанию.

            Используются ключевые фразы на русском и английском языках.
            По умолчанию (если ничего не распознано) возвращается `False`.

            Аргументы:
                s: Значение из колонки «Город».

            Возвращает:
                `True`, если указана готовность к переезду, иначе `False`.
            """
            t = safe_lower(s)
            if not t:
                return False
            if (
                "не готов к переезду" in t
                or "not ready to relocate" in t
                or "not willing to relocate" in t
            ):
                return False
            if (
                "готов к переезду" in t
                or "ready to relocate" in t
                or "willing to relocate" in t
            ):
                return True
            return False

        def parse_trips(s: object) -> bool:
            """Определить готовность к командировкам по текстовому описанию.

            Учитываются варианты на русском и английском языках.

            Аргументы:
                s: Значение из колонки «Город».

            Возвращает:
                `True`, если указана готовность к командировкам, иначе `False`.
            """
            t = safe_lower(s)
            if not t:
                return False
            if "не готов к командировкам" in t or "not ready for business trips" in t:
                return False
            if "готов к командировкам" in t or "готов к редким командировкам" in t:
                return True
            if "ready for business trips" in t or "willing to travel" in t:
                return True
            return False

        df["city"] = df[_CITY_COL].map(parse_city)
        df["relocate_ready"] = df[_CITY_COL].map(parse_relocate).astype(bool)
        df["trips_ready"] = df[_CITY_COL].map(parse_trips).astype(bool)

        ctx.df = df
        return ctx
