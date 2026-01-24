import logging

from ..context import PipelineContext
from ..utils.text import safe_lower
from .base import Handler

logger = logging.getLogger(__name__)


def categorize_job_title(title: object) -> str:
    """Сгруппировать название должности в укрупнённую категорию .

    Функция получает строку из таблицы набору ключевых слов
    возвращает одну из заранее заданных категорий.

    Аргументы:
        title: Значение из столбца с должностью (может быть строкой или NaN).

    Возвращает:
        Название укрупнённой категории. Если значение отсутствует или не распознано,
        возвращается «Не указано» или «Прочее».
    """
    t = safe_lower(title)
    if not t:
        return "Не указано"

    if "системный администратор" in t or "system administrator" in t or "sysadmin" in t:
        return "Системный администратор"
    if "devops" in t or "sre" in t or "site reliability" in t:
        return "DevOps/SRE"
    if "dba" in t or "администратор баз данных" in t or "database administrator" in t:
        return "Администратор баз данных"
    if any(
        k in t for k in ["data scientist", "ds ", "ml engineer", "machine learning"]
    ):
        return "Data Scientist/ML"
    if any(
        k in t
        for k in ["аналитик данных", "data analyst", "bi analyst", "business analyst"]
    ):
        return "Аналитик"
    if any(k in t for k in ["тестировщик", "qa", "quality assurance"]):
        return "Тестировщик"
    if any(
        k in t
        for k in [
            "разработчик",
            "программист",
            "developer",
            "software engineer",
            "backend",
            "frontend",
            "fullstack",
            "ios",
            "android",
            "java",
            "python",
            "c++",
            "golang",
            "php",
            "javascript",
            "node.js",
            "react",
            "vue",
            "1c",
            "1с",
            "unity",
        ]
    ):
        return "Программист/Разработчик"
    if "it" in t or "айти" in t:
        return "IT-специалист"

    if any(
        k in t
        for k in [
            "product manager",
            "product owner",
            "продакт",
            "product",
            "проектный менеджер",
            "project manager",
            "pm ",
        ]
    ):
        return "Менеджер проектов/Продукта"

    if any(
        k in t
        for k in [
            "маркетолог",
            "marketing",
            "smm",
            "таргет",
            "seo",
            "контент",
            "pr",
            "copywriter",
            "копирайтер",
        ]
    ):
        return "Маркетинг/PR/Контент"
    if any(
        k in t
        for k in [
            "продаж",
            "sales",
            "account manager",
            "менеджер по работе с клиентами",
            "клиентами",
            "торговый представитель",
            "кассир",
        ]
    ):
        return "Продажи/Клиенты"

    if any(
        k in t
        for k in [
            "бухгалтер",
            "accountant",
            "финанс",
            "экономист",
            "аудитор",
            "финансовый",
        ]
    ):
        return "Финансы/Бухгалтерия"

    if any(
        k in t for k in ["hr", "рекрутер", "подбор персонала", "recruiter", "talent"]
    ):
        return "HR/Рекрутер"

    if "юрист" in t or "lawyer" in t or "legal" in t:
        return "Юрист"

    if any(
        k in t
        for k in [
            "логист",
            "logistics",
            "склад",
            "warehouse",
            "курьер",
            "доставка",
            "водитель",
            "driver",
        ]
    ):
        return "Логистика/Склад/Транспорт"

    if any(
        k in t
        for k in [
            "дизайнер",
            "designer",
            "ux",
            "ui",
            "graphic",
            "графический",
            "иллюстратор",
            "illustrator",
            "3d",
            "2d",
        ]
    ):
        return "Дизайн/Креатив"

    if any(
        k in t
        for k in [
            "инженер",
            "engineer",
            "технолог",
            "электрик",
            "mechanic",
            "механик",
            "строител",
            "construction",
        ]
    ):
        return "Инженерия/Производство/Строительство"

    if any(
        k in t
        for k in [
            "секретарь",
            "assistant",
            "ассистент",
            "офис-менеджер",
            "администратор",
            "reception",
        ]
    ):
        return "Административный персонал"

    if "оператор" in t or "operator" in t:
        return "Оператор"

    if "специалист" in t or "specialist" in t:
        return "Специалист (общий)"

    return "Прочее"


class JobCategoryHandler(Handler):
    """Сформировать категориальные признаки профессии по желаемой и текущей должности."""

    def _handle(self, ctx: PipelineContext) -> PipelineContext:
        if ctx.df is None:
            raise ValueError("DataFrame is not loaded")
        df = ctx.df.copy()

        desired_col = next(
            (c for c in df.columns if "ищет работу на должность" in c.lower()), None
        )
        current_title_col = next(
            (
                c
                for c in df.columns
                if "нынешняя должность" in c.lower() or "нынешняя должност" in c.lower()
            ),
            None,
        )

        if desired_col is None:
            logger.warning(
                "Desired job title column not found; job_category='Не указано'"
            )
            df["job_category"] = "Не указано"
        else:
            df["job_category"] = df[desired_col].map(categorize_job_title)

        if current_title_col is None:
            logger.warning(
                "Current job title column not found; current_job_category='Не указано'"
            )
            df["current_job_category"] = "Не указано"
        else:
            df["current_job_category"] = df[current_title_col].map(categorize_job_title)

        drop_cols = [c for c in [desired_col, current_title_col] if c is not None]
        df = df.drop(columns=drop_cols, errors="ignore")

        employer_col = next(
            (c for c in df.columns if "место работы" in c.lower()), None
        )
        if employer_col is not None:
            df = df.drop(columns=[employer_col], errors="ignore")

        ctx.df = df
        return ctx
