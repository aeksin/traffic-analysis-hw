# HH preprocessing pipeline

Проект реализует пайплайн предобработки датасета HeadHunter
с использованием паттерна проектирования «Цепочка ответственности».

## Запуск

```bash
python app path/to/hh.csv
```

В директории с входным файлом будут созданы:
- `x_data.npy` — матрица признаков
- `y_data.npy` — целевая переменная (зарплата)

## Структура проекта

- `hh_preprocess/handlers` — обработчики цепочки
- `hh_preprocess/context.py` — контекст пайплайна
- `hh_preprocess/pipeline.py` — сборка цепочки
- `app` — CLI-точка входа
