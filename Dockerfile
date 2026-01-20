# Используем официальный образ Python как базовый
FROM python:3.12.5-slim

# Define environment variable
ENV PYTHONUNBUFFERED=1

# Устанавливаем необходимые системные библиотеки
RUN apt-get update && apt-get install -y \
    build-essential \
    python3-dev \
    && rm -rf /var/lib/apt/lists/* \
    sudo
    
# Устанавливаем необходимые пакеты для работы с локалями
RUN apt-get update && apt-get install -y locales && rm -rf /var/lib/apt/lists/*

# Добавляем локаль ru_RU.UTF-8 в систему
RUN echo "ru_RU.UTF-8 UTF-8" > /etc/locale.gen && locale-gen

# Устанавливаем локаль по умолчанию
ENV LANG=ru_RU.UTF-8 \
    LANGUAGE=ru_RU:ru \
    LC_ALL=ru_RU.UTF-8








# Устанавливаем рабочую директорию
WORKDIR /app

# Установка зависимостей
RUN apt-get update && apt-get install -y libpq-dev build-essential
# Копируем файл зависимостей и устанавливаем их
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && pip install --no-cache-dir -r requirements.txt

# Копируем код приложения в контейнер
COPY ./ /app

# Открываем порт, на котором будет работать приложение
EXPOSE 8000

# Команда для запуска приложения с Uvicorn
CMD ["python", "./main.py"]