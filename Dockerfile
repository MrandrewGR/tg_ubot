# tg_ubot/Dockerfile

# Build arguments (для кэш-бастинга и установки нужной версии внешней библиотеки)
ARG CACHEBUST=1
ARG LIB_COMMIT_HASH=main

FROM python:3.11-slim

# Показываем, какие ARG были переданы (необязательно, но полезно для отладки)
RUN echo "Cache bust: ${CACHEBUST}" && echo "Using mirco_services_data_management commit: ${LIB_COMMIT_HASH}"

# Установка окружения, необходимых пакетов
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends tzdata git && rm -rf /var/lib/apt/lists/*

# Часовой пояс
ENV TZ=Europe/Moscow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

WORKDIR /app

# Копируем и устанавливаем Python-зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Устанавливаем стороннюю библиотеку mirco_services_data_management из GitHub
# (Если вам она не нужна, удалите эти строки и уберите вызовы в коде)
RUN pip install --no-cache-dir --upgrade "git+https://github.com/MrandrewGR/mirco_services_data_management.git"

# Копируем исходный код приложения
COPY app/ ./app

# Создаём директории для логов и т.д. (по желанию)
RUN mkdir -p /app/logs/chat /app/logs/channel /app/logs/kafka /app/logs/utils /app/logs/userbot /app/media
RUN chmod -R 755 /app/logs /app/media

# Копируем скрипт ожидания сервисов (Kafka и пр.) и делаем исполняемым
COPY wait-for-it.sh /usr/local/bin/wait-for-it.sh
RUN chmod +x /usr/local/bin/wait-for-it.sh

# По умолчанию приложение ожидает Kafka, затем запускается
CMD ["wait-for-it.sh", "kafka:9092", "--", "python", "-m", "app.main"]
