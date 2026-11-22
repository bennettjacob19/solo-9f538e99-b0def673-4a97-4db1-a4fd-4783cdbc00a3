FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# 三个服务共用此镜像，通过 command 选择入口
EXPOSE 8080 8081 8000

CMD ["python", "-m", "app.gateway"]
