FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    # 让 malloc 更积极地把空闲堆归还操作系统，利于 256 MiB 上限
    PYTHONMALLOC=malloc
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY samples ./samples
COPY scripts ./scripts

# 作业 spool 目录：docker-compose 把它挂为受限 tmpfs
RUN mkdir -p /app/.jobs

EXPOSE 8000
# 单 worker 即可满足批处理场景；多 worker 会成倍放大内存占用
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
