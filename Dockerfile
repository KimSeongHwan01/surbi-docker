# Dockerfile
# multi-stage build: development / production

# ── 공통 base ────────────────────────────────
FROM python:3.12-slim AS base

WORKDIR /app

# 시스템 의존성 (PostGIS 클라이언트, GDAL)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    gdal-bin \
    libgdal-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


# ── 개발 스테이지 ─────────────────────────────
FROM base AS development

COPY requirements.dev.txt .
RUN pip install --no-cache-dir -r requirements.dev.txt

# 소스는 docker-compose.local.yml에서 볼륨 마운트로 주입
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]


# ── 프로덕션 스테이지 ─────────────────────────
FROM base AS production

# 소스 코드 복사 (이미지에 포함)
COPY . .

# 비root 사용자로 실행 (보안)
RUN addgroup --system surbi && adduser --system --ingroup surbi surbi
USER surbi

CMD ["gunicorn", "app.main:app", "-w", "2", "-k", "uvicorn.workers.UvicornWorker", "--bind", "0.0.0.0:8000"]
