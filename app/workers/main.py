# app/workers/main.py
# ARQ Worker 진입점
# 공공데이터 수집 파이프라인 자동화 스케줄러
#
# 실행 방식: docker-compose.yml worker 서비스가 자동 실행
# command: python -m arq app.workers.main.WorkerSettings
#
# 스케줄 정책:
# - populations        : 매일 새벽 3시 (API 약 5일 지연 제공)
# - government_supports: 매주 월요일 새벽 3시
# - sales_stats        : 월 1회 (매월 1일 새벽 3시)
# - market_trends      : 월 1회 (매월 1일 새벽 3시)
# - districts          : 월 1회 (매월 1일 새벽 4시)

import os
from arq import cron
from arq.connections import RedisSettings

from app.pipelines.populations import run_populations_pipeline
from app.pipelines.government_supports import run_government_supports_pipeline
from app.pipelines.sales_stats import run_sales_pipeline
from app.pipelines.market_trends import run_market_trends_pipeline
from app.pipelines.districts import run_districts_pipeline

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")


# ── 작업 함수 정의 ────────────────────────────────────────────
# ARQ는 함수에 ctx 인자가 필요함

async def task_populations(ctx: dict) -> str:
    """생활인구 수집 — 매일"""
    await run_populations_pipeline()
    return "ok"


async def task_government_supports(ctx: dict) -> str:
    """정부지원사업 수집 — 매주 월요일"""
    await run_government_supports_pipeline()
    return "ok"


async def task_sales_stats(ctx: dict) -> str:
    """추정매출 수집 — 월 1회"""
    await run_sales_pipeline()
    return "ok"


async def task_market_trends(ctx: dict) -> str:
    """상권변화지표 수집 — 월 1회"""
    await run_market_trends_pipeline()
    return "ok"


async def task_districts(ctx: dict) -> str:
    """상가정보 수집 — 월 1회"""
    await run_districts_pipeline()
    return "ok"


# ── WorkerSettings ────────────────────────────────────────────

class WorkerSettings:
    """
    ARQ WorkerSettings — arq CLI가 이 클래스를 참조
    docker-compose.yml: command: python -m arq app.workers.main.WorkerSettings
    """
    redis_settings = RedisSettings.from_dsn(REDIS_URL)

    functions = [
        task_populations,
        task_government_supports,
        task_sales_stats,
        task_market_trends,
        task_districts,
    ]

    cron_jobs = [
        # 생활인구 — 매일 새벽 3시
        cron(task_populations, hour=3, minute=0),

        # 정부지원사업 — 매주 월요일 새벽 3시
        cron(task_government_supports, weekday=0, hour=3, minute=0),

        # 추정매출 — 월 1회 (매월 1일 새벽 3시)
        cron(task_sales_stats, day=1, hour=3, minute=0),

        # 상권변화지표 — 월 1회 (매월 1일 새벽 3시)
        cron(task_market_trends, day=1, hour=3, minute=0),

        # 상가정보 — 월 1회 (매월 1일 새벽 4시)
        cron(task_districts, day=1, hour=4, minute=0),
    ]