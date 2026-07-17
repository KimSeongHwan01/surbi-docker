# workers/main.py
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


import logging
import os

import httpx
from arq import cron
from arq.connections import RedisSettings

from app.pipelines.districts import run_districts_pipeline
from app.pipelines.government_supports import run_government_supports_pipeline
from app.pipelines.market_trends import run_market_trends_pipeline
from app.pipelines.populations import run_populations_pipeline
from app.pipelines.sales_stats import run_sales_pipeline

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")


# ── Webhook 알림 함수 ─────────────────────────────────────────
async def send_webhook(message: str, success: bool = True) -> None:
    """
    Discord webhook으로 성공/실패 알림 전송
    - WEBHOOK_URL 미설정 시 조용히 무시
    - 전송 실패해도 파이프라인에 영향 없음
    """
    if not WEBHOOK_URL:
        return

    color = 3066993 if success else 15158332  # 초록: 성공, 빨강: 실패
    payload = {
        "embeds": [
            {
                "description": message,
                "color": color,
            }
        ]
    }

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.post(WEBHOOK_URL, json=payload)
            response.raise_for_status()
    except Exception:
        logger.warning(
            "Discord webhook 전송 실패 (파이프라인에 영향 없음)", exc_info=True
        )


# ── 로깅 설정 ─────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logging.getLogger("httpx").setLevel(logging.WARNING)


# ── 로거 설정 ─────────────────────────────────────────────────
logger = logging.getLogger(__name__)


# ── 작업 함수 정의 ────────────────────────────────────────────
# ARQ는 함수에 ctx 인자가 필요함
async def task_populations(ctx: dict) -> str:
    """생활인구 수집 — 매일"""
    try:
        await run_populations_pipeline()
        await send_webhook("✅ 생활인구 수집 완료")
        return "ok"
    except Exception as e:
        await send_webhook(f"❌ 생활인구 수집 실패\n{e}", success=False)
        raise


async def task_government_supports(ctx: dict) -> str:
    """정부지원사업 수집 — 매주 월요일"""
    try:
        await run_government_supports_pipeline()
        await send_webhook("✅ 정부지원사업 수집 완료")
        return "ok"
    except Exception as e:
        await send_webhook(f"❌ 정부지원사업 수집 실패\n{e}", success=False)
        raise


async def task_sales_stats(ctx: dict) -> str:
    """추정매출 수집 — 월 1회"""
    try:
        await run_sales_pipeline()
        await send_webhook("✅ 추정매출 수집 완료")
        return "ok"
    except Exception as e:
        await send_webhook(f"❌ 추정매출 수집 실패\n{e}", success=False)
        raise


async def task_market_trends(ctx: dict) -> str:
    """상권변화지표 수집 — 월 1회"""
    try:
        await run_market_trends_pipeline()
        await send_webhook("✅ 상권변화지표 수집 완료")
        return "ok"
    except Exception as e:
        await send_webhook(f"❌ 상권변화지표 수집 실패\n{e}", success=False)
        raise


async def task_districts(ctx: dict) -> str:
    """상가정보 수집 — 월 1회"""
    try:
        await run_districts_pipeline()
        await send_webhook("✅ 상가정보 수집 완료")
        return "ok"
    except Exception as e:
        await send_webhook(f"❌ 상가정보 수집 실패\n{e}", success=False)
        raise


# ── WorkerSettings ────────────────────────────────────────────
class WorkerSettings:
    """
    ARQ WorkerSettings — arq CLI가 이 클래스를 참조
    docker-compose.yml: command: python -m arq app.workers.main.WorkerSettings
    """

    redis_settings = RedisSettings.from_dsn(REDIS_URL)
    max_jobs = 1  # 파이프라인 순차 실행 (DB/API 경합 방지)
    job_timeout = 60 * 60  # 1시간 (대량 수집 작업 대비)

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
