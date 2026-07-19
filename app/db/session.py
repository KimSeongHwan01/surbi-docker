# app/db/session.py
# DB 연결 설정
# - 비동기 세션: FastAPI routers/services
# - 동기 세션: 배치 수집 파이프라인

import os
from collections.abc import AsyncGenerator

from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

# .env 파일의 DATABASE_URL 읽기
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL 환경변수가 설정되지 않았습니다.")

if not DATABASE_URL.startswith("postgresql+asyncpg://"):
    raise RuntimeError("DATABASE_URL은 postgresql+asyncpg:// 형식이어야 합니다.")


# ── 공통 Base ────────────────────────────────────────────────
class Base(DeclarativeBase):
    pass


# ── 비동기 세션: FastAPI routers/services용 ──────────────────
async_engine = create_async_engine(
    DATABASE_URL,
    echo=False,  # True로 바꾸면 SQL 쿼리 로그 출력 (디버깅용)
    pool_pre_ping=True,  # DB 연결 끊김 자동 복구
)

AsyncSessionLocal = sessionmaker(
    bind=async_engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


# ── 동기 세션: 배치 파이프라인용 ─────────────────────────────
SYNC_DATABASE_URL = DATABASE_URL.replace(
    "postgresql+asyncpg://",
    "postgresql+psycopg2://",
    1,
)

sync_engine = create_engine(
    SYNC_DATABASE_URL,
    echo=False,
    pool_pre_ping=True,
)

SyncSessionLocal = sessionmaker(
    bind=sync_engine,
    class_=Session,
    expire_on_commit=False,
)
