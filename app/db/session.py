# app/db/session.py
# DB 연결 설정 — SQLAlchemy 비동기 엔진

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import declarative_base, sessionmaker
import os

# .env 파일의 DATABASE_URL 읽기
DATABASE_URL = os.getenv("DATABASE_URL")

# DB 엔진 생성
engine = create_async_engine(
    DATABASE_URL,
    echo=False,       # True로 바꾸면 SQL 쿼리 로그 출력 (디버깅용)
    pool_pre_ping=True  # DB 연결 끊김 자동 복구
)

# 세션 팩토리 생성
AsyncSessionLocal = sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False
)

# 모든 모델의 기반 클래스
Base = declarative_base()


# DB 세션을 가져오는 함수 (파이프라인/routers에서 사용)
async def get_db():
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()