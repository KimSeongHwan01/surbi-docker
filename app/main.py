# app/main.py
# FastAPI 애플리케이션 시작점
# 모든 라우터와 DB 연결을 여기서 통합

from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.db.session import engine


# 앱 시작/종료 시 실행할 작업 정의
@asynccontextmanager
async def lifespan(app: FastAPI):
    # 서버 시작 시
    print("Surbi API 서버 시작")
    yield
    # 서버 종료 시
    print("Surbi API 서버 종료")
    await engine.dispose()


# FastAPI 앱 생성
app = FastAPI(
    title="Surbi API",
    description="AI 기반 창업 상권 분석 플랫폼 API",
    version="0.1.0",
    lifespan=lifespan,
)


# 헬스체크 엔드포인트 — 서버 정상 동작 확인용
@app.get("/health")
async def health_check():
    return {"status": "ok", "service": "surbi-api"}


# ── 라우터 연결(추가 예정) ──────────────────────────────
# from app.routers import districts, scores, businesses, supports, auth, favorites
# app.include_router(districts.router, prefix="/api")
# app.include_router(scores.router, prefix="/api")
# app.include_router(businesses.router, prefix="/api")
# app.include_router(supports.router, prefix="/api")
# app.include_router(auth.router, prefix="/api")
# app.include_router(favorites.router, prefix="/api")
