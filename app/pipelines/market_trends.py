# app/pipelines/market_trends.py
# market_trends 데이터 수집 파이프라인
# 서울시 상권분석서비스 상권변화지표(행정동) API — VwsmAdstrdIxQq

import httpx
import asyncio
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from app.db.session import AsyncSessionLocal
from app.models.districts import District
from app.models.market_trends import MarketTrend
import os

# ── 상수 정의 ────────────────────────────────────────────────

SEOUL_API_KEY = os.getenv("SEOUL_API_KEY")
BASE_URL = f"http://openapi.seoul.go.kr:8088/{SEOUL_API_KEY}/json/VwsmAdstrdIxQq"


# ── API 호출 함수 ─────────────────────────────────────────────

async def fetch_market_trends(start: int = 1, end: int = 1000):
    """서울시 상권변화지표 API 호출"""
    url = f"{BASE_URL}/{start}/{end}/"

    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.get(url)
        response.raise_for_status()
        return response.json()


# ── district_id 조회 함수 ─────────────────────────────────────

async def get_district_id(session, district_code: str):
    """district_code로 district_id 조회"""
    result = await session.execute(
        select(District.id).where(District.district_code == district_code)
    )
    return result.scalar_one_or_none()


# ── market_trends 적재 함수 ───────────────────────────────────

async def upsert_market_trend(session, row: dict, district_id: int):
    """
    market_trends 테이블 UPSERT
    - UNIQUE: (district_id, period_code)
    - HH=정체, HL=상권축소, LH=상권확장, LL=다이나믹
    """
    stmt = insert(MarketTrend).values(
        district_id=district_id,
        period_code=row["STDR_YYQU_CD"],
        trend_grade=row.get("TRDAR_CHNGE_IX"),
        trend_grade_nm=row.get("TRDAR_CHNGE_IX_NM"),
        opr_sale_mt_avrg=row.get("OPR_SALE_MT_AVRG"),
        cls_sale_mt_avrg=row.get("CLS_SALE_MT_AVRG"),
        su_opr_sale_mt_avrg=row.get("SU_OPR_SALE_MT_AVRG"),
        su_cls_sale_mt_avrg=row.get("SU_CLS_SALE_MT_AVRG"),
    ).on_conflict_do_update(
        index_elements=["district_id", "period_code"],
        set_=dict(
            trend_grade=row.get("TRDAR_CHNGE_IX"),
            trend_grade_nm=row.get("TRDAR_CHNGE_IX_NM"),
        )
    )
    await session.execute(stmt)


# ── 메인 파이프라인 함수 ──────────────────────────────────────

async def run_market_trends_pipeline():
    """
    market_trends 전체 파이프라인 실행
    - 분기 입력값 없음 — 전체 데이터 한번에 조회
    - 페이지네이션 처리
    """
    print("상권변화지표 수집 시작")

    async with AsyncSessionLocal() as session:
        start = 1
        end = 1000
        total_count = None

        while True:
            data = await fetch_market_trends(start=start, end=end)
            result = data.get("VwsmAdstrdIxQq", {})
            rows = result.get("row", [])

            if total_count is None:
                total_count = result.get("list_total_count", 0)
                print(f"  총 {total_count}건")

            if not rows:
                break

            for row in rows:
                district_id = await get_district_id(session, row["ADSTRD_CD"])
                if district_id is None:
                    continue

                await upsert_market_trend(session, row, district_id)

            await session.commit()
            print(f"  {start}~{end} 완료 ({len(rows)}건)")

            if end >= total_count:
                break
            start += 1000
            end += 1000

    print("상권변화지표 수집 완료!")


# ── 직접 실행 시 ──────────────────────────────────────────────
if __name__ == "__main__":
    asyncio.run(run_market_trends_pipeline())