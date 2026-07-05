# app/pipelines/sales_stats.py
# sales_stats 데이터 수집 파이프라인
# 서울시 상권분석서비스 추정매출(행정동) API
#
# 적재 흐름:
# Step 1: API 호출 (분기 코드 기준)
# Step 2: ADSTRD_CD → district_id 변환
# Step 3: float → int() 캐스팅 (금액/건수 컬럼)
# Step 4: sales_stats 테이블 UPSERT

import httpx
import asyncio
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from app.db.session import AsyncSessionLocal
from app.models.districts import District
from app.models.sales_stats import SalesStat
from datetime import datetime
import os

# ── 상수 정의 ────────────────────────────────────────────────

SEOUL_API_KEY = os.getenv("SEOUL_API_KEY")
BASE_URL = f"http://openapi.seoul.go.kr:8088/{SEOUL_API_KEY}/json/VwsmAdstrdSelngW"


# ── API 호출 결과 가장 최신 분기 반환 함수 ──────────────────────

async def get_latest_period_code() -> str:
    """
    실제 API 호출해서 데이터가 있는 가장 최신 분기 코드 반환
    """
    from datetime import datetime
    now = datetime.now()
    year = now.year
    month = now.month

    # 현재 분기부터 역순으로 확인
    if month <= 3:
        quarters = [(year-1, 4), (year-1, 3)]
    elif month <= 6:
        quarters = [(year, 1), (year-1, 4)]
    elif month <= 9:
        quarters = [(year, 2), (year, 1)]
    else:
        quarters = [(year, 3), (year, 2)]

    for y, q in quarters:
        period = f"{y}{q}"
        data = await fetch_sales(start=1, end=5, period_code=period)
        count = data.get("VwsmAdstrdSelngW", {}).get("list_total_count", 0)
        if count > 0:
            print(f"  최신 분기: {period}")
            return period

    return f"{year}1"


# ── API 호출 함수 ─────────────────────────────────────────────

async def fetch_sales(start: int = 1, end: int = 1000, period_code: str = None):
    """
    서울시 추정매출 API 호출
    - start/end: 페이지 범위
    - period_code: 기준년분기 (예: '20261'). 없으면 전체 조회
    """
    if period_code:
        url = f"{BASE_URL}/{start}/{end}/{period_code}/"
    else:
        url = f"{BASE_URL}/{start}/{end}/"

    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.get(url)
        response.raise_for_status()
        return response.json()


# ── 유틸리티 함수 ─────────────────────────────────────────────

def to_int(value):
    """
    float → int 변환
    API가 금액/건수를 float으로 내려옴 (예: 23947025.0 → 23947025)
    """
    if value is None or value == "":
        return None
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None


# ── district_id 조회 함수 ─────────────────────────────────────

async def get_district_id(session, district_code: str):
    """district_code로 district_id 조회"""
    result = await session.execute(
        select(District.id).where(District.district_code == district_code)
    )
    return result.scalar_one_or_none()


# ── sales_stats 적재 함수 ─────────────────────────────────────

async def upsert_sales_stat(session, row: dict, district_id: int):
    """
    sales_stats 테이블 UPSERT
    - UNIQUE: (district_id, category_code, period_code)
    - 금액/건수 컬럼 float → int() 캐스팅
    """
    stmt = insert(SalesStat).values(
        district_id=district_id,
        period_code=row["STDR_YYQU_CD"],
        category_code=row["SVC_INDUTY_CD"],
        category=row["SVC_INDUTY_CD_NM"],
        # 매출 금액 (API 응답 순서 기준)
        monthly_sales=to_int(row.get("THSMON_SELNG_AMT")),
        monthly_sales_count=to_int(row.get("THSMON_SELNG_CO")),
        weekday_sales=to_int(row.get("MDWK_SELNG_AMT")),
        weekend_sales=to_int(row.get("WKEND_SELNG_AMT")),
        # 요일별 매출금액
        mon_sales=to_int(row.get("MON_SELNG_AMT")),
        tue_sales=to_int(row.get("TUES_SELNG_AMT")),
        wed_sales=to_int(row.get("WED_SELNG_AMT")),
        thu_sales=to_int(row.get("THUR_SELNG_AMT")),
        fri_sales=to_int(row.get("FRI_SELNG_AMT")),
        sat_sales=to_int(row.get("SAT_SELNG_AMT")),
        sun_sales=to_int(row.get("SUN_SELNG_AMT")),
        # 시간대별 매출금액
        tmzon_00_06_sales=to_int(row.get("TMZON_00_06_SELNG_AMT")),
        tmzon_06_11_sales=to_int(row.get("TMZON_06_11_SELNG_AMT")),
        tmzon_11_14_sales=to_int(row.get("TMZON_11_14_SELNG_AMT")),
        tmzon_14_17_sales=to_int(row.get("TMZON_14_17_SELNG_AMT")),
        tmzon_17_21_sales=to_int(row.get("TMZON_17_21_SELNG_AMT")),
        tmzon_21_24_sales=to_int(row.get("TMZON_21_24_SELNG_AMT")),
        # 성별 매출금액
        male_sales=to_int(row.get("ML_SELNG_AMT")),
        female_sales=to_int(row.get("FML_SELNG_AMT")),
        # 연령대별 매출금액
        age10_sales=to_int(row.get("AGRDE_10_SELNG_AMT")),
        age20_sales=to_int(row.get("AGRDE_20_SELNG_AMT")),
        age30_sales=to_int(row.get("AGRDE_30_SELNG_AMT")),
        age40_sales=to_int(row.get("AGRDE_40_SELNG_AMT")),
        age50_sales=to_int(row.get("AGRDE_50_SELNG_AMT")),
        age60_sales=to_int(row.get("AGRDE_60_ABOVE_SELNG_AMT")),
        # 매출 건수 (API 응답 순서 기준)
        weekday_sales_count=to_int(row.get("MDWK_SELNG_CO")),
        weekend_sales_count=to_int(row.get("WKEND_SELNG_CO")),
        # 요일별 매출건수
        mon_sales_count=to_int(row.get("MON_SELNG_CO")),
        tue_sales_count=to_int(row.get("TUES_SELNG_CO")),
        wed_sales_count=to_int(row.get("WED_SELNG_CO")),
        thu_sales_count=to_int(row.get("THUR_SELNG_CO")),
        fri_sales_count=to_int(row.get("FRI_SELNG_CO")),
        sat_sales_count=to_int(row.get("SAT_SELNG_CO")),
        sun_sales_count=to_int(row.get("SUN_SELNG_CO")),
        # 시간대별 매출건수
        tmzon_00_06_count=to_int(row.get("TMZON_00_06_SELNG_CO")),
        tmzon_06_11_count=to_int(row.get("TMZON_06_11_SELNG_CO")),
        tmzon_11_14_count=to_int(row.get("TMZON_11_14_SELNG_CO")),
        tmzon_14_17_count=to_int(row.get("TMZON_14_17_SELNG_CO")),
        tmzon_17_21_count=to_int(row.get("TMZON_17_21_SELNG_CO")),
        tmzon_21_24_count=to_int(row.get("TMZON_21_24_SELNG_CO")),
        # 성별 매출건수
        male_sales_count=to_int(row.get("ML_SELNG_CO")),
        female_sales_count=to_int(row.get("FML_SELNG_CO")),
        # 연령대별 매출건수
        age10_sales_count=to_int(row.get("AGRDE_10_SELNG_CO")),
        age20_sales_count=to_int(row.get("AGRDE_20_SELNG_CO")),
        age30_sales_count=to_int(row.get("AGRDE_30_SELNG_CO")),
        age40_sales_count=to_int(row.get("AGRDE_40_SELNG_CO")),
        age50_sales_count=to_int(row.get("AGRDE_50_SELNG_CO")),
        age60_sales_count=to_int(row.get("AGRDE_60_ABOVE_SELNG_CO")),
    ).on_conflict_do_update(
        index_elements=["district_id", "category_code", "period_code"],
        set_=dict(
            monthly_sales=to_int(row.get("THSMON_SELNG_AMT")),
        )
    )
    await session.execute(stmt)


# ── 메인 파이프라인 함수 ──────────────────────────────────────

async def run_sales_pipeline(period_code: str = None):
    """
    sales_stats 전체 파이프라인 실행
    - period_code: 기준년분기 (예: '20261'). 없으면 최신 분기
    - 페이지네이션 처리
    """
    if period_code is None:
        period_code = await get_latest_period_code()

    print(f"추정매출 수집 시작 (기준분기: {period_code})")

    async with AsyncSessionLocal() as session:
        start = 1
        end = 1000
        total_count = None

        while True:
            data = await fetch_sales(start=start, end=end, period_code=period_code)
            result = data.get("VwsmAdstrdSelngW", {})
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

                await upsert_sales_stat(session, row, district_id)

            await session.commit()
            print(f"  {start}~{end} 완료 ({len(rows)}건)")

            if end >= total_count:
                break
            start += 1000
            end += 1000

    print("추정매출 수집 완료!")


# ── 직접 실행 시 ──────────────────────────────────────────────
if __name__ == "__main__":
    asyncio.run(run_sales_pipeline())