# app/pipelines/populations.py
# populations 데이터 수집 파이프라인
# 서울시 생활인구(내국인) API — SPOP_LOCAL_RESD_DONG
#
# 적재 흐름:
# Step 1: API 호출 (기준일자 기준)
# Step 2: ADSTRD_CODE_SE → district_id 변환
# Step 3: 문자열 → float() 변환
# Step 4: populations 테이블 UPSERT

import httpx
import asyncio
from datetime import datetime, timedelta
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select, func
from app.db.session import AsyncSessionLocal
from app.models.districts import District
from app.models.populations import Population
import os

# ── 상수 정의 ────────────────────────────────────────────────

SEOUL_API_KEY = os.getenv("SEOUL_API_KEY")
BASE_URL = f"http://openapi.seoul.go.kr:8088/{SEOUL_API_KEY}/json/SPOP_LOCAL_RESD_DONG"


# ── API 호출 결과 최신 날짜 반환 함수 ───────────────────────────

async def get_missing_std_dates() -> list[str]:
    """
    DB에 마지막으로 적재된 날짜 이후부터 오늘까지 누락된 날짜 목록 반환
    - DB가 비어있으면 가장 최신 날짜 1개만 반환
    - API 약 5일 지연 제공이므로 오늘 기준 5일 전까지만 확인
    """
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(func.max(Population.std_date)))
        last_date = result.scalar_one_or_none()

    # DB가 비어있으면 최신 날짜 1개 탐색
    if last_date is None:
        for days in range(1, 11):
            date = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")
            data = await fetch_populations(date, 1, 5)
            count = data.get("SPOP_LOCAL_RESD_DONG", {}).get("list_total_count", 0)
            if count > 0:
                return [date]
        raise ValueError("최근 10일 내 생활인구 데이터를 찾을 수 없습니다.")

    # 마지막 적재일 다음날부터 오늘 기준 1일 전까지 날짜 목록 생성
    from datetime import date as date_type
    start = last_date + timedelta(days=1)
    end = datetime.now().date() - timedelta(days=1)

    dates = []
    current = start
    while current <= end:
        dates.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)

    return dates if dates else []


# ── API 호출 함수 ─────────────────────────────────────────────

async def fetch_populations(std_date: str, start: int = 1, end: int = 1000):
    """
    서울시 생활인구 API 호출
    - std_date: 기준일자 (예: '20260610')
    - start/end: 페이지 범위
    """
    url = f"{BASE_URL}/{start}/{end}/{std_date}/"

    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.get(url)
        response.raise_for_status()
        return response.json()


# ── 유틸리티 함수 ─────────────────────────────────────────────

def to_float(value):
    """문자열 또는 None → float 변환. API가 문자열로 내려옴"""
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def parse_date(date_str: str):
    """20260610 → datetime.date(2026, 6, 10)"""
    return datetime.strptime(date_str, "%Y%m%d").date()


# ── district_id 조회 함수 ─────────────────────────────────────

async def get_district_id(session, district_code: str):
    """district_code로 district_id 조회"""
    result = await session.execute(
        select(District.id).where(District.district_code == district_code)
    )
    return result.scalar_one_or_none()


# ── populations 적재 함수 ─────────────────────────────────────

async def upsert_population(session, row: dict, district_id: int):
    """
    populations 테이블 UPSERT
    - UNIQUE: (district_id, std_date, time_zone)
    - 모든 수치값 문자열 → float() 변환
    """
    stmt = insert(Population).values(
        district_id=district_id,
        std_date=parse_date(row["STDR_DE_ID"]),
        time_zone=row["TMZON_PD_SE"],
        total_pop=to_float(row.get("TOT_LVPOP_CO")),
        # 남성 연령대별
        male_0t9=to_float(row.get("MALE_F0T9_LVPOP_CO")),
        male_10t14=to_float(row.get("MALE_F10T14_LVPOP_CO")),
        male_15t19=to_float(row.get("MALE_F15T19_LVPOP_CO")),
        male_20t24=to_float(row.get("MALE_F20T24_LVPOP_CO")),
        male_25t29=to_float(row.get("MALE_F25T29_LVPOP_CO")),
        male_30t34=to_float(row.get("MALE_F30T34_LVPOP_CO")),
        male_35t39=to_float(row.get("MALE_F35T39_LVPOP_CO")),
        male_40t44=to_float(row.get("MALE_F40T44_LVPOP_CO")),
        male_45t49=to_float(row.get("MALE_F45T49_LVPOP_CO")),
        male_50t54=to_float(row.get("MALE_F50T54_LVPOP_CO")),
        male_55t59=to_float(row.get("MALE_F55T59_LVPOP_CO")),
        male_60t64=to_float(row.get("MALE_F60T64_LVPOP_CO")),
        male_65t69=to_float(row.get("MALE_F65T69_LVPOP_CO")),
        male_70o=to_float(row.get("MALE_F70T74_LVPOP_CO")),
        # 여성 연령대별
        female_0t9=to_float(row.get("FEMALE_F0T9_LVPOP_CO")),
        female_10t14=to_float(row.get("FEMALE_F10T14_LVPOP_CO")),
        female_15t19=to_float(row.get("FEMALE_F15T19_LVPOP_CO")),
        female_20t24=to_float(row.get("FEMALE_F20T24_LVPOP_CO")),
        female_25t29=to_float(row.get("FEMALE_F25T29_LVPOP_CO")),
        female_30t34=to_float(row.get("FEMALE_F30T34_LVPOP_CO")),
        female_35t39=to_float(row.get("FEMALE_F35T39_LVPOP_CO")),
        female_40t44=to_float(row.get("FEMALE_F40T44_LVPOP_CO")),
        female_45t49=to_float(row.get("FEMALE_F45T49_LVPOP_CO")),
        female_50t54=to_float(row.get("FEMALE_F50T54_LVPOP_CO")),
        female_55t59=to_float(row.get("FEMALE_F55T59_LVPOP_CO")),
        female_60t64=to_float(row.get("FEMALE_F60T64_LVPOP_CO")),
        female_65t69=to_float(row.get("FEMALE_F65T69_LVPOP_CO")),
        female_70o=to_float(row.get("FEMALE_F70T74_LVPOP_CO")),
    ).on_conflict_do_update(
        index_elements=["district_id", "std_date", "time_zone"],
        set_=dict(
            total_pop=to_float(row.get("TOT_LVPOP_CO")),
        )
    )
    await session.execute(stmt)


# ── 메인 파이프라인 함수 ──────────────────────────────────────

async def run_populations_pipeline(std_date: str = None):
    if std_date is not None:
        # 날짜 직접 지정한 경우 그대로 실행
        dates = [std_date]
    else:
        dates = await get_missing_std_dates()

    if not dates:
        print("생활인구 수집 시작: 적재할 날짜 없음 (최신 상태)")
        return

    for date in dates:
        print(f"생활인구 수집 시작 (기준일: {date})")

        async with AsyncSessionLocal() as session:
            start = 1
            end = 1000
            total_count = None

            while True:
                data = await fetch_populations(date, start=start, end=end)
                result = data.get("SPOP_LOCAL_RESD_DONG", {})
                rows = result.get("row", [])

                if total_count is None:
                    total_count = result.get("list_total_count", 0)
                    print(f"  총 {total_count}건")

                if not rows:
                    break

                for row in rows:
                    district_id = await get_district_id(session, row["ADSTRD_CODE_SE"])
                    if district_id is None:
                        continue
                    await upsert_population(session, row, district_id)

                await session.commit()
                print(f"  {start}~{end} 완료 ({len(rows)}건)")

                if end >= total_count:
                    break
                start += 1000
                end += 1000

        print(f"생활인구 수집 완료! ({date})")


# ── 직접 실행 시 ──────────────────────────────────────────────
if __name__ == "__main__":
    asyncio.run(run_populations_pipeline())