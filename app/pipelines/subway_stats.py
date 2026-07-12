# app/pipelines/subway_stats.py
# subway_stats 데이터 수집 파이프라인
# 서울시 지하철 호선별 역별 승하차 인원 API — CardSubwayStatsNew
#
# 적재 흐름:
# Step 1: API 호출 (이용일자 기준)
# Step 2: 문자열 → int() 변환 (승하차 인원)
# Step 3: subway_stats 테이블 UPSERT (district_id/lat/lng는 NULL, MVP 후순위)

import httpx
import asyncio
from datetime import datetime, timedelta
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select, func
from app.models.districts import District
from app.db.session import AsyncSessionLocal
from app.models.subway_stats import SubwayStat
import os

# ── 상수 정의 ────────────────────────────────────────────────

SEOUL_API_KEY = os.getenv("SEOUL_API_KEY")
BASE_URL = f"http://openapi.seoul.go.kr:8088/{SEOUL_API_KEY}/json/CardSubwayStatsNew"


# ── API 호출 결과 최신 날짜 반환 함수 ───────────────────────────

async def get_missing_use_dates() -> list[str]:
    """
    DB에 마지막으로 적재된 날짜 이후부터 오늘까지 누락된 날짜 목록 반환
    - DB가 비어있으면 가장 최신 날짜 1개만 반환
    - API 약 5일 지연 제공이므로 오늘 기준 5일 전까지만 확인
    """
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(func.max(SubwayStat.use_date)))
        last_date = result.scalar_one_or_none()

    # DB가 비어있으면 최신 날짜 1개 탐색
    if last_date is None:
        for days in range(1, 11):
            date = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")
            data = await fetch_subway_stats(date, 1, 5)
            count = data.get("CardSubwayStatsNew", {}).get("list_total_count", 0)
            if count > 0:
                return [date]
        raise ValueError("최근 10일 내 지하철 승하차 인원 데이터를 찾을 수 없습니다.")
    
    # 마지막 적재일 다음날부터 오늘 기준 1일 전까지 날짜 목록 생성
    start = last_date + timedelta(days=1)
    end = datetime.now().date() - timedelta(days=1)

    dates = []
    current = start
    while current <= end:
        dates.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)

    return dates if dates else []


# ── API 호출 함수 ─────────────────────────────────────────────

async def fetch_subway_stats(use_date: str, start: int = 1, end: int = 1000):
    """
    서울시 지하철 승하차 인원 API 호출
    - use_date: 이용일자 (YYYYMMDD)
    - start, end: 페이지네이션 범위
    """
    url = f"{BASE_URL}/{start}/{end}/{use_date}/"

    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.get(url)
        response.raise_for_status()
        return response.json()


# ── 유틸리티 함수 ─────────────────────────────────────────────

def to_int(value):
    """
    문자열을 정수로 변환. 변환 불가 시 None 반환
    - API에서 승하차 인원이 문자열로 내려옴
    """
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def parse_date(date_str: str):
    """
    YYYYMMDD 형식의 문자열을 datetime.date 객체로 변환
    - 변환 불가 시 None 반환
    """
    try:
        return datetime.strptime(date_str, "%Y%m%d").date()
    except ValueError:
        return None


# ── subway_stats 적재 함수 ────────────────────────────────────

async def upsert_subway_stat(session, row: dict, use_date):
    """
    subway_stats 테이블 UPSERT
    - UNIQUE: (line_name, station_name, use_date)
    - district_id/lat/lng는 NULL 허용 (MVP 후순위)
    """
    stmt = insert(SubwayStat).values(
        line_name=row.get("SBWY_ROUT_LN_NM"),
        station_name=row.get("SBWY_STNS_NM"),
        use_date=use_date,
        boarding=to_int(row.get("GTON_TNOPE")),
        alighting=to_int(row.get("GTOFF_TNOPE")),
    ).on_conflict_do_update(
        index_elements=["line_name", "station_name", "use_date"],
        set_=dict(
            boarding=to_int(row.get("GTON_TNOPE")),
            alighting=to_int(row.get("GTOFF_TNOPE")),
        )
    )
    await session.execute(stmt)


# ── 메인 파이프라인 함수 ──────────────────────────────────────

async def run_subway_stats_pipeline(use_date: str = None):
    """
    subway_stats 데이터 수집 파이프라인 실행
    - use_date: 이용일자 (YYYYMMDD)
    """
    print("지하철 승하차 인원 수집 시작")

    if use_date is not None:
        dates = [use_date]
    else:
        dates = await get_missing_use_dates()
    
    if not dates:
        print("누락된 날짜가 없습니다. 수집을 종료합니다.")
        return
    
    for date in dates:
        print(f"수집 대상 날짜: {date}")

        parsed_date = parse_date(date)
    
        async with AsyncSessionLocal() as session:
            start = 1
            end = 1000
            total_count = None

            while True:
                data = await fetch_subway_stats(date, start=start, end=end)
                result = data.get("CardSubwayStatsNew", {})
                rows = result.get("row", [])

                if total_count is None:
                    total_count = result.get("list_total_count", 0)
                    print(f"  총 {total_count}건")

                if not rows:
                    break

                for row in rows:
                    await upsert_subway_stat(session, row, parsed_date)

                await session.commit()
                print(f"  {start}~{end} 완료 ({len(rows)}건)")

                if end >= total_count:
                    break
                start += 1000
                end += 1000

        print("지하철 승하차 인원 수집 완료!")


# ── 직접 실행 시 ──────────────────────────────────────────────

if __name__ == "__main__":
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    asyncio.run(run_subway_stats_pipeline(yesterday))

