# app/pipelines/populations.py
# populations 데이터 수집 파이프라인
# 서울시 생활인구(내국인) API — SPOP_LOCAL_RESD_DONG
#
# 적재 흐름:
# Step 1: API 호출 (기준일자 기준)
# Step 2: ADSTRD_CODE_SE → district_id 변환
# Step 3: 문자열 → float() 변환
# Step 4: populations 테이블 UPSERT

import asyncio
import logging
import os
from datetime import datetime, timedelta

import httpx
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert

from app.db.session import AsyncSessionLocal
from app.models.districts import District
from app.models.populations import Population

# ── 상수 정의 ────────────────────────────────────────────────
SEOUL_API_KEY = os.getenv("SEOUL_API_KEY")
BASE_URL = f"http://openapi.seoul.go.kr:8088/{SEOUL_API_KEY}/json/SPOP_LOCAL_RESD_DONG"
MAX_PAGES = 20  # 최대 20페이지 (20,000건) — 현재 약 10페이지(10,176건) 기준 여유값
DATA_DELAY_DAYS = 5  # 서울시 생활인구 API 공개 지연일 (약 4~5일)
BATCH_SIZE = (
    500  # populations Batch UPSERT 단위 (컬럼 34개 × 500건 = 17,000개 파라미터)
)

# ── 로거 설정 ─────────────────────────────────────────────────
logger = logging.getLogger(__name__)


# ── API 호출 결과 최신 날짜 반환 함수 ───────────────────────────
async def get_missing_std_dates(client: httpx.AsyncClient) -> list[str]:
    """
    DB 마지막 적재일 이후부터 API 최신 제공 가능일까지 누락 날짜 목록 반환
    - DB가 비어있으면 가장 최신 날짜 1개만 반환
    - 서울시 생활인구 API 약 5일 지연 제공 (DATA_DELAY_DAYS)
    - API 최대 2개월 제공 범위 초과 시 경고 로그 출력
    """
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(func.max(Population.std_date)))
        last_date = result.scalar_one_or_none()

    # DB가 비어있으면 최신 날짜 1개 탐색
    if last_date is None:
        for days in range(1, 11):
            date = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")
            data = await fetch_populations(client, date, 1, 5)
            count = data.get("SPOP_LOCAL_RESD_DONG", {}).get("list_total_count", 0)
            if count > 0:
                logger.info(f"  최신 날짜: {date}")
                return [date]
        raise ValueError(
            "최근 10일 내 생활인구 데이터를 찾을 수 없습니다. API 상태를 확인하세요."
        )

    latest_available = datetime.now().date() - timedelta(days=DATA_DELAY_DAYS)
    earliest_available = latest_available - timedelta(days=59)  # API 최대 2개월 제공

    requested_start = last_date + timedelta(days=1)

    if requested_start < earliest_available:
        logger.warning(
            f"API 제공 범위 초과 과거 데이터는 수집 불가: "
            f"{requested_start} ~ {earliest_available - timedelta(days=1)}"
        )

    start = max(requested_start, earliest_available)
    end = latest_available

    dates = []
    current = start
    while current <= end:
        dates.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)

    return dates if dates else []


# ── API 호출 함수 ─────────────────────────────────────────────
async def fetch_populations(
    client: httpx.AsyncClient, std_date: str, start: int = 1, end: int = 1000
):
    """
    서울시 생활인구 API 호출
    - client: 파이프라인 전체에서 재사용하는 AsyncClient (연결 풀 활용)
    - std_date: 기준일자 (예: '20260610')
    - start/end: 페이지 범위
    """
    url = f"{BASE_URL}/{start}/{end}/{std_date}/"
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


# ── district 일괄 조회 함수 ───────────────────────────────────
async def get_district_map(session, district_codes: set) -> dict:
    """
    district_code 목록을 한 번에 IN 쿼리로 조회 → {code: district_id} 딕셔너리 반환
    - 행마다 SELECT 하지 않고 페이지당 1회만 조회 (DB 왕복 최소화)
    """
    result = await session.execute(
        select(District.district_code, District.id).where(
            District.district_code.in_(district_codes)
        )
    )
    return {code: district_id for code, district_id in result.all()}


# ── 메인 파이프라인 함수 ──────────────────────────────────────
async def run_populations_pipeline(std_date: str = None):
    """
    populations 전체 파이프라인 실행
    - std_date: 기준일자 직접 지정 시 해당 날짜만 수집
    - 미지정 시 DB 마지막 적재일 이후 누락 날짜 자동 감지
    - AsyncClient를 파이프라인 전체에서 1회만 생성해 HTTP 연결 재사용
    - 페이지당 district 일괄 조회 + Batch UPSERT로 DB 왕복 최소화
    """
    if not SEOUL_API_KEY:
        raise RuntimeError("SEOUL_API_KEY 환경변수가 설정되지 않았습니다.")

    async with httpx.AsyncClient(timeout=30) as client:
        if std_date is not None:
            dates = [std_date]
        else:
            dates = await get_missing_std_dates(client)

        if not dates:
            logger.info("생활인구 수집 시작: 적재할 날짜 없음 (최신 상태)")
            return

        for date in dates:
            logger.info(f"생활인구 수집 시작 (기준일: {date})")

            async with AsyncSessionLocal() as session:
                start = 1
                end = 1000
                total_count = None
                page = 0
                all_missing_codes = set()

                while True:
                    page += 1
                    if page > MAX_PAGES:
                        raise RuntimeError(
                            f"최대 페이지 수 초과 ({MAX_PAGES}). 전체 데이터 수집이 완료되지 않았습니다."
                        )

                    data = await fetch_populations(client, date, start=start, end=end)

                    if "SPOP_LOCAL_RESD_DONG" not in data:
                        logger.error(f"서울시 생활인구 API 비정상 응답: {data}")
                        raise RuntimeError("서울시 생활인구 API 호출에 실패했습니다.")

                    result = data.get("SPOP_LOCAL_RESD_DONG", {})
                    rows = result.get("row", [])

                    if total_count is None:
                        total_count = int(result.get("list_total_count", 0))
                        logger.info(f"  총 {total_count}건")

                    if not rows:
                        break

                    # district 일괄 조회
                    codes = {row["ADSTRD_CODE_SE"] for row in rows}
                    district_map = await get_district_map(session, codes)

                    # 미매핑 코드 누적
                    missing_codes = codes - district_map.keys()
                    all_missing_codes.update(missing_codes)

                    # Batch UPSERT
                    values_by_key = {}
                    for row in rows:
                        district_id = district_map.get(row["ADSTRD_CODE_SE"])
                        if district_id is None:
                            continue
                        key = (
                            district_id,
                            parse_date(row["STDR_DE_ID"]),
                            row["TMZON_PD_SE"],
                        )
                        values_by_key[key] = dict(
                            district_id=district_id,
                            std_date=parse_date(row["STDR_DE_ID"]),
                            time_zone=row["TMZON_PD_SE"],
                            total_pop=to_float(row.get("TOT_LVPOP_CO")),
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
                        )

                    values_list = list(values_by_key.values())

                    if values_list:
                        try:
                            for i in range(0, len(values_list), BATCH_SIZE):
                                batch = values_list[i : i + BATCH_SIZE]
                                insert_stmt = insert(Population).values(batch)
                                stmt = insert_stmt.on_conflict_do_update(
                                    index_elements=[
                                        "district_id",
                                        "std_date",
                                        "time_zone",
                                    ],
                                    set_={
                                        "total_pop": insert_stmt.excluded.total_pop,
                                        "male_0t9": insert_stmt.excluded.male_0t9,
                                        "male_10t14": insert_stmt.excluded.male_10t14,
                                        "male_15t19": insert_stmt.excluded.male_15t19,
                                        "male_20t24": insert_stmt.excluded.male_20t24,
                                        "male_25t29": insert_stmt.excluded.male_25t29,
                                        "male_30t34": insert_stmt.excluded.male_30t34,
                                        "male_35t39": insert_stmt.excluded.male_35t39,
                                        "male_40t44": insert_stmt.excluded.male_40t44,
                                        "male_45t49": insert_stmt.excluded.male_45t49,
                                        "male_50t54": insert_stmt.excluded.male_50t54,
                                        "male_55t59": insert_stmt.excluded.male_55t59,
                                        "male_60t64": insert_stmt.excluded.male_60t64,
                                        "male_65t69": insert_stmt.excluded.male_65t69,
                                        "male_70o": insert_stmt.excluded.male_70o,
                                        "female_0t9": insert_stmt.excluded.female_0t9,
                                        "female_10t14": insert_stmt.excluded.female_10t14,
                                        "female_15t19": insert_stmt.excluded.female_15t19,
                                        "female_20t24": insert_stmt.excluded.female_20t24,
                                        "female_25t29": insert_stmt.excluded.female_25t29,
                                        "female_30t34": insert_stmt.excluded.female_30t34,
                                        "female_35t39": insert_stmt.excluded.female_35t39,
                                        "female_40t44": insert_stmt.excluded.female_40t44,
                                        "female_45t49": insert_stmt.excluded.female_45t49,
                                        "female_50t54": insert_stmt.excluded.female_50t54,
                                        "female_55t59": insert_stmt.excluded.female_55t59,
                                        "female_60t64": insert_stmt.excluded.female_60t64,
                                        "female_65t69": insert_stmt.excluded.female_65t69,
                                        "female_70o": insert_stmt.excluded.female_70o,
                                    },
                                )
                                await session.execute(stmt)
                            await session.commit()
                        except Exception:
                            await session.rollback()
                            logger.exception(
                                f"생활인구 적재 실패: {start}~{end} ({date})"
                            )
                            raise

                    logger.info(f"  {start}~{end} 완료 ({len(rows)}건)")

                    if end >= total_count:
                        break
                    start += 1000
                    end += 1000

            # 전체 순회 완료 후 미매핑 코드 한 번만 요약 출력
            if all_missing_codes:
                logger.info(
                    f"district 미매핑 코드 {len(all_missing_codes)}개 건너뜀: {sorted(all_missing_codes)}"
                )

            logger.info(f"생활인구 수집 완료! ({date})")


# ── 직접 실행 시 ──────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)

    asyncio.run(run_populations_pipeline())
