# app/pipelines/sales_stats.py
# sales_stats 데이터 수집 파이프라인
# 서울시 상권분석서비스 추정매출(행정동) API
#
# 적재 흐름:
# Step 1: API 호출 (분기 코드 기준)
# Step 2: ADSTRD_CD → district_id 변환
# Step 3: float → int() 캐스팅 (금액/건수 컬럼)
# Step 4: sales_stats 테이블 UPSERT

import asyncio
import logging
import os
from datetime import datetime

import httpx
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from app.db.session import AsyncSessionLocal
from app.models.districts import District
from app.models.sales_stats import SalesStat

# ── 상수 정의 ────────────────────────────────────────────────
SEOUL_API_KEY = os.getenv("SEOUL_API_KEY")
BASE_URL = f"http://openapi.seoul.go.kr:8088/{SEOUL_API_KEY}/json/VwsmAdstrdSelngW"
MAX_PAGES = 30  # 최대 30페이지 (30,000건) — 현재 약 17페이지(16,659건) 기준 여유값
BATCH_SIZE = (
    200  # sales_stats Batch UPSERT 단위 (컬럼 약 50개 × 200건 = 10,000개 파라미터)
)


# ── 로거 설정 ─────────────────────────────────────────────────
logger = logging.getLogger(__name__)


# ── API 호출 결과 가장 최신 분기 반환 함수 ──────────────────────
async def get_latest_period_code(client: httpx.AsyncClient) -> str:
    """
    실제 API 호출해서 데이터가 있는 가장 최신 분기 코드 반환
    - client: 파이프라인 전체에서 재사용하는 AsyncClient
    - 현재 분기부터 역순으로 확인
    """
    now = datetime.now()
    year = now.year
    month = now.month

    if month <= 3:
        quarters = [(year - 1, 4), (year - 1, 3)]
    elif month <= 6:
        quarters = [(year, 1), (year - 1, 4)]
    elif month <= 9:
        quarters = [(year, 2), (year, 1)]
    else:
        quarters = [(year, 3), (year, 2)]

    for y, q in quarters:
        period = f"{y}{q}"
        data = await fetch_sales(
            client,
            start=1,
            end=5,
            period_code=period,
        )
        count = data.get("VwsmAdstrdSelngW", {}).get("list_total_count", 0)
        if count > 0:
            logger.info(f"  최신 분기: {period}")
            return period

    raise RuntimeError(
        "최근 분기에서 추정매출 데이터를 찾을 수 없습니다. API 상태를 확인하세요."
    )


# ── API 호출 함수 ─────────────────────────────────────────────
async def fetch_sales(
    client: httpx.AsyncClient, start: int = 1, end: int = 1000, period_code: str = None
):
    """
    서울시 추정매출 API 호출
    - client: 파이프라인 전체에서 재사용하는 AsyncClient (연결 풀 활용)
    - start/end: 페이지 범위
    - period_code: 기준년분기 (예: '20261'). 없으면 전체 조회
    """
    if period_code:
        url = f"{BASE_URL}/{start}/{end}/{period_code}/"
    else:
        url = f"{BASE_URL}/{start}/{end}/"

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


# ── 매출 데이터 변환 함수 ─────────────────────────────────────
def build_sales_value(row: dict, district_id: int) -> dict:
    """
    API 응답 매출 1건 → DB INSERT용 딕셔너리 변환
    - 금액/건수 컬럼 float → int() 캐스팅 (API가 float으로 내려옴)
    - API 응답 순서 기준: 금액(AMT) 먼저, 건수(CO) 나중
    """
    return dict(
        district_id=district_id,
        period_code=row["STDR_YYQU_CD"],
        category_code=row["SVC_INDUTY_CD"],
        category=row["SVC_INDUTY_CD_NM"],
        monthly_sales=to_int(row.get("THSMON_SELNG_AMT")),
        monthly_sales_count=to_int(row.get("THSMON_SELNG_CO")),
        weekday_sales=to_int(row.get("MDWK_SELNG_AMT")),
        weekend_sales=to_int(row.get("WKEND_SELNG_AMT")),
        mon_sales=to_int(row.get("MON_SELNG_AMT")),
        tue_sales=to_int(row.get("TUES_SELNG_AMT")),
        wed_sales=to_int(row.get("WED_SELNG_AMT")),
        thu_sales=to_int(row.get("THUR_SELNG_AMT")),
        fri_sales=to_int(row.get("FRI_SELNG_AMT")),
        sat_sales=to_int(row.get("SAT_SELNG_AMT")),
        sun_sales=to_int(row.get("SUN_SELNG_AMT")),
        tmzon_00_06_sales=to_int(row.get("TMZON_00_06_SELNG_AMT")),
        tmzon_06_11_sales=to_int(row.get("TMZON_06_11_SELNG_AMT")),
        tmzon_11_14_sales=to_int(row.get("TMZON_11_14_SELNG_AMT")),
        tmzon_14_17_sales=to_int(row.get("TMZON_14_17_SELNG_AMT")),
        tmzon_17_21_sales=to_int(row.get("TMZON_17_21_SELNG_AMT")),
        tmzon_21_24_sales=to_int(row.get("TMZON_21_24_SELNG_AMT")),
        male_sales=to_int(row.get("ML_SELNG_AMT")),
        female_sales=to_int(row.get("FML_SELNG_AMT")),
        age10_sales=to_int(row.get("AGRDE_10_SELNG_AMT")),
        age20_sales=to_int(row.get("AGRDE_20_SELNG_AMT")),
        age30_sales=to_int(row.get("AGRDE_30_SELNG_AMT")),
        age40_sales=to_int(row.get("AGRDE_40_SELNG_AMT")),
        age50_sales=to_int(row.get("AGRDE_50_SELNG_AMT")),
        age60_sales=to_int(row.get("AGRDE_60_ABOVE_SELNG_AMT")),
        weekday_sales_count=to_int(row.get("MDWK_SELNG_CO")),
        weekend_sales_count=to_int(row.get("WKEND_SELNG_CO")),
        mon_sales_count=to_int(row.get("MON_SELNG_CO")),
        tue_sales_count=to_int(row.get("TUES_SELNG_CO")),
        wed_sales_count=to_int(row.get("WED_SELNG_CO")),
        thu_sales_count=to_int(row.get("THUR_SELNG_CO")),
        fri_sales_count=to_int(row.get("FRI_SELNG_CO")),
        sat_sales_count=to_int(row.get("SAT_SELNG_CO")),
        sun_sales_count=to_int(row.get("SUN_SELNG_CO")),
        tmzon_00_06_count=to_int(row.get("TMZON_00_06_SELNG_CO")),
        tmzon_06_11_count=to_int(row.get("TMZON_06_11_SELNG_CO")),
        tmzon_11_14_count=to_int(row.get("TMZON_11_14_SELNG_CO")),
        tmzon_14_17_count=to_int(row.get("TMZON_14_17_SELNG_CO")),
        tmzon_17_21_count=to_int(row.get("TMZON_17_21_SELNG_CO")),
        tmzon_21_24_count=to_int(row.get("TMZON_21_24_SELNG_CO")),
        male_sales_count=to_int(row.get("ML_SELNG_CO")),
        female_sales_count=to_int(row.get("FML_SELNG_CO")),
        age10_sales_count=to_int(row.get("AGRDE_10_SELNG_CO")),
        age20_sales_count=to_int(row.get("AGRDE_20_SELNG_CO")),
        age30_sales_count=to_int(row.get("AGRDE_30_SELNG_CO")),
        age40_sales_count=to_int(row.get("AGRDE_40_SELNG_CO")),
        age50_sales_count=to_int(row.get("AGRDE_50_SELNG_CO")),
        age60_sales_count=to_int(row.get("AGRDE_60_ABOVE_SELNG_CO")),
    )


# ── 메인 파이프라인 함수 ──────────────────────────────────────
async def run_sales_pipeline(period_code: str = None):
    """
    sales_stats 전체 파이프라인 실행
    - period_code: 기준년분기 직접 지정 시 해당 분기만 수집
    - 미지정 시 API 호출로 최신 분기 자동 감지
    - AsyncClient를 파이프라인 전체에서 1회만 생성해 HTTP 연결 재사용
    - 페이지당 district 일괄 조회 + Batch UPSERT로 DB 왕복 최소화
    """
    if not SEOUL_API_KEY:
        raise RuntimeError("SEOUL_API_KEY 환경변수가 설정되지 않았습니다.")

    async with httpx.AsyncClient(timeout=30) as client:
        if period_code is None:
            period_code = await get_latest_period_code(client)

        logger.info(f"추정매출 수집 시작 (기준분기: {period_code})")

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

                data = await fetch_sales(
                    client, start=start, end=end, period_code=period_code
                )

                if "VwsmAdstrdSelngW" not in data:
                    logger.error(f"서울시 추정매출 API 비정상 응답: {data}")
                    raise RuntimeError("서울시 추정매출 API 호출에 실패했습니다.")

                result = data.get("VwsmAdstrdSelngW", {})
                rows = result.get("row", [])

                if total_count is None:
                    total_count = int(result.get("list_total_count", 0))
                    logger.info(f"  총 {total_count}건")

                if not rows:
                    break

                # district 일괄 조회
                codes = {row["ADSTRD_CD"] for row in rows}
                district_map = await get_district_map(session, codes)

                # 미매핑 코드 누적
                missing_codes = codes - district_map.keys()
                all_missing_codes.update(missing_codes)

                # 복합 키 중복 제거 후 Batch UPSERT
                values_by_key = {}
                missing_row_count = 0
                for row in rows:
                    district_id = district_map.get(row["ADSTRD_CD"])
                    if district_id is None:
                        missing_row_count += 1
                        continue
                    key = (district_id, row["SVC_INDUTY_CD"], row["STDR_YYQU_CD"])
                    values_by_key[key] = build_sales_value(row, district_id)

                values_list = list(values_by_key.values())
                skipped = len(rows) - len(values_list)

                if values_list:
                    try:
                        for i in range(0, len(values_list), BATCH_SIZE):
                            batch = values_list[i : i + BATCH_SIZE]
                            insert_stmt = insert(SalesStat).values(batch)
                            stmt = insert_stmt.on_conflict_do_update(
                                index_elements=[
                                    "district_id",
                                    "category_code",
                                    "period_code",
                                ],
                                set_={
                                    "category": insert_stmt.excluded.category,
                                    "monthly_sales": insert_stmt.excluded.monthly_sales,
                                    "monthly_sales_count": insert_stmt.excluded.monthly_sales_count,
                                    "weekday_sales": insert_stmt.excluded.weekday_sales,
                                    "weekend_sales": insert_stmt.excluded.weekend_sales,
                                    "mon_sales": insert_stmt.excluded.mon_sales,
                                    "tue_sales": insert_stmt.excluded.tue_sales,
                                    "wed_sales": insert_stmt.excluded.wed_sales,
                                    "thu_sales": insert_stmt.excluded.thu_sales,
                                    "fri_sales": insert_stmt.excluded.fri_sales,
                                    "sat_sales": insert_stmt.excluded.sat_sales,
                                    "sun_sales": insert_stmt.excluded.sun_sales,
                                    "tmzon_00_06_sales": insert_stmt.excluded.tmzon_00_06_sales,
                                    "tmzon_06_11_sales": insert_stmt.excluded.tmzon_06_11_sales,
                                    "tmzon_11_14_sales": insert_stmt.excluded.tmzon_11_14_sales,
                                    "tmzon_14_17_sales": insert_stmt.excluded.tmzon_14_17_sales,
                                    "tmzon_17_21_sales": insert_stmt.excluded.tmzon_17_21_sales,
                                    "tmzon_21_24_sales": insert_stmt.excluded.tmzon_21_24_sales,
                                    "male_sales": insert_stmt.excluded.male_sales,
                                    "female_sales": insert_stmt.excluded.female_sales,
                                    "age10_sales": insert_stmt.excluded.age10_sales,
                                    "age20_sales": insert_stmt.excluded.age20_sales,
                                    "age30_sales": insert_stmt.excluded.age30_sales,
                                    "age40_sales": insert_stmt.excluded.age40_sales,
                                    "age50_sales": insert_stmt.excluded.age50_sales,
                                    "age60_sales": insert_stmt.excluded.age60_sales,
                                    "weekday_sales_count": insert_stmt.excluded.weekday_sales_count,
                                    "weekend_sales_count": insert_stmt.excluded.weekend_sales_count,
                                    "mon_sales_count": insert_stmt.excluded.mon_sales_count,
                                    "tue_sales_count": insert_stmt.excluded.tue_sales_count,
                                    "wed_sales_count": insert_stmt.excluded.wed_sales_count,
                                    "thu_sales_count": insert_stmt.excluded.thu_sales_count,
                                    "fri_sales_count": insert_stmt.excluded.fri_sales_count,
                                    "sat_sales_count": insert_stmt.excluded.sat_sales_count,
                                    "sun_sales_count": insert_stmt.excluded.sun_sales_count,
                                    "tmzon_00_06_count": insert_stmt.excluded.tmzon_00_06_count,
                                    "tmzon_06_11_count": insert_stmt.excluded.tmzon_06_11_count,
                                    "tmzon_11_14_count": insert_stmt.excluded.tmzon_11_14_count,
                                    "tmzon_14_17_count": insert_stmt.excluded.tmzon_14_17_count,
                                    "tmzon_17_21_count": insert_stmt.excluded.tmzon_17_21_count,
                                    "tmzon_21_24_count": insert_stmt.excluded.tmzon_21_24_count,
                                    "male_sales_count": insert_stmt.excluded.male_sales_count,
                                    "female_sales_count": insert_stmt.excluded.female_sales_count,
                                    "age10_sales_count": insert_stmt.excluded.age10_sales_count,
                                    "age20_sales_count": insert_stmt.excluded.age20_sales_count,
                                    "age30_sales_count": insert_stmt.excluded.age30_sales_count,
                                    "age40_sales_count": insert_stmt.excluded.age40_sales_count,
                                    "age50_sales_count": insert_stmt.excluded.age50_sales_count,
                                    "age60_sales_count": insert_stmt.excluded.age60_sales_count,
                                },
                            )
                            await session.execute(stmt)
                        await session.commit()
                    except Exception:
                        await session.rollback()
                        logger.exception(f"추정매출 적재 실패: {start}~{end}")
                        raise

                duplicate_count = len(rows) - missing_row_count - len(values_list)
                logger.info(
                    f"  {start}~{end} 완료 (조회 {len(rows)}건 / 적재 {len(values_list)}건 / 미매핑 {missing_row_count}건 / 중복 {duplicate_count}건)"
                )

                if end >= total_count:
                    break
                start += 1000
                end += 1000

        if all_missing_codes:
            logger.info(
                f"district 미매핑 코드 {len(all_missing_codes)}개 건너뜀: {sorted(all_missing_codes)}"
            )

    logger.info("추정매출 수집 완료!")


# ── 직접 실행 시 ──────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)

    asyncio.run(run_sales_pipeline())
