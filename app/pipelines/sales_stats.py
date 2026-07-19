# app/pipelines/sales_stats.py
# sales_stats 데이터 수집 파이프라인
# 서울시 상권분석서비스 추정매출(행정동) API
#
# 적재 흐름:
# Step 1: API 호출 (분기 코드 기준)
# Step 2: ADSTRD_CD → district_id 변환
# Step 3: float → int() 캐스팅 (금액/건수 컬럼)
# Step 4: sales_stats 테이블 UPSERT

import logging
import os
import time
from datetime import datetime

import httpx
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert

from app.db.session import SyncSessionLocal
from app.models.districts import District
from app.models.sales_stats import SalesStat

# ── 상수 정의 ────────────────────────────────────────────────
SEOUL_API_KEY = os.getenv("SEOUL_API_KEY")
BASE_URL = f"http://openapi.seoul.go.kr:8088/{SEOUL_API_KEY}/json/VwsmAdstrdSelngW"
PAGE_SIZE = 1000  # API 페이지당 조회 건수
MAX_PAGES = 30  # 최대 30페이지 (30,000건) — 현재 약 17페이지(16,659건) 기준 여유값
BATCH_SIZE = (
    200  # sales_stats Batch UPSERT 단위 (컬럼 약 50개 × 200건 = 10,000개 파라미터)
)


# ── 로거 설정 ─────────────────────────────────────────────────
logger = logging.getLogger(__name__)


# ── API 호출 결과 가장 최신 분기 반환 함수 ──────────────────────
def get_latest_period_code(client: httpx.Client) -> str:
    """
    실제 API 호출해서 데이터가 있는 가장 최신 분기 코드 반환
    - client: 파이프라인 전체에서 재사용하는 httpx.Client
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
        data = fetch_sales(
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
def fetch_sales(
    client: httpx.Client,
    start: int = 1,
    end: int = 1000,
    period_code: str = None,
    max_retries: int = 3,
):
    """
    서울시 추정매출 API 호출
    - client: 파이프라인 전체에서 재사용하는 httpx.Client (연결 풀 활용)
    - start/end: 페이지 범위
    - period_code: 기준년분기 (예: '20261'). 없으면 전체 조회
    - 429 및 일시적인 5xx 오류 재시도 (최대 3회, 3초 간격)
    """
    if period_code:
        url = f"{BASE_URL}/{start}/{end}/{period_code}/"
    else:
        url = f"{BASE_URL}/{start}/{end}/"

    retryable_status_codes = {429, 500, 502, 503, 504}

    for attempt in range(1, max_retries + 1):
        try:
            response = client.get(url)

            if response.status_code in retryable_status_codes:
                if attempt == max_retries:
                    response.raise_for_status()
                logger.warning(
                    f"서울시 추정매출 API 일시 오류 "
                    f"({start}~{end}, 상태 {response.status_code}, "
                    f"재시도 {attempt}/{max_retries})"
                )
                time.sleep(3)
                continue

            response.raise_for_status()
            return response.json()

        except (httpx.TimeoutException, httpx.RequestError) as exc:
            if attempt == max_retries:
                raise RuntimeError(
                    f"서울시 추정매출 API 요청 실패: {start}~{end}"
                ) from exc
            logger.warning(
                f"서울시 추정매출 API 연결 오류 "
                f"({start}~{end}, 재시도 {attempt}/{max_retries}): {exc}"
            )
            time.sleep(3)

    raise RuntimeError(f"서울시 추정매출 API 요청 실패: {start}~{end}")


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
def get_district_map(session, district_codes: set) -> dict:
    """
    district_code 목록을 한 번에 IN 쿼리로 조회 → {code: district_id} 딕셔너리 반환
    - 행마다 SELECT 하지 않고 페이지당 1회만 조회 (DB 왕복 최소화)
    """
    result = session.execute(
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
def run_sales_pipeline(period_code: str = None):
    """
    sales_stats 전체 파이프라인 실행
    - period_code: 기준년분기 직접 지정 시 해당 분기만 수집
    - 미지정 시 API 호출로 최신 분기 자동 감지
    - httpx.Client를 파이프라인 전체에서 1회만 생성해 HTTP 연결 재사용
    - 페이지당 district 일괄 조회 + Batch UPSERT로 DB 왕복 최소화
    """
    if not SEOUL_API_KEY:
        raise RuntimeError("SEOUL_API_KEY 환경변수가 설정되지 않았습니다.")

    with httpx.Client(timeout=30) as client:
        if period_code is None:
            period_code = get_latest_period_code(client)

        logger.info(f"추정매출 수집 시작 (기준분기: {period_code})")

        with SyncSessionLocal() as session:
            try:
                start = 1
                end = PAGE_SIZE
                total_count = None
                page = 0
                all_missing_codes = set()

                while True:
                    page += 1
                    if page > MAX_PAGES:
                        raise RuntimeError(
                            f"최대 페이지 수 초과 ({MAX_PAGES}). 전체 데이터 수집이 완료되지 않았습니다."
                        )

                    data = fetch_sales(
                        client, start=start, end=end, period_code=period_code
                    )

                    if not isinstance(data, dict):
                        raise RuntimeError(
                            f"서울시 추정매출 API 응답이 dict가 아닙니다: {type(data).__name__}"
                        )

                    result = data.get("VwsmAdstrdSelngW")
                    if not isinstance(result, dict):
                        api_result = data.get("RESULT")
                        raise RuntimeError(
                            f"서울시 추정매출 API 비정상 응답: {api_result or data}"
                        )

                    rows = result.get("row", [])

                    if not isinstance(rows, list):
                        raise RuntimeError(
                            f"서울시 추정매출 API row 형식이 리스트가 아닙니다: {type(rows).__name__}"
                        )

                    if not all(isinstance(row, dict) for row in rows):
                        raise RuntimeError(
                            "서울시 추정매출 API row에 dict가 아닌 항목이 포함되어 있습니다."
                        )

                    if total_count is None:
                        raw_total = result.get("list_total_count")
                        try:
                            total_count = int(str(raw_total).replace(",", "").strip())
                        except (TypeError, ValueError) as exc:
                            raise RuntimeError(
                                f"서울시 추정매출 API list_total_count 값이 올바르지 않습니다: {raw_total!r}"
                            ) from exc

                        if total_count > PAGE_SIZE * MAX_PAGES:
                            raise RuntimeError(
                                f"전체 데이터가 최대 수집 범위를 초과했습니다. "
                                f"전체 {total_count}건 / 최대 {PAGE_SIZE * MAX_PAGES}건"
                            )

                        logger.info(f"  총 {total_count}건")

                    if not rows:
                        if total_count is not None and start <= total_count:
                            raise RuntimeError(
                                f"전체 {total_count}건 중 {start}~{end} 구간이 비어 있습니다. "
                                "API 응답 누락 가능성이 있습니다."
                            )
                        break

                    # district 일괄 조회
                    codes = {str(row["ADSTRD_CD"]).strip() for row in rows}
                    district_map = get_district_map(session, codes)

                    # 미매핑 코드 누적
                    missing_codes = codes - district_map.keys()
                    all_missing_codes.update(missing_codes)

                    # 복합 키 중복 제거 후 Batch UPSERT
                    values_by_key = {}
                    missing_row_count = 0
                    for row in rows:
                        district_code = str(row["ADSTRD_CD"]).strip()
                        district_id = district_map.get(district_code)
                        if district_id is None:
                            missing_row_count += 1
                            continue
                        key = (district_id, row["SVC_INDUTY_CD"], row["STDR_YYQU_CD"])
                        values_by_key[key] = build_sales_value(row, district_id)

                    values_list = list(values_by_key.values())

                    if values_list:
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
                                    "monthly_sales": func.coalesce(
                                        insert_stmt.excluded.monthly_sales,
                                        SalesStat.monthly_sales,
                                    ),
                                    "monthly_sales_count": func.coalesce(
                                        insert_stmt.excluded.monthly_sales_count,
                                        SalesStat.monthly_sales_count,
                                    ),
                                    "weekday_sales": func.coalesce(
                                        insert_stmt.excluded.weekday_sales,
                                        SalesStat.weekday_sales,
                                    ),
                                    "weekend_sales": func.coalesce(
                                        insert_stmt.excluded.weekend_sales,
                                        SalesStat.weekend_sales,
                                    ),
                                    "mon_sales": func.coalesce(
                                        insert_stmt.excluded.mon_sales,
                                        SalesStat.mon_sales,
                                    ),
                                    "tue_sales": func.coalesce(
                                        insert_stmt.excluded.tue_sales,
                                        SalesStat.tue_sales,
                                    ),
                                    "wed_sales": func.coalesce(
                                        insert_stmt.excluded.wed_sales,
                                        SalesStat.wed_sales,
                                    ),
                                    "thu_sales": func.coalesce(
                                        insert_stmt.excluded.thu_sales,
                                        SalesStat.thu_sales,
                                    ),
                                    "fri_sales": func.coalesce(
                                        insert_stmt.excluded.fri_sales,
                                        SalesStat.fri_sales,
                                    ),
                                    "sat_sales": func.coalesce(
                                        insert_stmt.excluded.sat_sales,
                                        SalesStat.sat_sales,
                                    ),
                                    "sun_sales": func.coalesce(
                                        insert_stmt.excluded.sun_sales,
                                        SalesStat.sun_sales,
                                    ),
                                    "tmzon_00_06_sales": func.coalesce(
                                        insert_stmt.excluded.tmzon_00_06_sales,
                                        SalesStat.tmzon_00_06_sales,
                                    ),
                                    "tmzon_06_11_sales": func.coalesce(
                                        insert_stmt.excluded.tmzon_06_11_sales,
                                        SalesStat.tmzon_06_11_sales,
                                    ),
                                    "tmzon_11_14_sales": func.coalesce(
                                        insert_stmt.excluded.tmzon_11_14_sales,
                                        SalesStat.tmzon_11_14_sales,
                                    ),
                                    "tmzon_14_17_sales": func.coalesce(
                                        insert_stmt.excluded.tmzon_14_17_sales,
                                        SalesStat.tmzon_14_17_sales,
                                    ),
                                    "tmzon_17_21_sales": func.coalesce(
                                        insert_stmt.excluded.tmzon_17_21_sales,
                                        SalesStat.tmzon_17_21_sales,
                                    ),
                                    "tmzon_21_24_sales": func.coalesce(
                                        insert_stmt.excluded.tmzon_21_24_sales,
                                        SalesStat.tmzon_21_24_sales,
                                    ),
                                    "male_sales": func.coalesce(
                                        insert_stmt.excluded.male_sales,
                                        SalesStat.male_sales,
                                    ),
                                    "female_sales": func.coalesce(
                                        insert_stmt.excluded.female_sales,
                                        SalesStat.female_sales,
                                    ),
                                    "age10_sales": func.coalesce(
                                        insert_stmt.excluded.age10_sales,
                                        SalesStat.age10_sales,
                                    ),
                                    "age20_sales": func.coalesce(
                                        insert_stmt.excluded.age20_sales,
                                        SalesStat.age20_sales,
                                    ),
                                    "age30_sales": func.coalesce(
                                        insert_stmt.excluded.age30_sales,
                                        SalesStat.age30_sales,
                                    ),
                                    "age40_sales": func.coalesce(
                                        insert_stmt.excluded.age40_sales,
                                        SalesStat.age40_sales,
                                    ),
                                    "age50_sales": func.coalesce(
                                        insert_stmt.excluded.age50_sales,
                                        SalesStat.age50_sales,
                                    ),
                                    "age60_sales": func.coalesce(
                                        insert_stmt.excluded.age60_sales,
                                        SalesStat.age60_sales,
                                    ),
                                    "weekday_sales_count": func.coalesce(
                                        insert_stmt.excluded.weekday_sales_count,
                                        SalesStat.weekday_sales_count,
                                    ),
                                    "weekend_sales_count": func.coalesce(
                                        insert_stmt.excluded.weekend_sales_count,
                                        SalesStat.weekend_sales_count,
                                    ),
                                    "mon_sales_count": func.coalesce(
                                        insert_stmt.excluded.mon_sales_count,
                                        SalesStat.mon_sales_count,
                                    ),
                                    "tue_sales_count": func.coalesce(
                                        insert_stmt.excluded.tue_sales_count,
                                        SalesStat.tue_sales_count,
                                    ),
                                    "wed_sales_count": func.coalesce(
                                        insert_stmt.excluded.wed_sales_count,
                                        SalesStat.wed_sales_count,
                                    ),
                                    "thu_sales_count": func.coalesce(
                                        insert_stmt.excluded.thu_sales_count,
                                        SalesStat.thu_sales_count,
                                    ),
                                    "fri_sales_count": func.coalesce(
                                        insert_stmt.excluded.fri_sales_count,
                                        SalesStat.fri_sales_count,
                                    ),
                                    "sat_sales_count": func.coalesce(
                                        insert_stmt.excluded.sat_sales_count,
                                        SalesStat.sat_sales_count,
                                    ),
                                    "sun_sales_count": func.coalesce(
                                        insert_stmt.excluded.sun_sales_count,
                                        SalesStat.sun_sales_count,
                                    ),
                                    "tmzon_00_06_count": func.coalesce(
                                        insert_stmt.excluded.tmzon_00_06_count,
                                        SalesStat.tmzon_00_06_count,
                                    ),
                                    "tmzon_06_11_count": func.coalesce(
                                        insert_stmt.excluded.tmzon_06_11_count,
                                        SalesStat.tmzon_06_11_count,
                                    ),
                                    "tmzon_11_14_count": func.coalesce(
                                        insert_stmt.excluded.tmzon_11_14_count,
                                        SalesStat.tmzon_11_14_count,
                                    ),
                                    "tmzon_14_17_count": func.coalesce(
                                        insert_stmt.excluded.tmzon_14_17_count,
                                        SalesStat.tmzon_14_17_count,
                                    ),
                                    "tmzon_17_21_count": func.coalesce(
                                        insert_stmt.excluded.tmzon_17_21_count,
                                        SalesStat.tmzon_17_21_count,
                                    ),
                                    "tmzon_21_24_count": func.coalesce(
                                        insert_stmt.excluded.tmzon_21_24_count,
                                        SalesStat.tmzon_21_24_count,
                                    ),
                                    "male_sales_count": func.coalesce(
                                        insert_stmt.excluded.male_sales_count,
                                        SalesStat.male_sales_count,
                                    ),
                                    "female_sales_count": func.coalesce(
                                        insert_stmt.excluded.female_sales_count,
                                        SalesStat.female_sales_count,
                                    ),
                                    "age10_sales_count": func.coalesce(
                                        insert_stmt.excluded.age10_sales_count,
                                        SalesStat.age10_sales_count,
                                    ),
                                    "age20_sales_count": func.coalesce(
                                        insert_stmt.excluded.age20_sales_count,
                                        SalesStat.age20_sales_count,
                                    ),
                                    "age30_sales_count": func.coalesce(
                                        insert_stmt.excluded.age30_sales_count,
                                        SalesStat.age30_sales_count,
                                    ),
                                    "age40_sales_count": func.coalesce(
                                        insert_stmt.excluded.age40_sales_count,
                                        SalesStat.age40_sales_count,
                                    ),
                                    "age50_sales_count": func.coalesce(
                                        insert_stmt.excluded.age50_sales_count,
                                        SalesStat.age50_sales_count,
                                    ),
                                    "age60_sales_count": func.coalesce(
                                        insert_stmt.excluded.age60_sales_count,
                                        SalesStat.age60_sales_count,
                                    ),
                                },
                            )
                            session.execute(stmt)

                    duplicate_count = len(rows) - missing_row_count - len(values_list)
                    logger.info(
                        f"  {start}~{end} 완료 (조회 {len(rows)}건 / 적재 {len(values_list)}건 / 미매핑 {missing_row_count}건 / 중복 {duplicate_count}건)"
                    )

                    if end >= total_count:
                        break
                    start += PAGE_SIZE
                    end = min(start + PAGE_SIZE - 1, total_count)

                # while 종료 후 한 번만 커밋
                session.commit()
            except Exception:
                session.rollback()
                logger.exception(f"추정매출 적재 실패 (기준분기: {period_code})")
                raise
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
    run_sales_pipeline()
