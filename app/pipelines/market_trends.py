# app/pipelines/market_trends.py
# market_trends 데이터 수집 파이프라인
# 서울시 상권분석서비스 상권변화지표(행정동) API — VwsmAdstrdIxQq

import asyncio
import logging
import os

import httpx
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from app.db.session import AsyncSessionLocal
from app.models.districts import District
from app.models.market_trends import MarketTrend

# ── 상수 정의 ────────────────────────────────────────────────
SEOUL_API_KEY = os.getenv("SEOUL_API_KEY")
BASE_URL = f"http://openapi.seoul.go.kr:8088/{SEOUL_API_KEY}/json/VwsmAdstrdIxQq"
MAX_PAGES = 20  # 최대 20페이지 (20,000건) — 현재 약 9페이지(8,925건) 기준 여유값


# ── 로거 설정 ─────────────────────────────────────────────────
logger = logging.getLogger(__name__)


# ── API 호출 함수 ─────────────────────────────────────────────
async def fetch_market_trends(
    client: httpx.AsyncClient, start: int = 1, end: int = 1000
):
    """
    서울시 상권변화지표 API 호출
    - client: 파이프라인 전체에서 재사용하는 AsyncClient (연결 풀 활용)
    - start/end: 페이지 범위
    """
    url = f"{BASE_URL}/{start}/{end}/"
    response = await client.get(url)
    response.raise_for_status()
    return response.json()


# ── district 일괄 조회 함수 ────────────────────────────────────
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
async def run_market_trends_pipeline():
    """
    market_trends 전체 파이프라인 실행
    - 분기 입력값 없음 — 전체 데이터 한 번에 조회
    - AsyncClient를 파이프라인 전체에서 1회만 생성해 HTTP 연결 재사용
    - 페이지당 district 일괄 조회 + Batch UPSERT로 DB 왕복 최소화
    """
    logger.info("상권변화지표 수집 시작")

    if not SEOUL_API_KEY:
        raise RuntimeError("SEOUL_API_KEY 환경변수가 설정되지 않았습니다.")

    async with httpx.AsyncClient(timeout=30) as client:
        async with AsyncSessionLocal() as session:
            start = 1
            end = 1000
            total_count = None
            page = 0
            all_missing_codes = (
                set()
            )  # 전체 순회 중 미매핑 코드 누적 (마지막에 한 번만 출력)

            while True:
                page += 1

                # 무한 루프 방지
                if page > MAX_PAGES:
                    raise RuntimeError(
                        f"최대 페이지 수 초과 ({MAX_PAGES}). 전체 데이터 수집이 완료되지 않았습니다."
                    )

                data = await fetch_market_trends(client, start=start, end=end)

                if "VwsmAdstrdIxQq" not in data:
                    logger.error(f"서울시 API 비정상 응답: {data}")
                    raise RuntimeError("서울시 상권변화지표 API 호출에 실패했습니다.")

                result = data.get("VwsmAdstrdIxQq", {})
                rows = result.get("row", [])

                if total_count is None:
                    total_count = int(result.get("list_total_count", 0))
                    logger.info(f"  총 {total_count}건")

                if not rows:
                    break

                codes = {row["ADSTRD_CD"] for row in rows}
                district_map = await get_district_map(session, codes)

                # 미매핑 코드 누적 — 소상공인 API에 없는 행정동은 districts FK가 없어서 적재 불가
                missing_codes = codes - district_map.keys()
                all_missing_codes.update(missing_codes)

                missing_row_count = 0  # district 미매핑으로 건너뜀 건수

                # 딕셔너리 키로 (district_id, period_code) 중복 제거
                # — 같은 페이지 내 동일 복합 키가 있으면 마지막 값만 남김
                # — 단일 UPSERT에서 같은 행을 두 번 갱신하는 오류 방지
                values_by_key = {}
                for row in rows:
                    district_id = district_map.get(row["ADSTRD_CD"])
                    if district_id is None:
                        missing_row_count += 1
                        continue
                    key = (district_id, row["STDR_YYQU_CD"])
                    values_by_key[key] = dict(
                        district_id=district_id,
                        period_code=row["STDR_YYQU_CD"],
                        trend_grade=row.get("TRDAR_CHNGE_IX"),
                        trend_grade_nm=row.get("TRDAR_CHNGE_IX_NM"),
                        opr_sale_mt_avrg=row.get("OPR_SALE_MT_AVRG"),
                        cls_sale_mt_avrg=row.get("CLS_SALE_MT_AVRG"),
                        su_opr_sale_mt_avrg=row.get("SU_OPR_SALE_MT_AVRG"),
                        su_cls_sale_mt_avrg=row.get("SU_CLS_SALE_MT_AVRG"),
                    )

                values_list = list(values_by_key.values())
                valid_row_count = len(rows) - missing_row_count
                duplicate_count = valid_row_count - len(values_list)

                if values_list:
                    try:
                        insert_stmt = insert(MarketTrend).values(values_list)
                        stmt = insert_stmt.on_conflict_do_update(
                            index_elements=["district_id", "period_code"],
                            set_={
                                # insert_stmt.excluded: 충돌 시 새로 들어온 값을 참조하는 SQLAlchemy 문법
                                "trend_grade": insert_stmt.excluded.trend_grade,
                                "trend_grade_nm": insert_stmt.excluded.trend_grade_nm,
                                "opr_sale_mt_avrg": insert_stmt.excluded.opr_sale_mt_avrg,
                                "cls_sale_mt_avrg": insert_stmt.excluded.cls_sale_mt_avrg,
                                "su_opr_sale_mt_avrg": insert_stmt.excluded.su_opr_sale_mt_avrg,
                                "su_cls_sale_mt_avrg": insert_stmt.excluded.su_cls_sale_mt_avrg,
                            },
                        )
                        await session.execute(stmt)
                        await session.commit()
                    except Exception:
                        await session.rollback()  # 실패 시 해당 페이지 전체 롤백
                        logger.exception(f"상권변화지표 적재 실패: {start}~{end}")
                        raise

                logger.info(
                    f"  {start}~{end} 완료 (조회 {len(rows)}건 / 적재 {len(values_list)}건 / 미등록 {missing_row_count}건 / 중복 {duplicate_count}건)"
                )

                if end >= total_count:
                    break
                start += 1000
                end += 1000

    # 전체 순회 완료 후 미매핑 코드 한 번만 요약 출력
    # — 소상공인 API 미제공 행정동으로 설계상 정상 동작 (오류 아님)
    if all_missing_codes:
        logger.info(
            f"district 미매핑 코드 {len(all_missing_codes)}개 건너뜀: {sorted(all_missing_codes)}"
        )

    logger.info("상권변화지표 수집 완료!")


# ── 직접 실행 시 ──────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)

    asyncio.run(run_market_trends_pipeline())
