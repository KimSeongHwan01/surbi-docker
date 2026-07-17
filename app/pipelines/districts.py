# app/pipelines/districts.py
# districts + businesses 데이터 수집 파이프라인
# 소상공인시장진흥공단 상가(상권)정보 Open API
# API 1회 호출 → districts + businesses 두 테이블 동시 적재
#
# 적재 흐름:
# Step 1: API 호출 (signguCd 25개 구 코드 반복)
# Step 2: adongCd 기준 districts UPSERT (중복 시 무시)
# Step 3: district_id 확보
# Step 4: businesses INSERT (중복 시 UPDATE)
# Step 5: 빈 문자열('') → NULL 변환 처리

import asyncio
import logging
import os

import httpx
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert

from app.db.session import AsyncSessionLocal
from app.models.businesses import Business
from app.models.districts import District

# ── 상수 정의 ────────────────────────────────────────────────
# 소상공인 API 인증키 (.env에서 읽어옴)
API_KEY = os.getenv("SBSC_API_KEY")

# 소상공인 API 기본 URL
BASE_URL = "https://apis.data.go.kr/B553077/api/open/sdsc2/storeListInDong"

# 서울 25개 구 코드
GU_CODES = [
    "11110",
    "11140",
    "11170",
    "11200",
    "11215",
    "11230",
    "11260",
    "11290",
    "11305",
    "11320",
    "11350",
    "11380",
    "11410",
    "11440",
    "11470",
    "11500",
    "11530",
    "11545",
    "11560",
    "11590",
    "11620",
    "11650",
    "11680",
    "11710",
    "11740",
]

MAX_PAGES = 100  # 최대 100페이지 (100,000건) — 구당 최대 약 30페이지 기준 여유값
BATCH_SIZE = 500  # businesses Batch UPSERT 단위 (업소 1건당 컬럼 35개 → 500건 = 약 17,500개 파라미터, PostgreSQL 최대 32,767개 제한 대응)
PAGE_SIZE = 1000  # API 페이지당 조회 건수

# ── 로거 설정 ─────────────────────────────────────────────────
logger = logging.getLogger(__name__)


# ── 유틸리티 함수 ─────────────────────────────────────────────
def empty_to_none(value):
    """빈 문자열('') → None(NULL) 변환"""
    if value == "" or value is None:
        return None
    return value


# ── API 호출 함수 ─────────────────────────────────────────────
async def fetch_stores(
    client: httpx.AsyncClient,
    gu_code: str,
    page: int = 1,
    num_of_rows: int = 1000,
    max_retries: int = 3,
):
    """
    소상공인 API 호출
    - client: 파이프라인 전체에서 재사용하는 AsyncClient (연결 풀 활용)
    - 502 등 서버 오류 시 최대 3번 재시도 (3초 간격)
    """
    params = {
        "serviceKey": API_KEY,
        "divId": "signguCd",
        "key": gu_code,
        "type": "json",
        "pageNo": page,
        "numOfRows": num_of_rows,
    }

    for attempt in range(max_retries):
        try:
            response = await client.get(BASE_URL, params=params)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 502 and attempt < max_retries - 1:
                # 502 오류
                logger.warning(
                    f"  502 오류 발생. 3초 후 재시도... ({attempt + 1}/{max_retries})"
                )

                await asyncio.sleep(3)
            else:
                raise


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


# ── 업소 데이터 변환 함수 ─────────────────────────────────────
def build_business_value(item: dict, district_id: int) -> dict:
    """
    API 응답 업소 1건 → DB INSERT용 딕셔너리 변환
    - empty_to_none: 빈 문자열 → NULL 변환
    - land_sub_no, bld_sub_no: 문자열/정수 혼재 → str() 변환
    - API 응답명 lon → DB 저장명 lng
    """
    return dict(
        district_id=district_id,
        business_code=item["bizesId"],
        biz_name=item["bizesNm"],
        branch_name=empty_to_none(item.get("brchNm")),
        category_large_code=empty_to_none(item.get("indsLclsCd")),
        category_large_name=empty_to_none(item.get("indsLclsNm")),
        category_medium_code=empty_to_none(item.get("indsMclsCd")),
        category_medium_name=empty_to_none(item.get("indsMclsNm")),
        category_code=empty_to_none(item.get("indsSclsCd")),
        category_name=empty_to_none(item.get("indsSclsNm")),
        ksic_code=empty_to_none(item.get("ksicCd")),
        ksic_name=empty_to_none(item.get("ksicNm")),
        legal_dong_code=empty_to_none(item.get("ldongCd")),
        legal_dong_name=empty_to_none(item.get("ldongNm")),
        pnu_code=empty_to_none(item.get("lnoCd")),
        plot_sct_code=empty_to_none(item.get("plotSctCd")),
        plot_sct_name=empty_to_none(item.get("plotSctNm")),
        land_main_no=int(item.get("lnoMnno"))
        if item.get("lnoMnno") not in [None, ""]
        else None,
        land_sub_no=str(item.get("lnoSlno"))
        if item.get("lnoSlno") not in [None, ""]
        else None,
        land_address=empty_to_none(item.get("lnoAdr")),
        road_name_code=empty_to_none(item.get("rdnmCd")),
        road_name=empty_to_none(item.get("rdnm")),
        bld_main_no=int(item.get("bldMnno"))
        if item.get("bldMnno") not in [None, ""]
        else None,
        bld_sub_no=str(item.get("bldSlno"))
        if item.get("bldSlno") not in [None, ""]
        else None,
        building_mgmt_no=empty_to_none(item.get("bldMngNo")),
        building_name=empty_to_none(item.get("bldNm")),
        road_address=empty_to_none(item.get("rdnmAdr")),
        old_zipcode=empty_to_none(item.get("oldZipcd")),
        new_zipcode=empty_to_none(item.get("newZipcd")),
        dong_no=empty_to_none(item.get("dongNo")),
        floor_no=empty_to_none(item.get("flrNo")),
        unit_no=empty_to_none(item.get("hoNo")),
        lng=item.get("lon"),
        lat=item.get("lat"),
        open_status="영업중",
    )


# ── 메인 파이프라인 함수 ──────────────────────────────────────
async def run_districts_pipeline():
    """
    districts + businesses 전체 파이프라인 실행
    - 서울 25개 구 코드 반복 호출
    - 소상공인 API 1회 호출로 districts + businesses 동시 적재
    - AsyncClient를 파이프라인 전체에서 1회만 생성해 HTTP 연결 재사용
    - 페이지네이션 처리 (totalCount 기준)
    """
    if not API_KEY:
        raise RuntimeError("SBSC_API_KEY 환경변수가 설정되지 않았습니다.")

    logger.info("상가정보 수집 시작")

    async with httpx.AsyncClient(timeout=30) as client:
        async with AsyncSessionLocal() as session:
            for gu_code in GU_CODES:
                logger.info(f"[{gu_code}] 수집 시작...")

                page = 1
                total_count = None

                while True:
                    # 무한 루프 방지
                    if page > MAX_PAGES:
                        raise RuntimeError(
                            f"[{gu_code}] 최대 페이지 수 초과 ({MAX_PAGES}). 전체 데이터 수집이 완료되지 않았습니다."
                        )

                    data = await fetch_stores(
                        client, gu_code, page=page, num_of_rows=PAGE_SIZE
                    )

                    # API 비정상 응답 검사
                    if "body" not in data:
                        logger.error(f"소상공인 API 비정상 응답: {data}")
                        raise RuntimeError("소상공인 상가정보 API 호출에 실패했습니다.")

                    body = data.get("body", {})
                    items = body.get("items", [])

                    if total_count is None:
                        total_count = int(body.get("totalCount", 0))
                        logger.info(f"  총 {total_count}건")

                    if not items:
                        break

                    try:
                        # Step 1: 행정동 중복 제거 후 Batch INSERT
                        district_values_by_code = {}
                        for item in items:
                            code = item["adongCd"]
                            district_values_by_code[code] = dict(
                                district_code=code,
                                district_name=item["adongNm"],
                                gu_code=item["signguCd"],
                                gu=item["signguNm"],
                                si_code=item["ctprvnCd"],
                                si=item["ctprvnNm"],
                            )

                        district_stmt = (
                            insert(District)
                            .values(list(district_values_by_code.values()))
                            .on_conflict_do_nothing(index_elements=["district_code"])
                        )
                        await session.execute(district_stmt)

                        # Step 2: district_id 일괄 조회
                        district_map = await get_district_map(
                            session, set(district_values_by_code.keys())
                        )

                        # 방금 INSERT한 행정동이 조회되지 않으면 비정상 상황
                        missing_district_codes = (
                            set(district_values_by_code.keys()) - district_map.keys()
                        )
                        if missing_district_codes:
                            raise RuntimeError(
                                f"[{gu_code}] district_id 매핑 실패: {sorted(missing_district_codes)}"
                            )

                        # Step 3: 업소 중복 제거 후 Batch UPSERT
                        # PostgreSQL 최대 파라미터 수 제한(32,767개)으로 BATCH_SIZE 단위로 나눠서 처리
                        # 업소 1건당 컬럼 33개 → 500건 = 16,500개 파라미터로 여유있게 처리
                        business_values_by_code = {}
                        for item in items:
                            district_id = district_map.get(item["adongCd"])
                            if district_id is None:
                                continue
                            business_values_by_code[item["bizesId"]] = (
                                build_business_value(item, district_id)
                            )

                        business_values = list(business_values_by_code.values())

                        for i in range(0, len(business_values), BATCH_SIZE):
                            batch = business_values[i : i + BATCH_SIZE]
                            business_insert = insert(Business).values(batch)
                            business_stmt = business_insert.on_conflict_do_update(
                                index_elements=["business_code"],
                                set_={
                                    # 필수값 — 새 값으로 무조건 갱신
                                    "district_id": business_insert.excluded.district_id,
                                    "biz_name": business_insert.excluded.biz_name,
                                    "open_status": business_insert.excluded.open_status,
                                    # 선택값 — 새 값이 NULL이면 기존 값 유지 (COALESCE)
                                    "branch_name": func.coalesce(
                                        business_insert.excluded.branch_name,
                                        Business.branch_name,
                                    ),
                                    "category_large_code": func.coalesce(
                                        business_insert.excluded.category_large_code,
                                        Business.category_large_code,
                                    ),
                                    "category_large_name": func.coalesce(
                                        business_insert.excluded.category_large_name,
                                        Business.category_large_name,
                                    ),
                                    "category_medium_code": func.coalesce(
                                        business_insert.excluded.category_medium_code,
                                        Business.category_medium_code,
                                    ),
                                    "category_medium_name": func.coalesce(
                                        business_insert.excluded.category_medium_name,
                                        Business.category_medium_name,
                                    ),
                                    "category_code": func.coalesce(
                                        business_insert.excluded.category_code,
                                        Business.category_code,
                                    ),
                                    "category_name": func.coalesce(
                                        business_insert.excluded.category_name,
                                        Business.category_name,
                                    ),
                                    "ksic_code": func.coalesce(
                                        business_insert.excluded.ksic_code,
                                        Business.ksic_code,
                                    ),
                                    "ksic_name": func.coalesce(
                                        business_insert.excluded.ksic_name,
                                        Business.ksic_name,
                                    ),
                                    "legal_dong_code": func.coalesce(
                                        business_insert.excluded.legal_dong_code,
                                        Business.legal_dong_code,
                                    ),
                                    "legal_dong_name": func.coalesce(
                                        business_insert.excluded.legal_dong_name,
                                        Business.legal_dong_name,
                                    ),
                                    "pnu_code": func.coalesce(
                                        business_insert.excluded.pnu_code,
                                        Business.pnu_code,
                                    ),
                                    "plot_sct_code": func.coalesce(
                                        business_insert.excluded.plot_sct_code,
                                        Business.plot_sct_code,
                                    ),
                                    "plot_sct_name": func.coalesce(
                                        business_insert.excluded.plot_sct_name,
                                        Business.plot_sct_name,
                                    ),
                                    "land_main_no": func.coalesce(
                                        business_insert.excluded.land_main_no,
                                        Business.land_main_no,
                                    ),
                                    "land_sub_no": func.coalesce(
                                        business_insert.excluded.land_sub_no,
                                        Business.land_sub_no,
                                    ),
                                    "land_address": func.coalesce(
                                        business_insert.excluded.land_address,
                                        Business.land_address,
                                    ),
                                    "road_name_code": func.coalesce(
                                        business_insert.excluded.road_name_code,
                                        Business.road_name_code,
                                    ),
                                    "road_name": func.coalesce(
                                        business_insert.excluded.road_name,
                                        Business.road_name,
                                    ),
                                    "bld_main_no": func.coalesce(
                                        business_insert.excluded.bld_main_no,
                                        Business.bld_main_no,
                                    ),
                                    "bld_sub_no": func.coalesce(
                                        business_insert.excluded.bld_sub_no,
                                        Business.bld_sub_no,
                                    ),
                                    "building_mgmt_no": func.coalesce(
                                        business_insert.excluded.building_mgmt_no,
                                        Business.building_mgmt_no,
                                    ),
                                    "building_name": func.coalesce(
                                        business_insert.excluded.building_name,
                                        Business.building_name,
                                    ),
                                    "road_address": func.coalesce(
                                        business_insert.excluded.road_address,
                                        Business.road_address,
                                    ),
                                    "old_zipcode": func.coalesce(
                                        business_insert.excluded.old_zipcode,
                                        Business.old_zipcode,
                                    ),
                                    "new_zipcode": func.coalesce(
                                        business_insert.excluded.new_zipcode,
                                        Business.new_zipcode,
                                    ),
                                    "dong_no": func.coalesce(
                                        business_insert.excluded.dong_no,
                                        Business.dong_no,
                                    ),
                                    "floor_no": func.coalesce(
                                        business_insert.excluded.floor_no,
                                        Business.floor_no,
                                    ),
                                    "unit_no": func.coalesce(
                                        business_insert.excluded.unit_no,
                                        Business.unit_no,
                                    ),
                                    "lng": func.coalesce(
                                        business_insert.excluded.lng, Business.lng
                                    ),
                                    "lat": func.coalesce(
                                        business_insert.excluded.lat, Business.lat
                                    ),
                                },
                            )
                            await session.execute(business_stmt)

                        await session.commit()

                    except Exception:
                        await session.rollback()
                        logger.exception(f"[{gu_code}] 페이지 {page} 적재 실패")
                        raise

                    logger.info(f"  페이지 {page} 완료 ({len(items)}건)")

                    # 다음 페이지 여부 확인
                    if page * PAGE_SIZE >= total_count:
                        break
                    page += 1

    logger.info("상가정보 수집 완료!")


# ── 직접 실행 시 ──────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)

    asyncio.run(run_districts_pipeline())
