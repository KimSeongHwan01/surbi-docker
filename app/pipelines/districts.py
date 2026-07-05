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

import httpx
import asyncio
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from app.db.session import AsyncSessionLocal
from app.models.districts import District
from app.models.businesses import Business
import os

# ── 상수 정의 ────────────────────────────────────────────────

# 소상공인 API 인증키 (.env에서 읽어옴)
API_KEY = os.getenv("SBSC_API_KEY")

# 소상공인 API 기본 URL
BASE_URL = "https://apis.data.go.kr/B553077/api/open/sdsc2/storeListInDong"

# 서울 25개 구 코드
GU_CODES = [
    "11110", "11140", "11170", "11200", "11215",
    "11230", "11260", "11290", "11305", "11320",
    "11350", "11380", "11410", "11440", "11470",
    "11500", "11530", "11545", "11560", "11590",
    "11620", "11650", "11680", "11710", "11740"
]

# ── 유틸리티 함수 ─────────────────────────────────────────────

def empty_to_none(value):
    """빈 문자열('') → None(NULL) 변환"""
    if value == "" or value is None:
        return None
    return value


# ── API 호출 함수 ─────────────────────────────────────────────

async def fetch_stores(gu_code: str, page: int = 1, num_of_rows: int = 1000, max_retries: int = 3):
    """
    소상공인 API 호출
    - 502 등 서버 오류 시 최대 3번 재시도
    - 재시도 간격: 3초
    """
    params = {
        "serviceKey": API_KEY,
        "divId": "signguCd",
        "key": gu_code,
        "type": "json",
        "pageNo": page,
        "numOfRows": num_of_rows
    }

    for attempt in range(max_retries):
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.get(BASE_URL, params=params)
                response.raise_for_status()
                return response.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 502 and attempt < max_retries - 1:
                print(f"  502 오류 발생. {3}초 후 재시도... ({attempt + 1}/{max_retries})")
                await asyncio.sleep(3)
            else:
                raise


# ── districts 적재 함수 ───────────────────────────────────────

async def upsert_district(session, item: dict) -> int:
    """
    districts 테이블에 행정동 데이터 UPSERT
    - 이미 있으면 무시 (DO NOTHING)
    - 새로운 행정동이면 INSERT
    - district_id 반환
    """
    stmt = insert(District).values(
        district_code=item["adongCd"],
        district_name=item["adongNm"],
        gu_code=item["signguCd"],
        gu=item["signguNm"],
        si_code=item["ctprvnCd"],
        si=item["ctprvnNm"]
    ).on_conflict_do_nothing(index_elements=["district_code"])

    await session.execute(stmt)

    # district_id 조회
    result = await session.execute(
        select(District.id).where(District.district_code == item["adongCd"])
    )
    return result.scalar_one()


# ── businesses 적재 함수 ──────────────────────────────────────

async def upsert_business(session, item: dict, district_id: int):
    """
    businesses 테이블에 업소 데이터 UPSERT
    - 이미 있으면 최신 정보로 UPDATE
    - 새로운 업소면 INSERT
    """
    stmt = insert(Business).values(
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
        land_main_no=int(item.get("lnoMnno")) if item.get("lnoMnno") not in [None, ""] else None,
        land_sub_no=str(item.get("lnoSlno")) if item.get("lnoSlno") not in [None, ""] else None,
        land_address=empty_to_none(item.get("lnoAdr")),
        road_name_code=empty_to_none(item.get("rdnmCd")),
        road_name=empty_to_none(item.get("rdnm")),
        bld_main_no=int(item.get("bldMnno")) if item.get("bldMnno") not in [None, ""] else None,
        bld_sub_no=str(item.get("bldSlno")) if item.get("bldSlno") not in [None, ""] else None,
        building_mgmt_no=empty_to_none(item.get("bldMngNo")),
        building_name=empty_to_none(item.get("bldNm")),
        road_address=empty_to_none(item.get("rdnmAdr")),
        old_zipcode=empty_to_none(item.get("oldZipcd")),
        new_zipcode=empty_to_none(item.get("newZipcd")),
        dong_no=empty_to_none(item.get("dongNo")),
        floor_no=empty_to_none(item.get("flrNo")),
        unit_no=empty_to_none(item.get("hoNo")),
        lng=item.get("lon"),   # API 응답명 lon → DB 저장명 lng
        lat=item.get("lat"),
    ).on_conflict_do_update(
        index_elements=["business_code"],
        set_=dict(
            biz_name=item["bizesNm"],
            open_status="영업중",
        )
    )
    await session.execute(stmt)


# ── 메인 파이프라인 함수 ──────────────────────────────────────

async def run_districts_pipeline():
    """
    districts + businesses 전체 파이프라인 실행
    - 서울 25개 구 코드 반복 호출
    - 페이지네이션 처리 (totalCount 기준)
    """
    async with AsyncSessionLocal() as session:
        for gu_code in GU_CODES:
            print(f"[{gu_code}] 수집 시작...")

            page = 1
            total_count = None

            while True:
                # API 호출
                data = await fetch_stores(gu_code, page=page)
                body = data.get("body", {})
                items = body.get("items", [])

                if total_count is None:
                    total_count = body.get("totalCount", 0)
                    print(f"  총 {total_count}건")

                if not items:
                    break

                # 각 업소 데이터 처리
                for item in items:
                    # Step 1: districts UPSERT
                    district_id = await upsert_district(session, item)

                    # Step 2: businesses UPSERT
                    await upsert_business(session, item, district_id)

                await session.commit()
                print(f"  페이지 {page} 완료 ({len(items)}건)")

                # 다음 페이지 여부 확인
                if page * 1000 >= total_count:
                    break
                page += 1

            print(f"[{gu_code}] 완료!")

    print("전체 파이프라인 완료!")


# ── 직접 실행 시 ──────────────────────────────────────────────
if __name__ == "__main__":
    asyncio.run(run_districts_pipeline())