# app/pipelines/government_supports.py
# government_supports 데이터 수집 파이프라인
# 기업마당(bizinfo.go.kr) 지원사업 공고 API
#
# 적재 흐름:
# Step 1: API 호출 (전체 페이지 순회)
# Step 2: bsnsSumryCn HTML 태그 제거
# Step 3: reqstBeginEndDe 날짜 파싱 (start_date / end_date 분리)
# Step 4: government_supports 테이블 UPSERT

import httpx
import asyncio
import re
from datetime import datetime
from html.parser import HTMLParser
from sqlalchemy.dialects.postgresql import insert
from app.db.session import AsyncSessionLocal
from app.models.government_supports import GovernmentSupport
import os

# ── 상수 정의 ────────────────────────────────────────────────

BIZINFO_API_KEY = os.getenv("BIZINFO_API_KEY")
API_URL = "https://www.bizinfo.go.kr/uss/rss/bizinfoApi.do"
PAGE_SIZE = 100


# ── HTML 태그 제거 함수 ───────────────────────────────────────

class _MLStripper(HTMLParser):
    """HTML 태그를 제거하는 파서"""
    def __init__(self):
        super().__init__()
        self.reset()
        self.fed = []

    def handle_data(self, d):
        self.fed.append(d)

    def get_data(self):
        return " ".join(self.fed).strip()


def strip_html(raw: str) -> str | None:
    """HTML 태그 제거 후 연속 공백 정리"""
    if not raw:
        return None
    s = _MLStripper()
    s.feed(raw)
    cleaned = re.sub(r"\s+", " ", s.get_data()).strip()
    return cleaned if cleaned else None


# ── 날짜 파싱 함수 ────────────────────────────────────────────

def parse_date_range(raw: str | None) -> tuple:
    """
    'YYYY-MM-DD ~ YYYY-MM-DD' 형식 -> (start_date, end_date) 반환
    파싱 실패 시 (None, None) 반환
    """
    if not raw:
        return None, None
    parts = raw.split("~")
    if len(parts) != 2:
        return None, None
    try:
        start = datetime.strptime(parts[0].strip(), "%Y-%m-%d").date()
        end   = datetime.strptime(parts[1].strip(), "%Y-%m-%d").date()
        return start, end
    except ValueError:
        return None, None


# ── API 호출 함수 ─────────────────────────────────────────────

async def fetch_supports(page: int = 1) -> list[dict]:
    """
    기업마당 API 단일 페이지 호출 -> 아이템 리스트 반환
    - 응답 구조: {"jsonArray": [...]} 또는 직접 리스트
    - item이 단일 dict로 내려오는 경우 리스트로 감싸기
    """
    params = {
        "crtfcKey":  BIZINFO_API_KEY,
        "dataType":  "json",
        "pageUnit":  PAGE_SIZE,
        "pageIndex": page,
    }

    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.get(API_URL, params=params)
        response.raise_for_status()
        data = response.json()

    if isinstance(data, dict):
        items = data.get("jsonArray") or data.get("item") or []
    else:
        items = data or []

    if isinstance(items, dict):
        items = [items]

    return items


# ── government_supports 적재 함수 ────────────────────────────

async def upsert_support(session, item: dict) -> bool:
    """
    government_supports 테이블 UPSERT
    - UNIQUE: pblanc_id
    - 예약어 회피 컬럼: support_target, sprt_start_date, support_url
    - summary: bsnsSumryCn HTML 태그 제거 후 저장
    - sprt_start_date / end_date: reqstBeginEndDe 파싱
    """
    sprt_start_date, end_date = parse_date_range(item.get("reqstBeginEndDe"))

    pblanc_id = item.get("pblancId")
    if not pblanc_id:
        return False

    stmt = insert(GovernmentSupport).values(
        pblanc_id=str(pblanc_id),
        title=item.get("pblancNm"),
        category=item.get("pldirSportRealmLclasCodeNm"),
        support_type=item.get("pldirSportRealmMlsfcCodeNm"),
        support_target=item.get("trgetNm"),
        agency=item.get("excInsttNm"),
        jrsd_instt_nm=item.get("jrsdInsttNm"),
        summary=strip_html(item.get("bsnsSumryCn")),
        sprt_start_date=sprt_start_date,
        end_date=end_date,
        support_url=item.get("pblancUrl"),
    ).on_conflict_do_update(
        index_elements=["pblanc_id"],
        set_=dict(
            title=item.get("pblancNm"),
            sprt_start_date=sprt_start_date,
            end_date=end_date,
        )
    )
    await session.execute(stmt)
    return True


# ── 메인 파이프라인 함수 ──────────────────────────────────────

async def run_government_supports_pipeline():
    """
    government_supports 전체 파이프라인 실행
    - 전체 페이지 순회하며 모든 지원사업 공고 수집
    - PAGE_SIZE보다 적은 결과가 오면 마지막 페이지로 판단
    - totCnt: API 응답 내 전체 건수 필드
    """
    print("정부지원사업 수집 시작")

    async with AsyncSessionLocal() as session:
        page = 1
        total_count = None

        while True:
            items = await fetch_supports(page=page)

            if not items:
                break

            # 첫 페이지에서 전체 건수 출력
            if total_count is None:
                total_count = int(items[0].get("totCnt", 0))
                print(f"  총 {total_count}건")

            for item in items:
                await upsert_support(session, item)

            await session.commit()

            start = (page - 1) * PAGE_SIZE + 1
            end   = start + len(items) - 1
            print(f"  {start}~{end} 완료 ({len(items)}건)")

            # 마지막 페이지 감지
            if len(items) < PAGE_SIZE:
                break

            page += 1
            await asyncio.sleep(0.5)  # API 서버 부하 방지

    print("정부지원사업 수집 완료!")


# ── 직접 실행 시 ──────────────────────────────────────────────
if __name__ == "__main__":
    asyncio.run(run_government_supports_pipeline())