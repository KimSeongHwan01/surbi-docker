# app/pipelines/government_supports.py
# government_supports 데이터 수집 파이프라인
# 기업마당(bizinfo.go.kr) 지원사업 공고 API
#
# 적재 흐름:
# Step 1: API 호출 (전체 페이지 순회)
# Step 2: bsnsSumryCn HTML 태그 제거
# Step 3: reqstBeginEndDe 날짜 파싱 (start_date / end_date 분리)
# Step 4: government_supports 테이블 UPSERT

import logging
import os
import re
import time
from datetime import datetime
from html.parser import HTMLParser

import httpx
from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert

from app.db.session import SyncSessionLocal
from app.models.government_supports import GovernmentSupport

# ── 상수 정의 ────────────────────────────────────────────────
BIZINFO_API_KEY = os.getenv("BIZINFO_API_KEY")
API_URL = "https://www.bizinfo.go.kr/uss/rss/bizinfoApi.do"
PAGE_SIZE = 100
MAX_PAGES = 50  # 최대 50페이지 (5,000건)


# ── 로거 설정 ─────────────────────────────────────────────────
logger = logging.getLogger(__name__)


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


# ── 유틸리티 함수 ─────────────────────────────────────────────
def empty_to_none(value):
    """None 또는 공백 문자열을 None으로 변환"""
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        return value or None
    return value


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
        end = datetime.strptime(parts[1].strip(), "%Y-%m-%d").date()
        return start, end
    except ValueError:
        return None, None


# ── API 호출 함수 ─────────────────────────────────────────────
def fetch_supports(
    client: httpx.Client,
    page: int = 1,
    max_retries: int = 3,
) -> list[dict]:
    """
    기업마당 API 단일 페이지 호출 -> 아이템 리스트 반환
    - client: 파이프라인 전체에서 재사용하는 httpx.Client (연결 풀 활용)
    - 429 및 일시적인 5xx 오류 재시도 (최대 3회, 3초 간격)
    - 응답 구조: {"jsonArray": [...]} 또는 직접 리스트
    - item이 단일 dict로 내려오는 경우 리스트로 감싸기
    """

    params = {
        "crtfcKey": BIZINFO_API_KEY,
        "dataType": "json",
        "pageUnit": PAGE_SIZE,
        "pageIndex": page,
    }

    retryable_status_codes = {429, 500, 502, 503, 504}

    for attempt in range(1, max_retries + 1):
        try:
            response = client.get(API_URL, params=params)

            if response.status_code in retryable_status_codes:
                if attempt == max_retries:
                    response.raise_for_status()
                logger.warning(
                    f"기업마당 API 일시 오류 "
                    f"(페이지 {page}, 상태 {response.status_code}, "
                    f"재시도 {attempt}/{max_retries})"
                )
                time.sleep(3)
                continue

            response.raise_for_status()
            data = response.json()

            if isinstance(data, dict):
                items = data.get("jsonArray") or data.get("item") or []
            elif isinstance(data, list):
                items = data
            else:
                raise RuntimeError(
                    f"예상하지 못한 API 응답 형식: {type(data).__name__}"
                )

            if isinstance(items, dict):
                items = [items]

            if not isinstance(items, list):
                raise RuntimeError(
                    f"지원사업 목록 형식이 올바르지 않습니다: {type(items).__name__}"
                )

            if not all(isinstance(item, dict) for item in items):
                raise RuntimeError(
                    f"지원사업 항목 중 dict가 아닌 값이 포함되어 있습니다: 페이지 {page}"
                )

            return items

        except (httpx.TimeoutException, httpx.RequestError) as exc:
            if attempt == max_retries:
                raise RuntimeError(f"기업마당 API 요청 실패: 페이지 {page}") from exc
            logger.warning(
                f"기업마당 API 연결 오류 "
                f"(페이지 {page}, 재시도 {attempt}/{max_retries}): {exc}"
            )
            time.sleep(3)

    raise RuntimeError(f"기업마당 API 요청 실패: 페이지 {page}")


# ── government_supports 적재 함수 ────────────────────────────
def upsert_support(session, item: dict) -> bool:
    """
    government_supports 테이블 UPSERT
    - UNIQUE: pblanc_id
    - summary: bsnsSumryCn HTML 태그 제거 후 저장
    - sprt_start_date / end_date: reqstBeginEndDe 파싱
    - 선택값: 새 값이 NULL이면 기존 값 유지 (COALESCE)
    """
    sprt_start_date, end_date = parse_date_range(item.get("reqstBeginEndDe"))

    pblanc_id = empty_to_none(item.get("pblancId"))
    if not pblanc_id:
        return False

    insert_stmt = insert(GovernmentSupport).values(
        pblanc_id=str(pblanc_id),
        title=empty_to_none(item.get("pblancNm")),
        category=empty_to_none(item.get("pldirSportRealmLclasCodeNm")),
        support_type=empty_to_none(item.get("pldirSportRealmMlsfcCodeNm")),
        support_target=empty_to_none(item.get("trgetNm")),
        agency=empty_to_none(item.get("excInsttNm")),
        jrsd_instt_nm=empty_to_none(item.get("jrsdInsttNm")),
        summary=strip_html(item.get("bsnsSumryCn")),
        sprt_start_date=sprt_start_date,
        end_date=end_date,
        support_url=empty_to_none(item.get("pblancUrl")),
    )

    stmt = insert_stmt.on_conflict_do_update(
        index_elements=["pblanc_id"],
        set_={
            "title": func.coalesce(insert_stmt.excluded.title, GovernmentSupport.title),
            "category": func.coalesce(
                insert_stmt.excluded.category, GovernmentSupport.category
            ),
            "support_type": func.coalesce(
                insert_stmt.excluded.support_type, GovernmentSupport.support_type
            ),
            "support_target": func.coalesce(
                insert_stmt.excluded.support_target, GovernmentSupport.support_target
            ),
            "agency": func.coalesce(
                insert_stmt.excluded.agency, GovernmentSupport.agency
            ),
            "jrsd_instt_nm": func.coalesce(
                insert_stmt.excluded.jrsd_instt_nm, GovernmentSupport.jrsd_instt_nm
            ),
            "summary": func.coalesce(
                insert_stmt.excluded.summary, GovernmentSupport.summary
            ),
            "sprt_start_date": func.coalesce(
                insert_stmt.excluded.sprt_start_date, GovernmentSupport.sprt_start_date
            ),
            "end_date": func.coalesce(
                insert_stmt.excluded.end_date, GovernmentSupport.end_date
            ),
            "support_url": func.coalesce(
                insert_stmt.excluded.support_url, GovernmentSupport.support_url
            ),
        },
    )
    session.execute(stmt)
    return True


# ── 메인 파이프라인 함수 ──────────────────────────────────────
def run_government_supports_pipeline():
    """
    government_supports 전체 파이프라인 실행
    - 전체 페이지 순회하며 모든 지원사업 공고 수집
    - PAGE_SIZE보다 적은 결과가 오면 마지막 페이지로 판단
    - totCnt: API 응답 내 전체 건수 필드
    """
    logger.info("정부지원사업 수집 시작")

    if not BIZINFO_API_KEY:
        raise RuntimeError("BIZINFO_API_KEY 환경변수가 설정되지 않았습니다.")

    with httpx.Client(timeout=30) as client:
        with SyncSessionLocal() as session:
            page = 1
            total_count = None

            while True:
                # 무한 루프 방지
                if page > MAX_PAGES:
                    raise RuntimeError(
                        f"최대 페이지 수 초과 ({MAX_PAGES}). 전체 데이터 수집이 완료되지 않았습니다."
                    )

                items = fetch_supports(client, page=page)

                if not items:
                    break

                # 첫 페이지에서 전체 건수 출력
                if total_count is None:
                    raw_total = items[0].get("totCnt")
                    if raw_total not in (None, ""):
                        try:
                            total_count = int(str(raw_total).replace(",", "").strip())
                        except (TypeError, ValueError) as exc:
                            raise RuntimeError(
                                f"기업마당 API의 totCnt 값이 올바르지 않습니다: {raw_total!r}"
                            ) from exc
                        logger.info(f"  총 {total_count}건")
                    else:
                        logger.warning(
                            "API 응답에 totCnt가 없어 페이지 크기로 종료 여부를 판단합니다."
                        )

                start = (page - 1) * PAGE_SIZE + 1
                end = start + len(items) - 1

                try:
                    success_count = 0
                    skipped_count = 0
                    for item in items:
                        if upsert_support(session, item):
                            success_count += 1
                        else:
                            skipped_count += 1
                    session.commit()
                except Exception:
                    session.rollback()
                    logger.exception(f"정부지원사업 적재 실패: {start}~{end}")
                    raise

                logger.info(
                    f"  {start}~{end} 완료 (조회 {len(items)}건 / 적재 {success_count}건 / 누락 {skipped_count}건)"
                )

                # 마지막 페이지 감지
                reached_total = total_count is not None and end >= total_count
                if reached_total or len(items) < PAGE_SIZE:
                    break

                page += 1

    logger.info("정부지원사업 수집 완료!")


# ── 직접 실행 시 ──────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    run_government_supports_pipeline()
