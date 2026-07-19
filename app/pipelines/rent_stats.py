# app/pipelines/rent_stats.py
# rent_stats 데이터 수집 파이프라인
# 한국부동산원 소규모상가 분기별 임대료 JSON 파일 수동 적재
#
# 사용 방법:
# 1. 한국부동산원 사이트에서 JSON 파일 다운로드
#    https://www.reb.or.kr/r-one/portal/stat/easyStatPage/T248223134698125.do
# 2. 파일을 /surbi-docker/data/ 폴더에 저장
# 3. 파이프라인 실행
#
# 적재 흐름:
# Step 1: JSON 파일 읽기
# Step 2: 헤더 행 파싱 (분기 코드 추출)
# Step 3: 서울 데이터만 필터링
# Step 4: 상권명 → 자치구 수동 매핑
# Step 5: rent_stats 테이블 UPSERT

import json
import logging
import os
import re

from sqlalchemy.dialects.postgresql import insert

from app.db.session import SyncSessionLocal
from app.models.rent_stats import RentStat

# ── 로거 설정 ─────────────────────────────────────────────────
logger = logging.getLogger(__name__)


# ── 상권명 → 자치구 수동 매핑 딕셔너리 ──────────────────────
# 한국부동산원 자체 상권명을 서울시 자치구로 매핑
AREA_TO_GU = {
    # 도심
    "광화문": "종로구",
    "북촌": "종로구",
    "서촌": "종로구",
    "종로": "종로구",
    "시청": "중구",
    "을지로": "중구",
    "충무로": "중구",
    "명동": "중구",
    "남대문": "중구",
    "동대문": "중구",
    "방산시장": "중구",
    # 강남
    "강남": "강남구",
    "강남대로": "강남구",
    "논현역": "강남구",
    "도산대로": "강남구",
    "신사역": "강남구",
    "압구정": "강남구",
    "청담": "강남구",
    "테헤란로": "강남구",
    "교대역": "서초구",
    "남부터미널": "서초구",
    # 영등포신촌
    "영등포신촌": "영등포구",
    "영등포역": "영등포구",
    "당산역": "영등포구",
    "공덕역": "마포구",
    "홍대/합정": "마포구",
    "동교/연남": "마포구",
    "망원역": "마포구",
    "신촌/이대": "서대문구",
    # 기타 서울
    "가락시장": "송파구",
    "잠실/송파": "송파구",
    "잠실새내역": "송파구",
    "건대입구": "광진구",
    "뚝섬": "성동구",
    "왕십리": "성동구",
    "천호": "강동구",
    "길동": "강동구",
    "노량진": "동작구",
    "사당": "동작구",
    "신림역": "관악구",
    "서울대입구역": "관악구",
    "이태원": "용산구",
    "용산역": "용산구",
    "약수역": "중구",
    "청량리": "동대문구",
    "경희대": "동대문구",
    "성신여대": "성북구",
    "미아사거리": "성북구",
    "상계역": "노원구",
    "수유": "강북구",
    "연신내": "은평구",
    "불광역": "은평구",
    "혜화동": "종로구",
    "목동": "양천구",
    "화곡": "강서구",
    "오류동역": "구로구",
    "독산/시흥": "금천구",
    "까치산역": "강서구",
    "상봉역": "중랑구",
    "장안동": "동대문구",
    "군자": "광진구",
    "숙명여대": "용산구",
    "도심": "중구",
    "기타": None,
}


def parse_period_code(header_str: str) -> str | None:
    """
    헤더 문자열에서 분기 코드 추출
    예: '2024년 3분기' → '20243'
        '2026년 1분기' → '20261'
    - None 또는 숫자 등 비문자열 입력도 안전하게 처리
    - [1-4]분기로 잘못된 분기값(5분기 등) 방어
    """
    if header_str is None:
        return None

    match = re.search(r"(\d{4})년\s*([1-4])분기", str(header_str))
    if match:
        return f"{match.group(1)}{match.group(2)}"
    return None


def to_float(value):
    """임대료 값 변환 — 쉼표·대시 등 비정상 문자 처리"""
    if value is None:
        return None
    text = str(value).replace(",", "").strip()
    if text.upper() in {"", "-", "–", "—", "N/A"}:
        return None
    try:
        return float(text)
    except (ValueError, TypeError):
        return None


def run_rent_stats_pipeline(json_file_path: str = None):
    """
    rent_stats 전체 파이프라인 실행
    - json_file_path: JSON 파일 경로. 없으면 기본 경로 사용
    - API 없음 — JSON 파일 수동 다운로드 후 적재
    - 상권명 → 자치구 수동 매핑 딕셔너리 처리 (AREA_TO_GU)
    """
    if json_file_path is None:
        json_file_path = "/app/data/rent_stats.json"

    if not os.path.exists(json_file_path):
        raise FileNotFoundError(f"JSON 파일을 찾을 수 없습니다: {json_file_path}")

    logger.info(f"임대료 데이터 수집 시작 (파일: {json_file_path})")

    with open(json_file_path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    # JSON 구조 검증
    try:
        data = raw["sheet"]["1"]["data"]
    except (KeyError, TypeError) as exc:
        raise ValueError(
            "JSON 파일 구조가 예상 형식과 다릅니다. sheet → 1 → data 항목을 확인하세요."
        ) from exc

    if not isinstance(data, dict):
        raise ValueError(f"JSON data 형식이 dict가 아닙니다: {type(data).__name__}")

    # 헤더 행에서 분기 코드 추출 (인덱스 0번 행)
    header_row = data.get("0")
    if not isinstance(header_row, dict):
        raise ValueError("JSON 헤더 행인 data['0']을 찾을 수 없습니다.")

    period_codes = {}
    for col_idx in range(4, 11):
        header_value = header_row.get(str(col_idx), "")
        period_code = parse_period_code(header_value)
        if period_code:
            period_codes[str(col_idx)] = period_code

    # 분기 코드가 없으면 실패 처리
    if not period_codes:
        raise ValueError(
            "헤더에서 분기 코드를 찾을 수 없습니다. JSON 파일의 4~10번 컬럼을 확인하세요."
        )

    logger.info(f"  분기 코드: {list(period_codes.values())}")

    # 헤더 행 제외 (0, 1, 2번 행은 헤더)
    skip_rows = {"0", "1", "2"}

    with SyncSessionLocal() as session:
        count = 0
        unmapped_areas = set()

        try:
            for row_key, row in data.items():
                if row_key in skip_rows:
                    continue

                if not isinstance(row, dict):
                    logger.warning(
                        f"dict 형식이 아닌 행 건너뜀: {row_key} ({type(row).__name__})"
                    )
                    continue

                region = str(row.get("1", "")).strip()
                area_name = str(row.get("3", "")).strip()

                # 상권명 없는 행 건너뜀
                if not area_name:
                    logger.warning(f"상권명이 없는 행 건너뜀: {row_key}")
                    continue

                # 서울 데이터만 적재
                if region != "서울":
                    continue

                # 서울 전체 평균 집계 행 제외
                if area_name == "서울":
                    continue

                # 자치구 매핑 (AREA_TO_GU에 없는 상권명은 None으로 저장)
                gu = AREA_TO_GU.get(area_name)
                if area_name not in AREA_TO_GU:
                    unmapped_areas.add(area_name)

                for col_idx, period_code in period_codes.items():
                    avg_rent = to_float(row.get(col_idx))
                    if avg_rent is None:
                        continue

                    stmt = (
                        insert(RentStat)
                        .values(
                            rent_area_name=area_name,
                            gu=gu,
                            period_code=period_code,
                            avg_rent_per_sqm=avg_rent,
                        )
                        .on_conflict_do_update(
                            index_elements=["rent_area_name", "period_code"],
                            set_={
                                "gu": gu,  # AREA_TO_GU 매핑 수정 시 반영
                                "avg_rent_per_sqm": avg_rent,
                            },
                        )
                    )
                    session.execute(stmt)
                    count += 1

            session.commit()

        except Exception:
            session.rollback()
            logger.exception("임대료 데이터 적재 실패")
            raise

        logger.info(f"  UPSERT 처리 완료: {count}건")

    # 미매핑 상권 요약 출력
    if unmapped_areas:
        logger.warning(
            f"자치구 미매핑 상권 {len(unmapped_areas)}개: {sorted(unmapped_areas)}"
        )

    logger.info("임대료 데이터 수집 완료!")


# ── 직접 실행 시 ──────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )
    run_rent_stats_pipeline()
