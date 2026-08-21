# app/pipelines/subway_geocode.py
# 지하철역 좌표 수집 + 행정동 매핑 파이프라인
# 카카오 로컬 API (키워드 검색) — https://dapi.kakao.com/v2/local/search/keyword.json
#
# 서울시 승하차 API(CardSubwayStatsNew)는 역 이름만 주고 좌표를 주지 않는다.
# 그래서 subway_stats.lat/lng 와 district_id 가 비어 있는 상태로 쌓인다.
# 이 파이프라인이 그 두 칸을 채운다.
#
# 처리 흐름:
# Step 1: 좌표 없는 역 목록 조회 (line_name, station_name DISTINCT)
# Step 2: 역명 정제 후 카카오 키워드 검색 → 좌표 획득
# Step 3: subway_stats.lat/lng UPDATE (같은 역의 모든 날짜 행에 반영)
# Step 4: districts.geom 과 ST_Contains 로 district_id 매핑
#
# 실행 빈도: 신규 역 개통 시에만. 정기 스케줄 대상 아님.

import argparse
import logging
import os
import re
import time

import httpx
from sqlalchemy import text

from app.db.session import SyncSessionLocal

# ── 상수 정의 ────────────────────────────────────────────────
# 카카오 REST API 키 (JavaScript 키를 넣으면 401)
API_KEY = os.getenv("KAKAO_API_KEY")

BASE_URL = "https://dapi.kakao.com/v2/local/search/keyword.json"

# 카카오 카테고리 그룹 코드. SW8 = 지하철역
# 지정하지 않으면 '왕십리역 스타벅스' 같은 주변 상호가 섞여 나온다.
CATEGORY_SUBWAY = "SW8"

# 호출 간 간격(초). 카카오 로컬 API 초당 요청 제한 대응
REQUEST_INTERVAL = 0.1

# ── 로거 설정 ─────────────────────────────────────────────────
logger = logging.getLogger(__name__)


# ── 역명 정제 함수 ────────────────────────────────────────────
def build_queries(station_name: str, line_name: str | None = None) -> list[str]:
    """
    검색어 후보를 우선순위 순으로 만든다

    서울시 API 의 역명은 표기가 제각각이다.
        '왕십리(성동구청)'  → 괄호 안은 부역명
        '총신대입구(이수)'  → 괄호 안이 실제 통용 명칭인 경우도 있음
        '이수'             → '역' 접미사 없음

    그래서 한 번에 못 찾을 것을 전제로 후보를 여러 개 만들어 순서대로 시도한다.
    """
    base = station_name.strip()
    # 괄호 앞부분 (주역명)
    main = re.sub(r"\s*\(.*?\)\s*", "", base).strip()
    # 괄호 안부분 (부역명)
    sub_match = re.search(r"\((.*?)\)", base)
    sub = sub_match.group(1).strip() if sub_match else None

    candidates = []

    def add(name: str, with_line: bool = False) -> None:
        if not name:
            return
        # '역'으로 끝나지 않으면 붙여준다. 붙어야 검색 정확도가 크게 오른다.
        q = name if name.endswith("역") else f"{name}역"
        if with_line and line_name:
            q = f"{q} {line_name}"
        if q not in candidates:
            candidates.append(q)

    # 환승역은 호선을 같이 넣어야 해당 호선 승강장 좌표가 나온다
    add(main, with_line=True)
    add(main)
    add(sub)
    add(base)
    return candidates


# ── 카카오 API 호출 함수 ──────────────────────────────────────
def search_station(client: httpx.Client, query: str, max_retries: int = 3):
    """
    카카오 키워드 검색 1회 호출
    - 429(쿼터/속도 초과) 및 5xx 재시도
    - 결과 없으면 None 반환 (예외 아님)
    """
    params = {"query": query, "category_group_code": CATEGORY_SUBWAY, "size": 1}
    retryable = {429, 500, 502, 503, 504}

    for attempt in range(1, max_retries + 1):
        try:
            response = client.get(BASE_URL, params=params)
            if response.status_code == 401:
                raise RuntimeError(
                    "카카오 API 401 — KAKAO_API_KEY가 REST API 키가 맞는지 확인하세요. "
                    "(JavaScript 키/네이티브 앱 키는 서버 호출에 사용할 수 없습니다)"
                )
            response.raise_for_status()
            documents = response.json().get("documents", [])
            return documents[0] if documents else None

        except httpx.HTTPStatusError as e:
            if e.response.status_code not in retryable or attempt == max_retries:
                raise
            logger.warning(f"  '{query}' HTTP {e.response.status_code} — 3초 후 재시도 ({attempt}/{max_retries})")
        except (httpx.TimeoutException, httpx.RequestError) as e:
            if attempt == max_retries:
                raise
            logger.warning(f"  '{query}' 네트워크 오류: {e} — 3초 후 재시도 ({attempt}/{max_retries})")
        time.sleep(3)

    raise RuntimeError(f"'{query}' 재시도 한도를 초과했습니다.")


def normalize_name(name: str) -> str:
    """
    역명 비교용 정규화
    - '왕십리(성동구청)'  → '왕십리'   (괄호 부역명 제거)
    - '불광역 3호선'      → '불광'     (카카오 place_name은 '역명 호선' 형태)
    - '4.19민주묘지역'    → '419민주묘지' (기호 제거)
    """
    n = re.sub(r"\s*\(.*?\)\s*", "", name).strip()
    parts = n.split()
    if parts:
        n = parts[0]
    n = re.sub(r"역$", "", n)
    return re.sub(r"[^0-9A-Za-z가-힣]", "", n)


def is_same_station(station_name: str, place_name: str) -> bool:
    """
    검색 결과가 실제로 그 역인지 확인

    카카오 검색은 결과가 없으면 인근의 다른 역을 1순위로 돌려준다.
    실측 사례: '디지털미디어시티' → '수색역', '보라매병원' → '당곡역'.
    이름을 대조하지 않으면 엉뚱한 좌표가 그대로 들어간다.
    """
    return normalize_name(station_name) == normalize_name(place_name)


def geocode_station(client: httpx.Client, station_name: str, line_name: str):
    """
    후보 검색어를 순서대로 시도해 '이름이 일치하는' 첫 결과를 반환
    - 반환: (lng, lat, 매칭된_장소명, 사용된_검색어) 또는 None
    - 카카오 응답의 x가 경도(lng), y가 위도(lat). 순서가 헷갈리기 쉬운 지점이다.
    - 부역명으로 검색한 경우(예: '이수'로 '총신대입구'를 찾는 경우)는
      이름이 달라도 정상이므로, 검색어 기준으로도 한 번 더 대조한다.
    """
    for query in build_queries(station_name, line_name):
        doc = search_station(client, query)
        time.sleep(REQUEST_INTERVAL)
        if not doc:
            continue

        place_name = doc.get("place_name", "")
        # 원래 역명과 일치하거나, 사용한 검색어와 일치하면 채택
        if is_same_station(station_name, place_name) or is_same_station(query, place_name):
            return float(doc["x"]), float(doc["y"]), place_name, query

        logger.debug(f"    '{query}' → '{place_name}' 이름 불일치, 다음 후보 시도")
    return None


# ── 대상 조회 함수 ────────────────────────────────────────────
def get_pending_stations(
    session, limit: int | None = None, force: bool = False
) -> list[tuple[str, str]]:
    """
    처리 대상 역 목록을 (호선, 역명) 중복 없이 조회
    - 기본: 좌표가 없는 역만
    - force=True: 이미 좌표가 있는 역도 다시 조회해 검증/교정한다
    """
    where = "" if force else "WHERE lat IS NULL OR lng IS NULL"
    sql = f"""
        SELECT DISTINCT line_name, station_name
        FROM subway_stats
        {where}
        ORDER BY line_name, station_name
    """
    if limit:
        sql += f" LIMIT {int(limit)}"
    return [(row[0], row[1]) for row in session.execute(text(sql)).all()]


# ── 좌표 반영 함수 ────────────────────────────────────────────
def update_coordinates(session, line_name: str, station_name: str, lng: float, lat: float) -> int:
    """
    해당 역의 모든 행에 좌표를 반영
    - subway_stats는 (호선, 역명, 이용일자) 단위라 같은 역이 날짜 수만큼 존재한다
    """
    result = session.execute(
        text("""
            UPDATE subway_stats
            SET lat = :lat,
                lng = :lng,
                -- 좌표가 바뀌면 소속 행정동도 달라질 수 있으므로 매핑을 초기화한다.
                -- 뒤이어 map_districts()가 다시 채운다.
                district_id = CASE
                    WHEN lat IS DISTINCT FROM :lat OR lng IS DISTINCT FROM :lng
                    THEN NULL ELSE district_id
                END
            WHERE line_name = :line_name AND station_name = :station_name;
        """),
        {"lat": lat, "lng": lng, "line_name": line_name, "station_name": station_name},
    )
    return result.rowcount


# ── 행정동 매핑 함수 ──────────────────────────────────────────
def map_districts(session) -> None:
    """
    좌표를 행정동 경계 안에 넣어보고 district_id 를 채운다
    - districts.geom 이 적재되어 있어야 한다 (app.pipelines.dong_geom)
    - 서울 밖 역(1호선 경기 구간 등)은 매칭되지 않고 NULL 로 남는다. 정상이다.
    """
    has_geom = session.execute(
        text("SELECT count(geom) FROM districts;")
    ).scalar()
    if not has_geom:
        logger.warning("districts.geom 이 비어 있어 행정동 매핑을 건너뜁니다. dong_geom 파이프라인을 먼저 실행하세요.")
        return

    result = session.execute(
        text("""
            UPDATE subway_stats s
            SET district_id = d.id
            FROM districts d
            WHERE s.district_id IS NULL
              AND s.lat IS NOT NULL
              AND s.lng IS NOT NULL
              AND ST_Contains(
                    d.geom,
                    ST_SetSRID(ST_MakePoint(s.lng::float8, s.lat::float8), 4326)
                  );
        """)
    )
    logger.info(f"  행정동 매핑 {result.rowcount}건")

    # 좌표는 있는데 어느 행정동에도 안 들어간 역 = 서울 밖
    outside = session.execute(
        text("""
            SELECT DISTINCT line_name, station_name
            FROM subway_stats
            WHERE lat IS NOT NULL AND district_id IS NULL
            ORDER BY line_name, station_name;
        """)
    ).all()
    if outside:
        logger.info(f"  서울 외 지역 역 {len(outside)}개 (매핑 제외, 정상)")
        for line, station in outside[:10]:
            logger.info(f"    - {line} {station}")
        if len(outside) > 10:
            logger.info(f"    ... 외 {len(outside) - 10}개")


# ── 메인 파이프라인 함수 ──────────────────────────────────────
def run_subway_geocode_pipeline(limit: int | None = None, force: bool = False) -> None:
    """
    지하철역 좌표 수집 + 행정동 매핑 실행
    - limit: 테스트용. 지정하면 그 개수만 처리
    - force: 이미 좌표가 있는 역도 다시 조회해 검증/교정
    - 역 단위로 커밋하므로 중간에 끊겨도 처리한 만큼은 남는다
    """
    if not API_KEY:
        raise RuntimeError("KAKAO_API_KEY 환경변수가 설정되지 않았습니다.")

    logger.info("지하철역 좌표 수집 시작" + (" (전체 재검증)" if force else ""))

    headers = {"Authorization": f"KakaoAK {API_KEY}"}
    success = failed = 0
    not_found: list[str] = []

    with SyncSessionLocal() as session:
        stations = get_pending_stations(session, limit, force)
        if not stations:
            logger.info("좌표가 필요한 역이 없습니다.")
        else:
            logger.info(f"대상 역 {len(stations)}개")

            with httpx.Client(timeout=30, headers=headers) as client:
                for line_name, station_name in stations:
                    try:
                        found = geocode_station(client, station_name, line_name)
                        if found is None:
                            failed += 1
                            not_found.append(f"{line_name} {station_name}")
                            logger.warning(f"  검색 실패: {line_name} {station_name}")
                            continue

                        lng, lat, place_name, used_query = found
                        rows = update_coordinates(session, line_name, station_name, lng, lat)
                        session.commit()
                        success += 1
                        logger.info(
                            f"  {line_name} {station_name} → {place_name} "
                            f"({lat:.5f}, {lng:.5f}) / {rows}행"
                        )
                    except Exception:
                        session.rollback()
                        logger.exception(f"  {line_name} {station_name} 처리 실패")
                        raise

            logger.info(f"좌표 수집 완료 — 성공 {success}건 / 실패 {failed}건")
            if not_found:
                logger.warning("검색 실패 역 목록 (수동 확인 필요):")
                for name in not_found:
                    logger.warning(f"  - {name}")

        # Step 4: 행정동 매핑
        try:
            logger.info("행정동 매핑 시작")
            map_districts(session)
            session.commit()
        except Exception:
            session.rollback()
            logger.exception("행정동 매핑 실패")
            raise

    logger.info("지하철역 좌표 수집 완료!")


# ── 직접 실행 시 ──────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="지하철역 좌표 수집 + 행정동 매핑")
    parser.add_argument("--limit", type=int, help="처리할 역 개수 제한 (테스트용)")
    parser.add_argument(
        "--force",
        action="store_true",
        help="이미 좌표가 있는 역도 다시 조회해 검증/교정",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    run_subway_geocode_pipeline(limit=args.limit, force=args.force)
