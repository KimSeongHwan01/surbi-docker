# app/pipelines/dong_geom.py
# districts.geom 행정동 경계 적재 파이프라인
# vuski/admdongkor 저장소 (통계청 행정동 경계 GeoJSON)
#
# 다른 파이프라인과 달리 매일/매월 돌릴 필요가 없다.
# 행정동 경계는 개편이 있을 때만 바뀌므로 최초 1회 + 개편 시에만 실행한다.
#
# 적재 흐름:
# Step 1: 저장소에서 최신 ver 폴더 탐색 → GeoJSON 다운로드 (로컬 캐시)
# Step 2: adm_nm 기준 서울특별시 행정동만 추출
# Step 3: 임시 테이블 dong_raw 적재 (ST_MakeValid + ST_Multi 정규화)
# Step 4: 매칭 방식별 성공률 점검 (adm_cd / adm_cd2 / 구+동 이름)
# Step 5: districts.geom UPDATE + GiST 인덱스 생성
#
# 원본 좌표계가 이미 WGS84(EPSG:4326)라 변환 과정이 없다.
# 국토교통부 SHP를 쓸 경우엔 EPSG:5179이므로 변환이 필요하다.

import argparse
import json
import logging
import os
from pathlib import Path

import httpx
from sqlalchemy import text

from app.db.session import SyncSessionLocal

# ── 상수 정의 ────────────────────────────────────────────────
# 행정동 경계 저장소 (인증키 불필요)
REPO_API = "https://api.github.com/repos/vuski/admdongkor/contents"

# 추출 대상 시도. MVP 범위가 서울 한정이라 고정값으로 둔다.
SIDO = "서울특별시"

# 다운로드 캐시 경로. 원본이 30MB 내외라 재실행 시 다시 받지 않는다.
CACHE_DIR = Path(os.getenv("DONG_GEOM_CACHE", "./data/.cache"))

# 임시 테이블 INSERT 단위
BATCH_SIZE = 100

# 적재 허용 매칭률 하한. 이 아래면 적재하지 않고 실패 처리한다.
# 2026-08-07 실측 100%(427/427) 기준으로 여유를 둔 값.
MIN_MATCH_RATE = 0.95

# ── 로거 설정 ─────────────────────────────────────────────────
logger = logging.getLogger(__name__)


# ── 매칭 방식 정의 ────────────────────────────────────────────
# 2026-08-07 실측 결과 (서울 427개 기준):
#   adm_cd    32건 (7.5%)   — 소상공인 adongCd 와 통계청 코드 체계가 다름
#   adm_cd2    0건 (0.0%)   — 행안부 10자리와도 불일치
#   name     418건 (97.9%)  — 구 + 동 이름이 사실상 유일한 연결고리
#   name_norm             — 구분자 차이만 흡수한 보정판
#
# 코드가 안 붙는 이유는 소상공인 API 의 adongCd 가 자체 행정동 코드라
# 통계청/행안부 표준 코드와 체계가 다르기 때문이다. 따라서 이름 매칭이 정본이다.
# 행정동 이름 정규화
#  1) 구분자 제거      : '종로1·2·3·4가동' → '종로1234가동'
#  2) 서수 '제' 제거   : '상일제1동'       → '상일1동'
# '제' 는 숫자 앞에 올 때만 지운다. 그냥 지우면 '제기동'이 '기동'이 되어버린다.
NAME_NORM = (
    "regexp_replace("
    "  regexp_replace({col}, '[·.ㆍ・,\\s]', '', 'g'),"
    "  '제([0-9])', '\\1', 'g'"
    ")"
)

JOIN_CONDITIONS = {
    "adm_cd": "d.district_code = r.adm_cd",  # 통계청 8자리
    "adm_cd2": "d.district_code = r.adm_cd2",  # 행안부 10자리
    "name": "d.district_name = r.dong AND d.gu = r.gu",  # 구 + 동 이름 (완전일치)
    # 구분자 표기 차이 보정: '종로1·2·3·4가동' vs '종로1.2.3.4가동'
    "name_norm": (
        f"{NAME_NORM.format(col='d.district_name')} = {NAME_NORM.format(col='r.dong')}"
        " AND d.gu = r.gu"
    ),
}

# 자동 선택에서 제외할 방식. 코드 매칭은 실측상 신뢰할 수 없다.
AUTO_EXCLUDE = {"adm_cd", "adm_cd2"}


# ── 원본 다운로드 함수 ────────────────────────────────────────
def fetch_latest_geojson(client: httpx.Client) -> dict:
    """
    저장소에서 가장 최신 ver 폴더의 GeoJSON 을 내려받는다
    - 폴더명이 ver20YYMMDD 형태라 문자열 정렬만으로 최신본 선택 가능
    - 동일 버전을 이미 받았으면 캐시 파일을 재사용
    """
    logger.info("최신 경계 버전 확인 중...")
    root = client.get(REPO_API).raise_for_status().json()

    versions = sorted(
        item["name"]
        for item in root
        if item["type"] == "dir" and item["name"].startswith("ver20")
    )
    if not versions:
        raise RuntimeError("저장소에서 ver 폴더를 찾지 못했습니다. 구조가 변경되었을 수 있습니다.")

    latest = versions[-1]
    cache_path = CACHE_DIR / f"{latest}.geojson"

    if cache_path.exists():
        logger.info(f"  캐시 재사용: {cache_path.name}")
        return json.loads(cache_path.read_text(encoding="utf-8"))

    files = client.get(f"{REPO_API}/{latest}").raise_for_status().json()
    target = next((f for f in files if f["name"].endswith(".geojson")), None)
    if target is None:
        raise RuntimeError(f"{latest} 폴더에 geojson 파일이 없습니다.")

    logger.info(f"  다운로드: {latest}/{target['name']} ({target['size'] / 1024 / 1024:.1f}MB)")
    raw = client.get(target["download_url"], timeout=300).raise_for_status().text

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(raw, encoding="utf-8")
    return json.loads(raw)


# ── 서울 추출 함수 ────────────────────────────────────────────
def filter_seoul(geojson: dict) -> list[dict]:
    """
    전국 데이터에서 서울특별시 행정동만 추린다
    - adm_nm 에 '서울특별시 성동구 왕십리도선동' 형태로 전체 경로가 들어있음
    - 공백 분리로 구 이름과 동 이름을 확보 (이름 기반 폴백 매칭에 사용)
    """
    rows = []
    for feature in geojson.get("features", []):
        props = feature.get("properties", {})
        adm_nm = props.get("adm_nm", "")

        if not adm_nm.startswith(SIDO):
            continue

        parts = adm_nm.split()
        rows.append(
            dict(
                adm_cd=props.get("adm_cd"),
                adm_cd2=props.get("adm_cd2"),
                adm_nm=adm_nm,
                gu=parts[1] if len(parts) > 2 else None,
                dong=parts[-1],
                geometry=json.dumps(feature["geometry"], ensure_ascii=False),
            )
        )

    if not rows:
        raise RuntimeError(f"{SIDO} 행정동을 찾지 못했습니다. adm_nm 속성명이 변경되었을 수 있습니다.")

    logger.info(f"  {SIDO} 행정동 {len(rows)}개 추출")
    return rows


# ── 임시 테이블 적재 함수 ─────────────────────────────────────
def stage_raw(session, rows: list[dict]) -> None:
    """
    원본 경계를 임시 테이블 dong_raw 에 적재
    - ST_MakeValid: 자기교차 등 깨진 폴리곤 보정
    - ST_Multi: Polygon / MultiPolygon 혼재를 한 타입으로 통일
      (통일하지 않으면 UPDATE 시 타입 불일치로 실패)
    """
    session.execute(text("CREATE EXTENSION IF NOT EXISTS postgis;"))
    session.execute(text("DROP TABLE IF EXISTS dong_raw;"))
    session.execute(
        text("""
            CREATE TEMP TABLE dong_raw (
                adm_cd  TEXT,
                adm_cd2 TEXT,
                adm_nm  TEXT,
                gu      TEXT,
                dong    TEXT,
                geom    GEOMETRY(MultiPolygon, 4326)
            );
        """)
    )

    insert_sql = text("""
        INSERT INTO dong_raw (adm_cd, adm_cd2, adm_nm, gu, dong, geom)
        VALUES (
            :adm_cd, :adm_cd2, :adm_nm, :gu, :dong,
            ST_Multi(ST_MakeValid(ST_GeomFromGeoJSON(:geometry)))
        );
    """)
    for i in range(0, len(rows), BATCH_SIZE):
        session.execute(insert_sql, rows[i : i + BATCH_SIZE])

    session.execute(text("CREATE INDEX ON dong_raw (adm_cd, adm_cd2, gu, dong);"))
    logger.info(f"  임시 테이블 적재 완료 ({len(rows)}건)")


# ── 매칭률 점검 함수 ──────────────────────────────────────────
def report_match_rate(session) -> tuple[str, float]:
    """
    네 가지 매칭 방식을 모두 시도하고 (성공률이 가장 높은 방식, 그 비율)을 반환
    - 코드 체계(통계청 8자리 / 행안부 10자리)를 미리 알 필요 없이 판별 가능
    - 비율은 자동 실행 시 하한 검사(min_match_rate)에 쓰인다
    """
    total = session.execute(text("SELECT count(*) FROM districts;")).scalar()
    if not total:
        raise RuntimeError("districts 테이블이 비어 있습니다. 상가정보 파이프라인을 먼저 실행하세요.")

    results = {}
    for name, condition in JOIN_CONDITIONS.items():
        matched = session.execute(
            text(f"""
                SELECT count(DISTINCT d.id)
                FROM districts d JOIN dong_raw r ON {condition};
            """)
        ).scalar()
        results[name] = matched
        logger.info(f"  {name:<10} {matched}/{total} ({matched / total * 100:.1f}%)")

    # 코드 매칭은 부분적으로만 맞아서 오히려 위험하다. 이름 기반에서만 고른다.
    candidates = {k: v for k, v in results.items() if k not in AUTO_EXCLUDE}
    best = max(candidates, key=candidates.get)
    if candidates[best] == 0:
        raise RuntimeError("어떤 방식으로도 매칭되지 않았습니다. 원본 속성명을 확인하세요.")

    logger.info(f"  선택된 방식: {best}")
    log_unmatched(session, best)
    return best, candidates[best] / total


# ── 미매칭 진단 함수 ──────────────────────────────────────────
def log_unmatched(session, strategy: str) -> None:
    """
    선택된 방식으로 안 붙는 행정동을 양방향으로 출력
    - DB에만 있는 동: 폐지되었거나 이름 표기가 다른 경우
    - 원본에만 있는 동: 신설되었거나 업소가 없어 수집되지 않은 경우
    """
    db_only = session.execute(
        text(f"""
            SELECT d.district_code, d.gu, d.district_name
            FROM districts d
            WHERE NOT EXISTS (
                SELECT 1 FROM dong_raw r WHERE {JOIN_CONDITIONS[strategy]}
            )
            ORDER BY d.gu, d.district_name;
        """)
    ).all()

    raw_only = session.execute(
        text(f"""
            SELECT r.gu, r.dong
            FROM dong_raw r
            WHERE NOT EXISTS (
                SELECT 1 FROM districts d WHERE {JOIN_CONDITIONS[strategy]}
            )
            ORDER BY r.gu, r.dong;
        """)
    ).all()

    if db_only:
        logger.warning(f"  [DB에만 존재] {len(db_only)}개")
        for code, gu, name in db_only:
            logger.warning(f"    - {code} {gu} {name}")
    if raw_only:
        logger.warning(f"  [경계파일에만 존재] {len(raw_only)}개")
        for gu, dong in raw_only:
            logger.warning(f"    - {gu} {dong}")
    if not db_only and not raw_only:
        logger.info("  양방향 완전 매칭")


# ── geom 적재 함수 ────────────────────────────────────────────
def update_geom(session, strategy: str) -> None:
    """
    districts.geom 갱신 + 공간 인덱스 생성
    - GiST 인덱스가 없으면 ST_Contains / ST_DWithin 이 전부 풀스캔으로 돈다
    """
    result = session.execute(
        text(f"""
            UPDATE districts d
            SET geom = r.geom
            FROM dong_raw r
            WHERE {JOIN_CONDITIONS[strategy]};
        """)
    )
    logger.info(f"  {result.rowcount}개 행정동 경계 적재")

    session.execute(
        text("CREATE INDEX IF NOT EXISTS idx_districts_geom ON districts USING GIST (geom);")
    )

    # 적재 후 누락분 확인 — 코드 개편으로 사라지거나 신설된 동에서 발생한다
    missing = session.execute(
        text("""
            SELECT district_code, district_name, gu
            FROM districts WHERE geom IS NULL
            ORDER BY gu, district_name;
        """)
    ).all()

    if missing:
        logger.warning(f"  경계 미적재 행정동 {len(missing)}개")
        for code, name, gu in missing[:20]:
            logger.warning(f"    - {code} {gu} {name}")
        if len(missing) > 20:
            logger.warning(f"    ... 외 {len(missing) - 20}개")
    else:
        logger.info("  전체 행정동 매칭 완료")


# ── 좌표 범위 검증 함수 ───────────────────────────────────────
def verify_extent(session) -> None:
    """
    적재된 경계가 서울 좌표 범위 안에 있는지 확인
    - 서울은 대략 위도 37.4~37.7 / 경도 126.7~127.2
    - 값이 수십만 단위로 나오면 EPSG:5179 가 그대로 들어간 것이다
    """
    row = session.execute(
        text("""
            SELECT ST_XMin(e), ST_YMin(e), ST_XMax(e), ST_YMax(e)
            FROM (SELECT ST_Extent(geom) AS e FROM districts) t;
        """)
    ).one_or_none()

    if row is None or row[0] is None:
        logger.warning("  적재된 경계가 없어 범위 검증을 건너뜁니다.")
        return

    min_lng, min_lat, max_lng, max_lat = row
    logger.info(f"  좌표 범위: 경도 {min_lng:.4f}~{max_lng:.4f} / 위도 {min_lat:.4f}~{max_lat:.4f}")

    if not (126.0 < min_lng < 128.0 and 37.0 < min_lat < 38.0):
        raise RuntimeError(
            "좌표가 서울 범위를 벗어났습니다. 원본 좌표계가 EPSG:4326이 아닐 수 있습니다."
        )
    logger.info("  좌표계 정상 (EPSG:4326)")


# ── 메인 파이프라인 함수 ──────────────────────────────────────
def run_dong_geom_pipeline(
    apply: bool = False,
    strategy: str | None = None,
    min_match_rate: float = MIN_MATCH_RATE,
) -> None:
    """
    행정동 경계 적재 전체 파이프라인 실행
    - apply=False    : 매칭률만 확인하고 롤백 (기본값)
    - apply=True     : 실제 반영
    - strategy       : 매칭 방식 고정. None이면 성공률 기준 자동 선택
    - min_match_rate : 이 비율 미만이면 적재를 거부하고 실패 처리

    min_match_rate 는 무인 실행(ARQ Worker) 대비용이다.
    행정동 개편으로 이름 규칙이 바뀌면 매칭률이 뚝 떨어질 수 있는데,
    사람이 로그를 보지 않는 상황에서 그대로 적용하면 경계가 대량 누락된다.
    실패로 처리해야 Discord 알림이 나가서 인지할 수 있다.
    """
    logger.info("행정동 경계 적재 시작")

    with httpx.Client(timeout=60, follow_redirects=True) as client:
        rows = filter_seoul(fetch_latest_geojson(client))

    with SyncSessionLocal() as session:
        try:
            stage_raw(session, rows)

            if strategy:
                chosen, rate = strategy, None
                logger.info(f"  매칭 방식 수동 지정: {chosen}")
            else:
                chosen, rate = report_match_rate(session)

            if not apply:
                session.rollback()
                logger.info("확인 모드 — DB는 변경되지 않았습니다. 반영하려면 --apply 옵션을 주세요.")
                return

            # 하한 검사는 자동 선택일 때만. 수동 지정은 사람이 판단한 것으로 본다.
            if rate is not None and rate < min_match_rate:
                session.rollback()
                raise RuntimeError(
                    f"매칭률 {rate * 100:.1f}%가 하한 {min_match_rate * 100:.0f}% 미만이라 "
                    f"적재를 중단했습니다. 행정동 개편으로 이름 규칙이 바뀌었을 수 있습니다. "
                    f"확인 후 --min-match-rate 로 하한을 낮춰 재실행하세요."
                )

            update_geom(session, chosen)
            verify_extent(session)
            session.commit()

        except Exception:
            session.rollback()
            logger.exception("행정동 경계 적재 실패")
            raise

    logger.info("행정동 경계 적재 완료!")


# ── 직접 실행 시 ──────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="행정동 경계(geom) 적재")
    parser.add_argument("--apply", action="store_true", help="실제 DB 반영 (미지정 시 확인만)")
    parser.add_argument(
        "--strategy",
        choices=list(JOIN_CONDITIONS),
        help="매칭 방식 고정 (기본: 성공률 기준 자동 선택)",
    )
    parser.add_argument(
        "--min-match-rate",
        type=float,
        default=MIN_MATCH_RATE,
        help=f"적재 허용 매칭률 하한 (기본: {MIN_MATCH_RATE})",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    run_dong_geom_pipeline(
        apply=args.apply,
        strategy=args.strategy,
        min_match_rate=args.min_match_rate,
    )
