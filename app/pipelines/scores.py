# app/pipelines/scores.py
# ML 창업 점수 CSV → scores 테이블 적재 파이프라인
# 실행: python -m app.pipelines.scores
# 또는: python -m app.pipelines.scores --path /app/data/surbi_final_scores.csv
#
# CSV 컬럼 구조 (ML 팀 제공):
#   commercial_area_code: ML 전용 상권 코드 — DB 적재 시 무시
#   district_code: 행정동 코드 (8자리) → districts 테이블에서 district_id로 변환
#   category_code: CS코드 체계 (sales_stats와 동일)
#   score: 종합 창업 점수 (0~100)
#   expected_sales: 예상 월 매출 (원)
#   closure_risk: 폐업 위험도 (0~100)
#
# UPSERT 기준: (district_id, category_code) 조합
# → 동일 행정동 + 업종 조합 재적재 시 기존 값 덮어씀

import csv
import logging
import os

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert

from app.db.session import SyncSessionLocal
from app.models.scores import Score

logger = logging.getLogger(__name__)

CSV_FILE_PATH = os.getenv("SCORES_CSV_PATH", "/app/data/surbi_final_scores.csv")
BATCH_SIZE = 500  # scores Batch UPSERT 단위 (컬럼 5개 → 500건 = 2,500개 파라미터, PostgreSQL 최대 32,767개 제한 대응)


def run_scores_pipeline(csv_file_path: str = CSV_FILE_PATH):
    """
    ML 결과 CSV → scores 테이블 UPSERT 적재
    - commercial_area_code 컬럼 무시 (ML 전용)
    - district_code → district_id 일괄 변환 (IN 쿼리 1회)
    - 미매핑 행정동 코드는 건너뜀 (로그 출력)
    """
    if not os.path.exists(csv_file_path):
        raise FileNotFoundError(f"scores CSV 파일이 없습니다: {csv_file_path}")

    logger.info(f"ML 창업 점수 적재 시작 (파일: {csv_file_path})")

    # CSV 전체 읽기
    with open(csv_file_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    logger.info(f"  CSV 총 {len(rows):,}건")

    with SyncSessionLocal() as session:
        try:
            # district_code → district_id 일괄 조회 (DB 왕복 최소화)
            district_codes = {str(row["district_code"]).strip() for row in rows}
            result = session.execute(
                text(
                    "SELECT district_code, id FROM districts "
                    "WHERE district_code = ANY(:codes)"
                ),
                {"codes": list(district_codes)},
            )
            district_map = {code: did for code, did in result.all()}

            missing_codes = district_codes - district_map.keys()
            if missing_codes:
                logger.info(
                    f"  district 미매핑 코드 {len(missing_codes)}개 건너뜀: "
                    f"{sorted(missing_codes)}"
                )

            # 적재 대상 추출 (복합키 중복 제거)
            values_by_key = {}
            for row in rows:
                district_code = str(row["district_code"]).strip()
                district_id = district_map.get(district_code)
                if district_id is None:
                    continue

                key = (district_id, str(row["category_code"]).strip())
                values_by_key[key] = {
                    "district_id": district_id,
                    "category_code": str(row["category_code"]).strip(),
                    "score": float(row["score"]),
                    "expected_sales": int(float(row["expected_sales"])),
                    "closure_risk": float(row["closure_risk"]),
                    "foot_traffic_index": float(row["foot_traffic_index"]),
                    "category_spending_power": float(row["category_spending_power"]),
                    "competition_intensity": float(row["competition_intensity"]),
                    "accessibility": float(row["accessibility"]),
                    "operation_stability": float(row["operation_stability"]),
                }

            values_list = list(values_by_key.values())

            # Batch UPSERT
            # PostgreSQL 최대 파라미터 수 제한(32,767개)으로 BATCH_SIZE 단위로 나눠서 처리
            # 점수 1건당 컬럼 5개 → 500건 = 2,500개 파라미터로 여유있게 처리
            if values_list:
                for i in range(0, len(values_list), BATCH_SIZE):
                    batch = values_list[i : i + BATCH_SIZE]
                    stmt = insert(Score).values(batch)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=["district_id", "category_code"],
                        set_={
                            "score": stmt.excluded.score,
                            "expected_sales": stmt.excluded.expected_sales,
                            "closure_risk": stmt.excluded.closure_risk,
                            "foot_traffic_index": stmt.excluded.foot_traffic_index,
                            "category_spending_power": stmt.excluded.category_spending_power,
                            "competition_intensity": stmt.excluded.competition_intensity,
                            "accessibility": stmt.excluded.accessibility,
                            "operation_stability": stmt.excluded.operation_stability,
                            "recorded_at": stmt.excluded.recorded_at,
                        },
                    )
                    session.execute(stmt)

            session.commit()
            logger.info(
                f"  적재 완료: {len(values_list):,}건 / "
                f"미매핑 건너뜀: {len(rows) - len(values_list):,}건"
            )
            logger.info("ML 창업 점수 적재 완료!")

        except Exception:
            session.rollback()
            logger.exception("ML 창업 점수 적재 실패")
            raise


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )
    logging.getLogger("sqlalchemy").setLevel(logging.WARNING)
    run_scores_pipeline()
