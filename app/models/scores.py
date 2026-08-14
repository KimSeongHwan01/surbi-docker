# app/models/scores.py
# scores 테이블 모델 정의
# ML Microservice (XGBoost / Random Forest) 계산 결과 저장
# 입력: district_code + category_code (CS코드 체계)
# 출력: score(종합점수), expected_sales(예상매출), closure_risk(폐업위험도)
# ML 팀 협의 완료: CSV → 파이프라인 적재 방식 확정

from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import relationship

from app.db.session import Base


class Score(Base):
    """
    창업 점수 테이블
    - ML 모델 계산 결과 저장
    - UNIQUE: (district_id, category_code) — 중복 적재 방지
    - category_code: CS코드 체계 (sales_stats 테이블과 동일)
    - district_id + category_code 조합으로 sales_stats, districts JOIN 가능
    - period_code 미사용: ML이 여러 분기 데이터를 종합 학습한 결과값이므로
      특정 분기에 귀속되지 않음
    """

    __tablename__ = "scores"
    __table_args__ = (
        UniqueConstraint(
            "district_id", "category_code", name="uq_scores_district_category"
        ),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    district_id = Column(
        Integer, ForeignKey("districts.id"), nullable=False
    )  # FK → districts

    category_code = Column(String(20), nullable=False)  # CS코드 (예: CS100010)

    # ML 모델 출력값
    score = Column(Numeric(5, 2), nullable=True)  # 종합 창업 점수 (0~100)
    expected_sales = Column(BigInteger, nullable=True)  # 예상 월 매출 (원)
    closure_risk = Column(
        Numeric(5, 2), nullable=True
    )  # 폐업 위험도 (0~100, 높을수록 위험)

    recorded_at = Column(DateTime, server_default=func.now())  # 적재 시각

    # 관계 정의
    district = relationship("District", backref="scores")
