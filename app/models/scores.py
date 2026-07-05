# app/models/scores.py
# scores 테이블 모델 정의
# ML Microservice (XGBoost / Random Forest / SHAP) 계산 결과 저장
# ML 팀원 협의 필요: 결과 전달 방식 (POST 요청 / CSV / 직접 INSERT)
# score_reason: LLM 창업 보고서 생성 핵심 입력값

from sqlalchemy import Column, Integer, String, Numeric, Text, DateTime, ForeignKey, UniqueConstraint, func
from sqlalchemy.orm import relationship
from app.db.session import Base


class Score(Base):
    """
    창업 점수 테이블
    - ML 계산 결과 저장
    - UNIQUE: (district_id, category, period_code)
    - 점수 배점: 유동인구(20) + 소비금액(20) + 타겟연령대(15) +
                경쟁업소(15) + AI예상매출(15) + 교통(10) + 임대료(5) = 100점
    """
    __tablename__ = "scores"
    __table_args__ = (
        UniqueConstraint("district_id", "category", "period_code"),  # 중복 적재 방지
    )

    id               = Column(Integer, primary_key=True, autoincrement=True)
    district_id      = Column(Integer, ForeignKey("districts.id"), nullable=False)  # FK → districts
    category         = Column(String(50), nullable=False)   # 업종명
    period_code      = Column(String(10), nullable=False)   # 계산 기준 분기

    # 점수 항목
    total_score      = Column(Numeric(5, 2), nullable=True)  # 종합 점수 0~100
    population_score = Column(Numeric(5, 2), nullable=True)  # 유동인구 점수 (20점 만점)
    sales_score      = Column(Numeric(5, 2), nullable=True)  # 소비금액 점수 (20점 만점)
    age_target_score = Column(Numeric(5, 2), nullable=True)  # 타겟 연령대 점수 (15점 만점)
    competitor_score = Column(Numeric(5, 2), nullable=True)  # 경쟁업소 점수 (15점 만점)
    ml_sales_score   = Column(Numeric(5, 2), nullable=True)  # AI 예상매출 점수 (15점 만점)
    transport_score  = Column(Numeric(5, 2), nullable=True)  # 교통 접근성 점수 (10점 만점)
    rent_score       = Column(Numeric(5, 2), nullable=True)  # 임대 부담도 점수 (5점 만점)

    # ML 모델 정보
    model_version    = Column(String(50), nullable=True)  # ML 모델 버전. 예: v1.0
    score_reason     = Column(Text, nullable=True)        # 점수 산출 근거. LLM 보고서 생성 핵심 입력값

    calculated_at    = Column(DateTime, server_default=func.now())  # 계산 시각

    # 관계 정의
    district = relationship("District", backref="scores")