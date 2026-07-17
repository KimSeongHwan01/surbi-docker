# app/models/rent_stats.py
# rent_stats 테이블 모델 정의
# 한국부동산원 소규모상가 분기별 임대료 CSV
# 독립 테이블 — districts FK 없음
# gu 컬럼으로 districts.gu JOIN하여 간접 연결
# 분기마다 수동 다운로드 및 적재

from sqlalchemy import (
    Column,
    DateTime,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
    func,
)

from app.db.session import Base


class RentStat(Base):
    """
    한국부동산원 소규모상가 임대료 테이블
    - 독립 테이블 — districts FK 없음
    - 행정동 단위 아닌 자체 상권명 단위 데이터
    - gu 컬럼으로 구 단위 간접 연결
    - UNIQUE: (rent_area_name, period_code)
    - avg_rent_per_sqm 단위: 천원/㎡ (예: 62.9 = 62,900원/㎡)
    """

    __tablename__ = "rent_stats"
    __table_args__ = (
        UniqueConstraint("rent_area_name", "period_code"),  # 중복 적재 방지
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    rent_area_name = Column(
        String(100), nullable=False
    )  # 한국부동산원 자체 상권명 (CSV 지역3)
    gu = Column(String(50), nullable=True)  # 자치구명. 상권명→구 수동 매핑
    period_code = Column(String(10), nullable=False)  # 기준 분기. 예: 20261
    avg_rent_per_sqm = Column(
        Numeric(8, 2), nullable=True
    )  # ㎡당 평균 임대료 (단위: 천원/㎡)
    recorded_at = Column(DateTime, server_default=func.now())  # DB 적재 시각
