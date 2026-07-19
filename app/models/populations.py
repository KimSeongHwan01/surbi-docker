# app/models/populations.py
# populations 테이블 모델 정의
# 서울시 생활인구(내국인) API — SPOP_LOCAL_RESD_DONG
# 모든 수치값이 문자열로 내려옴 → float() 변환 후 DECIMAL 저장

from sqlalchemy import (
    Column,
    Date,
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


class Population(Base):
    """
    서울시 생활인구(내국인) 테이블
    - API 응답이 가로형(Wide) 구조라 그대로 저장
    - 75세 이상 컬럼 없음 (API 확인됨) — 70세 이상이 최고령 구간
    - UNIQUE: (district_id, std_date, time_zone)
    """

    __tablename__ = "populations"
    __table_args__ = (
        UniqueConstraint("district_id", "std_date", "time_zone"),  # 중복 적재 방지
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    district_id = Column(
        Integer, ForeignKey("districts.id"), nullable=False
    )  # FK → districts
    std_date = Column(
        Date, nullable=False
    )  # 기준일자 (STDR_DE_ID). 20260610→2026-06-10
    time_zone = Column(
        String(5), nullable=False
    )  # 시간대구분 (TMZON_PD_SE). 00=해당 시간대
    total_pop = Column(Numeric(12, 4), nullable=True)  # 총생활인구수 (TOT_LVPOP_CO)

    # 남성 연령대별 (5세 단위)
    male_0t9 = Column(Numeric(10, 4), nullable=True)  # MALE_F0T9_LVPOP_CO
    male_10t14 = Column(Numeric(10, 4), nullable=True)  # MALE_F10T14_LVPOP_CO
    male_15t19 = Column(Numeric(10, 4), nullable=True)  # MALE_F15T19_LVPOP_CO
    male_20t24 = Column(Numeric(10, 4), nullable=True)  # MALE_F20T24_LVPOP_CO
    male_25t29 = Column(Numeric(10, 4), nullable=True)  # MALE_F25T29_LVPOP_CO
    male_30t34 = Column(Numeric(10, 4), nullable=True)  # MALE_F30T34_LVPOP_CO
    male_35t39 = Column(Numeric(10, 4), nullable=True)  # MALE_F35T39_LVPOP_CO
    male_40t44 = Column(Numeric(10, 4), nullable=True)  # MALE_F40T44_LVPOP_CO
    male_45t49 = Column(Numeric(10, 4), nullable=True)  # MALE_F45T49_LVPOP_CO
    male_50t54 = Column(Numeric(10, 4), nullable=True)  # MALE_F50T54_LVPOP_CO
    male_55t59 = Column(Numeric(10, 4), nullable=True)  # MALE_F55T59_LVPOP_CO
    male_60t64 = Column(Numeric(10, 4), nullable=True)  # MALE_F60T64_LVPOP_CO
    male_65t69 = Column(Numeric(10, 4), nullable=True)  # MALE_F65T69_LVPOP_CO
    male_70o = Column(
        Numeric(10, 4), nullable=True
    )  # MALE_F70T74_LVPOP_CO (70세 이상 최고령)

    # 여성 연령대별 (5세 단위)
    female_0t9 = Column(Numeric(10, 4), nullable=True)  # FEMALE_F0T9_LVPOP_CO
    female_10t14 = Column(Numeric(10, 4), nullable=True)  # FEMALE_F10T14_LVPOP_CO
    female_15t19 = Column(Numeric(10, 4), nullable=True)  # FEMALE_F15T19_LVPOP_CO
    female_20t24 = Column(Numeric(10, 4), nullable=True)  # FEMALE_F20T24_LVPOP_CO
    female_25t29 = Column(Numeric(10, 4), nullable=True)  # FEMALE_F25T29_LVPOP_CO
    female_30t34 = Column(Numeric(10, 4), nullable=True)  # FEMALE_F30T34_LVPOP_CO
    female_35t39 = Column(Numeric(10, 4), nullable=True)  # FEMALE_F35T39_LVPOP_CO
    female_40t44 = Column(Numeric(10, 4), nullable=True)  # FEMALE_F40T44_LVPOP_CO
    female_45t49 = Column(Numeric(10, 4), nullable=True)  # FEMALE_F45T49_LVPOP_CO
    female_50t54 = Column(Numeric(10, 4), nullable=True)  # FEMALE_F50T54_LVPOP_CO
    female_55t59 = Column(Numeric(10, 4), nullable=True)  # FEMALE_F55T59_LVPOP_CO
    female_60t64 = Column(Numeric(10, 4), nullable=True)  # FEMALE_F60T64_LVPOP_CO
    female_65t69 = Column(Numeric(10, 4), nullable=True)  # FEMALE_F65T69_LVPOP_CO
    female_70o = Column(
        Numeric(10, 4), nullable=True
    )  # FEMALE_F70T74_LVPOP_CO (70세 이상 최고령)

    recorded_at = Column(DateTime, server_default=func.now())  # DB 적재 시각

    # 관계 정의
    district = relationship("District", backref="populations")
