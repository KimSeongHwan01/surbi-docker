# app/models/market_trends.py
# market_trends 테이블 모델 정의
# 서울시 상권분석서비스 상권변화지표(행정동) API
# 상권 등급: HH=정체, HL=상권축소, LH=상권확장, LL=다이나믹

from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, UniqueConstraint, func
from sqlalchemy.orm import relationship
from app.db.session import Base


class MarketTrend(Base):
    """
    서울시 상권변화지표(행정동) 테이블
    - API 서비스명: VwsmAdstrdIxQq
    - UNIQUE: (district_id, period_code)
    - su_opr/cls_sale_mt_avrg: 서울시 전체 평균 — 상대 비교용
    """
    __tablename__ = "market_trends"
    __table_args__ = (
        UniqueConstraint("district_id", "period_code"),  # 중복 적재 방지
    )

    id                  = Column(Integer, primary_key=True, autoincrement=True)
    district_id         = Column(Integer, ForeignKey("districts.id"), nullable=False)  # FK → districts
    period_code         = Column(String(10), nullable=False)  # 기준년분기 (STDR_YYQU_CD)

    # 상권변화지표
    trend_grade         = Column(String(10), nullable=True)  # 상권변화지표 코드 (TRDAR_CHNGE_IX). HH/HL/LH/LL
    trend_grade_nm      = Column(String(10), nullable=True)  # 상권변화지표명 (TRDAR_CHNGE_IX_NM)

    # 행정동 평균 영업/폐업 기간
    opr_sale_mt_avrg    = Column(Integer, nullable=True)  # 행정동 평균 영업기간 개월 (OPR_SALE_MT_AVRG)
    cls_sale_mt_avrg    = Column(Integer, nullable=True)  # 행정동 평균 폐업기간 개월 (CLS_SALE_MT_AVRG)

    # 서울시 전체 평균 (상대 비교용)
    su_opr_sale_mt_avrg = Column(Integer, nullable=True)  # 서울시 전체 평균 영업기간 (SU_OPR_SALE_MT_AVRG)
    su_cls_sale_mt_avrg = Column(Integer, nullable=True)  # 서울시 전체 평균 폐업기간 (SU_CLS_SALE_MT_AVRG)

    recorded_at         = Column(DateTime, server_default=func.now())  # DB 적재 시각

    # 관계 정의
    district = relationship("District", backref="market_trends")