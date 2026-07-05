# app/models/sales_stats.py
# sales_stats 테이블 모델 정의
# 서울시 상권분석서비스 추정매출(행정동) API
# API 응답 순서 기준으로 컬럼 정렬 (금액 먼저, 건수 나중)
# ⚠️ 모든 금액/건수 컬럼: API float로 내려옴 → 파이프라인에서 int() 캐스팅 후 BIGINT 저장

from sqlalchemy import Column, Integer, String, BigInteger, DateTime, ForeignKey, UniqueConstraint, func
from sqlalchemy.orm import relationship
from app.db.session import Base


class SalesStat(Base):
    """
    서울시 추정매출(행정동) 테이블
    - API 서비스명: VwsmAdstrdSelngW
    - UNIQUE: (district_id, category_code, period_code)
    - category_code 기준 UNIQUE — 업종명 변경에 안전
    - 금액/건수 컬럼 모두 API float → int() 캐스팅 → BIGINT 저장
    """
    __tablename__ = "sales_stats"
    __table_args__ = (
        UniqueConstraint("district_id", "category_code", "period_code"),  # 중복 적재 방지
    )

    id            = Column(Integer, primary_key=True, autoincrement=True)
    district_id   = Column(Integer, ForeignKey("districts.id"), nullable=False)  # FK → districts
    period_code   = Column(String(10), nullable=False)  # 기준년분기 (STDR_YYQU_CD). 예: 20211
    category_code = Column(String(20), nullable=False)  # 서비스업종코드 (SVC_INDUTY_CD). UNIQUE 기준키
    category      = Column(String(50), nullable=False)  # 서비스업종명 (SVC_INDUTY_CD_NM)

    # 매출 금액 (API 응답 순서: 당월→주중/주말→요일→시간대→성별→연령대)
    monthly_sales      = Column(BigInteger, nullable=True)  # 당월매출금액 (THSMON_SELNG_AMT)
    monthly_sales_count= Column(BigInteger, nullable=True)  # 당월매출건수 (THSMON_SELNG_CO)
    weekday_sales      = Column(BigInteger, nullable=True)  # 주중매출금액 (MDWK_SELNG_AMT)
    weekend_sales      = Column(BigInteger, nullable=True)  # 주말매출금액 (WKEND_SELNG_AMT)

    # 요일별 매출금액
    mon_sales     = Column(BigInteger, nullable=True)  # 월요일 (MON_SELNG_AMT)
    tue_sales     = Column(BigInteger, nullable=True)  # 화요일 (TUES_SELNG_AMT)
    wed_sales     = Column(BigInteger, nullable=True)  # 수요일 (WED_SELNG_AMT)
    thu_sales     = Column(BigInteger, nullable=True)  # 목요일 (THUR_SELNG_AMT)
    fri_sales     = Column(BigInteger, nullable=True)  # 금요일 (FRI_SELNG_AMT)
    sat_sales     = Column(BigInteger, nullable=True)  # 토요일 (SAT_SELNG_AMT)
    sun_sales     = Column(BigInteger, nullable=True)  # 일요일 (SUN_SELNG_AMT)

    # 시간대별 매출금액
    tmzon_00_06_sales = Column(BigInteger, nullable=True)  # 00~06시 (TMZON_00_06_SELNG_AMT)
    tmzon_06_11_sales = Column(BigInteger, nullable=True)  # 06~11시 (TMZON_06_11_SELNG_AMT)
    tmzon_11_14_sales = Column(BigInteger, nullable=True)  # 11~14시 (TMZON_11_14_SELNG_AMT)
    tmzon_14_17_sales = Column(BigInteger, nullable=True)  # 14~17시 (TMZON_14_17_SELNG_AMT)
    tmzon_17_21_sales = Column(BigInteger, nullable=True)  # 17~21시 (TMZON_17_21_SELNG_AMT)
    tmzon_21_24_sales = Column(BigInteger, nullable=True)  # 21~24시 (TMZON_21_24_SELNG_AMT)

    # 성별 매출금액
    male_sales    = Column(BigInteger, nullable=True)  # 남성 (ML_SELNG_AMT)
    female_sales  = Column(BigInteger, nullable=True)  # 여성 (FML_SELNG_AMT)

    # 연령대별 매출금액
    age10_sales   = Column(BigInteger, nullable=True)  # 10대 (AGRDE_10_SELNG_AMT)
    age20_sales   = Column(BigInteger, nullable=True)  # 20대 (AGRDE_20_SELNG_AMT)
    age30_sales   = Column(BigInteger, nullable=True)  # 30대 (AGRDE_30_SELNG_AMT)
    age40_sales   = Column(BigInteger, nullable=True)  # 40대 (AGRDE_40_SELNG_AMT)
    age50_sales   = Column(BigInteger, nullable=True)  # 50대 (AGRDE_50_SELNG_AMT)
    age60_sales   = Column(BigInteger, nullable=True)  # 60대이상 (AGRDE_60_ABOVE_SELNG_AMT)

    # 매출 건수 (API 응답 순서: 주중/주말→요일→시간대→성별→연령대)
    weekday_sales_count = Column(BigInteger, nullable=True)  # 주중 (MDWK_SELNG_CO)
    weekend_sales_count = Column(BigInteger, nullable=True)  # 주말 (WKEND_SELNG_CO)

    # 요일별 매출건수
    mon_sales_count  = Column(BigInteger, nullable=True)  # 월요일 (MON_SELNG_CO)
    tue_sales_count  = Column(BigInteger, nullable=True)  # 화요일 (TUES_SELNG_CO)
    wed_sales_count  = Column(BigInteger, nullable=True)  # 수요일 (WED_SELNG_CO)
    thu_sales_count  = Column(BigInteger, nullable=True)  # 목요일 (THUR_SELNG_CO)
    fri_sales_count  = Column(BigInteger, nullable=True)  # 금요일 (FRI_SELNG_CO)
    sat_sales_count  = Column(BigInteger, nullable=True)  # 토요일 (SAT_SELNG_CO)
    sun_sales_count  = Column(BigInteger, nullable=True)  # 일요일 (SUN_SELNG_CO)

    # 시간대별 매출건수
    tmzon_00_06_count = Column(BigInteger, nullable=True)  # 00~06시 (TMZON_00_06_SELNG_CO)
    tmzon_06_11_count = Column(BigInteger, nullable=True)  # 06~11시 (TMZON_06_11_SELNG_CO)
    tmzon_11_14_count = Column(BigInteger, nullable=True)  # 11~14시 (TMZON_11_14_SELNG_CO)
    tmzon_14_17_count = Column(BigInteger, nullable=True)  # 14~17시 (TMZON_14_17_SELNG_CO)
    tmzon_17_21_count = Column(BigInteger, nullable=True)  # 17~21시 (TMZON_17_21_SELNG_CO)
    tmzon_21_24_count = Column(BigInteger, nullable=True)  # 21~24시 (TMZON_21_24_SELNG_CO)

    # 성별 매출건수
    male_sales_count   = Column(BigInteger, nullable=True)  # 남성 (ML_SELNG_CO)
    female_sales_count = Column(BigInteger, nullable=True)  # 여성 (FML_SELNG_CO)

    # 연령대별 매출건수
    age10_sales_count  = Column(BigInteger, nullable=True)  # 10대 (AGRDE_10_SELNG_CO)
    age20_sales_count  = Column(BigInteger, nullable=True)  # 20대 (AGRDE_20_SELNG_CO)
    age30_sales_count  = Column(BigInteger, nullable=True)  # 30대 (AGRDE_30_SELNG_CO)
    age40_sales_count  = Column(BigInteger, nullable=True)  # 40대 (AGRDE_40_SELNG_CO)
    age50_sales_count  = Column(BigInteger, nullable=True)  # 50대 (AGRDE_50_SELNG_CO)
    age60_sales_count  = Column(BigInteger, nullable=True)  # 60대이상 (AGRDE_60_ABOVE_SELNG_CO)

    recorded_at = Column(DateTime, server_default=func.now())  # DB 적재 시각

    # 관계 정의
    district = relationship("District", backref="sales_stats")