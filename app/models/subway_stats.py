# app/models/subway_stats.py
# subway_stats 테이블 모델 정의
# 서울시 지하철 호선별 역별 승하차 인원 API — CardSubwayStatsNew
# MVP 후순위 — populations로 교통 접근성 대체 가능
# district_id: NULL 허용 — 역 좌표 확보 후 PostGIS ST_Within() 매핑 예정

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


class SubwayStat(Base):
    """
    서울시 지하철 승하차 인원 테이블
    - MVP 후순위 (populations로 교통 접근성 대체 가능)
    - district_id NULL 허용 — 역 좌표 확보 후 PostGIS 매핑 예정
    - UNIQUE: (line_name, station_name, use_date)
    - lat/lng: 카카오 Local API로 별도 수집 후 UPDATE
    - use_date: 실제 이용일자 (USE_YMD). 등록일(REG_YMD) 아님
    - boarding/alighting: API 문자열로 내려옴 → int() 변환 필요
    """

    __tablename__ = "subway_stats"
    __table_args__ = (
        UniqueConstraint("line_name", "station_name", "use_date"),  # 중복 적재 방지
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    district_id = Column(
        Integer, ForeignKey("districts.id"), nullable=True
    )  # NULL 허용
    line_name = Column(String(20), nullable=False)  # 호선명 (SBWY_ROUT_LN_NM)
    station_name = Column(String(50), nullable=False)  # 역명 (SBWY_STNS_NM)
    lat = Column(Numeric(14, 10), nullable=True)  # 역 위도. 카카오 API 별도 수집
    lng = Column(Numeric(14, 10), nullable=True)  # 역 경도. 카카오 API 별도 수집
    use_date = Column(Date, nullable=False)  # 실제 이용일자 (USE_YMD)
    boarding = Column(Integer, nullable=True)  # 승차 인원 (GTON_TNOPE)
    alighting = Column(Integer, nullable=True)  # 하차 인원 (GTOFF_TNOPE)
    recorded_at = Column(DateTime, server_default=func.now())  # DB 적재 시각

    # 관계 정의
    district = relationship("District", backref="subway_stats")
