# app/models/districts.py
# districts 테이블 모델 정의
# 소상공인 API에서 추출한 행정동 정보를 저장하는 테이블
# 모든 테이블의 FK 기준점 (허브 테이블)

from geoalchemy2 import Geometry
from sqlalchemy import Column, DateTime, Integer, String, func

from app.db.session import Base


class District(Base):
    """
    행정동 기준 허브 테이블
    - 소상공인 API 25개 구 반복 호출로 약 424개 행정동 적재
    - 모든 테이블이 district_id(FK)로 이 테이블을 참조
    """

    __tablename__ = "districts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    district_code = Column(
        String(10), unique=True, nullable=False
    )  # 행정동 코드 8자리 (adongCd)
    district_name = Column(String(30), nullable=False)  # 행정동명 (adongNm)
    gu_code = Column(String(10), nullable=False)  # 시군구 코드 (signguCd)
    gu = Column(String(30), nullable=False)  # 구명 (signguNm)
    si_code = Column(String(10), nullable=False)  # 시도 코드 (ctprvnCd). 항상 '11'
    si = Column(String(30), nullable=False)  # 시도명 (ctprvnNm). 항상 '서울특별시'
    geom = Column(Geometry, nullable=True)  # MVP 제외. 히트맵 기능 추가 시 SHP 적재
    created_at = Column(DateTime, server_default=func.now())  # DB 자동 생성
