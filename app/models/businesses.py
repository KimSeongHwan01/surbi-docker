# app/models/businesses.py
# businesses 테이블 모델 정의
# 소상공인 상가(상권)정보 API 모든 컬럼 전체 적재
# districts 테이블을 FK로 참조 (district_id)

from sqlalchemy import Column, DateTime, ForeignKey, Integer, Numeric, String, func
from sqlalchemy.orm import relationship

from app.db.session import Base


class Business(Base):
    """
    업소 정보 테이블
    - 소상공인 API 39개 컬럼 전체 적재
    - 월별 수집 시 사라진 업소 → open_status='폐업' UPDATE
    - 빈 문자열('') → NULL 변환 필수
    - API 응답 lon → DB 저장 lng 컬럼명 변환
    """

    __tablename__ = "businesses"

    id = Column(Integer, primary_key=True, autoincrement=True)
    district_id = Column(
        Integer, ForeignKey("districts.id"), nullable=False
    )  # FK → districts

    # 업소 기본 정보
    business_code = Column(
        String(30), unique=True, nullable=False
    )  # 상가업소번호 (bizesId)
    biz_name = Column(String(100), nullable=False)  # 상호명 (bizesNm)
    branch_name = Column(String(100), nullable=True)  # 지점명 (brchNm). 빈값→NULL

    # 업종 분류 (대/중/소)
    category_large_code = Column(
        String(10), nullable=True
    )  # 상권업종대분류코드 (indsLclsCd)
    category_large_name = Column(
        String(30), nullable=True
    )  # 상권업종대분류명 (indsLclsNm)
    category_medium_code = Column(
        String(20), nullable=True
    )  # 상권업종중분류코드 (indsMclsCd)
    category_medium_name = Column(
        String(50), nullable=True
    )  # 상권업종중분류명 (indsMclsNm)
    category_code = Column(String(20), nullable=True)  # 상권업종소분류코드 (indsSclsCd)
    category_name = Column(String(50), nullable=True)  # 상권업종소분류명 (indsSclsNm)

    # 표준산업분류
    ksic_code = Column(String(20), nullable=True)  # 표준산업분류코드 (ksicCd)
    ksic_name = Column(String(50), nullable=True)  # 표준산업분류명 (ksicNm)

    # 법정동 정보
    legal_dong_code = Column(String(20), nullable=True)  # 법정동코드 (ldongCd)
    legal_dong_name = Column(String(30), nullable=True)  # 법정동명 (ldongNm)

    # 지번 정보
    pnu_code = Column(String(30), nullable=True)  # PNU코드 (lnoCd)
    plot_sct_code = Column(
        String(5), nullable=True
    )  # 대지구분코드 (plotSctCd). 1=대지,2=산
    plot_sct_name = Column(String(20), nullable=True)  # 대지구분명 (plotSctNm)
    land_main_no = Column(Integer, nullable=True)  # 지번본번지 (lnoMnno)
    land_sub_no = Column(String(10), nullable=True)  # 지번부번지 (lnoSlno). 빈값→NULL
    land_address = Column(String(200), nullable=True)  # 지번주소 (lnoAdr)

    # 도로명 정보
    road_name_code = Column(String(20), nullable=True)  # 도로명코드 (rdnmCd)
    road_name = Column(String(100), nullable=True)  # 도로명 (rdnm)

    # 건물 정보
    bld_main_no = Column(Integer, nullable=True)  # 건물본번지 (bldMnno)
    bld_sub_no = Column(String(10), nullable=True)  # 건물부번지 (bldSlno). 빈값→NULL
    building_mgmt_no = Column(String(30), nullable=True)  # 건물관리번호 (bldMngNo)
    building_name = Column(String(100), nullable=True)  # 건물명 (bldNm). 빈값→NULL

    # 주소 정보
    road_address = Column(String(200), nullable=True)  # 도로명주소 (rdnmAdr). 메인 주소
    old_zipcode = Column(String(10), nullable=True)  # 구우편번호 (oldZipcd)
    new_zipcode = Column(String(10), nullable=True)  # 신우편번호 (newZipcd)

    # 상세 위치
    dong_no = Column(String(20), nullable=True)  # 동정보 (dongNo). 빈값→NULL
    floor_no = Column(String(10), nullable=True)  # 층정보 (flrNo). B1 등 문자 가능
    unit_no = Column(String(20), nullable=True)  # 호정보 (hoNo). 빈값→NULL

    # 좌표
    lng = Column(Numeric(17, 14), nullable=True)  # 경도 (lon→lng 변환)
    lat = Column(Numeric(17, 14), nullable=True)  # 위도. 좌표 없는 업소→NULL

    # 영업 상태
    open_status = Column(
        String(10), nullable=False, server_default="영업중"
    )  # API 미제공. 월별 수집 시 폐업 UPDATE

    # 시간 정보
    created_at = Column(DateTime, server_default=func.now())  # 최초 적재 시각
    updated_at = Column(
        DateTime, server_default=func.now(), onupdate=func.now()
    )  # 마지막 갱신 시각

    # 관계 정의
    district = relationship("District", backref="businesses")  # districts 테이블과 연결
