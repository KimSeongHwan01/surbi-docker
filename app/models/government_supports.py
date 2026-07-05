# app/models/government_supports.py
# government_supports 테이블 모델 정의
# 기업마당 지원사업정보 API (bizinfo.go.kr)
# 독립 테이블 — districts FK 없음 (행정동과 무관)
# 수집 시 소상공인/창업벤처 대상 공고만 필터링 권장

from sqlalchemy import Column, Integer, String, Text, Date, DateTime, UniqueConstraint, func
from app.db.session import Base


class GovernmentSupport(Base):
    """
    정부 지원 사업 테이블
    - 독립 테이블 — districts FK 없음
    - 업종/대상 기준 공고 (행정동과 무관)
    - UNIQUE: pblanc_id (공고ID 기준 중복 방지)
    - summary: HTML 태그 제거 후 저장. LLM 보고서 생성 입력값
    - start_date/end_date: reqstBeginEndDe 문자열 파싱
      예: '2026-06-24 ~ 2026-07-15' → start=2026-06-24, end=2026-07-15
    """
    __tablename__ = "government_supports"
    __table_args__ = (
        UniqueConstraint("pblanc_id"),  # 공고ID 기준 중복 방지
    )

    id              = Column(Integer, primary_key=True, autoincrement=True)
    pblanc_id       = Column(String(100), unique=True, nullable=False)  # 공고ID (pblancId)
    title           = Column(String(200), nullable=False)  # 지원사업명 (pblancNm)
    category        = Column(String(50),  nullable=True)   # 지원 분야 (pldirSportRealmLclasCodeNm)
    support_type    = Column(String(50),  nullable=True)   # 지원 유형 (pldirSportRealmMlsfcCodeNm)
    support_target  = Column(String(100), nullable=True)   # 신청 대상 (trgetNm)
    agency          = Column(String(100), nullable=True)   # 실행기관 (excInsttNm)
    jrsd_instt_nm   = Column(String(100), nullable=True)   # 주관기관 (jrsdInsttNm)
    summary         = Column(Text,        nullable=True)   # 사업요약. HTML 제거 후 저장
    sprt_start_date = Column(Date,        nullable=True)   # 신청 시작일. reqstBeginEndDe 앞부분 파싱
    end_date        = Column(Date,        nullable=True)   # 신청 종료일. reqstBeginEndDe 뒷부분 파싱
    support_url     = Column(String(500), nullable=True)   # 공고 상세 링크 (pblancUrl)
    created_at      = Column(DateTime, server_default=func.now())  # DB 적재 시각