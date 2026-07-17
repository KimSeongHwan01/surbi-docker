# app/models/favorites.py
# favorites 테이블 모델 정의
# 사용자 즐겨찾기 테이블
# 앱에서 버튼 클릭 시 저장
# users + districts 두 테이블을 동시에 FK로 참조

from sqlalchemy import Column, DateTime, ForeignKey, Integer, UniqueConstraint, func
from sqlalchemy.orm import relationship

from app.db.session import Base


class Favorite(Base):
    """
    즐겨찾기 테이블
    - UNIQUE: (user_id, district_id) — 중복 즐겨찾기 방지
    - users + districts 두 테이블 동시 참조
    """

    __tablename__ = "favorites"
    __table_args__ = (
        UniqueConstraint("user_id", "district_id"),  # 같은 행정동 중복 즐겨찾기 방지
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)  # FK → users
    district_id = Column(
        Integer, ForeignKey("districts.id"), nullable=False
    )  # FK → districts
    created_at = Column(DateTime, server_default=func.now())  # 즐겨찾기 등록 시각

    # 관계 정의
    user = relationship("User", backref="favorites")
    district = relationship("District", backref="favorites")
