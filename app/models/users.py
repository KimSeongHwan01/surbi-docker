# app/models/users.py
# users 테이블 모델 정의
# Firebase Auth 연동 회원 테이블
# 로그인 성공 시 firebase_uid 백엔드 전달 → 자동 저장

from sqlalchemy import Column, DateTime, Integer, String, func

from app.db.session import Base


class User(Base):
    """
    회원 테이블
    - Firebase Auth 연동
    - 로그인 성공 시 firebase_uid 백엔드 전달 → 자동 저장
    - UNIQUE: firebase_uid
    """

    __tablename__ = "users"

    id = Column(Integer, primary_key=True, autoincrement=True)
    firebase_uid = Column(String(100), unique=True, nullable=False)  # Firebase Auth UID
    email = Column(String(100), nullable=True)  # Firebase Auth 이메일
    created_at = Column(DateTime, server_default=func.now())  # 최초 가입 시각
