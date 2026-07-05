import os
import firebase_admin

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi import APIRouter

from firebase_admin import credentials
from firebase_admin import auth as firebase_auth

# .env 파일에서 Firebase 서비스 계정 키 경로를 가져옵니다.
cred_path = os.getenv("FIREBASE_CREDENTIALS_PATH")

# JSON 파일로 인증서 생성
cred = credentials.Certificate(cred_path)

# 인증서로 Firebase 앱 연결 (서버 켜질 때 최초 1회)
firebase_admin.initialize_app(cred)

bearer_scheme = HTTPBearer()

router = APIRouter()

async def get_current_user(
        creds: HTTPAuthorizationCredentials = Depends(bearer_scheme),
):
    token = creds.credentials

    try:
        decoded_token = firebase_auth.verify_id_token(token)
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid Authentication credentials",
        )
    
    return decoded_token

@router.get("/me")
async def read_current_user(current_user: dict = Depends(get_current_user)):
    return {
        "uid": current_user["uid"],
        "email": current_user.get("email"),
    }

