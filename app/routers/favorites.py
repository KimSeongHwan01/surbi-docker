from fastapi import APIRouter, Depends
from pydantic import BaseModel
from app.routers.auth import get_current_user

router = APIRouter(prefix="/favorites", tags=["favorites"])

MOCK_FAVORITES = []

class FavoriteRequest(BaseModel):
    district_code: str

@router.post("")
async def create_favorite(
    payload: FavoriteRequest,
    current_user: dict = Depends(get_current_user),
):
    favorite = {
        "user_id": current_user["uid"],
        "district_code": payload.district_code,
    }
    MOCK_FAVORITES.append(favorite)
    return favorite