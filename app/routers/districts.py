from fastapi import APIRouter

router = APIRouter(prefix="/districts", tags=["districts"])

MOCK_DISTRICTS = [
    {"district_code": "11440510", "district_name": "합정동", "gu": "마포구", "si": "서울특별시"},
    {"district_code": "11440590", "district_name": "연남동", "gu": "마포구", "si": "서울특별시"},
]

@router.get("")
async def get_districts():
    return MOCK_DISTRICTS