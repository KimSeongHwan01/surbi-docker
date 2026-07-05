from fastapi import APIRouter

router = APIRouter(prefix="/districts", tags=["businesses"])

MOCK_BUSINESSES = [
    {"business_code": "B001", "biz_name": "biz1", "category_name": "cat1", "road_address": "road1", "district_code": "11440510"},
    {"business_code": "B002", "biz_name": "biz2", "category_name": "cat2", "road_address": "road2", "district_code": "11440590"},
]

@router.get("/{district_code}/businesses")
async def get_businesses_by_district(district_code: str):
    district_businesses = [business for business in MOCK_BUSINESSES
                           if business["district_code"] == district_code]
    return district_businesses