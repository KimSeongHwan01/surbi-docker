from fastapi import APIRouter

router = APIRouter(prefix="/districts", tags=["scores"])

MOCK_SCORES = [
    {"district_code": "11440510", "category": "cat1", "total_score": 85, "score_reason": "점수에 대한 근거1..."},
    {"district_code": "11440590", "category": "cat2", "total_score": 90, "score_reason": "점수에 대한 근거2..."},
]

@router.get("/{district_code}/score")
async def get_scores_by_district(district_code: str):
    district_scores = [score for score in MOCK_SCORES
                       if score["district_code"] == district_code]
    return district_scores