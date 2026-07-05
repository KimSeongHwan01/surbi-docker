from fastapi import APIRouter

router = APIRouter(prefix="/supports", tags=["supports"])

MOCK_SUPPORTS = [
    {"pblanc_id": "S001", "title": "지원사업 1", "support_target": "신청 대상 1", "end_date": "2026-07-05", "support_url": "https://example.com/support1"},
    {"pblanc_id": "S002", "title": "지원사업 2", "support_target": "신청 대상 2", "end_date": "2026-08-15", "support_url": "https://example.com/support2"},
]

@router.get("")
async def get_supports():
    return MOCK_SUPPORTS