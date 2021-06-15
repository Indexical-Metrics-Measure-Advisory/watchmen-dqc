from fastapi import APIRouter

router = APIRouter()


@router.get("/health", tags=["common"])
async def health():
    return {"health": True}
