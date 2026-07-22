"""Health / root routes."""
from fastapi import APIRouter

router = APIRouter()


@router.get("/")
async def root():
    return {"service": "keep-automation-consumer", "status": "ok"}


@router.get("/health")
async def health():
    return {"status": "ok"}
