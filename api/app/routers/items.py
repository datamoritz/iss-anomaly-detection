from typing import List

from fastapi import APIRouter, HTTPException

from config.items import ITEM_METADATA
from app.schemas import ItemMetadata

router = APIRouter(tags=["items"])


@router.get("/items", response_model=List[ItemMetadata])
def get_items():
    return ITEM_METADATA


@router.get("/items/{item_id}", response_model=ItemMetadata)
def get_item(item_id: str):
    for item in ITEM_METADATA:
        if item["item"] == item_id:
            return item
    raise HTTPException(status_code=404, detail=f"Item '{item_id}' not found")