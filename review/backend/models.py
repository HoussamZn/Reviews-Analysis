from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime

class Review(BaseModel):
    id: Optional[str] = Field(None, alias="_id")
    reviewerID : str
    asin: str
    reviewerName: str
    reviewText : str
    prediction : str
    processing_time : datetime
    unixReviewTime :str