from datetime import datetime
import numpy as np
from bson import ObjectId
from numpy import int64
from pydantic import BaseModel, BaseConfig


class SummaryModel(BaseModel):

    class Config(BaseConfig):
        json_encoders = {
            datetime: lambda dt: dt.isoformat(),
            ObjectId: lambda oid: str(oid),
            int64: lambda num: np.int16(num).item()
        }
