from typing import Optional, Any

from pydantic import BaseModel


# Shared properties
class Response(BaseModel):
    form1Data: Optional[Any] = None
    form2Data: Optional[Any] = None
    form3Data: Optional[Any] = None
    form4Data: Optional[Any] = None
