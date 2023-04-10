from pydantic import BaseModel, Field
from typing import Union, Optional, List

class Command(BaseModel):
    Name:str
    ScriptLocation: str
    
class Glue_job(BaseModel):
    Name : str = Field(min_length=1, max_length=255)
    Role: str
    Command: Command
    
class SuccessResponse(BaseModel):
    success: bool = True
    status:int = 200
    data: Union[None, dict, list] = None

class ErrorResponse(BaseModel):
    success: bool = False
    status:int = 404
    message: Union[None, dict, list] = {"message":"Something wrong"}

class ExceptionResponse(BaseModel):
    success: bool = False
    status:int = 404
    message: Union[None, dict, list] = {"message":"Unhandled Exception"}