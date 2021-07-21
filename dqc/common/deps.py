import traceback

from fastapi import HTTPException
from starlette import status
from starlette.requests import Request

from dqc.sdk.auth.auth_sdk import validate_token


def get_current_user(request: Request) :
    authorization: str = request.headers.get("Authorization")

    print(authorization)
    if not authorization:
        scheme, param = "", ""
    else:
        scheme, _, param = authorization.partition(" ")
    if not authorization or (scheme.lower() != "bearer" and scheme.lower() != "pat"):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
            headers={"WWW-Authenticate": "Bearer"},
        )
    token = param
    if scheme.lower() == "bearer":
        try:
            user = validate_token(token)
            return user
        except :
            print(traceback.format_exc())
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Not authenticated",
                headers={"WWW-Authenticate": "Bearer"},
            )







