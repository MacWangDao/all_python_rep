from typing import Optional, MutableMapping, List, Union
from datetime import datetime, timedelta

from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.orm.session import Session
from jose import jwt

from app.models.user import User
from app.core.config import settings
from app.core.security import verify_password

JWTPayloadMapping = MutableMapping[
    str, Union[datetime, bool, str, List[str], List[int]]
]

oauth2_scheme = OAuth2PasswordBearer(tokenUrl=f"/bjhy/login")


def authenticate(
        *,
        username: str,
        password: str,
        db: Session,
) -> Optional[User]:
    user = db.query(User).filter(User.username == username).first()
    if not user:
        return None
    if not verify_password(password, user.userhashed_password):
        return None
    return user


def create_access_token(*, userid: str, username: str, roleid: str, sub: str) -> str:
    return _create_token(
        token_type="access_token",
        lifetime=timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES),
        userid=userid,
        username=username,
        roleid=roleid,
        sub=sub,
    )


def _create_token(
        token_type: str,
        lifetime: timedelta, userid: str, username: str, roleid: str,
        sub: str,
) -> str:
    payload = {}
    expire = datetime.utcnow() + lifetime
    # expire = datetime.utcnow() + timedelta(seconds=60)
    payload["type"] = token_type

    # https://datatracker.ietf.org/doc/html/rfc7519#section-4.1.3
    # The "exp" (expiration time) claim identifies the expiration time on
    # or after which the JWT MUST NOT be accepted for processing
    payload["exp"] = expire

    # The "iat" (issued at) claim identifies the time at which the
    # JWT was issued.
    payload["iat"] = datetime.utcnow()

    # The "sub" (subject) claim identifies the principal that is the
    # subject of the JWT
    payload["userid"] = str(userid)
    payload["username"] = str(username)
    payload["roleid"] = str(roleid)
    payload["sub"] = str(sub)
    return jwt.encode(payload, settings.JWT_SECRET, algorithm=settings.ALGORITHM)
