import time
from typing import TypedDict

import jwt

from vulquantafs.config import get_config
from vulquantafs.errors import LoginFailed

from .user import AdminUser, ReadonlyUser, User

config = get_config()
jwt_config = config.vulquantafs.jwt


class JWTPayload(TypedDict):
    username: str
    exp: int
    readonly: bool


def login(username: str, password: str) -> str:
    if not config.vulquantafs.users:
        return jwt.encode(
            dict(
                JWTPayload(
                    username="anonymous",
                    exp=int(time.time()) + jwt_config.life,
                    readonly=True,
                )
            ),
            key=jwt_config.secret,
            algorithm=jwt_config.algorithm,
        )
    if not username:
        raise LoginFailed("Anonymous login is disabled.")
    if (user := config.vulquantafs.users.get(username)) and user.password == password:
        return jwt.encode(
            dict(
                JWTPayload(
                    username=username,
                    exp=int(time.time()) + jwt_config.life,
                    readonly=user.readonly,
                )
            ),
            key=jwt_config.secret,
            algorithm=jwt_config.algorithm,
        )
    raise LoginFailed(f"No such user ({username}) or password incorrect.")


def authenticate(token: str) -> User:
    payload = jwt.decode(
        token, key=jwt_config.secret, algorithms=[jwt_config.algorithm]
    )

    username = payload["username"]
    return AdminUser(username) if not payload["readonly"] else ReadonlyUser(username)
