"""
Authentication utilities: password hashing (bcrypt) and JWT token management.
"""

import os
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import bcrypt
import jwt
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

JWT_SECRET = os.getenv("JWT_SECRET", "change-me-in-production")
JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")
JWT_EXPIRATION_HOURS = int(os.getenv("JWT_EXPIRATION_HOURS", "24"))


def hash_password(password: str) -> str:
    """Hash a plaintext password using bcrypt."""
    salt = bcrypt.gensalt(rounds=12)
    return bcrypt.hashpw(password.encode("utf-8"), salt).decode("utf-8")


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a plaintext password against a bcrypt hash."""
    return bcrypt.checkpw(
        plain_password.encode("utf-8"),
        hashed_password.encode("utf-8"),
    )


def create_access_token(
    user_id: str,
    email: str,
    expires_delta: Optional[timedelta] = None,
) -> str:
    """Create a JWT access token."""
    now = datetime.now(timezone.utc)
    expire = now + (expires_delta or timedelta(hours=JWT_EXPIRATION_HOURS))

    payload = {
        "sub": user_id,
        "email": email,
        "iat": now,
        "exp": expire,
    }

    token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)
    return token


def decode_token(token: str) -> dict:
    """
    Decode and validate a JWT token.
    Returns the payload dict on success.
    Raises jwt.ExpiredSignatureError or jwt.InvalidTokenError on failure.
    """
    payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    return payload
