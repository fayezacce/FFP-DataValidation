import pytest
from datetime import timedelta
from app.auth import hash_password, verify_password, create_access_token, SECRET_KEY, ALGORITHM
from jose import jwt

def test_password_hashing():
    password = "secure_password"
    hashed = hash_password(password)
    assert hashed != password
    assert verify_password(password, hashed) is True
    assert verify_password("wrong_password", hashed) is False

def test_create_access_token():
    data = {"sub": "testuser"}
    token = create_access_token(data)
    assert token is not None
    
    payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    assert payload.get("sub") == "testuser"
    assert "exp" in payload

def test_token_expiration():
    data = {"sub": "testuser"}
    # Token that expires in the past
    expires_delta = timedelta(minutes=-1)
    token = create_access_token(data, expires_delta=expires_delta)
    
    with pytest.raises(Exception): # jwt.ExpiredSignatureError or similar
        jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
