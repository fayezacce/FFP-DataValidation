import pytest
import os
import sys
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, StaticPool, text
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timezone

# 1. Force environment variables BEFORE importing app
os.environ["DATABASE_URL"] = "sqlite:///:memory:"
os.environ["JWT_SECRET"] = "test_secret_that_is_at_least_64_characters_long_for_development_and_testing_only"
os.environ["DISABLE_DOCS"] = "true"

from app.main import app
from app.database import get_db, Base
from app.models import User, Permission, RolePermission
from app.auth import hash_password, create_access_token

# 2. Setup SQLite in-memory engine for fast testing
engine = create_engine(
    "sqlite:///:memory:",
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

@pytest.fixture(scope="session", autouse=True)
def setup_database():
    """Create tables and seed minimal data once per session."""
    Base.metadata.create_all(bind=engine)
    
    db = TestingSessionLocal()
    try:
        # Seed permissions if missing
        if db.query(Permission).count() == 0:
            perms = [
                ("upload_data", "Can upload"),
                ("view_stats", "Can view stats"),
                ("view_admin", "Can view admin"),
            ]
            for name, desc in perms:
                db.add(Permission(name=name, description=desc))
            
            role_map = {
                "admin": ["upload_data", "view_stats", "view_admin"],
                "uploader": ["upload_data"],
                "viewer": ["view_stats"],
            }
            for role, p_names in role_map.items():
                for pname in p_names:
                    db.add(RolePermission(role=role, permission_name=pname))
            db.commit()
    finally:
        db.close()
    
    yield
    # No cleanup needed for in-memory DB

@pytest.fixture
def db():
    """Provide a clean DB session for each test, managing transactions."""
    connection = engine.connect()
    transaction = connection.begin()
    session = TestingSessionLocal(bind=connection)

    yield session

    session.close()
    transaction.rollback()
    connection.close()

@pytest.fixture
def client(db):
    """Provide a TestClient with the DB dependency overridden."""
    def override_get_db():
        try:
            yield db
        finally:
            pass

    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()

@pytest.fixture
def admin_user(db):
    """Create and return an admin user."""
    user = User(username="admin_test", hashed_password=hash_password("test"), role="admin")
    db.add(user)
    db.commit()
    db.refresh(user)
    return user

@pytest.fixture
def admin_token(admin_user):
    """Return an auth header for the admin user."""
    token = create_access_token({"sub": admin_user.username})
    return {"Authorization": f"Bearer {token}"}

@pytest.fixture
def uploader_user(db):
    """Create and return an uploader user."""
    user = User(username="uploader_test", hashed_password=hash_password("test"), role="uploader")
    db.add(user)
    db.commit()
    db.refresh(user)
    return user

@pytest.fixture
def uploader_token(uploader_user):
    """Return an auth header for the uploader user."""
    token = create_access_token({"sub": uploader_user.username})
    return {"Authorization": f"Bearer {token}"}
