import pytest
from fastapi import HTTPException
from app.rbac import PermissionChecker
from app.models import User

class MockRequest:
    def __init__(self):
        self.state = type('obj', (object,), {'user': None})

def test_permission_checker_admin():
    # Admin should bypass all checks
    checker = PermissionChecker("any_permission")
    admin_user = User(username="admin", role="admin")
    
    # Should not raise any exception
    result = checker(current_user=admin_user, db=None)
    assert result == admin_user

def test_permission_checker_forbidden():
    checker = PermissionChecker("manage_users")
    viewer_user = User(username="viewer", role="viewer")
    
    # Mock DB query to return None (no permission)
    class MockDB:
        def query(self, model):
            return self
        def filter(self, *args):
            return self
        def first(self):
            return None
            
    with pytest.raises(HTTPException) as excinfo:
        checker(current_user=viewer_user, db=MockDB())
    
    assert excinfo.value.status_code == 403
    assert "Not enough permissions" in excinfo.value.detail
