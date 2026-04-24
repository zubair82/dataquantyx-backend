from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)
# We need an admin token to access the route
from app.utils.database import SessionLocal, Admin, File, User
from app.services.admin_service import create_admin_session_token
from app.services.user_service import UserService
import uuid
import datetime

db = SessionLocal()
admin = db.query(Admin).first()
if not admin:
    admin = Admin(admin_id=str(uuid.uuid4()), email="test@admin.com", hashed_password="123", full_name="Test Admin")
    db.add(admin)
    db.commit()

user = db.query(User).first()
if not user:
    user = User(user_id=str(uuid.uuid4()), email="test@user.com", hashed_password="123")
    db.add(user)
    db.commit()

token = create_admin_session_token(db, admin)
response = client.get(f"/api/admin/users/{user.user_id}/files", headers={"Authorization": f"Bearer {token}"})
print(response.status_code)
print(response.json())
