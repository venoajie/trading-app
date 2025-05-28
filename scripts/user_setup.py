# Simplified user setup
from shared.lib.database import init_db
from shared.lib.models import User
import uuid

def create_user(email, exchange):
    session_maker = init_db("postgresql://trading:strongpassword@postgres/trading")
    
    with session_maker() as session:
        user = User(
            id=str(uuid.uuid4()),
            exchange_configs={
                exchange: {"status": "active"}
            },
            api_keys={}
        )
        session.add(user)
        session.commit()
    print(f"Created user {email} with ID {user.id}")