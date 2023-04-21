from loguru import logger
from sqlalchemy.orm import Session
from app import crud, schemas
from app.db.session import SessionLocal

admin_email = "admin@bjhy.com"


def init_db_start(db: Session) -> None:
    user_in = schemas.UserCreate(
        username="SuperUser",
        useremail=admin_email,
        user_active=True,
        password="admin"
    )
    user = crud.user.create(db, obj_in=user_in)  # noqa: F841


def init_db(db: Session) -> None:
    user = crud.user.get_by_email(db, email=admin_email)
    if not user:
        user_in = schemas.UserCreate(
            full_name="Initial Super User",
            email=admin_email,
            is_superuser=True,
        )
        user = crud.user.create(db, obj_in=user_in)  # noqa: F841
    else:
        logger.warning(
            "Skipping creating superuser. User with email "
            f"{admin_email} already exists. "
        )


if __name__ == '__main__':
    db = SessionLocal()
    init_db(db)
