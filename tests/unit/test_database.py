import os
import argparse
from typing import Optional, List

from dotenv import load_dotenv
from faker import Faker
from sqlalchemy import create_engine, Column, Integer, String, Sequence
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.exc import IntegrityError
from pydantic import BaseSettings, BaseModel, EmailStr, ValidationError

# Load environment variables from .env file
load_dotenv()

# Pydantic Settings for Configuration
class Settings(BaseSettings):
    db_host: str
    db_user: str
    db_password: str
    db_name: str
    db_port: Optional[int] = 5432  # Default PostgreSQL port

    class Config:
        env_file = ".env"
        env_prefix = "DB_"  # Prefix for environment variables

# Instantiate settings
try:
    settings = Settings()
except ValidationError as e:
    print("Configuration Error:")
    print(e)
    exit(1)

# Define the database URL (example for PostgreSQL)
DATABASE_URL = f'postgresql://{settings.db_user}:{settings.db_password}@{settings.db_host}:{settings.db_port}/{settings.db_name}'

# Initialize SQLAlchemy base and engine
Base = declarative_base()
engine = create_engine(DATABASE_URL, echo=True)  # echo=True for SQL logging

# Define the SQLAlchemy User model
class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, Sequence('user_id_seq'), primary_key=True)
    first_name = Column(String(50), nullable=False)
    last_name = Column(String(50), nullable=False)
    email = Column(String(120), unique=True, nullable=False)
    username = Column(String(50), unique=True, nullable=False)

    def __repr__(self):
        return f"<User(name={self.first_name} {self.last_name}, email={self.email})>"

# Create a session maker
Session = sessionmaker(bind=engine)

# Pydantic Model for User Data Validation
class UserData(BaseModel):
    first_name: str
    last_name: str
    email: EmailStr
    username: str

# User factory using Faker with Pydantic validation
fake = Faker()

def generate_fake_user(existing_emails: set, existing_usernames: set) -> UserData:
    while True:
        user_data = UserData(
            first_name=fake.first_name(),
            last_name=fake.last_name(),
            email=fake.unique.email(),
            username=fake.unique.user_name()
        )
        if user_data.email not in existing_emails and user_data.username not in existing_usernames:
            existing_emails.add(user_data.email)
            existing_usernames.add(user_data.username)
            return user_data

def seed_users(count: int):
    # Create tables if they don't exist
    Base.metadata.create_all(engine)

    session = Session()
    try:
        # Fetch existing emails and usernames to prevent duplicates
        existing_emails = set(email for (email,) in session.query(User.email).all())
        existing_usernames = set(username for (username,) in session.query(User.username).all())

        users_to_add: List[User] = []
        for _ in range(count):
            user_data = generate_fake_user(existing_emails, existing_usernames)
            user = User(
                first_name=user_data.first_name,
                last_name=user_data.last_name,
                email=user_data.email,
                username=user_data.username
            )
            users_to_add.append(user)

        session.bulk_save_objects(users_to_add)
        session.commit()
        print(f"Successfully added {count} users to the database.")
    except IntegrityError as ie:
        session.rollback()
        print("Integrity Error:", ie)
    except ValidationError as ve:
        session.rollback()
        print("Validation Error:", ve)
    except Exception as e:
        session.rollback()
        print("An unexpected error occurred:", e)
    finally:
        session.close()

def parse_arguments():
    parser = argparse.ArgumentParser(description='Seed the users table with fake data.')
    parser.add_argument('-n', '--number', type=int, default=10,
                        help='Number of fake users to generate (default: 10)')
    return parser.parse_args()

def main():
    args = parse_arguments()
    seed_users(args.number)

if __name__ == '__main__':
    main()