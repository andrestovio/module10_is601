import os
import argparse
from typing import Optional, List
from datetime import datetime
import uuid  # Import Python's uuid module

from dotenv import load_dotenv
from faker import Faker
from pydantic_settings import BaseSettings
from sqlalchemy import create_engine, Column, String, DateTime
from sqlalchemy.dialects.postgresql import UUID  # Import SQLAlchemy's UUID type for PostgreSQL
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.exc import IntegrityError
from pydantic import BaseModel, EmailStr, ValidationError
from passlib.context import CryptContext

# Load environment variables from .env file
load_dotenv()

# Pydantic Settings for Configuration
class Settings(BaseSettings):
    db_host: str
    db_user: str
    db_password: str
    db_name: str
    db_port: Optional[int] = 5432  # Default PostgreSQL port
    salt: str  # Additional salt for password hashing

    class Config:
        env_file = ".env"
        env_prefix = "DB_"  # Prefix only for database-related environment variables
        fields = {
            'salt': {'env': 'SALT'},  # Map 'salt' to 'SALT' without the 'DB_' prefix
        }

# Instantiate settings
try:
    settings = Settings()
    print(f"Loaded settings: db_host={settings.db_host}, db_user={settings.db_user}, salt={settings.salt[:4]}****")
except ValidationError as e:
    print("Configuration Error:")
    print(e)
    exit(1)

# Define the database URL (example for PostgreSQL)
DATABASE_URL = f'postgresql://{settings.db_user}:{settings.db_password}@{settings.db_host}:{settings.db_port}/{settings.db_name}'

# Initialize SQLAlchemy base and engine
Base = declarative_base()
engine = create_engine(DATABASE_URL, echo=True)  # echo=True for SQL logging

# Define the SQLAlchemy User model with UUID as primary key
class User(Base):
    __tablename__ = 'users'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)  # UUID primary key
    first_name = Column(String(50), nullable=False)
    last_name = Column(String(50), nullable=False)
    email = Column(String(120), unique=True, nullable=False)
    username = Column(String(50), unique=True, nullable=False)
    password = Column(String(255), nullable=False)  # Hashed password
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

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
    password: str  # Plain password for hashing

# Initialize Faker
fake = Faker()

# Initialize Passlib's CryptContext for hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def hash_password(plain_password: str, salt: str) -> str:
    """
    Hashes a password using bcrypt and an additional salt.
    """
    # Combine the plain password with the salt (acting as a pepper)
    salted_password = plain_password + salt
    return pwd_context.hash(salted_password)

def generate_fake_user(existing_emails: set, existing_usernames: set) -> UserData:
    """
    Generates a fake user with unique email and username.
    """
    while True:
        # Generate a random password
        plain_password = fake.password(length=12)
        
        user_data = UserData(
            first_name=fake.first_name(),
            last_name=fake.last_name(),
            email=fake.unique.email(),
            username=fake.unique.user_name(),
            password=plain_password
        )
        if user_data.email not in existing_emails and user_data.username not in existing_usernames:
            existing_emails.add(user_data.email)
            existing_usernames.add(user_data.username)
            return user_data

def seed_users(count: int):
    """
    Seeds the users table with fake data.
    """
    print("Creating tables if they don't exist...")
    # Create tables if they don't exist
    Base.metadata.create_all(engine)

    session = Session()
    try:
        print("Fetching existing emails and usernames to prevent duplicates...")
        # Fetch existing emails and usernames to prevent duplicates
        existing_emails = set(email for (email,) in session.query(User.email).all())
        existing_usernames = set(username for (username,) in session.query(User.username).all())
        print(f"Found {len(existing_emails)} existing emails and {len(existing_usernames)} existing usernames.")

        users_to_add: List[User] = []
        for i in range(1, count + 1):
            print(f"Generating user {i}...")
            user_data = generate_fake_user(existing_emails, existing_usernames)
            hashed_password = hash_password(user_data.password, settings.salt)
            user = User(
                first_name=user_data.first_name,
                last_name=user_data.last_name,
                email=user_data.email,
                username=user_data.username,
                password=hashed_password
            )
            users_to_add.append(user)
            print(f"User {i}: {user.username} created.")

        print("Adding users to the session...")
        session.add_all(users_to_add)  # Use add_all instead of bulk_save_objects
        print("Committing the session...")
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
        print("Session closed.")

def parse_arguments():
    """
    Parses command-line arguments.
    """
    parser = argparse.ArgumentParser(description='Seed the users table with fake data.')
    parser.add_argument('-n', '--number', type=int, default=10,
                        help='Number of fake users to generate (default: 10)')
    return parser.parse_args()

def main():
    args = parse_arguments()
    seed_users(args.number)

if __name__ == '__main__':
    main()
