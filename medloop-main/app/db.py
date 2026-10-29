"""Database connection module for MedLoop.

Provides a single shared MongoClient instance and a helper function get_db()
that reads connection settings from environment variables loaded via python-dotenv.
"""

import os
from typing import Optional
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.database import Database

# Load environment variables from .env file if present (never hardcode credentials)
load_dotenv()

_client: Optional[MongoClient] = None


def get_client() -> MongoClient:
    """Return a singleton MongoClient configured from MONGO_URI.

    Creating a single MongoClient per process is recommended by PyMongo because
    MongoClient manages its own internal connection pool and is thread-safe.

    Returns:
        MongoClient: Shared synchronous MongoDB client instance.
    """
    global _client
    if _client is None:
        mongo_uri: str = os.getenv("MONGO_URI", "mongodb://localhost:27017")
        _client = MongoClient(mongo_uri)
    return _client


def get_db() -> Database:
    """Return the configured MedLoop MongoDB Database instance.

    Reads DB_NAME from environment variables (defaults to 'medloop' if unset).

    Returns:
        Database: PyMongo Database handle for MedLoop collections.
    """
    db_name: str = os.getenv("DB_NAME", "medloop")
    return get_client()[db_name]
