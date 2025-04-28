import os
import logging
import tempfile
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.orm import DeclarativeBase

# Configure logging
logging.basicConfig(level=logging.DEBUG)

# Set up database base class
class Base(DeclarativeBase):
    pass

# Create database instance
db = SQLAlchemy(model_class=Base)

# Create app
app = Flask(__name__)
app.secret_key = os.environ.get("SESSION_SECRET", "fas-faiz-accounting-system-key")

# Set temp folder for file uploads
app.config['UPLOAD_FOLDER'] = tempfile.gettempdir()
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16 MB max file size

# Configure database
app.config["SQLALCHEMY_DATABASE_URI"] = os.environ.get("DATABASE_URL", "sqlite:///fas.db")
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {
    "pool_recycle": 300,
    "pool_pre_ping": True,
}

# Initialize the app with the extension
db.init_app(app)

with app.app_context():
    # Import models
    import models  # noqa: F401
    
    # Create database tables if they don't exist
    db.create_all()
