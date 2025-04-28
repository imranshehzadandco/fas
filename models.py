from datetime import datetime
from app import db
import base64
from werkzeug.security import check_password_hash, generate_password_hash

class Company(db.Model):
    """Model for storing company information"""
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), unique=True, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.now)
    updated_at = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now)
    
    # Relationships
    transactions = db.relationship('Transaction', backref='company', lazy=True, cascade="all, delete-orphan")
    file = db.relationship('CompanyFile', backref='company', uselist=False, cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<Company {self.name}>"
        
class CompanyFile(db.Model):
    """Model for storing company Excel files"""
    id = db.Column(db.Integer, primary_key=True)
    filename = db.Column(db.String(255), nullable=False)
    file_data = db.Column(db.LargeBinary, nullable=False)  # Store the actual file data
    content_type = db.Column(db.String(100), nullable=False, default='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    company_id = db.Column(db.Integer, db.ForeignKey('company.id'), nullable=False, unique=True)
    uploaded_at = db.Column(db.DateTime, default=datetime.now)
    file_password_hash = db.Column(db.String(256), nullable=True)  # Store password hash for file access
    
    def __repr__(self):
        return f"<CompanyFile {self.filename} for company {self.company_id}>"
        
    def set_password(self, password):
        """Set the password hash for file access"""
        if password and password.strip():
            self.file_password_hash = generate_password_hash(password)
        else:
            self.file_password_hash = None
            
    def check_password(self, password):
        """Check if the password is correct"""
        if not self.file_password_hash:
            return True  # No password set
        return check_password_hash(self.file_password_hash, password)

class Transaction(db.Model):
    """Model for storing accounting transactions"""
    id = db.Column(db.Integer, primary_key=True)
    date = db.Column(db.Date, nullable=False)
    head_of_account = db.Column(db.String(200), nullable=False)
    category = db.Column(db.String(200), nullable=True)
    description = db.Column(db.Text, nullable=True)  # Using Text type for unlimited length
    reference = db.Column(db.String(200), nullable=True)
    debit = db.Column(db.Float, default=0.0)
    credit = db.Column(db.Float, default=0.0)
    company_id = db.Column(db.Integer, db.ForeignKey('company.id'), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.now)
    
    def __repr__(self):
        return f"<Transaction {self.id}: {self.head_of_account} - {self.debit}/{self.credit}>"
        
    @property
    def formatted_date(self):
        """Return date in DD/MM/YYYY format"""
        return self.date.strftime("%d/%m/%Y") if self.date else ""
