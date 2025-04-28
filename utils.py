import pandas as pd
import numpy as np
import io
from datetime import datetime
from sqlalchemy import func, extract
from app import db
from models import Transaction, CompanyFile

def process_excel_file(filepath, company):
    """
    Process Excel file and add transactions to database
    Returns the number of rows processed
    """
    try:
        # Read Excel file
        df = pd.read_excel(filepath)
        
        # Check required columns
        required_columns = ['Date', 'Head of Accounts', 'Debit', 'Credit']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")
        
        # Rename columns to match model attributes
        column_mapping = {
            'Date': 'date',
            'Head of Accounts': 'head_of_account',
            'Category': 'category',
            'Description': 'description',
            'Ref': 'reference',
            'Debit': 'debit',
            'Credit': 'credit'
        }
        
        df = df.rename(columns=column_mapping)
        
        # Fill NaN values
        df['category'] = df.get('category', pd.Series()).fillna('')
        df['description'] = df.get('description', pd.Series()).fillna('')
        df['reference'] = df.get('reference', pd.Series()).fillna('')
        df['debit'] = df.get('debit', pd.Series()).fillna(0.0)
        df['credit'] = df.get('credit', pd.Series()).fillna(0.0)
        
        # Convert date to datetime
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        
        # Filter out invalid records
        df = df.dropna(subset=['date', 'head_of_account'])
        
        # First pass: collect all unique head of accounts (case insensitive)
        head_of_accounts_map = {}
        for _, row in df.iterrows():
            if not pd.isna(row['head_of_account']) and row['head_of_account'] != '':
                head = str(row['head_of_account']).strip()
                head_upper = head.upper()
                
                # Store the first occurrence of each head of account
                if head_upper not in head_of_accounts_map:
                    head_of_accounts_map[head_upper] = head
        
        # Add transactions to database
        transaction_count = 0
        for _, row in df.iterrows():
            if not pd.isna(row['head_of_account']) and row['head_of_account'] != '':
                # Use consistent capitalization for each head of account
                head = str(row['head_of_account']).strip()
                normalized_head = head_of_accounts_map[head.upper()]
                
                transaction = Transaction(
                    date=row['date'].date(),
                    head_of_account=normalized_head,  # Use normalized version
                    category=row.get('category', ''),
                    description=row.get('description', ''),
                    reference=row.get('reference', ''),
                    debit=float(row.get('debit', 0) or 0),
                    credit=float(row.get('credit', 0) or 0),
                    company_id=company.id
                )
                db.session.add(transaction)
                transaction_count += 1
        
        return transaction_count
    
    except Exception as e:
        # Re-raise the exception to be caught and handled by the caller
        raise Exception(f"Failed to process Excel file: {str(e)}")
        
def process_excel_data(file_data, company):
    """
    Process Excel file data from memory
    Returns the number of rows processed
    """
    try:
        # Read Excel file from binary data
        excel_data = io.BytesIO(file_data)
        df = pd.read_excel(excel_data)
        
        # Check required columns
        required_columns = ['Date', 'Head of Accounts', 'Debit', 'Credit']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")
        
        # Rename columns to match model attributes
        column_mapping = {
            'Date': 'date',
            'Head of Accounts': 'head_of_account',
            'Category': 'category',
            'Description': 'description',
            'Ref': 'reference',
            'Debit': 'debit',
            'Credit': 'credit'
        }
        
        df = df.rename(columns=column_mapping)
        
        # Fill NaN values
        df['category'] = df.get('category', pd.Series()).fillna('')
        df['description'] = df.get('description', pd.Series()).fillna('')
        df['reference'] = df.get('reference', pd.Series()).fillna('')
        df['debit'] = df.get('debit', pd.Series()).fillna(0.0)
        df['credit'] = df.get('credit', pd.Series()).fillna(0.0)
        
        # Convert date to datetime
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        
        # Filter out invalid records
        df = df.dropna(subset=['date', 'head_of_account'])
        
        # First pass: collect all unique head of accounts (case insensitive)
        head_of_accounts_map = {}
        for _, row in df.iterrows():
            if not pd.isna(row['head_of_account']) and row['head_of_account'] != '':
                head = str(row['head_of_account']).strip()
                head_upper = head.upper()
                
                # Store the first occurrence of each head of account
                if head_upper not in head_of_accounts_map:
                    head_of_accounts_map[head_upper] = head
        
        # Add transactions to database
        transaction_count = 0
        for _, row in df.iterrows():
            if not pd.isna(row['head_of_account']) and row['head_of_account'] != '':
                # Use consistent capitalization for each head of account
                head = str(row['head_of_account']).strip()
                normalized_head = head_of_accounts_map[head.upper()]
                
                transaction = Transaction(
                    date=row['date'].date(),
                    head_of_account=normalized_head,  # Use normalized version
                    category=row.get('category', ''),
                    description=row.get('description', ''),
                    reference=row.get('reference', ''),
                    debit=float(row.get('debit', 0) or 0),
                    credit=float(row.get('credit', 0) or 0),
                    company_id=company.id
                )
                db.session.add(transaction)
                transaction_count += 1
        
        return transaction_count
    
    except Exception as e:
        # Re-raise the exception to be caught and handled by the caller
        raise Exception(f"Failed to process Excel data: {str(e)}")

def get_ledger_data(company_id, head_of_account):
    """
    Get ledger data for a specific head of account
    Returns a list of transaction dictionaries with running balance
    """
    # Normalize the head of account name to prevent duplicates (case insensitive comparison)
    normalized_head = head_of_account.strip().upper()
    
    # Get all transactions for this head of account (matching by normalized name)
    transactions = Transaction.query\
        .filter(
            Transaction.company_id == company_id,
            func.upper(Transaction.head_of_account) == normalized_head
        )\
        .order_by(Transaction.date, Transaction.id)\
        .all()
    
    ledger_data = []
    balance = 0
    today = datetime.now().date()
    
    for t in transactions:
        balance += t.debit - t.credit
        
        # Calculate days from transaction date to today
        days_diff = (today - t.date).days
        
        ledger_data.append({
            'date': t.formatted_date,
            'description': t.description,
            'reference': t.reference,
            'debit': t.debit,
            'credit': t.credit,
            'balance': balance,
            'days': days_diff
        })
    
    return ledger_data

def get_special_report_data(company_id, category):
    """
    Get special report data for a specific category
    Returns a list of head of accounts with their balances
    """
    # Get distinct head of accounts for this category
    heads = db.session.query(Transaction.head_of_account)\
        .filter(Transaction.company_id == company_id, 
                Transaction.category == category,
                Transaction.head_of_account != '')\
        .distinct()\
        .order_by(Transaction.head_of_account)\
        .all()
    
    report_data = []
    
    for head in heads:
        head_of_account = head[0]
        
        # Calculate total debit, credit and balance
        result = db.session.query(
            func.sum(Transaction.debit).label('total_debit'),
            func.sum(Transaction.credit).label('total_credit')
        ).filter(
            Transaction.company_id == company_id,
            Transaction.head_of_account == head_of_account
        ).first()
        
        total_debit = float(result.total_debit or 0)
        total_credit = float(result.total_credit or 0)
        balance = total_debit - total_credit
        
        # Skip if balance is zero
        if balance != 0:
            report_data.append({
                'head_of_account': head_of_account,
                'debit': total_debit,
                'credit': total_credit,
                'balance': balance
            })
    
    return report_data

def get_trial_balance_data(company_id, period='all'):
    """
    Get trial balance data
    Returns a list of head of accounts with their balances
    """
    # Build query base
    query = db.session.query(
        Transaction.head_of_account,
        func.sum(Transaction.debit).label('total_debit'),
        func.sum(Transaction.credit).label('total_credit')
    ).filter(
        Transaction.company_id == company_id,
        Transaction.head_of_account != ''
    )
    
    # Add period filter if needed
    if period != 'all':
        month, year = period.split('/')
        query = query.filter(
            extract('month', Transaction.date) == int(month),
            extract('year', Transaction.date) == int(year)
        )
    
    # Group and sort
    results = query.group_by(Transaction.head_of_account)\
        .order_by(Transaction.head_of_account)\
        .all()
    
    trial_balance_data = []
    total_debit = 0
    total_credit = 0
    
    # Process results
    for result in results:
        head_of_account = result[0]
        account_debit = float(result[1] or 0)
        account_credit = float(result[2] or 0)
        balance = account_debit - account_credit
        
        # Skip if balance is zero
        if balance != 0:
            if balance > 0:
                debit_balance = balance
                credit_balance = 0
            else:
                debit_balance = 0
                credit_balance = -balance
            
            trial_balance_data.append({
                'head_of_account': head_of_account,
                'debit': debit_balance,
                'credit': credit_balance
            })
            
            total_debit += debit_balance
            total_credit += credit_balance
    
    # Add totals and difference
    trial_balance_data.append({
        'head_of_account': 'TOTAL',
        'debit': total_debit,
        'credit': total_credit,
        'is_total': True
    })
    
    difference = total_debit - total_credit
    if difference != 0:
        trial_balance_data.append({
            'head_of_account': 'DIFFERENCE',
            'debit': difference if difference > 0 else 0,
            'credit': -difference if difference < 0 else 0,
            'is_difference': True
        })
    
    return trial_balance_data
