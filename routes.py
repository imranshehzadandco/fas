import os
import io
import pandas as pd
from datetime import datetime
from functools import wraps
from flask import render_template, request, redirect, url_for, flash, session, send_file, abort
from werkzeug.utils import secure_filename
from werkzeug.security import check_password_hash, generate_password_hash
from sqlalchemy import func
from app import app, db
from models import Company, Transaction, CompanyFile
from utils import process_excel_file, get_ledger_data, get_special_report_data, get_trial_balance_data, process_excel_data

# Add context processor to make functions available to templates
@app.context_processor
def utility_processor():
    return {
        'now': datetime.now
    }

# Add Jinja filter for currency formatting
@app.template_filter('format_currency')
def format_currency(value):
    """Format a number as PKR currency"""
    if value is None:
        return "PKR 0.00"
    return f"{value:,.2f}"

@app.route("/")
def index():
    """Home page route"""
    companies = Company.query.order_by(Company.name).all()
    selected_company_id = session.get('selected_company_id')
    
    return render_template(
        "index.html",
        companies=companies,
        selected_company_id=selected_company_id
    )

@app.route("/upload", methods=["POST"])
def upload_file():
    """Handle Excel file upload"""
    if 'file' not in request.files:
        flash('No file part', 'error')
        return redirect(request.url)
    
    file = request.files['file']
    
    if file.filename == '':
        flash('No selected file', 'error')
        return redirect(request.url)
    
    if file and file.filename.endswith(('.xlsx', '.xls')):
        # Use company name from the form
        company_name = request.form.get('company_name', '').strip()
        if not company_name:
            company_name = os.path.splitext(secure_filename(file.filename))[0]
        
        # Get password from form and verify
        file_password = request.form.get('file_password', '').strip()
        
        # Check if the password is correct (fixed system password)
        if file_password != "Faiz5683":
            flash('Incorrect password. Please try again.', 'error')
            return redirect(url_for('index'))
        
        # Check if company already exists
        company = Company.query.filter_by(name=company_name).first()
        
        if company:
            # Delete existing transactions for this company
            Transaction.query.filter_by(company_id=company.id).delete()
            company.updated_at = datetime.now()
        else:
            # Create new company
            company = Company(name=company_name)
            db.session.add(company)
            # Commit to get a valid ID before creating transactions
            db.session.commit()
        
        # Get file data
        file_data = file.read()
        filename = secure_filename(file.filename)
        
        try:
            # Delete existing file for this company if it exists
            if company.file:
                db.session.delete(company.file)
                db.session.flush()
            
            # Store the file in the database
            company_file = CompanyFile(
                filename=filename,
                file_data=file_data,
                content_type=file.content_type or 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                company_id=company.id
            )
            
            db.session.add(company_file)
            db.session.flush()
            
            # Process Excel file
            transaction_count = process_excel_data(file_data, company)
            db.session.commit()
            
            # Store selected company in session
            session['selected_company_id'] = company.id
            
            flash(f'Successfully processed {transaction_count} transactions for {company_name}', 'success')
        except Exception as e:
            db.session.rollback()
            flash(f'Error processing file: {str(e)}', 'error')
    else:
        flash('File must be an Excel file (.xlsx or .xls)', 'error')
    
    return redirect(url_for('index'))

@app.route("/select_company/<int:company_id>")
def select_company(company_id):
    """Select a company to view reports"""
    company = Company.query.get_or_404(company_id)
    session['selected_company_id'] = company.id
    return redirect(url_for('index'))

@app.route("/ledger")
def ledger():
    """Ledger report route"""
    company_id = session.get('selected_company_id')
    if not company_id:
        flash('Please select a company first', 'warning')
        return redirect(url_for('index'))
    
    company = Company.query.get_or_404(company_id)
    
    # Get distinct head of accounts with case-insensitive comparison
    # Use func.upper to normalize the head_of_account for consistent comparison
    head_of_accounts = db.session.query(func.upper(Transaction.head_of_account).label('normalized_head'), 
                                        Transaction.head_of_account)\
        .filter(Transaction.company_id == company_id, Transaction.head_of_account != '')\
        .distinct(func.upper(Transaction.head_of_account))\
        .order_by(func.upper(Transaction.head_of_account))\
        .all()
    
    # Extract the actual head_of_account values (index 1 in each tuple)
    unique_accounts = [account[1] for account in head_of_accounts]
    
    # Get data for selected account or first account
    selected_account = request.args.get('account', unique_accounts[0] if unique_accounts else None)
    
    ledger_data = get_ledger_data(company_id, selected_account) if selected_account else []
    
    return render_template(
        "ledger.html",
        company=company,
        head_of_accounts=unique_accounts,
        selected_account=selected_account,
        ledger_data=ledger_data
    )

@app.route("/special_report")
def special_report():
    """Special report route"""
    company_id = session.get('selected_company_id')
    if not company_id:
        flash('Please select a company first', 'warning')
        return redirect(url_for('index'))
    
    company = Company.query.get_or_404(company_id)
    categories = db.session.query(Transaction.category)\
        .filter(Transaction.company_id == company_id, Transaction.category != '')\
        .distinct()\
        .order_by(Transaction.category)\
        .all()
    
    # Get data for selected category or first category
    selected_category = request.args.get('category', categories[0][0] if categories else None)
    
    report_data = get_special_report_data(company_id, selected_category) if selected_category else []
    
    return render_template(
        "special_report.html",
        company=company,
        categories=[category[0] for category in categories],
        selected_category=selected_category,
        report_data=report_data
    )

@app.route("/trial_balance")
def trial_balance():
    """Trial balance report route"""
    company_id = session.get('selected_company_id')
    if not company_id:
        flash('Please select a company first', 'warning')
        return redirect(url_for('index'))
    
    company = Company.query.get_or_404(company_id)
    
    # Get periods (month and year combinations)
    transactions = Transaction.query.filter_by(company_id=company_id).order_by(Transaction.date).all()
    periods = set()
    for t in transactions:
        periods.add(t.date.strftime("%m/%Y"))
    
    # Sort periods
    periods = sorted(periods, key=lambda x: datetime.strptime(x, "%m/%Y"))
    
    # Selected period (default to all)
    selected_period = request.args.get('period', 'all')
    
    trial_balance_data = get_trial_balance_data(company_id, selected_period)
    
    return render_template(
        "trial_balance.html",
        company=company,
        periods=periods,
        selected_period=selected_period,
        trial_balance_data=trial_balance_data
    )
    
@app.route("/download_file/<int:company_id>", methods=["GET", "POST"])
def download_file(company_id):
    """Download the Excel file for a company"""
    company = Company.query.get_or_404(company_id)
    
    if not company.file:
        flash('No file found for this company', 'error')
        return redirect(url_for('index'))
    
    # Always require password for downloads
    if request.method == "GET":
        # Show password prompt
        return render_template(
            "password_prompt.html",
            company=company,
            action_url=url_for('download_file', company_id=company_id)
        )
    
    # If POST method, verify the system password
    if request.method == "POST":
        # Verify password against fixed system password
        password = request.form.get('file_password', '')
        if password != "Faiz5683":
            flash('Incorrect password', 'error')
            return render_template(
                "password_prompt.html",
                company=company,
                action_url=url_for('download_file', company_id=company_id)
            )
    
    # Create a BytesIO object from the file data
    file_data = io.BytesIO(company.file.file_data)
    
    # Return the file as a response
    return send_file(
        file_data,
        mimetype=company.file.content_type,
        as_attachment=True,
        download_name=company.file.filename
    )
