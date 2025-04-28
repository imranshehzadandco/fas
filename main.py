from app import app  # noqa: F401
import routes  # noqa: F401
from datetime import datetime

# Add context processor to make functions available to all templates
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

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
