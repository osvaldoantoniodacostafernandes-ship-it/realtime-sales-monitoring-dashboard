# ==============================================================================
# DATABASE CONFIGURATION FOR MYSQL 8.0
# ==============================================================================
import os

MYSQL_HOST = os.environ.get('MYSQL_HOST', 'localhost')
MYSQL_PORT = int(os.environ.get('MYSQL_PORT', 3306))
MYSQL_USER = os.environ.get('MYSQL_USER', 'root')
MYSQL_PASSWORD = os.environ.get('MYSQL_PASSWORD', 'dafa')
MYSQL_DATABASE = os.environ.get('MYSQL_DATABASE', 'sales_dashboard')
