import os
import sys
from pathlib import Path

# SETUP WINDOWS HADOOP_HOME & JVM MEMORY OPTIMIZATION
if os.name == 'nt':
    hadoop_dir = r'C:\hadoop'
    if os.path.exists(hadoop_dir):
        os.environ['HADOOP_HOME'] = hadoop_dir
        hadoop_bin = os.path.join(hadoop_dir, 'bin')
        if hadoop_bin not in os.environ.get('PATH', ''):
            os.environ['PATH'] = hadoop_bin + ';' + os.environ.get('PATH', '')
    
    # Bersihkan _JAVA_OPTIONS yang membatasi memori stack secara ekstrem
    if '_JAVA_OPTIONS' in os.environ:
        del os.environ['_JAVA_OPTIONS']

# ==============================================================================
# CENTRAL PROJECT CONFIGURATION
# REAL-TIME SALES DASHBOARD
# ==============================================================================

# KAFKA SETTINGS
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'sales_transactions'
PRODUCER_DELAY_MIN = 0.5
PRODUCER_DELAY_MAX = 2.0
PAYMENT_METHODS = ['Cash', 'QRIS (Mosan)', 'QRIS (BRImo)', 'QRIS (BNCTL)']

# PRODUCTS FOR BACKUP PRODUCER
PRODUCTS_BY_CATEGORY = {
    'Beverages': {
        'Timor Arabica Coffee 250g': 4.50,
        'Timor Robusta Coffee 250g': 3.50,
        'Mineral Water 600ml': 0.50,
        'Fresh Orange Juice': 2.00,
    },
    'Food': {
        'Chocolate Croissant': 1.75,
        'Cheese Toast': 2.50,
        'Potato Chips BBQ': 1.25,
    },
    'Groceries': {
        'Organic Rice 5kg': 8.00,
        'Cooking Oil 2L': 3.75,
        'White Sugar 1kg': 1.20,
    },
    'Electronics': {
        'USB Cable': 5.00,
        'Earphone': 15.00,
        'Power Adapter': 7.50,
    }
}

# MYSQL SETTINGS
MYSQL_HOST = 'localhost'
MYSQL_PORT = 3306
MYSQL_USER = 'root'
MYSQL_PASSWORD = 'dafa'
MYSQL_DATABASE = 'sales_dashboard'

# MYSQL JDBC CONFIGURATION
MYSQL_JDBC_URL = (
    f'jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}'
    '?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC'
)

MYSQL_JDBC_PROPERTIES = {
    'user': MYSQL_USER,
    'password': MYSQL_PASSWORD,
    'driver': 'com.mysql.cj.jdbc.Driver',
}

# PRINT KONFIGURASAUN SUMARIU (TETUN)
print('=' * 60)
print('KONFIGURASAUN SISTEMA PAINEL VENDAS REAL-TIME (DILI)')
print('=' * 60)
print(f'Servidor Kafka : {KAFKA_BOOTSTRAP_SERVERS}')
print(f'Topiku Kafka   : {KAFKA_TOPIC}')
print(f'Host MySQL     : {MYSQL_HOST}:{MYSQL_PORT}')
print(f'Baze Dadus     : {MYSQL_DATABASE}')
print('=' * 60)
