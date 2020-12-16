import mysql.connector
import os

def get_mysql_connection(config_file = "~/.my.cnf", database = None):
    """Returns a connection to a MySQL server."""
    if config_file == '~/.my.cnf':
        config_file = os.path.join(os.path.expanduser("~"), '.my.cnf')
    if database:
        cnx = mysql.connector.connect(option_files=config_file, database=database)
    else:
        cnx = mysql.connector.connect(option_files=config_file)

    return cnx