import mysql.connector

def get_mysql_connection(config_file = "~/.my.cnf"):
    """Returns a connection to a MySQL server."""
    cnx = mysql.connector.connect(option_files=config_file)

    return cnx