from yaml import load
try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

from .sql_io import get_mysql_connection


class GraphDBBuilder():
    def __init__(self, db_name, config_file_path, mysql_config_file):
        self.db_name = db_name
        self.config_file = read_config_file(config_file_path)

        try:
            if mysql_config_file:
                self.mysql_conn = get_mysql_connection(mysql_config_file)
            else:
                self.mysql_conn = get_mysql_connection()
        except RuntimeError:
            print("Warning: No MySQL database connection available. Skipping MySQL sources.")
            self.mysql_conn = None

        self.parse_config()

    def parse_sources(self):
        pass

    def parse_nodes(self):
        pass

    def parse_relationships(self):
        pass

    def serialize_data(self):
        pass


def read_config_file(conf_file_path):
    """Parse a YAML config file for PyGraphETL."""
    with open(conf_file_path, 'r') as cf:
        yml_config = load(cf, Loader=Loader)
    return yml_config