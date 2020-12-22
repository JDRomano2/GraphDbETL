from yaml import load
try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader
from tqdm import tqdm
from collections import defaultdict
import mysql.connector
from mysql.connector import FieldType

import tables
import numpy as np

from .sql_io import get_mysql_connection
from .dtypes import *

import ipdb


class GraphDBBuilder():
    def __init__(self, config_file_path, mysql_config_file):
        self.config = read_config_file(config_file_path)
        self.mysql_config_file = mysql_config_file

        try:
            if mysql_config_file:
                self.mysql_conn = get_mysql_connection(mysql_config_file)
            else:
                self.mysql_conn = get_mysql_connection()
        except mysql.connector.Error as err:
            print("Warning: No MySQL database connection available. Skipping MySQL sources.")
            self.mysql_conn = None

        self._process_config()

        self._initialize_tables()

    def build(self):
        self.parse_nodes()

    def _initialize_tables(self):
        # Go through each node, and create a list of (column_name, dtype) tuples

        db_name = self.config['Database']['name']
        db_version = self.config['Database']['version']

        # Make pytables file and groups
        self.h5_file = tables.open_file("{0}-{1}.h5".format(db_name, db_version), mode='w', title=db_name)
        self.node_tables = self.h5_file.create_group("/", 'nodes', 'Node tables')
        self.relationship_tables = self.h5_file.create_group("/", 'relationships', 'Relationship tables')

        mysql_query_template = "SELECT * FROM {0} LIMIT 1;"

        node_tables_pre = {}
        relationship_tables_pre = {}

        for node_label, node_label_config in self.config['Nodes'].items():

            all_sources_fields = []
            for s_name, s_config in node_label_config['sources'].items():
                if self.source_type_map[s_name] == 'mysql':
                    # query the database, pull out fields
                    this_qry = mysql_query_template.format(s_config['table'])
                    this_cur = self.mysql_dbs[s_name].cursor(buffered=True)
                    this_cur.execute(this_qry)

                    field_descr = description_to_fields(this_cur.description)
                    field_names, field_types = list(zip(*field_descr))

                    # now, map fields to pytables types
                    np_types = [map_pytables[x] for x in field_types]
                    types_structured = list(zip(field_names, np_types))
                else:
                    raise NotImplementedError

                all_sources_fields += types_structured
            
            # merge fields from all sources
            node_fields_merged = merge_fields(all_sources_fields)

            #data_descr = np.dtype(node_fields_merged)
            # See: https://stackoverflow.com/questions/58261748 ("Option 2")
            data_descr = make_table_dict_descr(node_fields_merged)

            node_tables_pre[node_label] = data_descr

        for label, fields in node_tables_pre.items():
            make_table(self.h5_file, self.node_tables, label, fields)

        # TODO: Make tables for relationship types

        return True

    def _process_config(self):
        try:
            self.db_name = self.config['Database']['name']
            self.db_version = self.config['Database']['version']
            self.db_author = self.config['Database']['author']
            self.node_config = self.config['Nodes']
            self.rel_config = self.config['Relationships']
            self.source_config = self.config['Sources']

            mysql_dbs = dict()
            self.source_type_map = dict()

            for source_name, source_config in self.config['Sources'].items():
                self.source_type_map[source_name] = source_config['source type']
                
                if source_config['source type'] == 'mysql':
                    try:
                        if self.mysql_config_file:
                            cnx = get_mysql_connection(self.mysql_config_file, database=source_config['database name'])
                        else:
                            cnx = get_mysql_connection(database = source_config['database name'])
                        mysql_dbs[source_name] = cnx
                    except mysql.connector.Error as err:
                        print("Warning: Couldn't establish connection to MySQL database for {0}. Skipping this source.".format(source_name))
                        print(err)
                else:
                    raise NotImplementedError("Need to implement parsing of non-MySQL sources.")

            self.mysql_dbs = mysql_dbs

        except KeyError as e:
            print("Error: Key not found.")
            print(e)
            print("Your config file is probably not correctly formatted.")
            print("Please check the documentation.")

    def parse_nodes(self):
        for node_label, node_config in self.config['Nodes'].items():
            for this_node_source, source_options in node_config['sources'].items():
                
                self._parse_source_dispatcher(node_label, this_node_source, source_options)
            
    def _parse_source_dispatcher(self, node_label, source_name, node_source_options):
        source_type = self.source_type_map[source_name]

        if source_type == 'mysql':
            source_cnx = self.mysql_dbs[source_name]
            source_table = node_source_options['table']
            source_id_key = node_source_options['id_key']
            source_uri_key = node_source_options['uri_key']
            parse_mysql_source(source_cnx, node_label, source_table, source_id_key, source_uri_key)

    def parse_relationships(self):
        pass

    def serialize_data(self):
        pass

def merge_fields(all_sources_fields):
    """Aggregate a list of tuples describing table columns, combining any
    duplicates and ensuring that datatypes aren't mixed.
    """
    fields_merged = []

    field_names, _ = zip(*all_sources_fields)
    field_names_unique = list(set(field_names))

    for name in field_names_unique:
        dtypes_list = [v for n, v in all_sources_fields if n == name]

        assert len(set([x.type for x in dtypes_list])) <= 1
        
        fields_merged.append((name, dtypes_list[0]))

    return fields_merged

def description_to_fields(mysql_cur_description):
    """Convert a MySQL cursor description to a list of fields for inclusion in
    the graph data."""
    fields = []
    for f_data in mysql_cur_description:
        column_name = f_data[0]
        dtype_num = f_data[1]
        dtype_name = FieldType.get_info(f_data[1])
        null_ok = f_data[6]
        column_flags = f_data[7]
        fields.append((
            column_name,
            dtype_name
        ))

    return fields

def parse_mysql_source(cnx, node_label, source_table, source_id_key, source_uri_key):
    cursor = cnx.cursor()
    sql_query = "SELECT * FROM {0};".format(source_table)
    cursor.execute(sql_query)

    ipdb.set_trace()

def read_config_file(conf_file_path):
    """Parse a YAML config file for PyGraphETL."""
    with open(conf_file_path, 'r') as cf:
        yml_config = load(cf, Loader=Loader)
    return yml_config

def make_table_dict_descr(field_tuples):
    descr = dict()
    for ft_name, ft_type in field_tuples:
        descr[ft_name] = ft_type
    return descr

def make_table(h5file, group, table_name, table_fields):
    """Create a PyTables table to store graph data in HDF5 format.
    """

    #column_description = np.dtype(table_fields)

    tab = h5file.create_table(group, table_name, table_fields, "{0} table".format(table_name))
    
    return tab