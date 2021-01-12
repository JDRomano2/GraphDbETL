"""
graph_db_builder.py
===================

Code for building new graph databases using PyGraphETL.

Algorithm overview
------------------
- Initialize database builder
    - Process configuration file
    - Connect to source databases
        - Connect to MySQL databases using `mysql.connector`
        - # TODO: Connect to other DBMS sources
        - # TODO: Get relative paths to tabular data / flat files
    - Initialize output tables
        - For each node type $N$ in $\mathcal{N}$:
            - Find all source databases $\mathcal{S}_N$ containing instances of $N$
            - Get a merged list of data fields (and their types) across $\mathcal{S}_N$
            - Initialize a PyTables table for $N$ with appropriate fields
        - For each relationship type $R$ in $\mathcal{R}$:
            - # TODO
- Build database
    - Build node tables
        - For each node type $N$:
            - For each source database $S$ containing instances of $N$:
                - Fetch the output table $T$ that holds instances of $N$
                - Map fields in $S$ to columns in $T$
                - Stream entries in $S$ to $T$, handling missing values as needed
    - Build relationship tables
        - # TODO
"""

from yaml import load
from dataclasses import dataclass, field
import logging
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

logging.basicConfig(level=logging.DEBUG)

@dataclass
class NodeType:
    node_type_label: str
    dest_table: tables.Table
    sources: list = field(default_factory=list)

@dataclass
class RelationshipType:
    rel_type_label: str
    start_node_type: NodeType
    end_node_type: NodeType
    dest_table: tables.Table
    sources: list = field(default_factory=list)

class GraphDBBuilder():
    """
    Builder class for constructing a graph database.

    Parameters
    ----------
    config_file_path : str
        Path to a configuration file. File should follow the format described
        in the PyGraphETL documentation (# TODO: write and provide link).
    mysql_config_file : str
        Path to a MySQL configuration file (e.g., `~/.my.cnf`) with read
        privileges for any MySQL source databases mentioned in the config file.
    """
    def __init__(self, config_file_path, mysql_config_file):
        logging.info("Reading configuration file.")
        self.config = read_config_file(config_file_path)
        self.mysql_config_file = mysql_config_file

        self.nodes = dict()  # keys: node labels; values: NodeType
        self.relationships = dict()  # keys: relationship type labels; values: RelationshipType

        try:
            if mysql_config_file:
                logging.info("Establishing MySQL connection...")
                self.mysql_conn = get_mysql_connection(mysql_config_file)
                logging.info("...done.")
            else:
                logging.warning("No MySQL configuration provided - was this intentional?")
                self.mysql_conn = get_mysql_connection()
        except mysql.connector.Error as err:
            logging.warning("Warning: Error connecting to MySQL using the provided connection details. Skipping MySQL sources.")
            print(err)
            self.mysql_conn = None

        self._process_config()

        self._initialize_tables()

    def build_hdf5_database(self):
        """
        Make an HDF5 file containing a graph database.

        The HDF5 version of the graph database is comprised of a set of Node
        tables (each table is a node type and each row in a table is a node,
        including node features), and a set of Relationship tables (similarly,
        each table corresponding to a relationship type and each row
        corresponding to a relationship plus any relationship features).

        Other PyGraphETL routines can be used to stream the HDF5 database into
        a GDMS (e.g., Neo4j).
        """
        # TODO: Make sure we've processed a configuration
        self.parse_nodes()
        self.parse_relationships()

    def _initialize_tables(self):
        """
        Create empty PyTables tables for all node types and relationship types.

        Notes
        -----
        This method is a bit more sophisticated than just creating an empty
        table for each node and relationship type. It has to actually determine
        the set of data fields and their corresponding (harmonized) datatypes
        across all relevant sources. In other words, it "peeks" into each
        source, finds out fields and their types, and then merges them.
        """
        # Go through each node, and create a list of (column_name, dtype) tuples

        db_name = self.config['Database']['name']
        db_version = self.config['Database']['version']

        # Make pytables file and groups
        logging.info("Making HDF5 database...")
        self.h5_file = tables.open_file("{0}-{1}.h5".format(db_name, db_version), mode='w', title=db_name)
        self.node_tables = self.h5_file.create_group("/", 'nodes', 'Node tables')
        self.relationship_tables = self.h5_file.create_group("/", 'relationships', 'Relationship tables')

        mysql_query_template = "SELECT * FROM {0} LIMIT 1;"

        node_tables_pre = {}
        relationship_tables_pre = {}

        # Determine node table names and fields
        logging.info(" Making tables for each node type...")
        for node_label, node_label_config in self.config['Nodes'].items():
            logging.info(f"  NODE TYPE: {node_label}")

            all_sources_fields = []
            for s_name, s_config in node_label_config['sources'].items():
                logging.info(f"   SOURCE DB: {s_name}")
                if self.source_type_map[s_name] == 'mysql':
                    logging.info(f"    DB TYPE: mysql")
                    # query the database, pull out fields
                    this_qry = mysql_query_template.format(s_config['table'])
                    this_cur = self.mysql_dbs[s_name].cursor(buffered=True)
                    this_cur.execute(this_qry)

                    field_descr = description_to_fields(this_cur.description)
                    field_names, field_types = list(zip(*field_descr))

                    # now, map fields to pytables types
                    np_types = [map_pytables[x] for x in field_types]
                    types_structured = list(zip(field_names, np_types))
                    logging.info(f"    FIELDS: {field_names}")
                else:
                    raise NotImplementedError

                all_sources_fields += types_structured
            
            # merge fields from all sources
            node_fields_merged = merge_fields(all_sources_fields)

            #data_descr = np.dtype(node_fields_merged)
            # See: https://stackoverflow.com/questions/58261748 ("Option 2")
            data_descr = make_table_dict_descr(node_fields_merged)

            node_tables_pre[node_label] = data_descr

        # Build the node tables
        for label, fields in node_tables_pre.items():
            tab_ref = make_table(self.h5_file, self.node_tables, label, fields)
            #ipdb.set_trace()
            # TODO: Need to feed sources in (4th argument)
            self._store_table_details(tab_ref, label, fields, None, 'node')

        # TODO: Rinse and repeat for relationship tables

        return True

    def _store_table_details(self, table_ref, node_or_rel_label, table_fields, table_sources, table_type):
        """Store internal description of an HDF5 table to be organized into a
        'directory of node tables'.
        
        We already created the HDF5 table, now we just are storing a reference
        to it and what the fields/sources for that table are.
        """

        if table_type == 'node':
            node = NodeType(
                node_type_label=node_or_rel_label,
                dest_table=table_ref,
                sources=table_sources
            )
            self.nodes[node_or_rel_label] = node

            logging.info("New node type added to PyGraphETL:")
            logging.info(node)
        elif table_type == 'relationship':
            rel = RelationshipType(
                rel_type_label=node_or_rel_label, start_node_type=None,
                end_node_type=None, dest_table=table_ref, sources=table_sources
            )
            self.relationships[node_or_rel_label] = rel

            logging.info("New relationship type added to PyGraphETL:")
            logging.info(rel)
        else:
            raise TypeError("`table_type` must one of {'node', 'relationship'}.")
        
    def _process_config(self):
        logging.info("Parsing configuration...")
        try:
            self.db_name = self.config['Database']['name']
            self.db_version = self.config['Database']['version']
            self.db_author = self.config['Database']['author']
            self.node_config = self.config['Nodes']
            self.rel_config = self.config['Relationships']
            self.source_config = self.config['Sources']

            mysql_dbs = dict()
            self.source_type_map = dict()

            logging.info(" Parsing sources:")
            for source_name, source_config in self.config['Sources'].items():
                logging.info(f"  {source_name}")
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
        """
        For each node type in the config, and for each source containing nodes
        of that type, read the node data and feed into the appropriate pytables
        object.
        """
        logging.info("Parsing nodes...")
        for node_label, node_config in self.config['Nodes'].items():
            logging.info(f"  node label: {node_label}")
            for this_node_source, source_options in node_config['sources'].items():
                logging.info(f"    source: {this_node_source}")
                self._parse_source_dispatcher(node_label, this_node_source, source_options)

    def parse_relationships(self):
        raise NotImplementedError
            
    def _parse_source_dispatcher(self, node_label, source_name, node_source_options):
        source_type = self.source_type_map[source_name]

        if source_type == 'mysql':
            source_cnx = self.mysql_dbs[source_name]
            source_table = node_source_options['table']
            source_id_key = node_source_options['id_key']
            source_uri_key = node_source_options['uri_key']

            #ipdb.set_trace()

            # For each table in the mysql source, find matching
            # destination tables

            dest_table = self.find_destination_tables(source_name)
            
            parse_mysql_source(source_cnx, node_label, source_table, source_id_key, source_uri_key, dest_table)

    def find_destination_tables(self, source_name):
        pass

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

def parse_mysql_source(cnx, node_label, source_table, source_id_key, source_uri_key, destination_table):
    cursor = cnx.cursor()
    sql_query = "SELECT * FROM {0};".format(source_table)
    cursor.execute(sql_query)

    # Stream results into the PyTables table

    result = safe_stream_mysql_to_pytable(cursor, destination_table)

    #ipdb.set_trace()

def safe_stream_mysql_to_pytable(mysql_cur, output_table):
    for query_res in mysql_cur:
        #ipdb.set_trace()
        #print()
        break

    # TODO: Return something intelligent to check for possible errors
    return 1

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

    tab = h5file.create_table(group, table_name, table_fields, "{0} table".format(table_name))

    return tab