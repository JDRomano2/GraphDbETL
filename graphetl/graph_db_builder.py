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
import unicodedata

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

def safe_itemgetter(*items):
    if len(items) == 1:
        item = items[0]
        def g(obj):
            return obj[item]
    else:
        def g(obj):
            return tuple(obj[item] for item in items )

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
        self.source_field_lists = dict()  # dict (key - source) of dicts (key - table; value - list of field names)

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

                    # If we haven't seen this source already, store field names

                    # Make sure we have a dict for the source
                    if not s_name in self.source_field_lists:
                        self.source_field_lists[s_name] = dict()
                    # Then, add the field names for the table to this dict
                    if not s_config['table'] in self.source_field_lists[s_name]:
                        self.source_field_lists[s_name][s_config['table']] = field_names

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
            # Make the table
            tab_ref = make_table(self.h5_file, self.node_tables, label, fields)
            # Store reference to table along with metadata
            self._store_table_details(tab_ref, label, fields, 'node')

        # TODO: Rinse and repeat for relationship tables

        return True

    def _store_table_details(self, table_ref, node_or_rel_label, table_fields, table_type):
        """Store internal description of an HDF5 table to be organized into a
        'directory of node tables'.
        
        We already created the HDF5 table, now we just are storing a reference
        to it and what the fields/sources for that table are.
        """

        if table_type == 'node':
            node = NodeType(
                node_type_label=node_or_rel_label,
                dest_table=table_ref,
                #sources=table_sources
            )
            self.nodes[node_or_rel_label] = node

            logging.info("New node type added to PyGraphETL:")
            logging.info(node)

            # Add source mapping info
            # TODO: Can we move this out of the 'if' block and repurpose for rels?
            for s_name, s_config in self.config['Nodes'][node_or_rel_label]['sources'].items():
                self.add_source_to_node_type(self.nodes[node_or_rel_label], s_name, s_config, table_fields)
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
    
    def add_source_to_node_type(self, node_type, source_name, source_config, target_fields):
        """
        Append details for a data source to a specific NodeType instance.

        Parameters
        ----------
        node_type : NodeType
            Instance of NodeType to which we will add a new source.
        source_name : str
            String name of the source as given in the config file/dict. 
        source_config : dict
            Dictionary containing the config information for the source with
            respect to the current node type, as given in the config file/dict.
        source_fields : list
        target_fields : list
        """

        #ipdb.set_trace()
        source_fields = self.source_field_lists[source_name][source_config['table']]

        # Make mapping function (maps source fields to target fields).
        # If a target field isn't defined for a source, the value should map to
        # `None`.
        field_idx_map = []
        for field in target_fields:
            # Find field in source (if it exists)
            try:
                field_idx_map.append(source_fields.index(field))
            except ValueError:
                field_idx_map.append(None)

        node_type.sources.append({
            'source_name': source_name,
            'source_table_name': source_config['table'],
            'config_file_data': source_config,
            'field_names': source_fields,
            'field_idx_map': field_idx_map
        })
        
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
        """
        Infer and then call the correct function to parse a node type from a single source.

        The actual parsing functions are implemented outside of the GraphDBBuilder class. 

        Parameters
        ----------
        node_label : str
            Node label for the class of node you are going to parse.
        source_name : str
            Name of the source database from which nodes will be parsed.
        node_source_options : dict
            Dict containing other necessary details about the source pertaining
            to the current node label.
        """
        source_type = self.source_type_map[source_name]

        sources = self.nodes[node_label].sources
        this_source = next(s for s in sources if s['source_name'] == source_name)
        
        source_fields = this_source['field_names']
        source_field_idx_map = this_source['field_idx_map']

        if source_type == 'mysql':
            source_cnx = self.mysql_dbs[source_name]
            source_name = this_source['source_name']
            source_table = node_source_options['table']
            source_id_key = node_source_options['id_key']
            source_uri_key = node_source_options['uri_key']

            # For each table in the mysql source, find matching
            # destination tables

            dest_table = self.find_destination_table(node_label)
            
            parse_mysql_source(source_cnx, node_label, source_name, source_table, 
                               source_id_key, source_uri_key, dest_table,
                               source_fields, source_field_idx_map)

    def find_destination_table(self, node_label):
        """
        Return a reference to the PyTables object corresponding to a certain
        node label.

        Parameters
        ----------
        node_label : str
            Node label corresponding to the desired destination table.

        Returns
        -------
        tables.Table
            PyTables table corresponding to `node_label`.
        """
        return self.nodes[node_label].dest_table

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

def parse_mysql_source(cnx, node_type, source_name, source_table, source_id_key,
                       source_uri_key, destination_table, source_fields,
                       source_field_idx_map):
    """
    Parse records from a source MySQL table and stream the results into a
    destination PyTables table.
    
    Arguments
    ---------
    cnx : mysql.connector.connection_cext.CMySQLConnection
        An active connection to a MySQL database containing the source tables,
        with read permissions.
    node_type : NodeType
        Instance of NodeType describing the node type that will be populated.
    source_name : str
        Name of the source database containing nodes of interest.
    source_table : str
        Name of the table in the source database containing nodes of interest.
    source_id_key : str
        Column name corresponding to the main ID key used to determine whether
        a node has already been seen (e.g., 'do we merge the node data into an
        existing record in the output table, or do we create a new record?')
    source_uri_key : str
        Column name that will be used to determine the URI of the node in the
        output graph database.
    destination_table : tables.Table
        A PyTables table object where the parsed nodes will be placed.
    source_fields : tuple of str
        Ordered tuple of fields corresponding to columns in the data source.
    source_field_idx_map : list of int
        List of indices of the source table mapped to their equivalent order in
        the destination table. If a source table doesn't contain one (or more)
        of the fields present in the destination table, they should map to
        `None` (i.e., they should be filled with the default value for that
        field).
    """
    cursor = cnx.cursor()
    sql_query = "SELECT * FROM {0};".format(source_table)
    cursor.execute(sql_query)

    # Stream results into the PyTables table
    result = safe_stream_mysql_to_pytable(cursor, destination_table, source_fields, source_field_idx_map)

def convert_fields_from_descr():
    pass

def safe_stream_mysql_to_pytable(mysql_cur, output_table, qry_fields, map_idxs, verbosity=0):
    """
    Stream results of a MySQL query into a PyTables table.

    Notably, this function maintains a record of already-seen nodes (based on
    the primary ID) and either merges data or creates a new entry, accordingly.

    Rows are appended to the table individually.

    TODO: Add functionality to insert data in blocks, for the sake of
    efficiency.

    Arguments
    ---------

    """

    # TODO: Check qry_fields against fields in destination_table to make sure
    # everything matches up

    BLOCK_SIZE = 10 # number of rows to add at once
    
    #ipdb.set_trace()
    # block = []  # a block of rows we are going to add to the table
    row = output_table.row
    for query_res in tqdm(mysql_cur):
        #row = [query_res[x] for x in map_idxs]
        # Use dict-like API for inserting data by field name rather than as an
        # ordered tuple.
        # TODO: Evaluate speed difference of these two alternate methods
        for f, d in zip(qry_fields, query_res):
            try:
                row[f] = d
            except TypeError as e:
                if d is None:
                    continue
                row[f] = unicodedata.normalize('NFKD', d).encode('ascii', 'ignore')
                #ipdb.set_trace()
                #print(e)
        row.append()

        #ipdb.set_trace()
        #block.append(row)
        # if len(block) == BLOCK_SIZE:
        #     output_table.append(block)
        #     block.clear()
    # Edge case - for last len(mysql_cur) \ BLOCK_SIZE rows
    # output_table.append(block)
    # block.clear()

    #ipdb.set_trace()
    # TODO: Return something intelligent to check for possible errors
    return 1

def read_config_file(conf_file_path):
    """Parse a YAML config file for PyGraphETL."""
    with open(conf_file_path, 'r') as cf:
        yml_config = load(cf, Loader=Loader)
    return yml_config

def make_table_dict_descr(field_tuples):
    descr = defaultdict()
    for ft_name, ft_type in field_tuples:
        descr[ft_name] = ft_type
    return descr

def make_table(h5file, group, table_name, table_fields):
    """Create a PyTables table to store graph data in HDF5 format.
    """

    tab = h5file.create_table(group, table_name, table_fields, "{0} table".format(table_name))

    return tab