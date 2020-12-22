"""
dtypes.py

Dicts used to map datatypes from one format into another.

PyTables supported datatypes
----------------------------

.. table:: **Data types supported for PyTables (copied from the PyTables documentation).**

    ================== ========================== ====================== =============== ==================
    Type Code          Description                C Type                 Size (in bytes) Python Counterpart
    ================== ========================== ====================== =============== ==================
    bool               boolean                    unsigned char          1               bool
    int8               8-bit integer              signed char            1               int
    uint8              8-bit unsigned integer     unsigned char          1               int
    int16              16-bit integer             short                  2               int
    uint16             16-bit unsigned integer    unsigned short         2               int
    int32              integer                    int                    4               int
    uint32             unsigned integer           unsigned int           4               long
    int64              64-bit integer             long long              8               long
    uint64             unsigned 64-bit integer    unsigned long long     8               long
    float16            half-precision float       -                      2               -
    float32            single-precision float     float                  4               float
    float64            double-precision float     double                 8               float
    float96 [1]_       extended precision float   -                      12              -
    float128 [1]_      extended precision float   -                      16              -
    complex64          single-precision complex   struct {float r, i;}   8               complex
    complex128         double-precision complex   struct {double r, i;}  16              complex
    complex192         extended precision complex -                      24              -
    complex256         extended precision complex -                      32              -
    string             arbitrary length string    char[]                 *               str
    time32             integer time               POSIX's time_t         4               int
    time64             floating point time        POSIX's struct timeval 8               float
    enum               enumerated value           enum                   -               -
    ================== ========================== ====================== =============== ==================

.. rubric:: Footnotes

.. [1] currently in numpy_. "float96" and "float128" are equivalent of
       "longdouble" i.e. 80 bit extended precision floating point.

MySQL Python connector datatypes
--------------------------------

.. table:: **Datatypes returned by MySQL queries in Python using `mysql-connector-python`.**

    ============== ================= =============
    Type name      Field type number Type category
    ============== ================= =============
    DECIMAL        0                 number
    TINY           1                 number
    SHORT          2                 number
    LONG           3                 number
    FLOAT          4                 number
    DOUBLE         5                 number
    TIMESTAMP      7                 timestamp
    LONGLONG       8                 number
    INT24          9                 number
    DATETIME       12                timestamp
    YEAR           13                number
    VARCHAR        15                string
    BIT            16                number
    NEWDECIMAL     246               number
    ENUM           247               string
    TINY_BLOB      249               binary
    MEDIUM_BLOB    250               binary
    LONG_BLOB      251               binary
    BLOB           252               binary
    VAR_STRING     253               string
    STRING         254               string
    ============== ================= =============
    
"""

import tables as tb
import numpy as np

# see also:
# http://mysql-python.sourceforge.net/MySQLdb-1.2.2/public/MySQLdb.constants.FIELD_TYPE-module.html
map_numpy = {
    'VAR_STRING': str,
    'LONG': np.int32,
    'FLOAT': np.float
}

map_pytables = {
    'VAR_STRING': tb.StringCol(256),  # 256 is relatively arbitrary - pytables doesn't support variable length strings
    'LONG': tb.Int32Col(),
    'FLOAT': tb.Float32Col()
}