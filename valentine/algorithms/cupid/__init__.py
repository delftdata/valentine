DATATYPE_COMPATIBILITY_TABLE = {
    "text": {"keyword": 1.0, "varchar": 1.0, "nvarchar": 0.9, "nchar": 0.8, "char": 0.6},
    "keyword": {"text": 1.0, "varchar": 1.0, "nvarchar": 0.9, "nchar": 0.8, "char": 0.6},
    "varchar": {"text": 1.0, "keyword": 1.0, "nvarchar": 0.9, "nchar": 0.8, "char": 0.6, "int": 0.1},
    "nvarchar": {"text": 0.9, "keyword":  0.9, "varchar": 0.9, "nchar": 0.8, "char": 0.6},
    "nchar": {"text": 0.7, "keyword":  0.7, "varchar": 0.7, "nvarchar": 1.0, "char": 0.7},
    "char": {"text": 0.7, "keyword":  0.7, "varchar": 0.7, "nchar": 0.8, "nvarchar": 0.6},
    "date": {"double": 0.1, "int": 0.1, "decimal": 0.1, "bit": 0.1},
    "double": {"date": 0.1, "float": 1.0, "decimal": 1.0},
    "decimal": {"date": 0.1, "float": 1.0, "double": 1.0},
    "int": {"date": 0.1, "long": 0.8, "short": 0.7, "smallint": 0.7, "integer": 1.0, "varchar": 0.1},
    "integer": {"date": 0.1, "long": 0.8, "short": 0.7, "smallint": 0.7, "int": 1.0},
    "bit": {"time": 0.1, "date": 0.1},
    "time": {"bit": 0.1},
    "float": {"double": 0.9},
    "long": {"short": 0.6, "int": 0.8, "bigint": 1.0, "smallint": 0.6, "integer": 0.8},
    "bigint": {"short": 0.6, "int": 0.8, "long": 1.0, "smallint": 0.6, "integer": 0.8},
    "short": {"long": 0.6, "int": 0.8, "bigint": 0.6, "smallint": 1.0, "integer": 0.8},
    "smallint": {"long": 0.6, "int": 0.8, "bigint": 0.6, "short": 1.0, "integer": 0.8}
}

__all__ = [
    "cupid_model",
]
