from math import ceil
from tempfile import mkdtemp

import lmdb

ONE_MB = 1 * 1024 * 1024
ONE_GB = ONE_MB * 1024


def create_database(map_size):
    database_directory = mkdtemp()
    return lmdb.open(database_directory, writemap=True, map_size=map_size), database_directory


def put(content, database):
    with database.begin(write=True) as transaction:
        transaction.put("my_data", content)


def calculate_stored_size(database, content):
    size = len(content)
    page_size = database.stat()["psize"]
    max_key_size = database.max_key_size()
    return int(ceil(float(16 + max_key_size + size) / float(page_size)) * page_size)