from math import ceil, floor
from tempfile import mkdtemp

import lmdb

EXAMPLE_KEY = "my_content"
ONE_MB = 1 * 1024 * 1024
ONE_GB = ONE_MB * 1024


def create_database(map_size):
    database_directory = mkdtemp()
    return lmdb.open(database_directory, writemap=True, map_size=map_size), database_directory


def put(key, content, database):
    with database.begin(write=True) as transaction:
        transaction.put(key, content)


def delete(key, database):
    with database.begin(write=True) as transaction:
        return transaction.delete(key)


def calculate_stored_size(content, database):
    size = len(content)
    page_size = database.stat()["psize"]
    max_key_size = database.max_key_size()
    return int(ceil(float(16 + max_key_size + size) / float(page_size)) * page_size)


def calculate_largest_possible_entry(database, map_size):
    page_size = database.stat()["psize"]
    fixed_cost = 4 * page_size + 16
    max_key_size = database.max_key_size()
    available_pages = floor(map_size / page_size)
    available_size = int((available_pages * page_size) - max_key_size - 16) - fixed_cost
    assert calculate_stored_size(bytearray(available_size + fixed_cost), database) == map_size
    return available_size
