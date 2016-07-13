import shutil
from math import floor, ceil
from tempfile import mkdtemp

import lmdb

ONE_MB = 1 * 1024 * 1024
ONE_GB = ONE_MB * 1024


def run():
    map_size = ONE_MB
    while map_size < ONE_GB:
        map_size *= 2
        run_with_map_size(map_size)


def run_with_map_size(map_size):
    actual_size = calculate_largest_possible_entry(create_database(map_size)[0], map_size)
    for i in range(20):
        content_size = int((i + 1) * 0.05 * actual_size)
        content = bytearray(content_size)
        success = create_and_insert(content, map_size)
        percentage_capacity = ceil((float(content_size) / float(actual_size)) * 100)
        print("Insert %d bytes into database with map_size %d (%d%% actual capacity)... %s"
              % (content_size, map_size, percentage_capacity, "Success" if success else "Fail"))
    print("")


def create_and_insert(content, map_size):
    database, database_directory = create_database(map_size)
    try:
        put(content, database)
        return True
    except:
        return False
    finally:
        shutil.rmtree(database_directory)


def create_database(map_size):
    database_directory = mkdtemp()
    return lmdb.open(database_directory, writemap=True, map_size=map_size), database_directory


def put(content, database):
    with database.begin(write=True) as transaction:
        transaction.put("my_data", content)


def calculate_largest_possible_entry(database, map_size):
    page_size = database.stat()["psize"]
    max_key_size = database.max_key_size()
    available_pages = floor(map_size / page_size)
    available_size = int((available_pages * page_size) - max_key_size - 16)
    assert calculate_stored_size(database, bytearray(available_size)) == map_size
    return available_size


def calculate_stored_size(database, content):
    size = len(content)
    page_size = database.stat()["psize"]
    max_key_size = database.max_key_size()
    return int(ceil(float(16 + max_key_size + size) / float(page_size)) * page_size)


if __name__ == "__main__":
    run()
