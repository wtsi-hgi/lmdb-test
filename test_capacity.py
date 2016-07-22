import shutil
from math import ceil

from _common import ONE_MB, ONE_GB, create_database, put, EXAMPLE_KEY, \
    calculate_largest_possible_entry


def run():
    map_size = ONE_MB
    while map_size < ONE_GB:
        map_size *= 2
        run_with_map_size(map_size)


def run_with_map_size(map_size):
    actual_size = calculate_largest_possible_entry(create_database(map_size)[0], map_size)
    divisions = 20
    for i in range(divisions):
        content_size = int((i + 1) * (1.0 / float(divisions)) * actual_size)
        content = bytearray(content_size)
        success = create_and_insert(content, map_size)
        percentage_capacity = ceil((float(content_size) / float(actual_size)) * 100)
        print("Insert %d bytes into database with map_size %d (%d%% actual capacity)... %s"
              % (content_size, map_size, percentage_capacity, "Success" if success else "Fail"))
    print("")


def create_and_insert(content, map_size):
    database, database_directory = create_database(map_size)
    try:
        put(EXAMPLE_KEY, content, database)
        return True
    except:
        return False
    finally:
        shutil.rmtree(database_directory)


if __name__ == "__main__":
    run()
