import shutil

from _common import ONE_GB, ONE_MB, create_database, put, EXAMPLE_KEY, delete, \
    calculate_largest_possible_entry


def run():
    map_size = ONE_MB
    while map_size < ONE_GB:
        map_size *= 2
        run_with_map_size(map_size)
        print("")


def run_with_map_size(map_size):
    print("Running with map size %d" % map_size)

    division = 10
    for i in range(division):
        proportion_of_capacity = (1.0 / division) * (i + 1)
        run_with_map_size_and_proportion_of_capacity(map_size, proportion_of_capacity)


def run_with_map_size_and_proportion_of_capacity(map_size, proportion_of_capacity):
    database, database_location = create_database(map_size)

    available_size = calculate_largest_possible_entry(database, map_size)
    content = bytearray(int(available_size * proportion_of_capacity))

    print("Testing with content of %d%% capacity with a database of %d bytes"
          % ((100 * proportion_of_capacity), map_size))

    for i in range(100):
        try:
            put(EXAMPLE_KEY, content, database)
        except Exception as e:
            print("Exception adding content on iteration %d: %s" % (i + 1, e))
            break
        try:
            deleted = delete(EXAMPLE_KEY, database)
            assert deleted
        except Exception as e:
            print("Exception deleting content on iteration %d: %s" % (i + 1, e))
            break

    shutil.rmtree(database_location)


if __name__ == "__main__":
    run()
