import shutil

from _common import ONE_GB, ONE_MB, create_database, put, \
    EXAMPLE_KEY, delete, calculate_largest_possible_entry


def run():
    map_size = ONE_MB
    while map_size < ONE_GB:
        map_size *= 2
        run_with_map_size(map_size)
        print("")


def run_with_map_size(map_size):
    print("Running with map size %d" % map_size)
    database, database_location = create_database(map_size)

    available_size = calculate_largest_possible_entry(database, map_size)
    content = bytearray(int(available_size / 2))

    for i in range(100):
        try:
            put(EXAMPLE_KEY, content, database)
            print("Added content")
        except Exception as e:
            print("Exception adding content on iteration %d" % (i + 1))
            break
        print("Deleting previously added content")
        deleted = delete(EXAMPLE_KEY, database)
        assert deleted

    shutil.rmtree(database_location)


if __name__ == "__main__":
    run()
