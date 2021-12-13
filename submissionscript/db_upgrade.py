import os
import re
import sys

import mysql.connector


def match_sql_script_version(filename):
    # We don't care about leading zeroes, but we want to be able to handle any positive integer
    return re.search("^([0-9]*[1-9][0-9]*)(.*)(\\.sql)$", filename)


def update_db_version(new_version, db_cursor):
    try:
        print("Updating db to version: " + str(new_version))
        db_cursor.execute("UPDATE versionTable SET version='" + str(new_version) + "'")
    except Exception as e:
        print("Aborting upgrade. Error updating database version.", e)
        sys.exit()


def get_current_db_version(db_cursor):
    try:
        db_cursor.execute("SELECT version FROM versionTable")
        ver = db_cursor.fetchone()[0]  # Only ever 1 row
        print("Current db version: " + str(ver))
        return ver
    except Exception as e:
        print("Aborting upgrade. Error retrieving current version.", e)
        sys.exit()


def execute_db_updates(version_to_scripts, db_cursor):
    for version, script in version_to_scripts:
        print("Executing upgrade script version: " + str(version))
        # Future scripts may have more than one statement in them
        for statement in script.split(";"):
            try:
                db_cursor.execute(statement)
            except Exception as e:
                print(
                    "Aborting upgrade. Error executing script version: " + str(version), e)
                sys.exit()


def get_ordered_valid_scripts(directory, current_db_version):
    upgrade_scipts_by_version = {}
    for filename in os.listdir(directory):
        valid_script_match = match_sql_script_version(filename)
        if (valid_script_match is not None):  # Is a .sql file prepended with a valid version number
            # Get the matched positive integer, without leading zeroes
            version = int(valid_script_match.group(1).lstrip("0"))
            if (version > current_db_version):
                if (version not in upgrade_scipts_by_version):
                    upgrade_scipts_by_version[version] = open(directory + filename).read()
                else:
                    print(f"Aborting upgrade. Conflicting scripts with version: {version}")
                    sys.exit()
    return sorted(upgrade_scipts_by_version.items())


def get_db_connection(db_user, db_host, db_name, db_password):
    return mysql.connector.connect(
        user=db_user,
        host=db_host,
        database=db_name,
        password=db_password
    )


if __name__ == "__main__":
    db_connection = get_db_connection(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
    db_cursor = db_connection.cursor()

    current_db_version = get_current_db_version(db_cursor)
    upgrade_scipts_by_version = get_ordered_valid_scripts(sys.argv[1], current_db_version)

    if (len(upgrade_scipts_by_version) > 0):
        execute_db_updates(upgrade_scipts_by_version, db_cursor)
        # Update to the version number of the last tuple of versions to script tuples
        update_db_version(upgrade_scipts_by_version[-1][0], db_cursor)
        # Commit once upon completion, if the script exits before this point our transaction will be discarded
        db_connection.commit()
        print("Database upgrade complete")
    else:
        print("Database version is up to date")

    db_cursor.close()
    db_connection.close()
