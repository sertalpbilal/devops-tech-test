import sys
import re
import os
import mysql.connector

def matchSqlScriptVersion(filename):
    # We don't care about leading zeroes, but we want to be able to handle any positive integer
    return re.search("^([0-9]*[1-9][0-9]*)(.*)(\\.sql)$", filename)

def updateDbVersion(new_version, db_cursor):
    try: 
        print("Updating db to version: " + str(new_version))
        db_cursor.execute("UPDATE versionTable SET version='" + str(new_version) + "'")
    except Exception as e:
        print("Aborting upgrade. Error updating database version.", e)
        sys.exit()

def getCurrentDbVersion(db_cursor):
    try: 
        db_cursor.execute("SELECT version FROM versionTable")
        ver = db_cursor.fetchone()[0] # Only ever 1 row
        print("Current db version: " + str(ver))
        return ver
    except Exception as e:
        print("Aborting upgrade. Error retrieving current version.", e)
        sys.exit()

def executeDbUpdates(version_to_scripts, db_cursor):
    for version, script in version_to_scripts:
        print("Executing upgrade script version: " + str(version))
        for statement in script.split(";"): # Future scripts may have more than one statement in them
            try: 
                db_cursor.execute(statement)
            except Exception as e:
                print("Aborting upgrade. Error executing script version: " + str(version), e)
                sys.exit()

def getOrderedValidScripts(directory, current_db_version):
    upgrade_scipts_by_version = {}
    for filename in os.listdir(directory):
        valid_script_match = matchSqlScriptVersion(filename)
        if (valid_script_match is not None): # Is a .sql file prepended with a valid version number
            version = int(valid_script_match.group(1).lstrip("0")) # Get the matched positive integer, without leading zeroes
            if (version > current_db_version):
                if (version not in upgrade_scipts_by_version):
                        upgrade_scipts_by_version[version] = open(directory + filename).read()
                else:
                    print("Aborting upgrade. Conflicting scripts with version: " + str(version))
                    sys.exit()
    return sorted(upgrade_scipts_by_version.items())

def getDbConnection(db_user, db_host, db_name, db_password):
    return mysql.connector.connect(
            user=db_user,
            host=db_host,
            database=db_name,
            password=db_password
        )

if __name__ == "__main__":
    db_connection = getDbConnection(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
    db_cursor = db_connection.cursor()

    current_db_version = getCurrentDbVersion(db_cursor)
    upgrade_scipts_by_version = getOrderedValidScripts(sys.argv[1], current_db_version)

    if (len(upgrade_scipts_by_version) > 0):
        executeDbUpdates(upgrade_scipts_by_version, db_cursor)
        updateDbVersion(upgrade_scipts_by_version[-1][0], db_cursor) # Update to the version number of the last tuple of versions to script tuples
        db_connection.commit() # Commit once upon completion, if the script exits before this point our transaction will be discarded
        print("Database upgrade complete")
    else:
        print("Database version is up to date")
    
    db_cursor.close()
    db_connection.close()