import json
import mysql.connector

db_connection = mysql.connector.connect(
  host="mysql_container",
  user="dev",
  passwd="123456",
  database="devopstt"
)
db_cursor = db_connection.cursor(dictionary=True)

def test_version():
    db_cursor.execute("SELECT max(version) as version FROM versionTable;")
    resultVersion = db_cursor.fetchone()
    f = open('scripts/expecteddbstate/versionTable.json')
    version = json.load(f)
    assert resultVersion == version

def test_appTable():
    db_cursor.execute(f"SELECT * FROM appTable;")
    result = db_cursor.fetchone()
    fileoutput = open("scripts/expecteddbstate/appTable.json")
    test = json.load(fileoutput)
    assert result == test

def test_someTable():
    db_cursor.execute(f"SELECT * FROM someTable;")
    result = db_cursor.fetchone()
    fileoutput = open("scripts/expecteddbstate/someTable.json")
    test = json.load(fileoutput)
    assert result == test
    