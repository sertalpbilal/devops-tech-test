# DevOps Tech Test

## Preperation

Please Clone this repository and push it up to your own github repository.
Do **not** fork this repository

## Challenge

The following use case might be a real-life example from one of our customers, please deliver your best possible solution. Please go through the described scenario and write a script, in one of the below languages, implementing a fix to the issue below.

For the development of the scripts you have 4 hours and are allowed to use Google and any other material as long as the work submitted was written by you.

### Use Case

- A database upgrade requires the execution of numbered SQL scripts stored in a specified folder, named such as `045.createtable.sql`
  - Sample scripts are provided in the `dbscripts` directory
- The scripts may contain any simple SQL statement(s) to any table of your choice, e.g. `INSERT INTO testTable VALUES("045.createtable.sql");`
- There may be gaps in the SQL file name numbering and there isn't always a . (dot) after the beginning number
- The database upgrade is based on looking up the current version in the database and comparing this number to the numbers in the script names
- The table where the current db version is stored is called `versionTable`, with a single row for the version, called `version`
- If the version number from the db matches the highest number from the scripts then nothing is executed
- All scripts that contain a number higher than the current db version will be executed against the database in numerical order
- In addition, the database version table is updated after the script execution with the executed script's number
- Your script will be executed automatically via a program, and must satisfy these command line input parameters exactly in order to run:
  - `./db-upgrade.your-lang directory-with-sql-scripts username-for-the-db db-host db-name db-password`
  - Example (bash): `python3 /submissionscript/db_upgrade.py /scripts/ dev mysql_container devopstt 123456`

### Requirements

- Supported Languages (No other languages will be accepted):
  - Bash
  - Python 3
  - Ruby 2.7
  - Powershell 7
- You will have to use a MySQL 8.0 database

How would you implement this in order to create an automated solution to the above requirements?

## Submission

Please send us:

- a link to your github repository
- any associated notes for our review

We will come back to you asap regarding next steps.

We are looking forward to your submission.

## Environment Setup

### Adding your solution

Add you submission to the `submissionsscript` directory.

This will make the script available within the container due to a volume mount within `docker-compose.yml`

### Running the containers

To start the testing environment please run:

```sh
docker-compose up -d
```

This will create two containers called:

- exec_container
- mysql_container

Required language dependencies are installed in the `exec_container`, your solution should be invoked on the `exec_container`.

### Adding script dependencies

Any other dependencies you require to complete the tech test should be added to the `entrypoint.sh` file in the root directory of the repository.

e.g. `pip3 install mysql-client`

This ensures they are automatically installed when the container is run. Once dependencies have been added to the file you must restart the environment for them to take effect.

```sh
docker compose restart -d
```

**do not delete** `sleep infinity` leave this as the last command in `entrypoint.sh`

### Testing your script

Once you're ready to test you script you can connect to the `exec_container`. Due to the volume mount mentioned in [adding your solution](#adding-your-solution) it will already be available within the `exec-container`.

```sh
docker exec -it exec_container /bin/bash
```

Run your script using

```sh
python3 /submissionscript/db_upgrade.py /scripts/ dev mysql_container devopstt 123456
```

You can then run the automated test to check if successful

``` sh
pytest /scripts/db_test.py
```

## Database credentials

The database credentials are set in `docker-compose.yml` and are as follows;

```
User: dev
Password: 123456
Database name: devopstt
Database host: mysql_container
```