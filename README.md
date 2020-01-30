## Introduction

This is a simple python script to attempt to collect Fedora 3 statistics and generate some reports.

It has two distinct processes that can be run separately or together. These are:

1. Collecting statistics
    Running the script with the `-o` parameter will attempt to collect statistics from the object store path and storing them in the SQLlite3 database.
1. Generating reports
    Running the script with the --print-yearly will generate the report from the statistics in the SQLlite3 database.

These steps can be run together, but as the statistic gathering will take time it is desirable to be able to run reports without parsing the filesystem each time.

**Note**: This tool needs to be able to read all of the objectStore files, so you may need to run it with `sudo`. If it encounters a file it cannot access it will fail with a relavant warning.

## Usage

```bash
usage: processor.py [-h] [-o OBJECT_DIR] [-w] [-p] db_file

positional arguments:
  db_file               Sqllite3 database file

optional arguments:
  -h, --help            show this help message and exit
  -o OBJECT_DIR, --obj-dir OBJECT_DIR
                        ObjectStore directory to be scanned.
  -w, --wipe-db         Erase the DB tables if they exist.
  -p, --print-yearly    Print yearly statistics.
```
  
### Examples

1. Parse the Fedora 3 object store to the database.
    ```
    ./processor.py -o /path/to/objectstore my\_data\_file
    ```
    **Caveat**: I hope to have this work incrementally (ie. add only new entries), but currently it will just skip objects that already exist in the database entirely.

2. Parse the Fedora 3 object store and clear the database first.
    ```
    ./processor.py --obj-dir /path/to/objectstore --wipe-db my\_data\_file
    ```

3. Generate statistics from the database.
    ```
    ./processor.py --print-yearly my\_data\_file
    ```

4. Parse the file system and then print the report.
    ```
    ./processor.py -o /path/to/objectstore --print-yearly my\_data\_file
    ```

## License
* MIT

## In progress
* Allow additional runs to just add new/update existing data. 