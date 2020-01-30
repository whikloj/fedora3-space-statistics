#!/usr/bin/env python3

import argparse
import os.path
import sqlite3
from models import Object

db_file = None
objects = {}


def progress_bar(message, last=None):
    steps = [
        '|', '/', '-', '\\',
    ]
    next_step = steps[steps.index(last)+1] if last is not None and steps.index(last) < len(steps) - 1 else steps[0]
    print("\r%s .... %s" % (message, next_step), end="")
    return next_step


def parse_dir():
    file_counter = 0
    progress = None
    for path, dirs, files in os.walk(args.object_dir, followlinks=True):
        for name in files:
            if name == '.' or name == '..':
                continue
            filename = os.path.join(path, name)
            if not os.access(filename, os.R_OK):
                print("Cannot read file ({}), must be able to read all files for accurate statistics. Exiting".format(
                    filename
                ))
                quit(1)
            file_counter += 1
            progress = progress_bar("Processing file number {}".format(file_counter), progress)
            obj = Object(filename)
            write_to_db(obj)
    print("\r\033[K", end="")
    print("\rParsed {} files".format(file_counter))


def write_to_db(obj):
    if not pid_exists(obj.get_pid()):
        write_obj_to_db(obj)
        for ds in obj.get_datastreams():
            write_ds_to_db(ds)


def write_obj_to_db(obj):
    values = obj.get_data()
    sql = '''   INSERT INTO objects VALUES (
                :pid,
                :owner,
                :label,
                :created,
                :last_modified,
                :object_size,
                :total_size); '''
    db_write(sql, values)


def write_ds_to_db(datastream):
    values = datastream.get_data()
    sql = '''   INSERT INTO datastreams VALUES (
                :parent_pid,
                :dsid,
                :label,
                :mimetype,
                :total_size); '''
    db_write(sql, values)

    versions = datastream.get_versions()
    for created, size in versions:
        version_id = get_next_version(values['parent_pid'], values['dsid'])
        data = {
            'parent_pid': values['parent_pid'],
            'dsid': values['dsid'],
            'created': created.isoformat(),
            'size': size,
            'version_id': version_id
        }
        sql = '''   INSERT INTO datastream_versions VALUES (
                    :parent_pid, 
                    :dsid, 
                    :version_id, 
                    :created, 
                    :size); '''
        db_write(sql, data)


def pid_exists(pid):
    conn = get_db_connection()
    statement = "SELECT pid from objects WHERE pid = ?"
    with conn:
        c = conn.cursor()
        c.execute(statement, (pid.strip(),))
        pid = c.fetchone()
        c.close()
    return pid is not None


def get_next_version(object_id, datastream_id):
    sql = "SELECT MAX(version_num) from datastream_versions WHERE object_id = ? and dsid = ?"
    connection = get_db_connection()
    with connection:
        c = connection.cursor()
        c.execute(sql, (object_id, datastream_id))
        id = c.fetchone()[0]
    if id is None:
        id = 0
    id += 1
    return id


def db_write(statement, values=None):
    connection = get_db_connection()
    with connection:
        c = connection.cursor()
        if values is not None:
            c.execute(statement, values)
        else:
            c.execute(statement)
        c.close()
        connection.commit()


def get_db_connection():
    try:
        return sqlite3.connect(db_file)
    except sqlite3.Error as e:
        print("Error connecting to database: {}".format(e))


def setup():
    global db_file
    db_file = args.db_file
    if args.wipe_db and os.path.exists(db_file):
        if args.object_dir is None:
            print("You do not want this. You can't wipe the database without rebuilding it. Manually "
                  "delete the file if you just want it removed.")
            quit()
        db_write("DROP TABLE datastream_versions;")
        db_write("DROP TABLE datastreams;")
        db_write("DROP TABLE objects;")
    if not os.path.exists(db_file) or args.wipe_db:
        sql = '''   CREATE TABLE objects (
                    pid text,
                    owner text,
                    label text,
                    created text,
                    modified text,
                    object_storage integer,
                    total_storage integer); '''
        db_write(sql)
        sql = '''   CREATE TABLE datastreams (
                    object_id text,
                    dsid text,
                    label text,
                    mimetype text,
                    storage integer); '''
        db_write(sql)
        sql = '''   CREATE TABLE datastream_versions (
                    object_id text,
                    dsid text,
                    version_num integer,
                    created text,
                    size integer); '''
        db_write(sql)


def print_yearly():
    conn = get_db_connection()
    information = {}
    with conn:
        sql = "SELECT created, object_storage from objects;"
        c = conn.cursor()
        c.execute(sql)
        iterate_cursor(information, c, 'objects')
        c.close()

        sql = "SELECT created, size FROM datastream_versions;"
        c = conn.cursor()
        c.execute(sql)
        iterate_cursor(information, c, 'datastreams')
        c.close()

    years = list(information.keys())
    years.sort()
    print("Digital Storage By Year\n")
    row = ["Year", "Objects", "Size (bytes)", "Datastreams", "Size (bytes)"]
    col_width = max(len(str(information[p1][p2][p3])) for p1 in information for p2 in information[p1] for p3 in information[p1][p2]) + 2  # padding
    if max(len(word) for word in row) + 2 > col_width:
        col_width = max(len(word) for word in row) + 2
    print("".join(word.ljust(col_width) for word in row))
    for year in years:
        row = [
            year,
            str(information[year]['objects']['count']),
            str(information[year]['objects']['size']),
            str(information[year]['datastreams']['count']),
            str(information[year]['datastreams']['size']),
        ]
        print("".join(word.ljust(col_width) for word in row))


def iterate_cursor(data_dict, cursor, field_name):
    for data in cursor.fetchall():
        created = Object.fromisoformat(data[0])
        size = data[1]
        year = created.strftime('%Y')
        if year not in data_dict.keys():
            data_dict[year] = {
                'objects': {
                    'count': 0,
                    'size': 0,
                },
                'datastreams': {
                    'count': 0,
                    'size': 0,
                }
            }
        data_dict[year][field_name]['count'] += 1
        data_dict[year][field_name]['size'] += size


def get_db_years():
    # Get all years
    sql = "SELECT created from datastream_versions;"
    conn = get_db_connection()
    years = []
    with conn:
        c = conn.cursor()
        c.execute(sql)
        for row in c.fetchall():
            d = Object.fromisoformat(row[0])
            y = d.strftime('%Y')
            if y not in years:
                years.append(y)
        c.close()
    years.sort()
    return years


def main():
    setup()
    if args.object_dir is not None:
        parse_dir()
    if args.print_yearly:
        print_yearly()
    print("Done")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('db_file', help="Sqllite3 database file")
    parser.add_argument('-o', '--obj-dir', dest='object_dir', help="ObjectStore directory to be scanned.")
    parser.add_argument('-w', '--wipe-db', dest="wipe_db", action="store_true",
                        help="Erase the DB tables if they exist.")
    parser.add_argument('-p', '--print-yearly', dest="print_yearly", action="store_true",
                        help="Print yearly statistics.")
    args = parser.parse_args()
    if args.db_file[0] != '/':
        args.db_file = os.path.join(os.getcwd(), args.db_file)
    if args.object_dir is not None:
        if args.object_dir[0] != '/':
            args.object_dir = os.path.join(os.getcwd(), args.object_dir)
        args.object_dir = os.path.realpath(args.object_dir)

    if args.object_dir is not None:
        if not os.path.exists(args.object_dir) or not os.access(args.object_dir, os.R_OK):
            parser.error("{} is not a directory or is not readable.".format(args.object_dir))
    main()
