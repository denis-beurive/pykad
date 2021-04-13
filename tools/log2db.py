from typing import Optional, List
import sqlite3
import argparse
import os
import sys


SCHEMA = """
CREATE TABLE node (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    name     INTEGER NOT NULL
);
CREATE UNIQUE INDEX node_name_index ON node (name);

CREATE TABLE rt (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    fk_message_uid   INTEGER NOT NULL,
    data             TEXT,
    FOREIGN KEY(fk_message_uid) REFERENCES "message" (uid)
);
CREATE INDEX message_fk_message_uid_index ON rt (fk_message_uid);

CREATE TABLE message (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    direction     INTEGER NOT NULL,
    type          TEXT NOT NULL,
    message_id    INTEGER NOT NULL,
    fk_origin     INTEGER NOT NULL,
    fk_recipient  INTEGER NOT NULL,
    args          TEXT,
    uid           INTEGER DEFAULT NULL,
    FOREIGN KEY(fk_origin) REFERENCES "node"(id),
    FOREIGN KEY(fk_recipient) REFERENCES "node"(id)
);
CREATE INDEX message_direction_index ON message (direction);
CREATE INDEX message_type_index ON message (type);
CREATE INDEX origin_type_index ON message (fk_origin);
CREATE INDEX recipient_type_index ON message (fk_recipient);



"""

# Parse the command line.

parser = argparse.ArgumentParser(description='Log to DB')
parser.add_argument('--log',
                    dest='log',
                    type=str,
                    required=False,
                    help='the path to the LOG file')
parser.add_argument('--db',
                    dest='db',
                    type=str,
                    required=False,
                    help='the path to the database')

args = parser.parse_args()
log_path = args.log if args.log is not None else "kad.txt"
db_path = args.db if args.db is not None else "log.db"

# Create the database

try:
    if os.path.exists(db_path):
        os.remove(db_path)
except Exception as e:
    print("Cannot remove the file \"{0:s}\"!".format(db_path))
    sys.exit(1)

con = sqlite3.connect(db_path)
cur: sqlite3.Cursor = con.cursor()

try:
    for statement in SCHEMA.split(";"):
        cur.execute(statement)
except sqlite3.Error as e:
    print("An error occurred:", e.args[0])

# Parse the LOG file and load the database.


def create_node_id(cursor: sqlite3.Cursor, node: str) -> int:
    """
    Insert a node into the table "node" if it is not already present to the table.
    :param cursor: the database cursor.
    :param node: the node ID to insert.
    :return: the value of the table field "node.id".
    """
    cursor.execute("SELECT id FROM node WHERE name=?", (node, ))
    entry = cursor.fetchone()
    if entry is not None:
        return entry[0]
    cursor.execute("INSERT INTO node (name) VALUES (?)", (node, ))
    cursor.execute("select last_insert_rowid()")
    entry = cursor.fetchone()
    return entry[0]


def process_message(cursor: sqlite3.Cursor, fields: List[str]) -> None:
    """
    Insert a message into the database.
    :param cursor: the database cursor.
    :param fields: the fields that composes the message to insert.
    """
    direction = fields[1]
    uid = fields[2]
    message_type = fields[3]
    message_id = fields[4]
    sender = fields[5]
    recipient = fields[6]
    # arguments = ",".join(fields[6:]) if len(fields) > 6 else "no data"
    arguments = fields[7] if len(fields) > 7 else ""

    if sender == "None":
        return

    if direction not in ("S", "R"):
        raise Exception("Invalid direction <{0:s}>!".format(direction))

    id_sender = create_node_id(cursor, sender)
    id_recipient = create_node_id(cursor, recipient)
    cursor.execute("INSERT INTO message (direction, uid, type, message_id, fk_origin, fk_recipient, args) VALUES (?, ?, ?, ?, ?, ?, ?)",
                   (direction, uid, message_type, message_id, id_sender, id_recipient, arguments))


def process_data_rt(cursor: sqlite3.Cursor, fields: List[str]) -> None:
    type = fields[1]
    uid = fields[2]
    data = fields[3]
    cursor.execute("INSERT INTO rt (fk_message_uid, data) VALUES (?, ?)", (uid, data))


with open(log_path, "r") as fd:
    while True:
        line: Optional[str] = fd.readline()
        # The readline() method doesn't trigger the end-of-file condition. Instead, when data is exhausted,
        # it returns an empty string.
        if len(line) == 0:
            break
        line = line.rstrip()
        if len(line) == 0:
            print("ERROR: Unexpected line: \"{0:s}\" (line is empty).".format(line))
            sys.exit(1)
        if line.startswith("RT>"):
            continue
        fields: List[str] = line.split("|")
        if not len(fields):
            print("Unexpected line: \"{0:s}\" (line is empty).".format(line))
            break

        if fields[0] == 'M':
            process_message(cur, fields)
            continue

        # By design, we know that the field "message.uid" is set **BEFORE** the field "rt.fk_message_uid".
        if fields[0] == 'D' and fields[1] == 'RT':
            process_data_rt(cur, fields)
            continue

con.commit()
con.close()

