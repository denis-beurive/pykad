from typing import Optional, List, Pattern, Match
import sqlite3
import argparse
import os
import sys
import json
import re


SCHEMA = """
CREATE TABLE data (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    type             TEXT NOT NULL,
    message_uid      INTEGER NOT NULL,
    node_id          INTEGER NOT NULL,
    data             TEXT
);
CREATE INDEX message_message_uid_index ON data (message_uid);
CREATE INDEX message_node_id_index ON data (node_id);

CREATE TABLE message (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    action        TEXT,
    type          TEXT NOT NULL,
    uid           INTEGER NOT NULL,
    request_id    INTEGER NOT NULL,
    sender_id     INTEGER NOT NULL,
    recipient_id  INTEGER NOT NULL,
    args          TEXT
);
CREATE INDEX message_action_index ON message (action);
CREATE INDEX message_type_index ON message (type);
CREATE INDEX uid_index ON message (uid);
CREATE INDEX request_id_index ON message (request_id);
CREATE INDEX sender_id_index ON message (sender_id);
CREATE INDEX recipient_id ON message (recipient_id);



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
cursor: sqlite3.Cursor = con.cursor()

try:
    for statement in SCHEMA.split(";"):
        cursor.execute(statement)
except sqlite3.Error as e:
    print("An error occurred:", e.args[0])
    sys.exit(1)

# Parse the LOG file and load the database.

comment: Pattern = re.compile('^#')


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
        m: Optional[Match] = comment.match(line)
        if m is not None:
            continue

        log = json.loads(line)
        if log['log-type'] == 'message':
            # For the message "TERMINATE_NODE", the sender_id is not defined.
            if log['type'] == 'TERMINATE_NODE':
                continue
            cursor.execute("INSERT INTO message (action, type, uid, request_id, sender_id, recipient_id, args) "
                           "VALUES (?, ?, ?, ?, ?, ?, ?)", (log['action'],
                                                            log['type'],
                                                            log["uid"],
                                                            log['request_id'],
                                                            log['sender_id'],
                                                            log['recipient_id'],
                                                            log['args'] if 'args' in log else None))
            continue

        if log['log-type'] == 'data':
            cursor.execute("INSERT INTO data (type, message_uid, node_id, data) VALUES (?, ?, ?, ?)",
                           (log['type'], log['message_uid'], log['node_id'], log['data']))
            continue


con.commit()
con.close()

