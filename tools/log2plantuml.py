from typing import Dict, Optional
import sys
import sqlite3
import argparse


colors: Dict[str, str] = {
    "FIND_NODE": "#FF0000",
    "FIND_NODE_RESPONSE": "#FF0000",
    "PING_NODE": "#006400",
    "PING_NODE_RESPONSE": "#006400"
}


def type2color(type: str) -> str:
    if type in colors:
        return colors[type]
    return "#000000"

# Parse the command line.


def get_node_name(cursor: sqlite3.Cursor, node_id: int) -> Optional[int]:
    cursor.execute("SELECT name FROM node WHERE id=?", (node_id,))
    entry = cursor.fetchone()
    if entry is None:
        return None
    return entry[0]


def get_data_for_message_uid(cursor: sqlite3.Cursor, uid: int) -> Optional[str]:
    print("' >>>> SELECT data FROM rt WHERE fk_message_uid={}".format(uid))
    cursor.execute("SELECT data FROM rt WHERE fk_message_uid=?", (uid,))
    entry = cursor.fetchone()
    if entry is None:
        return None
    return entry[0]


parser = argparse.ArgumentParser(description='DB to PlantUML')
parser.add_argument('--db',
                    dest='db',
                    type=str,
                    required=False,
                    help='the path to the database')
args = parser.parse_args()
db_path = args.db if args.db is not None else "log.db"

# Extract data from the database.

con = sqlite3.connect(db_path)
cursor1: sqlite3.Cursor = con.cursor()

cursor1.execute("SELECT name FROM node ORDER BY name")
entry1 = cursor1.fetchall()
if entry1 is None:
    print("The database is empty. Abort!")
    sys.exit(0)

nodes = ["entity node{0:d}".format(n[0]) for n in entry1]

print("@startuml")
print("\n".join(nodes))

cursor1.execute("SELECT type, uid, message_id, fk_origin, fk_recipient, args, direction FROM message WHERE "
                "direction='S' ORDER BY message_id")
while True:
    entry1 = cursor1.fetchone()
    if entry1 is None:
        break
    type = entry1[0]
    uid_send = entry1[1]
    message_id = entry1[2]
    origin_id = entry1[3]
    recipient_id = entry1[4]
    data = entry1[5]
    direction = entry1[6]

    cursor2: sqlite3.Cursor = con.cursor()

    origin = get_node_name(cursor2, origin_id)
    if origin is None:
        print("ERROR: origin ID {0:d} does not exist".format(origin_id))
        sys.exit(1)
    recipient = get_node_name(cursor2, recipient_id)
    if recipient is None:
        print("ERROR: recipient ID {0:d} does not exist".format(recipient_id))
        sys.exit(1)

    # Populate table "message"

    cursor2.execute("SELECT id, uid FROM message WHERE direction='R' AND message_id=?", (message_id,))
    entry2 = cursor2.fetchone()

    uid_receive = None
    if entry2 is None:
        print("node{0:d} -[{1:s}]>X node{2:d}: {3:s} [{4:d}]".format(origin, type2color(type), recipient, type, message_id))
    else:
        uid_receive = entry2[1]
        print("node{0:d} -[{1:s}]> node{2:d}: {3:s} [{4:d}] /{5:d}/".format(origin, type2color(type), recipient, type, message_id, uid_send))
        if type == "FIND_NODE_RESPONSE":
            print("rnote over node{0:d}: {1:s}".format(recipient, "no data" if data == "" else data))

    # Populate table "rt"

    if uid_receive is not None:
        data_receive = get_data_for_message_uid(cursor2, uid_receive)
        if data_receive is not None:
            print("note across: {}".format(data_receive))

    data_send = get_data_for_message_uid(cursor2, uid_send)
    if data_send is not None:
        print("note across: {}".format(data_send))

print("@endtuml")
print("# \"C:\\Program Files (x86)\\Common Files\\Oracle\\Java\\javapath\"\\java.exe -jar \"C:\\Users\\Denis BEURIVE\\Documents\\software\"\\plantuml.jar <file>")







