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


parser = argparse.ArgumentParser(description='DB to PlantUML')
parser.add_argument('--db',
                    dest='db',
                    type=str,
                    required=False,
                    help='the path to the database')
args = parser.parse_args()
db_path = args.db if args.db is not None else "log.db"

# Extract the nodes IDs from the database

con = sqlite3.connect(db_path)
cursor1: sqlite3.Cursor = con.cursor()
cursor2: sqlite3.Cursor = con.cursor()

cursor1.execute("SELECT DISTINCT(sender_id) FROM message ORDER BY sender_id")
entry = cursor1.fetchall()
if entry is None:
    print("The database is empty. Abort!")
    sys.exit(0)
sender_ids = [n for n in entry]

cursor1.execute("SELECT DISTINCT(recipient_id) FROM message ORDER BY recipient_id")
entry = cursor1.fetchall()
if entry is None:
    print("The database is empty. Abort!")
    sys.exit(0)
recipient_ids = [n for n in entry]

sender_ids.extend(recipient_ids)
nodes_ids = {identifier: None for identifier in sender_ids}.keys()
nodes = ["entity node{0:d}".format(n[0]) for n in nodes_ids]

print("@startuml")
print("\n".join(nodes))

cursor1.execute("SELECT direction, type, uid, request_id, sender_id, recipient_id, args FROM message "
                "WHERE direction='send' ORDER BY id")

while True:

    entry1 = cursor1.fetchone()
    if entry1 is None:
        break
    direction = entry1[0]
    type = entry1[1]
    uid = entry1[2]
    request_id = entry1[3]
    sender_id = entry1[4]
    recipient_id = entry1[5]
    args = entry1[6]

    cursor2.execute("SELECT id FROM message WHERE direction='receive' AND request_id=?", (request_id,))
    entry2 = cursor2.fetchone()

    uid_receive = None
    if entry2 is None:
        print("no response")
        print("node{0:d} -[{1:s}]>X node{2:d}: {3:s} [{4:d}]".format(sender_id,
                                                                     type2color(type),
                                                                     recipient_id,
                                                                     type,
                                                                     request_id))
    else:


        print("node{0:d} -[{1:s}]> node{2:d}: {3:s} [{4:d}]".format(sender_id,
                                                                    type2color(type),
                                                                    recipient_id,
                                                                    type,
                                                                    request_id))
        if type == "FIND_NODE_RESPONSE":
            print("rnote over node{0:d}: {1:s}".format(recipient_id, "no data" if args == "" else args))

    # Populate table "rt"

    # if uid_receive is not None:
    #     data_receive = get_data_for_message_uid(cursor2, uid_receive)
    #     if data_receive is not None:
    #         print("note across: {}".format(data_receive))
    #
    # data_send = get_data_for_message_uid(cursor2, uid_send)
    # if data_send is not None:
    #     print("note across: {}".format(data_send))

print("@endtuml")
print("# \"C:\\Program Files (x86)\\Common Files\\Oracle\\Java\\javapath\"\\java.exe -jar \"C:\\Users\\Denis BEURIVE\\Documents\\software\"\\plantuml.jar <file>")

