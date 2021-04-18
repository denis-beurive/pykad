from typing import Dict, Optional, List, Any
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


def get_nodes(connexion: sqlite3.Connection) -> List[str]:
    """
    Get the list of nodes.
    :param connexion: the connexion handler.
    :return: the list of nodes.
    """
    cursor = sqlite3.Cursor = connexion.cursor()
    cursor.execute("SELECT DISTINCT(sender_id) FROM message ORDER BY sender_id")
    entry = cursor.fetchall()
    if entry is None:
        print("The database is empty. Abort!")
        sys.exit(0)
    sender_ids = [n for n in entry]

    cursor.execute("SELECT DISTINCT(recipient_id) FROM message ORDER BY recipient_id")
    entry = cursor.fetchall()
    if entry is None:
        print("The database is empty. Abort!")
        sys.exit(0)
    recipient_ids = [n for n in entry]

    sender_ids.extend(recipient_ids)
    nodes_ids = {identifier: None for identifier in sender_ids}.keys()
    return ["entity node{0:d}".format(n[0]) for n in nodes_ids]


def get_log(cursor: sqlite3.Cursor) -> Optional[Dict[str, Any]]:
    """
    Get the next row from the cursor that points to the messages selection.
    :param cursor: the cursor that points to the messages selection.
    :return: the next row.
    """
    entry = cursor.fetchone()
    if entry is None:
        return None
    return {
        "action": entry[0],
        "type": entry[1],
        "uid": entry[2],
        "request_id": entry[3],
        "sender_id": entry[4],
        "recipient_id": entry[5],
        "args": entry[6]
    }


def has_response(connexion: sqlite3.Connection, request_id: int) -> bool:
    cursor = sqlite3.Cursor = connexion.cursor()
    cursor.execute("SELECT id FROM message WHERE action='receive' AND request_id=?", (request_id,))
    # print("SELECT id FROM message WHERE action='receive' AND request_id={}".format(request_id,))
    count = len(cursor.fetchall())
    if count > 2:
        raise Exception("ERROR: SELECT id FROM message WHERE action='receive' AND request_id={} "
                        "=> count > 1".format(log["request_id"]))
    return count > 1


def get_data(connexion: sqlite3.Connection, uid: int) -> Optional[Dict[str, Any]]:
    cursor = sqlite3.Cursor = connexion.cursor()
    cursor.execute("SELECT id, type, node_id, data FROM data WHERE message_uid=?", (uid,))
    entry = cursor.fetchone()
    if entry is None:
        return None
    return {
        "id": entry[0],
        "type": entry[1],
        "node_id": entry[2],
        "data": entry[3]
    }


# Parse the command line.

parser = argparse.ArgumentParser(description='DB to PlantUML')
parser.add_argument('--db',
                    dest='db',
                    type=str,
                    required=False,
                    help='the path to the database')
args = parser.parse_args()
db_path = args.db if args.db is not None else "log.db"

con: sqlite3.Connection = sqlite3.connect(db_path)
cursor1: sqlite3.Cursor = con.cursor()
cursor2: sqlite3.Cursor = con.cursor()
cursor3: sqlite3.Cursor = con.cursor()

# Extract the nodes IDs from the database
nodes = get_nodes(con)

print("@startuml")
print("\n".join(nodes))

cursor1.execute("SELECT action, type, uid, request_id, sender_id, recipient_id, args FROM message WHERE "
                "action='receive' ORDER BY id")

while True:

    log = get_log(cursor1)
    if log is None:
        break

    data = get_data(con, log["uid"])
    message_has_response = has_response(con, log["request_id"])
    color = type2color(log["type"])
    arrow = "-[{:s}]>".format(color) if message_has_response else "-[{:s}]>X".format(color)
    print("node{0:d} {1:s} node{2:d}:[{4:d}] {3:s}".format(log["sender_id"],
                                                           arrow,
                                                           log["recipient_id"],
                                                           log["type"],
                                                           log["request_id"]))

    if type == "FIND_NODE_RESPONSE":
        print("rnote over node{:d}: {:s}".format(log["recipient_id"], "no data" if (args is None) else args))

    if data is not None:
        print("rnote over node{:d}: {:s} {:s}".format(data['node_id'], data['type'], data["data"]))


print("@endtuml")
print("# \"C:\\Program Files (x86)\\Common Files\\Oracle\\Java\\javapath\"\\java.exe -jar \"C:\\Users\\Denis BEURIVE\\Documents\\software\"\\plantuml.jar <file>")

