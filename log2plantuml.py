from typing import List, Dict, NewType, Optional, Callable
import sys

MessageId = NewType("MessageId", int)
data: Dict[str, List[bool]] = {}


def process_message(fields: List[str]) -> Optional[str]:
    direction = fields[1]
    message_type = fields[2]
    message_id = fields[3]
    sender = fields[4]
    recipient = fields[5]

    if direction not in ("S", "R"):
        return None
    if message_id not in data:
        data[message_id] = [direction == "S", direction == "R"]
    else:
        data[message_id][0 if direction == "S" else 1] = True
    if direction == "R":
        return None
    return "{0:s} {1:s} {2:s}: {3:s}[{4:s}]".format(
        sender,
        "->" if direction == "S" else "<-",
        recipient,
        message_type,
        message_id
    )


switch: Dict[str, Callable] = {
    "M": process_message
}


lines: List[str] = []

with open(sys.argv[1], "r") as fd:
    while True:
        line: Optional[str] = fd.readline().rstrip()
        if line == "":
            break
        fields: List[str] = line.split("|")
        if not len(fields):
            print("Unexpected line: \"{0:s}\" (line is empty).".format(line))
            break
        if fields[0] not in switch:
            print("Unexpected line: \"{0:s}\" (invalid first field \"{1:s}\").".format(line, fields[0]))
            break
        processor: Callable = switch[fields[0]]
        res: str = processor(fields)
        if res is None:
            continue
        lines.append(res)

print("@startuml")
print("\n".join(lines))
print("@endtuml")
print("# \"C:\\Program Files (x86)\\Common Files\\Oracle\\Java\\javapath\"\\java.exe -jar \"C:\\Users\\Denis BEURIVE\\Documents\\software\"\\plantuml.jar <file>")







