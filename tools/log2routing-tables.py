from typing import Optional, Pattern, Match, List, Tuple
import argparse
import os
import sys
import json
import re


document_header = """
<html>

<head>
    <style>
        th {
            background-color: #4CAF50;
            color: white;
            padding-right: 20px;
            text-align: left;
        }
        
        td {
            padding-right: 20px;
            text-align: left;
        }
    
        table {
            border-collapse:collapse;
        }
        
        span.dist {
            font-family: "Monospace";
            color: #0000FF;
        }
    </style>
</head>

<body>
"""

document_footer = """
</body></html>
"""


def get_mask(in_current_node: int, bucket_index: int):
    return ((in_current_node >> bucket_index) ^ 1) << bucket_index


def dec_to_bin(n, in_len: int) -> str:
    v = bin(n).replace("0b", "")
    return '0'*(in_len-len(v)) + v


def get_expected_ids(bucket_index: int, in_masks: List[int]) -> Tuple[int, int]:
    right_max = pow(2, bucket_index) - 1
    v_min = in_masks[bucket_index]
    v_max = in_masks[bucket_index] | right_max
    return v_min, v_max


def rt2html(in_rt: dict, in_config: dict) -> str:
    rt: dict = in_rt['data']
    current_node: int = in_rt['node_id']
    masks: List[int] = [get_mask(current_node, i) for i in range(in_config['id_length'])]
    lines: List[str] = ["<b>Node {}</b><table>\n".format(current_node),
                        '  <tr>',
                        '<th>bucket</th><th>min</th><th>max</th>']
    lines.extend(['<th>&nbsp;.&nbsp;</th>' for _ in range(in_config['k'])])
    lines.append("<th>expected</th></tr>\n")

    for bucket_index in range(in_config['id_length']):
        dist_min = pow(2, bucket_index)
        dist_max = 2*dist_min

        lines.append('  <tr><td>{}</td>'
                     '<td>{}</td><td>{}</td>'.format(bucket_index, dist_min, dist_max))
        for list_index in range(in_config['k']):
            if len(rt[str(bucket_index)]) > list_index:
                node = rt[str(bucket_index)][list_index]
                distance = current_node ^ node
                lines.append('<td>{}(<span class="dist">{}</span>)</td>'.format(node, distance))
            else:
                lines.append('<td>&nbsp;</td>')
        v_min, v_max = get_expected_ids(bucket_index, masks)
        if v_min != v_max:
            lines.append('<td>[{}, {}]</td>'.format(v_min, v_max))
        else:
            lines.append('<td>{}</td>'.format(v_min))
        lines.append("</tr>\n")
    lines.append("</table>\n")
    return "".join(lines)


# Parse the command line.

parser = argparse.ArgumentParser(description='Log to routing tables')
parser.add_argument('--log',
                    dest='log',
                    type=str,
                    required=False,
                    help='the path to the LOG file')
parser.add_argument('--output',
                    dest='output',
                    type=str,
                    required=False,
                    help='the path to the database')

args = parser.parse_args()
log_path = args.log if args.log is not None else "kad.txt"
output_path = args.output if args.output is not None else "routing_tables.html"

try:
    if os.path.exists(output_path):
        os.remove(output_path)
except Exception as e:
    print("Cannot remove the file \"{0:s}\"!".format(output_path))
    sys.exit(1)

comment: Pattern = re.compile('^#')
config: Optional[dict] = None
lines: List[str] = [document_header]
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

        if log['log-type'] == 'config':
            if config is not None:
                print('ERROR: config LOG found more than once!')
                sys.exit(1)
            config = log
            continue

        if log['log-type'] != 'routing_table':
            continue

        lines.append(rt2html(log, config))

lines.append(document_footer)

with open(output_path, 'w') as fd:
    fd.write("\n<br/><br/>".join(lines))

