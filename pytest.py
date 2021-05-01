from typing import Dict, Tuple
from pprint import pprint

v: Tuple[Dict[int, None], ...] = tuple({} for _ in range(3))
pprint(v)
