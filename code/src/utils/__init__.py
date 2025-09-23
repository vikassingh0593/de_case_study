from pathlib import Path
import sys

# Find the first entry whose basename is 'src'
src_idx = next((i for i, p in enumerate(sys.path) if Path(p).name == "src"), None)
if src_idx is not None:
    code_path = Path(sys.path[src_idx]).parent
    code_str = str(code_path)
    if code_str not in sys.path:
        sys.path.insert(0, code_str)
else:
    # Optional fallback: if sys.path[0] is already a 'src' child, use its parent
    p0 = Path(sys.path[0])
    if p0.name == "src":
        code_str = str(p0.parent)
        if code_str not in sys.path:
            sys.path.insert(0, code_str)