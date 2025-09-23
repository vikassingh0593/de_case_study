from pathlib import Path
import sys

anchor = "de_case_study/code/src"

code_path = None
for p in sys.path:
    if anchor in p:
        # Slice up to the start of '/src' so we end at '.../de_case_study/code'
        start_idx = p.find(anchor)
        base = p[:start_idx] + "de_case_study/code"
        candidate = Path(base).resolve()
        if candidate.name == "code":
            code_path = candidate
            break

if code_path:
    s = str(code_path)
    # Ensure uniqueness and highest precedence
    try:
        sys.path.remove(s)
    except ValueError:
        pass
    sys.path.insert(0, s)
    print(f"Using code path (till src excluded): {s}")
else:
    print(f"Could not locate '{anchor}' within sys.path entries.")