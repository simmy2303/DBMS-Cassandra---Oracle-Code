import csv, sys

# usage: python metrics_eval.py baseline.csv after_recovery.csv scenario id_list
# scenario: delete | modify | insert
# id_list: comma-separated values for the FIRST COLUMN (as strings)

def read_csv_by_firstcol(path):
    with open(path, newline='', encoding='utf-8-sig') as f:
        r = csv.DictReader(f)
        assert r.fieldnames, "No header in " + path
        cols = [c.strip() for c in r.fieldnames]
        key = cols[0]  # first column is the key
        data = {}
        for row in r:
            rid = (row.get(key, "") or "").strip()
            if rid:
                data[rid] = {k.strip(): (v or "").strip() for k,v in row.items()}
        return data, cols

base, cols = read_csv_by_firstcol(sys.argv[1])
aft,  _    = read_csv_by_firstcol(sys.argv[2])
scenario = sys.argv[3].lower()
ids = [s.strip() for s in sys.argv[4].split(",")] if len(sys.argv) > 4 else []

rows_tampered = len(ids)
rows_recovered_present = 0
rows_fully_correct = 0
total_fields = 0
correct_fields = 0

for rid in ids:
    b = base.get(rid)
    a = aft.get(rid)

    if scenario in ('delete','modify'):
        if a is not None:
            rows_recovered_present += 1
            row_ok = True
            for c in cols:
                total_fields += 1
                if (b or {}).get(c,"") == (a or {}).get(c,""):
                    correct_fields += 1
                else:
                    row_ok = False
            if row_ok: rows_fully_correct += 1
        else:
            total_fields += len(cols)
    elif scenario == 'insert':
        if b is None:
            if a is None:
                rows_recovered_present += 1
                rows_fully_correct += 1
                correct_fields += len(cols)
                total_fields += len(cols)
            else:
                total_fields += len(cols)
        else:
            if a is not None:
                rows_recovered_present += 1
                row_ok = True
                for c in cols:
                    total_fields += 1
                    if b.get(c,"") == a.get(c,""):
                        correct_fields += 1
                    else:
                        row_ok = False
                if row_ok: rows_fully_correct += 1
            else:
                total_fields += len(cols)

quality = (correct_fields/total_fields*100) if total_fields else 0.0
complete = (rows_fully_correct/rows_recovered_present*100) if rows_recovered_present else 0.0

print(f"Rows tampered: {rows_tampered}")
print(f"Rows recovered (present again): {rows_recovered_present}")
print(f"Rows fully correct (complete): {rows_fully_correct}")
print(f"Quality of recovered data: {quality:.2f}%")
print(f"Completeness: {complete:.2f}%")
