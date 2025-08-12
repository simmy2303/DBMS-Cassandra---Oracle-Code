# ============================
# Cassandra Forensics: Full Run
# Baseline CSV restore method
# ============================

$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot

# -------- Write the metrics script (no manual step) --------
$metricsPy = @"
import csv, sys

# usage: python metrics_eval.py baseline.csv after_recovery.csv scenario id_list
# scenario: delete | modify | insert
# id_list: comma-separated EmployeeID values (e.g., "1,3,5")

def read_csv_by_id(path):
    with open(path, newline='', encoding='utf-8-sig') as f:
        r = csv.DictReader(f)
        cols = [c.strip() for c in r.fieldnames]
        lower = {c.lower(): c for c in cols}
        idcol = lower.get('employeeid', cols[0])
        data = {}
        for row in r:
            row = {k.strip(): (v or "").strip() for k,v in row.items()}
            rid = row.get(idcol, "")
            try: rid = int(rid)
            except: continue
            data[rid] = row
        return data, cols, idcol

def parse_ids(s):
    out=[]
    for p in s.split(','):
        p=p.strip()
        if not p: continue
        try: out.append(int(p))
        except: pass
    return out

base, cols, idcol = read_csv_by_id(sys.argv[1])
aft,  _,   _      = read_csv_by_id(sys.argv[2])
scenario = sys.argv[3].lower()
ids = parse_ids(sys.argv[4]) if len(sys.argv) > 4 else []

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
                if (b or {}).get(c,"") == a.get(c,""):
                    correct_fields += 1
                else:
                    row_ok = False
            if row_ok: rows_fully_correct += 1
        else:
            total_fields += len(cols)
    elif scenario == 'insert':
        # ids given for inserted rows (not in baseline) should be ABSENT after recovery
        if b is None:
            if a is None:
                rows_recovered_present += 1
                rows_fully_correct += 1
                correct_fields += len(cols)
                total_fields += len(cols)
            else:
                total_fields += len(cols)
        else:
            # if id existed in baseline, treat as equality check
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
"@
Set-Content -Encoding utf8 metrics_eval.py $metricsPy

# -------- Create keyspace & table to match your CSV --------
$schemaCql = @"
CREATE KEYSPACE IF NOT EXISTS hr 
WITH replication = {'class':'SimpleStrategy','replication_factor':1};

DROP TABLE IF EXISTS hr.employees_cas;

CREATE TABLE IF NOT EXISTS hr.employees_cas (
  employeeid int PRIMARY KEY,
  firstname text,
  lastname text,
  phonenumber text,
  email text,
  department text,
  position text,
  hiredate date,
  salaryrm decimal
);
"@
Measure-Command {
  docker exec -it cassandra cqlsh -e $schemaCql
} | Select-Object TotalMilliseconds | ForEach-Object { "Schema setup: $($_.TotalMilliseconds) ms" }

# -------- Helpers --------
function Load-Dataset {
  param([string]$CsvName)

  docker cp ".\${CsvName}" cassandra:/tmp/${CsvName}
  docker exec -it cassandra cqlsh -e "TRUNCATE hr.employees_cas;"

  $t = Measure-Command {
    docker exec -it cassandra cqlsh -e "COPY hr.employees_cas (employeeid,firstname,lastname,phonenumber,email,department,position,hiredate,salaryrm) FROM '/tmp/${CsvName}' WITH HEADER=TRUE AND NULL='';"
  }
  "Loaded $CsvName in $($t.TotalMilliseconds) ms" | Write-Host

  docker exec -it cassandra cqlsh -e "COPY hr.employees_cas TO '/tmp/baseline_${CsvName}' WITH HEADER=TRUE;"
  docker cp cassandra:/tmp/baseline_${CsvName} .

  return @{ ImportMs = $t.TotalMilliseconds; Baseline = "baseline_${CsvName}" }
}

function Recover-FromBaseline {
  param([string]$BaselineFile)

  docker cp ".\${BaselineFile}" cassandra:/tmp/${BaselineFile}
  $t = Measure-Command {
    docker exec -it cassandra cqlsh -e "TRUNCATE hr.employees_cas; COPY hr.employees_cas FROM '/tmp/${BaselineFile}' WITH HEADER=TRUE;"
  }
  return [int]$t.TotalMilliseconds
}

function Export-After {
  param([string]$Tag,[string]$CsvName)
  $out = "after_${Tag}_${CsvName}"
  docker exec -it cassandra cqlsh -e "COPY hr.employees_cas TO '/tmp/${out}' WITH HEADER=TRUE;"
  docker cp cassandra:/tmp/${out} .
  return $out
}

function Eval-Metrics {
  param([string]$Baseline,[string]$After,[string]$Scenario,[string]$IdList)

  $out = & python metrics_eval.py ".\${Baseline}" ".\${After}" $Scenario "$IdList"
  $map = @{}
  $out -split "`n" | ForEach-Object {
    if ($_ -match "^\s*([^:]+):\s*(.+)$") {
      $map[$matches[1].Trim()] = $matches[2].Trim()
    }
  }
  return $map
}

function Append-Result {
  param(
    [string]$Dataset,[string]$Scenario,[string]$Size,[int]$IdsCount,
    [int]$ImportMs,[int]$TamperMs,[int]$RecoveryMs,
    [hashtable]$M
  )
  $line = "{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11}" -f `
    $Dataset,$Scenario,$Size,$IdsCount,$ImportMs,$TamperMs,$RecoveryMs, `
    ($M['Rows tampered']   -as [string]),
    ($M['Rows recovered (present again)'] -as [string]),
    ($M['Rows fully correct (complete)']  -as [string]),
    ($M['Quality of recovered data']      -replace '%',''),
    ($M['Completeness']                   -replace '%','')
  Add-Content -Encoding utf8 cassandra_results.csv $line
}

# init results file
"Dataset,Scenario,Size,IDsCount,ImportMs,TamperMs,RecoveryMs,RowsTampered,RowsRecovered,RowsFullyCorrect,QualityPct,CompletenessPct" | Out-File -Encoding utf8 cassandra_results.csv

# -------- Main loop over datasets --------
$datasets = @('employees_1k.csv','employees_5k.csv','employees_10k.csv')

foreach ($csv in $datasets) {

  if (-not (Test-Path ".\${csv}")) {
    Write-Warning "Missing $csv — skipping."
    continue
  }

  "=== DATASET: $csv ===" | Write-Host

  $load = Load-Dataset -CsvName $csv
  $importMs = [int]$load.ImportMs
  $baseline = $load.Baseline

  # Pick IDs from baseline
  $rows = Import-Csv ".\${baseline}"
  $idsAll = $rows | Select-Object -ExpandProperty EmployeeID
  $ids1   = ($idsAll | Select-Object -First 1)
  $ids20  = ($idsAll | Select-Object -First 20)
  $ids50  = ($idsAll | Select-Object -First 50)
  $mod5   = ($idsAll | Select-Object -First 5)

  $ids1s  = ($ids1  -join ",")
  $ids20s = ($ids20 -join ",")
  $ids50s = ($ids50 -join ",")
  $mod5s  = ($mod5  -join ",")

  # --- Scenario A: Multiple row deletion (1,20,50) ---
  foreach ($pair in @(@($ids1s,"n1",1), @($ids20s,"n20",20), @($ids50s,"n50",50))) {
    $idList = $pair[0]; $tag = $pair[1]; $count = [int]$pair[2]

    $tTamper = Measure-Command {
      docker exec -it cassandra cqlsh -e "DELETE FROM hr.employees_cas WHERE employeeid IN ($idList);"
    }
    $tamperMs = [int]$tTamper.TotalMilliseconds

    $afterTamper = Export-After -Tag "delete_${tag}" -CsvName $csv

    $recMs = Recover-FromBaseline -BaselineFile $baseline

    $afterRec = Export-After -Tag "delete_${tag}_rec" -CsvName $csv

    $m = Eval-Metrics -Baseline $baseline -After $afterRec -Scenario 'delete' -IdList $idList
    Append-Result -Dataset $csv -Scenario 'delete' -Size $tag -IdsCount $count -ImportMs $importMs -TamperMs $tamperMs -RecoveryMs $recMs -M $m
  }

  # --- Scenario B: Unauthorized modification (5 salary updates) ---
  $updStmts = ($mod5s.Split(",") | ForEach-Object { "UPDATE hr.employees_cas SET salaryrm = salaryrm + 100 WHERE employeeid=$($_);" }) -join " "
  $tTamper = Measure-Command {
    docker exec -it cassandra cqlsh -e $updStmts
  }
  $tamperMs = [int]$tTamper.TotalMilliseconds

  $afterTamper = Export-After -Tag "modify" -CsvName $csv

  $recMs = Recover-FromBaseline -BaselineFile $baseline

  $afterRec = Export-After -Tag "modify_rec" -CsvName $csv

  $m = Eval-Metrics -Baseline $baseline -After $afterRec -Scenario 'modify' -IdList $mod5s
  Append-Result -Dataset $csv -Scenario 'modify' -Size 'n5' -IdsCount 5 -ImportMs $importMs -TamperMs $tamperMs -RecoveryMs $recMs -M $m

  # --- Scenario C: Insertions (1,20,50) ---
  function Build-InsertSql {
    param([int]$Count,[int]$StartId)
    $stmts = @()
    for ($i=0; $i -lt $Count; $i++) {
      $id = $StartId + $i
      $stmts += "INSERT INTO hr.employees_cas (employeeid, firstname, lastname, phonenumber, email, department, position, hiredate, salaryrm) VALUES ($id, 'Fake$id','User$id','000$id','fake$id@example.com','Fraud','Hacker','2020-01-01', 1.23);"
    }
    return ($stmts -join " ")
  }

  foreach ($pair in @(@(1, 900001, "n1"), @(20, 900101, "n20"), @(50, 900201, "n50"))) {
    $cnt = [int]$pair[0]; $startId = [int]$pair[1]; $tag = $pair[2]
    $sql = Build-InsertSql -Count $cnt -StartId $startId
    $idList = (($startId)..($startId+$cnt-1)) -join ","

    $tTamper = Measure-Command {
      docker exec -it cassandra cqlsh -e $sql
    }
    $tamperMs = [int]$tTamper.TotalMilliseconds

    $afterTamper = Export-After -Tag "insert_${tag}" -CsvName $csv

    $recMs = Recover-FromBaseline -BaselineFile $baseline

    $afterRec = Export-After -Tag "insert_${tag}_rec" -CsvName $csv

    $m = Eval-Metrics -Baseline $baseline -After $afterRec -Scenario 'insert' -IdList $idList
    Append-Result -Dataset $csv -Scenario 'insert' -Size $tag -IdsCount $cnt -ImportMs $importMs -TamperMs $tamperMs -RecoveryMs $recMs -M $m
  }
}

"All done. Results -> cassandra_results.csv" | Write-Host
