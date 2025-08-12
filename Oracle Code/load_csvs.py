import oracledb
import csv
import time
import os

# ==== DB Connection ====
DB_USER = "bench"
DB_PASSWORD = "Bench123"
DB_DSN = "localhost/XEPDB1"

# CSV paths 
CSV_FILES = {
    "1k": "C:/Users/yongj/OneDrive/Desktop/dbms_forensic/employees_large_1k_fixed.csv",
    "5k": "C:/Users/yongj/OneDrive/Desktop/dbms_forensic/employees_small_5k_fixed.csv",
    "10k": "C:/Users/yongj/OneDrive/Desktop/dbms_forensic/employees_medium_10k_fixed.csv"
}

# ==== Connect to Oracle ====
conn = oracledb.connect(user=DB_USER, password=DB_PASSWORD, dsn=DB_DSN)
cur = conn.cursor()

# ==== Timing results ====
results = {
    "DBMS": "Oracle",
    "db_creation": 0,
    "table_creation": 0,
    "1k_insert": 0,
    "5k_insert": 0,
    "10k_insert": 0
}

# ==== Step 1: DB/Table creation timing ====
start_db = time.time()

start_table = time.time()

# Drop table if exists
cur.execute("""
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE employees';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -942 THEN
            RAISE;
        END IF;
END;
""")

# Create table with 9 columns
cur.execute("""
CREATE TABLE employees (
    EmployeeID  NUMBER PRIMARY KEY,
    FirstName   VARCHAR2(50),
    LastName    VARCHAR2(50),
    PhoneNumber VARCHAR2(20),
    Email       VARCHAR2(100),
    Department  VARCHAR2(50),
    Position    VARCHAR2(100),
    HireDate    DATE,
    SalaryRM    NUMBER(10,2)
)
""")
results["table_creation"] = round(time.time() - start_table, 4)
results["db_creation"] = round(results["table_creation"], 4)  # Same timing for now

# ==== Step 2: Function to load CSV ====
def load_csv_to_table(csv_path):
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = [
            (
                int(row["EmployeeID"]),
                row["FirstName"],
                row["LastName"],
                row["PhoneNumber"],
                row["Email"],
                row["Department"],
                row["Position"],
                row["HireDate"],   # Already YYYY-MM-DD
                float(row["SalaryRM"])
            )
            for row in reader
        ]

    cur.executemany("""
        INSERT INTO employees (
            EmployeeID, FirstName, LastName, PhoneNumber, Email,
            Department, Position, HireDate, SalaryRM
        ) VALUES (
            :1, :2, :3, :4, :5, :6, :7, TO_DATE(:8, 'YYYY-MM-DD'), :9
        )
    """, rows)
    conn.commit()


# ==== Step 3: Measure insertion times ====
for size, path in CSV_FILES.items():
    # Clear table before each insert
    cur.execute("DELETE FROM employees")
    conn.commit()

    start_insert = time.time()
    load_csv_to_table(path)
    elapsed = round(time.time() - start_insert, 4)

    if size == "1k":
        results["1k_insert"] = elapsed
    elif size == "5k":
        results["5k_insert"] = elapsed
    elif size == "10k":
        results["10k_insert"] = elapsed

# ==== Step 4: Print table ====
print("----------------------------------------------------------------------------------------------------------------------------")
print("|   DBMS   |  db creation (s)  | table creation (s) | 1,000 insertion time | 5,000 insertion time | 10,000 insertion time |")
print("----------------------------------------------------------------------------------------------------------------------------")
print(f"|  {results['DBMS']}  |       {results['db_creation']}      |       {results['table_creation']}       |        {results['1k_insert']}        |         {results['5k_insert']}       |         {results['10k_insert']}        |")
print("----------------------------------------------------------------------------------------------------------------------------")

# Close
cur.close()
conn.close()
