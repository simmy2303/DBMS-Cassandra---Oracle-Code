import csv
import oracledb

# ======== CONFIGURATION ========
dsn = "localhost:1521/XEPDB1"
username = "bench"
password = "Bench123"

csv_files = {
    "table_1k": r"C:\path\to\employees_1k.csv",
    "table_5k": r"C:\path\to\employees_5k.csv",
    "table_10k": r"C:\path\to\employees_10k.csv"
}

# ======== CONNECT TO ORACLE ========
conn = oracledb.connect(user=username, password=password, dsn=dsn)
cur = conn.cursor()

# ======== CREATE STAGING TABLES AND LOAD DATA ========
for table_name, csv_path in csv_files.items():
    print(f"\nPreparing {table_name} from {csv_path}...")

    # Drop if exists
    try:
        cur.execute(f"DROP TABLE {table_name} PURGE")
    except oracledb.DatabaseError:
        pass

    # Create empty table with same structure as employees
    cur.execute(f"""
        CREATE TABLE {table_name} (
            emp_id       NUMBER PRIMARY KEY,
            first_name   VARCHAR2(50),
            last_name    VARCHAR2(50),
            phone        VARCHAR2(30),
            email        VARCHAR2(100),
            department   VARCHAR2(50),
            position     VARCHAR2(100),
            hire_date    DATE,
            salary       NUMBER(10,2)
        )
    """)

    # Load CSV data
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = []
        for row in reader:
            rows.append((
                int(row['EmployeeID']),
                row['FirstName'],
                row['LastName'],
                row['PhoneNumber'],
                row['Email'],
                row['Department'],
                row['Position'],
                row['HireDate'],   # Ensure format is YYYY-MM-DD
                float(row['SalaryRM'])
            ))

    cur.executemany(f"""
        INSERT INTO {table_name} (
            emp_id, first_name, last_name, phone, email,
            department, position, hire_date, salary
        )
        VALUES (:1, :2, :3, :4, :5, :6, :7, TO_DATE(:8,'YYYY-MM-DD'), :9)
    """, rows)
    conn.commit()
    print(f"{len(rows)} rows inserted into {table_name}.")

# ======== CALL THE PROCEDURE TO MEASURE TIMES ========
print("\nRunning data creation test...")
cur.callproc("run_data_creation_test", ["table_1k", "table_5k", "table_10k"])
print("Data creation test completed and results saved in data_creation_results.")

# ======== DISPLAY RESULTS ========
print("\n=== Data Creation Results ===")
for row in cur.execute("SELECT * FROM data_creation_results"):
    print(row)

conn.close()
