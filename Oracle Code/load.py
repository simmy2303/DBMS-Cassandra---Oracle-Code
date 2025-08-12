import oracledb
import csv

conn = oracledb.connect(
    user="bench",
    password="Bench123",
    dsn="localhost/XEPDB1"  # Change if needed
)
cur = conn.cursor()

def load_csv_to_table(csv_path, table_name):
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)  # skip header
        for row in reader:
            cur.execute(f"""
                INSERT INTO {table_name} 
                (EmployeeID, FirstName, LastName, PhoneNumber, Email, Department, Position, HireDate, SalaryRM)
                VALUES (:1, :2, :3, :4, :5, :6, :7, TO_DATE(:8, 'YYYY-MM-DD'), :9)
            """, row)
    conn.commit()
    print(f"Loaded {csv_path} into {table_name}")

# Load all three datasets
load_csv_to_table(r"C:\Users\yongj\OneDrive\Desktop\dbms_forensic\employees_large_1k_fixed.csv", "EMP_1K_BASE")
load_csv_to_table(r"C:\Users\yongj\OneDrive\Desktop\dbms_forensic\employees_small_5k_fixed.csv", "EMP_5K_BASE")
load_csv_to_table(r"C:\Users\yongj\OneDrive\Desktop\dbms_forensic\employees_medium_10k_fixed.csv", "EMP_10K_BASE")

conn.close()
