SET SERVEROUTPUT ON
SET TIMING ON

-- Scenario A: restore emp_id=2
MERGE INTO employees_orcl tgt
USING (SELECT * FROM employees_orcl AS OF TIMESTAMP :baseline_ts WHERE emp_id=2) src
ON (tgt.emp_id = src.emp_id)
WHEN NOT MATCHED THEN
  INSERT (emp_id,name,phone,salary,hire_date,last_updated,updated_by)
  VALUES (src.emp_id,src.name,src.phone,src.salary,src.hire_date,src.last_updated,src.updated_by)
WHEN MATCHED THEN
  UPDATE SET tgt.name=src.name, tgt.phone=src.phone, tgt.salary=src.salary, tgt.hire_date=src.hire_date,
             tgt.last_updated=src.last_updated, tgt.updated_by=src.updated_by;

-- Scenario B: restore 1,3,5,7
MERGE INTO employees_orcl tgt
USING (SELECT * FROM employees_orcl AS OF TIMESTAMP :baseline_ts WHERE emp_id IN (1,3,5,7)) src
ON (tgt.emp_id = src.emp_id)
WHEN NOT MATCHED THEN
  INSERT (emp_id,name,phone,salary,hire_date,last_updated,updated_by)
  VALUES (src.emp_id,src.name,src.phone,src.salary,src.hire_date,src.last_updated,src.updated_by)
WHEN MATCHED THEN
  UPDATE SET tgt.name=src.name, tgt.phone=src.phone, tgt.salary=src.salary, tgt.hire_date=src.hire_date,
             tgt.last_updated=src.last_updated, tgt.updated_by=src.updated_by;

-- Scenario C: fix hacked fields on 1,3
MERGE INTO employees_orcl tgt
USING (SELECT * FROM employees_orcl AS OF TIMESTAMP :baseline_ts WHERE emp_id IN (1,3)) src
ON (tgt.emp_id = src.emp_id)
WHEN MATCHED THEN
  UPDATE SET tgt.name=src.name, tgt.phone=src.phone, tgt.salary=src.salary, tgt.hire_date=src.hire_date,
             tgt.last_updated=src.last_updated, tgt.updated_by=src.updated_by;

-- Scenario D: remove fake insert 999
DELETE FROM employees_orcl WHERE emp_id=999;

-- Scenario E: fix hire_date for 1,3
MERGE INTO employees_orcl tgt
USING (SELECT * FROM employees_orcl AS OF TIMESTAMP :baseline_ts WHERE emp_id IN (1,3)) src
ON (tgt.emp_id = src.emp_id)
WHEN MATCHED THEN
  UPDATE SET tgt.hire_date=src.hire_date, tgt.last_updated=src.last_updated, tgt.updated_by=src.updated_by;

COMMIT;
