SET SERVEROUTPUT ON
SET FEEDBACK ON
SET TIMING ON

-- baseline timestamp for flashback query later
VAR baseline_ts TIMESTAMP
EXEC :baseline_ts := SYSTIMESTAMP;

PROMPT === Scenario A: Single row deletion ===
DELETE FROM employees_orcl WHERE emp_id=2;
COMMIT;

PROMPT === Scenario B: Multiple row deletion ===
DELETE FROM employees_orcl WHERE emp_id IN (1,3,5,7);
COMMIT;

PROMPT === Scenario C: Unauthorized modification (name/phone/salary) ===
UPDATE employees_orcl SET name='ALICE HACKED' WHERE emp_id=1;
UPDATE employees_orcl SET phone='000-0000000' WHERE emp_id=3;
UPDATE employees_orcl SET salary=9999 WHERE emp_id=1;
COMMIT;

PROMPT === Scenario D: Fake insertion ===
INSERT INTO employees_orcl (emp_id,name,phone,salary,hire_date,updated_by)
VALUES (999,'Ghost Emp','019-0000000',12345, DATE '2010-01-01', 'attacker');
COMMIT;

PROMPT === Scenario E: Timestamp manipulation ===
UPDATE employees_orcl SET hire_date = DATE '2000-01-01' WHERE emp_id IN (1,3);
COMMIT;

PROMPT === Done. Use oracle_recover.sql to restore from :baseline_ts ===
