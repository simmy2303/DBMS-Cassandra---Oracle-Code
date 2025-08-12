SET SERVEROUTPUT ON

BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE employees_orcl PURGE';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

CREATE TABLE employees_orcl (
  emp_id        NUMBER PRIMARY KEY,
  name          VARCHAR2(100),
  phone         VARCHAR2(20),
  salary        NUMBER(10,2),
  hire_date     DATE,
  last_updated  TIMESTAMP DEFAULT SYSTIMESTAMP,
  updated_by    VARCHAR2(30)
);

-- Enable flashback-friendly operations (for FLASHBACK TABLE, optional)
ALTER TABLE employees_orcl ENABLE ROW MOVEMENT;

-- Load sample data (or import the CSV through SQL Developer if you prefer)
INSERT INTO employees_orcl (emp_id,name,phone,salary,hire_date,updated_by) VALUES (1,'Alice Tan','012-3456789',4200, DATE '2018-03-14','seed');
INSERT INTO employees_orcl (emp_id,name,phone,salary,hire_date,updated_by) VALUES (2,'Brian Lee','013-9876543',5100, DATE '2019-07-01','seed');
INSERT INTO employees_orcl (emp_id,name,phone,salary,hire_date,updated_by) VALUES (3,'Chong Wai','017-2223344',3800, DATE '2020-01-11','seed');
INSERT INTO employees_orcl (emp_id,name,phone,salary,hire_date,updated_by) VALUES (4,'Divya K','016-1122334',6100, DATE '2016-10-22','seed');
INSERT INTO employees_orcl (emp_id,name,phone,salary,hire_date,updated_by) VALUES (5,'Ethan Ng','019-5566778',4500, DATE '2021-05-03','seed');

COMMIT;

COLUMN c NEW_VALUE v_count
SELECT COUNT(*) c FROM employees_orcl;
PROMPT Seeded rows: &v_count
