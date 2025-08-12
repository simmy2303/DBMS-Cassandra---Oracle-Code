-- Keyspace and table for the employee dataset
CREATE KEYSPACE IF NOT EXISTS hr
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS hr.employees_cas (
  emp_id int PRIMARY KEY,
  name text,
  phone text,
  salary decimal,
  hire_date date,
  last_updated timestamp,
  updated_by text
)
WITH cdc = true;  -- optional if you plan to collect CDC artifacts
