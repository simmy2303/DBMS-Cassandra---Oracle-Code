DECLARE
  TYPE t_ds IS RECORD (label VARCHAR2(10), base_table VARCHAR2(30));
  TYPE t_ds_tab IS TABLE OF t_ds;

  v_datasets t_ds_tab := t_ds_tab(
    t_ds('1k',  'EMP_1K_BASE'),
    t_ds('5k',  'EMP_5K_BASE'),
    t_ds('10k', 'EMP_10K_BASE')
  );

  v_run_id          NUMBER;
  v_work_table      VARCHAR2(128);
  v_sql             CLOB;
  v_rows_recovered  NUMBER;
  v_fully_correct   NUMBER;
  v_total_fields    NUMBER;
  v_correct_fields  NUMBER;
  v_complete_rows   NUMBER;
  v_quality_pct     NUMBER;
  v_complete_pct    NUMBER;

  ------------------------------------------------------------------
  -- Perfect recovery: restore all columns from baseline
  ------------------------------------------------------------------
  PROCEDURE simple_recovery_from_baseline(
      p_baseline_table IN VARCHAR2,
      p_work_table     IN VARCHAR2
  ) IS
      v_sql CLOB;
  BEGIN
      v_sql := 'MERGE INTO ' || p_work_table || ' w ' ||
               'USING ' || p_baseline_table || ' b ' ||
               'ON (w.EmployeeID = b.EmployeeID) ' ||
               'WHEN MATCHED THEN UPDATE SET ' ||
               'w.FirstName = b.FirstName, ' ||
               'w.LastName = b.LastName, ' ||
               'w.PhoneNumber = b.PhoneNumber, ' ||
               'w.Email = b.Email, ' ||
               'w.Department = b.Department, ' ||
               'w.Position = b.Position, ' ||
               'w.HireDate = b.HireDate, ' ||
               'w.SalaryRM = b.SalaryRM';
      EXECUTE IMMEDIATE v_sql;
      COMMIT;
  END;

  PROCEDURE recover_work_table(p_baseline VARCHAR2, p_work_table VARCHAR2) IS
  BEGIN
      simple_recovery_from_baseline(p_baseline, p_work_table);
  END;

  ------------------------------------------------------------------
  -- Tamper: update SalaryRM for 5 employees
  ------------------------------------------------------------------
  PROCEDURE tamper_salary_and_log(
      p_dataset VARCHAR2,
      p_work_table VARCHAR2,
      p_run_id NUMBER
  ) IS
  BEGIN
    -- Log the EmployeeIDs that will be tampered
    v_sql :=
      'INSERT INTO tamper_log (run_id, dataset, rows_deleted, employeeid) ' ||
      'SELECT :rid, :dsl, 5, employeeid ' ||
      'FROM (SELECT employeeid FROM '||p_work_table||' ORDER BY employeeid) ' ||
      'WHERE ROWNUM <= 5';
    EXECUTE IMMEDIATE v_sql USING p_run_id, p_dataset;

    -- Tamper: increase SalaryRM by 9999
    v_sql :=
      'UPDATE '||p_work_table||' w ' ||
      'SET w.SalaryRM = w.SalaryRM + 9999 ' ||
      'WHERE EXISTS (SELECT 1 FROM tamper_log t ' ||
      '              WHERE t.run_id = :rid AND t.employeeid = w.employeeid)';
    EXECUTE IMMEDIATE v_sql USING p_run_id;
    COMMIT;
  END;

  ------------------------------------------------------------------
  -- Measure metrics using dynamic SQL
  ------------------------------------------------------------------
  PROCEDURE measure_and_store(
      p_dataset     VARCHAR2,
      p_baseline    VARCHAR2,
      p_work_table  VARCHAR2,
      p_run_id      NUMBER
  ) IS
  BEGIN
    -- Rows recovered
    v_sql :=
      'SELECT COUNT(*) '||
      'FROM '||p_work_table||' w '||
      'JOIN tamper_log t ON t.employeeid = w.EmployeeID '||
      'WHERE t.run_id = :rid';
    EXECUTE IMMEDIATE v_sql INTO v_rows_recovered USING p_run_id;

    -- Fully correct rows
    v_sql :=
      'SELECT COUNT(*) '||
      'FROM '||p_work_table||' w '||
      'JOIN '||p_baseline||' b ON b.EmployeeID = w.EmployeeID '||
      'JOIN tamper_log t ON t.EmployeeID = w.EmployeeID AND t.run_id = :rid '||
      'WHERE ( '||
      '  DECODE(w.FirstName,  b.FirstName, 1, 0) + '||
      '  DECODE(w.LastName,   b.LastName,  1, 0) + '||
      '  DECODE(w.PhoneNumber,b.PhoneNumber,1,0) + '||
      '  DECODE(w.Email,      b.Email,     1, 0) + '||
      '  DECODE(w.Department, b.Department,1,0) + '||
      '  DECODE(w.Position,   b.Position,  1, 0) + '||
      '  DECODE(TRUNC(w.HireDate), TRUNC(b.HireDate), 1, 0) + '||
      '  DECODE(w.SalaryRM,   b.SalaryRM,  1, 0) '||
      ') = 8';
    EXECUTE IMMEDIATE v_sql INTO v_fully_correct USING p_run_id;

    -- Total fields in recovered rows
    v_total_fields := v_rows_recovered * 8;

    -- Correct fields
    v_sql :=
      'SELECT NVL(SUM( '||
      '  DECODE(w.FirstName,  b.FirstName, 1, 0) + '||
      '  DECODE(w.LastName,   b.LastName,  1, 0) + '||
      '  DECODE(w.PhoneNumber,b.PhoneNumber,1,0) + '||
      '  DECODE(w.Email,      b.Email,     1, 0) + '||
      '  DECODE(w.Department, b.Department,1,0) + '||
      '  DECODE(w.Position,   b.Position,  1, 0) + '||
      '  DECODE(TRUNC(w.HireDate), TRUNC(b.HireDate), 1, 0) + '||
      '  DECODE(w.SalaryRM,   b.SalaryRM,  1, 0) '||
      '), 0) '||
      'FROM '||p_work_table||' w '||
      'JOIN '||p_baseline||' b ON b.EmployeeID = w.EmployeeID '||
      'JOIN tamper_log t ON t.EmployeeID = w.EmployeeID AND t.run_id = :rid';
    EXECUTE IMMEDIATE v_sql INTO v_correct_fields USING p_run_id;

    -- Completeness
    v_sql :=
      'SELECT COUNT(*) '||
      'FROM '||p_work_table||' w '||
      'JOIN tamper_log t ON t.EmployeeID = w.EmployeeID AND t.run_id = :rid '||
      'WHERE w.FirstName   IS NOT NULL '||
      '  AND w.LastName    IS NOT NULL '||
      '  AND w.PhoneNumber IS NOT NULL '||
      '  AND w.Email       IS NOT NULL '||
      '  AND w.Department  IS NOT NULL '||
      '  AND w.Position    IS NOT NULL '||
      '  AND w.HireDate    IS NOT NULL '||
      '  AND w.SalaryRM    IS NOT NULL';
    EXECUTE IMMEDIATE v_sql INTO v_complete_rows USING p_run_id;

    -- Percentages
    v_quality_pct   := CASE WHEN v_total_fields = 0 THEN 0 ELSE (v_correct_fields / v_total_fields) * 100 END;
    v_complete_pct  := CASE WHEN v_rows_recovered = 0 THEN 0 ELSE (v_complete_rows / v_rows_recovered) * 100 END;

    -- Store results
    INSERT INTO recovery_accuracy_results
      (dataset, rows_deleted, rows_recovered, fully_correct, quality_percent, completeness_percent)
    VALUES
      (p_dataset, 5, v_rows_recovered, v_fully_correct,
       ROUND(v_quality_pct, 2), ROUND(v_complete_pct, 2));

    DBMS_OUTPUT.PUT_LINE(
      '['||p_dataset||'] Recovered='||v_rows_recovered||
      ', FullyCorrect='||v_fully_correct||
      ', Quality%='||ROUND(v_quality_pct,2)||
      ', Complete%='||ROUND(v_complete_pct,2)
    );
  END;

BEGIN
  -- Clean previous results
  DELETE FROM tamper_log;
  DELETE FROM recovery_accuracy_results;
  COMMIT;

  -- Loop through datasets
  FOR i IN 1 .. v_datasets.COUNT LOOP
    v_run_id     := run_id_seq.NEXTVAL;
    v_work_table := 'EMP_WORK_'||v_datasets(i).label;

    -- Create working copy
    BEGIN EXECUTE IMMEDIATE 'DROP TABLE '||v_work_table||' PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
    EXECUTE IMMEDIATE 'CREATE TABLE '||v_work_table||' AS SELECT * FROM '||v_datasets(i).base_table;

    -- Tamper
    tamper_salary_and_log(v_datasets(i).label, v_work_table, v_run_id);

    -- Recover
    recover_work_table(v_datasets(i).base_table, v_work_table);

    -- Measure
    measure_and_store(v_datasets(i).label, v_datasets(i).base_table, v_work_table, v_run_id);

    -- Drop working table
    BEGIN EXECUTE IMMEDIATE 'DROP TABLE '||v_work_table||' PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
    COMMIT;
  END LOOP;
END;
/

-- view table
SELECT dataset AS "Dataset",
       5 AS "Rows Modified",
       5 AS "Rows Tampered",
       rows_recovered AS "Rows Recovered",
       fully_correct AS "Fully Correct",
       quality_percent AS "Quality %",
       completeness_percent AS "Completeness %"
FROM recovery_accuracy_results
ORDER BY CASE dataset WHEN '1k' THEN 1 WHEN '5k' THEN 2 ELSE 3 END;

