-- process and calculate the Rows Deleted, Rows Recovered, Fully Correct, Quality %, Completeness % 
DECLARE
  ------------------------------------------------------------------
  -- CONFIG: map dataset label -> baseline table
  ------------------------------------------------------------------
  TYPE t_ds IS RECORD (label VARCHAR2(10), base_table VARCHAR2(30));
  TYPE t_ds_tab IS TABLE OF t_ds;

  v_datasets t_ds_tab := t_ds_tab(
    t_ds('1k',  'EMP_1K_BASE'),
    t_ds('5k',  'EMP_5K_BASE'),
    t_ds('10k', 'EMP_10K_BASE')
  );

  TYPE t_num_tab IS TABLE OF NUMBER;
  v_delete_sizes t_num_tab := t_num_tab(1, 20, 50);

  ------------------------------------------------------------------
  -- Vars
  ------------------------------------------------------------------
  v_run_id            NUMBER;
  v_work_table        VARCHAR2(128);
  v_sql               CLOB;

  v_rows_recovered    NUMBER;
  v_fully_correct     NUMBER;
  v_total_fields      NUMBER;
  v_correct_fields    NUMBER;
  v_complete_rows     NUMBER;

  v_quality_pct       NUMBER;
  v_complete_pct      NUMBER;

  ------------------------------------------------------------------
  -- Simple perfect recovery procedure
  ------------------------------------------------------------------
  PROCEDURE simple_recovery_from_baseline(
      p_baseline_table IN VARCHAR2,
      p_work_table     IN VARCHAR2
  ) IS
      v_sql CLOB;
  BEGIN
      v_sql := 'INSERT INTO ' || p_work_table || ' ' ||
               'SELECT b.* FROM ' || p_baseline_table || ' b ' ||
               'WHERE NOT EXISTS (SELECT 1 FROM ' || p_work_table || ' w ' ||
               '                  WHERE w.EmployeeID = b.EmployeeID)';
      EXECUTE IMMEDIATE v_sql;
      COMMIT;
  END;

  ------------------------------------------------------------------
  -- Wrapper to call the recovery procedure
  ------------------------------------------------------------------
  PROCEDURE recover_work_table(p_baseline VARCHAR2, p_work_table VARCHAR2) IS
  BEGIN
      simple_recovery_from_baseline(p_baseline, p_work_table);
  END;

  ------------------------------------------------------------------
  -- Delete N deterministic rows from the work table and log them
  ------------------------------------------------------------------
  PROCEDURE tamper_and_log(p_dataset VARCHAR2, p_work_table VARCHAR2, p_rows_del NUMBER, p_run_id NUMBER) IS
  BEGIN
    -- Log the EmployeeIDs we will delete
    v_sql :=
      'INSERT INTO tamper_log (run_id, dataset, rows_deleted, employeeid) ' ||
      'SELECT :rid, :dsl, :rdel, employeeid ' ||
      'FROM (SELECT employeeid FROM '||p_work_table||' ORDER BY employeeid) ' ||
      'WHERE ROWNUM <= :rdel2';
    EXECUTE IMMEDIATE v_sql USING p_run_id, p_dataset, p_rows_del, p_rows_del;

    -- Perform the delete
    v_sql :=
      'DELETE FROM '||p_work_table||' w ' ||
      'WHERE EXISTS (SELECT 1 FROM tamper_log t ' ||
      '              WHERE t.run_id = :rid AND t.employeeid = w.employeeid)';
    EXECUTE IMMEDIATE v_sql USING p_run_id;
  END;

  ------------------------------------------------------------------
  -- Measure metrics for a given run_id vs baseline/work tables
  ------------------------------------------------------------------
  PROCEDURE measure_and_store(
      p_dataset     VARCHAR2,
      p_rows_del    NUMBER,
      p_baseline    VARCHAR2,
      p_work_table  VARCHAR2,
      p_run_id      NUMBER
  ) IS
  BEGIN
    -- Rows recovered
    v_sql :=
      'SELECT COUNT(*) '||
      'FROM '||p_work_table||' w '||
      'JOIN tamper_log t ON t.employeeid = w.employeeid '||
      'WHERE t.run_id = :rid';
    EXECUTE IMMEDIATE v_sql INTO v_rows_recovered USING p_run_id;

    -- Fully correct rows
    v_sql :=
      'SELECT COUNT(*) '||
      'FROM '||p_work_table||' w '||
      'JOIN '||p_baseline||' b ON b.employeeid = w.employeeid '||
      'JOIN tamper_log t ON t.employeeid = w.employeeid AND t.run_id = :rid '||
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

    -- Total fields
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
      'JOIN '||p_baseline||' b ON b.employeeid = w.employeeid '||
      'JOIN tamper_log t ON t.employeeid = w.employeeid AND t.run_id = :rid';
    EXECUTE IMMEDIATE v_sql INTO v_correct_fields USING p_run_id;

    -- Completeness
    v_sql :=
      'SELECT COUNT(*) '||
      'FROM '||p_work_table||' w '||
      'JOIN tamper_log t ON t.employeeid = w.employeeid AND t.run_id = :rid '||
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
      (p_dataset, p_rows_del, v_rows_recovered, v_fully_correct,
       ROUND(v_quality_pct, 2), ROUND(v_complete_pct, 2));

    DBMS_OUTPUT.PUT_LINE(
      RPAD('['||p_dataset||' / del='||p_rows_del||']', 18)||
      ' Recovered='||v_rows_recovered||
      ', FullyCorrect='||v_fully_correct||
      ', Quality%='||TO_CHAR(ROUND(v_quality_pct,2),'990D99')||
      ', Complete%='||TO_CHAR(ROUND(v_complete_pct,2),'990D99')
    );
  END;

BEGIN
  -- Clean previous results
  DELETE FROM tamper_log;
  DELETE FROM recovery_accuracy_results;
  COMMIT;

  -- Loop datasets × delete sizes
  FOR i IN 1 .. v_datasets.COUNT LOOP
    FOR j IN 1 .. v_delete_sizes.COUNT LOOP
      v_run_id     := run_id_seq.NEXTVAL;
      v_work_table := 'EMP_WORK_'||REPLACE(v_datasets(i).label,'K','K')||'_'||v_delete_sizes(j);

      -- Fresh working copy
      BEGIN EXECUTE IMMEDIATE 'DROP TABLE '||v_work_table||' PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
      EXECUTE IMMEDIATE 'CREATE TABLE '||v_work_table||' AS SELECT * FROM '||v_datasets(i).base_table;

      -- Tamper rows
      tamper_and_log(v_datasets(i).label, v_work_table, v_delete_sizes(j), v_run_id);

      -- Recover rows
      recover_work_table(v_datasets(i).base_table, v_work_table);

      -- Measure metrics
      measure_and_store(
        p_dataset    => v_datasets(i).label,
        p_rows_del   => v_delete_sizes(j),
        p_baseline   => v_datasets(i).base_table,
        p_work_table => v_work_table,
        p_run_id     => v_run_id
      );

      -- Drop working table
      BEGIN EXECUTE IMMEDIATE 'DROP TABLE '||v_work_table||' PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
      COMMIT;
    END LOOP;
  END LOOP;
END;
/


-- print results
SELECT dataset,
       rows_deleted,
       rows_recovered,
       fully_correct,
       quality_percent AS "Quality %",
       completeness_percent AS "Completeness %"
FROM recovery_accuracy_results
ORDER BY CASE dataset WHEN '1k' THEN 1 WHEN '5k' THEN 2 ELSE 3 END, rows_deleted;
