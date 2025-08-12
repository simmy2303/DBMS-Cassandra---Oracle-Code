DECLARE
  TYPE t_ds IS RECORD (label VARCHAR2(10), base_table VARCHAR2(30));
  TYPE t_ds_tab IS TABLE OF t_ds;

  v_datasets t_ds_tab := t_ds_tab(
    t_ds('1k',  'EMP_1K_BASE'),
    t_ds('5k',  'EMP_5K_BASE'),
    t_ds('10k', 'EMP_10K_BASE')
  );

  TYPE t_num_tab IS TABLE OF NUMBER;
  v_insert_counts t_num_tab := t_num_tab(1, 20, 50);

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
  -- Recovery: remove inserted fake rows (those not in baseline)
  ------------------------------------------------------------------
  PROCEDURE remove_inserted_rows(
      p_baseline_table IN VARCHAR2,
      p_work_table     IN VARCHAR2
  ) IS
      v_sql CLOB;
  BEGIN
      v_sql := 'DELETE FROM ' || p_work_table || ' w ' ||
               'WHERE NOT EXISTS (SELECT 1 FROM ' || p_baseline_table || ' b ' ||
               '                  WHERE b.EmployeeID = w.EmployeeID)';
      EXECUTE IMMEDIATE v_sql;
      COMMIT;
  END;

  PROCEDURE recover_work_table(p_baseline VARCHAR2, p_work_table VARCHAR2) IS
  BEGIN
      remove_inserted_rows(p_baseline, p_work_table);
  END;

  ------------------------------------------------------------------
  -- Tamper: insert fake rows
  ------------------------------------------------------------------
  PROCEDURE insert_fake_rows_and_log(
      p_dataset VARCHAR2,
      p_work_table VARCHAR2,
      p_num_rows NUMBER,
      p_run_id NUMBER
  ) IS
  BEGIN
    FOR i IN 1..p_num_rows LOOP
      v_sql :=
        'INSERT INTO '||p_work_table||
        ' (EmployeeID, FirstName, LastName, PhoneNumber, Email, Department, Position, HireDate, SalaryRM) '||
        'VALUES (:id, ''FakeFirst'', ''FakeLast'', ''0000000000'', ''fake'||i||'@test.com'', '||
        '''FakeDept'', ''FakePos'', SYSDATE, 0)';
      EXECUTE IMMEDIATE v_sql USING (999999 + i + p_num_rows); -- big ID to avoid conflict
    END LOOP;

    -- Log inserted fake IDs
    v_sql :=
      'INSERT INTO tamper_log (run_id, dataset, rows_deleted, employeeid) '||
      'SELECT :rid, :dsl, :cnt, employeeid '||
      'FROM (SELECT employeeid FROM '||p_work_table||' '||
      'WHERE employeeid >= 999999 ORDER BY employeeid)';
    EXECUTE IMMEDIATE v_sql USING p_run_id, p_dataset, p_num_rows;

    COMMIT;
  END;

  ------------------------------------------------------------------
  -- Measure metrics
  ------------------------------------------------------------------
  PROCEDURE measure_and_store(
      p_dataset     VARCHAR2,
      p_baseline    VARCHAR2,
      p_work_table  VARCHAR2,
      p_run_id      NUMBER,
      p_rows_inserted NUMBER
  ) IS
  BEGIN
    -- After recovery, "rows recovered" = rows removed
    v_sql :=
      'SELECT COUNT(*) FROM '||p_baseline||' b '||
      'WHERE EXISTS (SELECT 1 FROM '||p_work_table||' w '||
      '              WHERE w.EmployeeID = b.EmployeeID)';
    EXECUTE IMMEDIATE v_sql INTO v_rows_recovered;

    -- Fully correct rows
    v_sql :=
      'SELECT COUNT(*) FROM '||p_work_table||' w '||
      'JOIN '||p_baseline||' b ON b.EmployeeID = w.EmployeeID '||
      'WHERE ( '||
      '  DECODE(w.FirstName,  b.FirstName, 1, 0) + '||
      '  DECODE(w.LastName,   b.LastName,  1, 0) + '||
      '  DECODE(w.PhoneNumber,b.PhoneNumber,1,0) + '||
      '  DECODE(w.Email,      b.Email,     1, 0) + '||
      '  DECODE(w.Department, b.Department,1, 0) + '||
      '  DECODE(w.Position,   b.Position,  1, 0) + '||
      '  DECODE(TRUNC(w.HireDate), TRUNC(b.HireDate), 1, 0) + '||
      '  DECODE(w.SalaryRM,   b.SalaryRM,  1, 0) '||
      ') = 8';
    EXECUTE IMMEDIATE v_sql INTO v_fully_correct;

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
      '), 0) FROM '||p_work_table||' w '||
      'JOIN '||p_baseline||' b ON b.EmployeeID = w.EmployeeID';
    EXECUTE IMMEDIATE v_sql INTO v_correct_fields;

    -- Completeness
    v_sql :=
      'SELECT COUNT(*) FROM '||p_work_table||' w '||
      'WHERE w.FirstName IS NOT NULL '||
      '  AND w.LastName IS NOT NULL '||
      '  AND w.PhoneNumber IS NOT NULL '||
      '  AND w.Email IS NOT NULL '||
      '  AND w.Department IS NOT NULL '||
      '  AND w.Position IS NOT NULL '||
      '  AND w.HireDate IS NOT NULL '||
      '  AND w.SalaryRM IS NOT NULL';
    EXECUTE IMMEDIATE v_sql INTO v_complete_rows;

    -- Percentages
    v_quality_pct   := CASE WHEN v_total_fields = 0 THEN 0 ELSE (v_correct_fields / v_total_fields) * 100 END;
    v_complete_pct  := CASE WHEN v_rows_recovered = 0 THEN 0 ELSE (v_complete_rows / v_rows_recovered) * 100 END;

    INSERT INTO recovery_accuracy_results
      (dataset, rows_deleted, rows_recovered, fully_correct, quality_percent, completeness_percent)
    VALUES
      (p_dataset, p_rows_inserted, v_rows_recovered, v_fully_correct,
       ROUND(v_quality_pct, 2), ROUND(v_complete_pct, 2));
  END;

BEGIN
  DELETE FROM tamper_log;
  DELETE FROM recovery_accuracy_results;
  COMMIT;

  FOR i IN 1 .. v_datasets.COUNT LOOP
    FOR j IN 1 .. v_insert_counts.COUNT LOOP
      v_run_id     := run_id_seq.NEXTVAL;
      v_work_table := 'EMP_WORK_'||v_datasets(i).label||'_INS_'||v_insert_counts(j);

      -- Fresh working copy
      BEGIN EXECUTE IMMEDIATE 'DROP TABLE '||v_work_table||' PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
      EXECUTE IMMEDIATE 'CREATE TABLE '||v_work_table||' AS SELECT * FROM '||v_datasets(i).base_table;

      -- Insert fake rows
      insert_fake_rows_and_log(v_datasets(i).label, v_work_table, v_insert_counts(j), v_run_id);

      -- Recover (remove inserted)
      recover_work_table(v_datasets(i).base_table, v_work_table);

      -- Measure
      measure_and_store(v_datasets(i).label, v_datasets(i).base_table, v_work_table, v_run_id, v_insert_counts(j));

      -- Cleanup
      BEGIN EXECUTE IMMEDIATE 'DROP TABLE '||v_work_table||' PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
      COMMIT;
    END LOOP;
  END LOOP;
END;
/


-- view table
SELECT dataset AS "Dataset",
       rows_deleted AS "Rows Inserted",
       rows_recovered AS "Rows Recovered",
       fully_correct AS "Fully Correct",
       quality_percent AS "Quality %",
       completeness_percent AS "Completeness %"
FROM recovery_accuracy_results
ORDER BY CASE dataset WHEN '1k' THEN 1 WHEN '5k' THEN 2 ELSE 3 END, rows_deleted;
