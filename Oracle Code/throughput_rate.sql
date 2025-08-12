-- Drop throughput table if exists
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE throughput_results PURGE';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -942 THEN
            RAISE;
        END IF;
END;
/

-- Create fresh throughput table
CREATE TABLE throughput_results (
    scenario VARCHAR2(20),
    dataset VARCHAR2(10),
    rows_processed NUMBER,
    elapsed_ms NUMBER,
    throughput_rows_ms NUMBER
);


DECLARE
    TYPE t_ds IS RECORD (label VARCHAR2(10), base_table VARCHAR2(30));
    TYPE t_ds_tab IS TABLE OF t_ds;
    v_datasets t_ds_tab := t_ds_tab(
        t_ds('1k',  'EMP_1K_BASE'),
        t_ds('5k',  'EMP_5K_BASE'),
        t_ds('10k', 'EMP_10K_BASE')
    );

    TYPE t_num_tab IS TABLE OF NUMBER;
    v_delete_sizes t_num_tab := t_num_tab(1, 20, 50);
    v_insert_sizes t_num_tab := t_num_tab(1, 20, 50);

    v_run_id            NUMBER;
    v_work_table        VARCHAR2(128);
    v_sql               CLOB;
    v_start_time        TIMESTAMP;
    v_end_time          TIMESTAMP;
    v_elapsed_ms        NUMBER;
    v_max_id            NUMBER;

    ----------------------------------------------------------------------
    -- Simple perfect recovery procedure
    ----------------------------------------------------------------------
    PROCEDURE simple_recovery_from_baseline(
        p_baseline_table IN VARCHAR2,
        p_work_table     IN VARCHAR2
    ) IS
    BEGIN
        EXECUTE IMMEDIATE 'INSERT INTO ' || p_work_table ||
                          ' SELECT b.* FROM ' || p_baseline_table || ' b ' ||
                          ' WHERE NOT EXISTS (SELECT 1 FROM ' || p_work_table ||
                          ' w WHERE w.EMPLOYEEID = b.EMPLOYEEID)';
        COMMIT;
    END;

    ----------------------------------------------------------------------
    -- Recovery wrapper
    ----------------------------------------------------------------------
    PROCEDURE recover_work_table(p_baseline VARCHAR2, p_work_table VARCHAR2) IS
    BEGIN
        simple_recovery_from_baseline(p_baseline, p_work_table);
    END;

BEGIN
    ----------------------------------------------------------------------
    -- SCENARIO 1: DELETE
    ----------------------------------------------------------------------
    FOR i IN 1 .. v_datasets.COUNT LOOP
        FOR j IN 1 .. v_delete_sizes.COUNT LOOP
            v_run_id := run_id_seq.NEXTVAL;
            v_work_table := 'EMP_WORK_DEL_'||v_datasets(i).label||'_'||v_delete_sizes(j);

            BEGIN EXECUTE IMMEDIATE 'DROP TABLE '||v_work_table||' PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
            EXECUTE IMMEDIATE 'CREATE TABLE '||v_work_table||' AS SELECT * FROM '||v_datasets(i).base_table;

            -- Delete rows
            EXECUTE IMMEDIATE 'DELETE FROM '||v_work_table||' WHERE ROWNUM <= :n' USING v_delete_sizes(j);
            COMMIT;

            -- Timing start
            v_start_time := SYSTIMESTAMP;
            recover_work_table(v_datasets(i).base_table, v_work_table);
            v_end_time := SYSTIMESTAMP;

            v_elapsed_ms :=
                EXTRACT(DAY FROM (v_end_time - v_start_time)) * 86400000 +
                EXTRACT(HOUR FROM (v_end_time - v_start_time)) * 3600000 +
                EXTRACT(MINUTE FROM (v_end_time - v_start_time)) * 60000 +
                EXTRACT(SECOND FROM (v_end_time - v_start_time)) * 1000;

            INSERT INTO throughput_results
            VALUES ('Delete', v_datasets(i).label, v_delete_sizes(j),
                    v_elapsed_ms, ROUND(v_delete_sizes(j) / v_elapsed_ms, 6));
            COMMIT;
        END LOOP;
    END LOOP;

    ----------------------------------------------------------------------
    -- SCENARIO 2: MODIFY
    ----------------------------------------------------------------------
    FOR i IN 1 .. v_datasets.COUNT LOOP
        v_run_id := run_id_seq.NEXTVAL;
        v_work_table := 'EMP_WORK_MOD_'||v_datasets(i).label;

        BEGIN EXECUTE IMMEDIATE 'DROP TABLE '||v_work_table||' PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
        EXECUTE IMMEDIATE 'CREATE TABLE '||v_work_table||' AS SELECT * FROM '||v_datasets(i).base_table;

        -- Modify salary for first 5 employees
        EXECUTE IMMEDIATE 'UPDATE '||v_work_table||' SET SALARYRM = SALARYRM + 1000 WHERE ROWNUM <= 5';
        COMMIT;

        -- Timing start
        v_start_time := SYSTIMESTAMP;
        recover_work_table(v_datasets(i).base_table, v_work_table);
        v_end_time := SYSTIMESTAMP;

        v_elapsed_ms :=
            EXTRACT(DAY FROM (v_end_time - v_start_time)) * 86400000 +
            EXTRACT(HOUR FROM (v_end_time - v_start_time)) * 3600000 +
            EXTRACT(MINUTE FROM (v_end_time - v_start_time)) * 60000 +
            EXTRACT(SECOND FROM (v_end_time - v_start_time)) * 1000;

        INSERT INTO throughput_results
        VALUES ('Modify', v_datasets(i).label, 5,
                v_elapsed_ms, ROUND(5 / v_elapsed_ms, 6));
        COMMIT;
    END LOOP;

    ----------------------------------------------------------------------
    -- SCENARIO 3: INSERT
    ----------------------------------------------------------------------
    FOR i IN 1 .. v_datasets.COUNT LOOP
        FOR j IN 1 .. v_insert_sizes.COUNT LOOP
            v_run_id := run_id_seq.NEXTVAL;
            v_work_table := 'EMP_WORK_INS_'||v_datasets(i).label||'_'||v_insert_sizes(j);

            BEGIN EXECUTE IMMEDIATE 'DROP TABLE '||v_work_table||' PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
            EXECUTE IMMEDIATE 'CREATE TABLE '||v_work_table||' AS SELECT * FROM '||v_datasets(i).base_table;

            -- Get max EmployeeID
            v_sql := 'SELECT MAX(EMPLOYEEID) FROM '||v_work_table;
            EXECUTE IMMEDIATE v_sql INTO v_max_id;

            -- Insert fake rows
            EXECUTE IMMEDIATE
                'INSERT INTO '||v_work_table||
                ' (EMPLOYEEID, FIRSTNAME, LASTNAME, PHONENUMBER, EMAIL, DEPARTMENT, POSITION, HIREDATE, SALARYRM) '||
                ' SELECT '|| (v_max_id + 1) ||' + LEVEL - 1, ''FakeFirst'', ''FakeLast'', ''000'', ''fake@example.com'', '||
                ' ''Dept'', ''Pos'', SYSDATE, 0 FROM dual CONNECT BY LEVEL <= :n'
            USING v_insert_sizes(j);
            COMMIT;

            -- Timing start
            v_start_time := SYSTIMESTAMP;
            recover_work_table(v_datasets(i).base_table, v_work_table);
            v_end_time := SYSTIMESTAMP;

            v_elapsed_ms :=
                EXTRACT(DAY FROM (v_end_time - v_start_time)) * 86400000 +
                EXTRACT(HOUR FROM (v_end_time - v_start_time)) * 3600000 +
                EXTRACT(MINUTE FROM (v_end_time - v_start_time)) * 60000 +
                EXTRACT(SECOND FROM (v_end_time - v_start_time)) * 1000;

            INSERT INTO throughput_results
            VALUES ('Insert', v_datasets(i).label, v_insert_sizes(j),
                    v_elapsed_ms, ROUND(v_insert_sizes(j) / v_elapsed_ms, 6));
            COMMIT;
        END LOOP;
    END LOOP;
END;
/



-- View throughput results
SELECT * FROM throughput_results ORDER BY scenario, dataset, rows_processed;