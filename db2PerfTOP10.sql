--                                                                   --
-- Analyzes dynamic SQL snapshot data to identify 'top 10' most      --
-- expensive statements in a number of metrics.                      --
-- Also identifies statements using literals which could be replaced --
-- by parameter markers.                                             --
--                                                                   --
-----------------------------------------------------------------------


   ----------------------------------------------------------------------
   -- We're going to select all dynamic SQL snapshot data into a work table.
   -- We don't want to have to know what the format of the dynamic SQL
   -- snapshot table function is, so we use a view over it, and then
   -- create a table to match the view.

   drop VIEW db2perf_dynsql_view;
   drop TABLE db2perf_dynsql;

   CREATE VIEW db2perf_dynsql_view AS SELECT * FROM table(snap_get_dyn_sql(CAST (NULL as varchar(256)),-1)) as t;

   CREATE TABLE db2perf_dynsql LIKE db2perf_dynsql_view;

   -- Get the snapshot data, and then drop the view since we no longer need it.
   INSERT INTO db2perf_dynsql SELECT * FROM table(snap_get_dyn_sql(CAST (NULL as varchar(256)),-1)) as t;
   DROP VIEW db2perf_dynsql_view;


   ----------------------------------------------------------------------
   -- We want to keep some extra information about each statement in the snapshot
   -- table, so we add extra columns to track rank (1-10) by each of our
   -- metrics, and percent of total statistics as well.

   ALTER TABLE db2perf_dynsql
        ADD COLUMN top10_elapsed CHAR(2)
        ADD COLUMN top10_CPU CHAR(2)
        ADD COLUMN top10_phys_read CHAR(2)
        ADD COLUMN top10_rows_read CHAR(2)
        ADD COLUMN top10_sorts CHAR(2)
        ADD COLUMN top10_spilled CHAR(2)
        ADD COLUMN pct_of_total_elapsed float
        ADD COLUMN pct_of_total_CPU float
        ADD COLUMN pct_of_total_data_preads float
        ADD COLUMN pct_of_total_index_preads float
        ADD COLUMN pct_of_total_rows_read float
        ADD COLUMN pct_of_total_sorts float
        ADD COLUMN pct_of_total_spilled_sorts float
        ADD COLUMN compressed_statement VARCHAR(3000);

   -- We'll fill in the ranks later.
   UPDATE db2perf_dynsql SET (top10_elapsed,top10_cpu,top10_phys_read,top10_rows_read,top10_sorts,top10_spilled) =
        ('  ','  ','  ','  ','  ','  ');

   -- We now calculate the percentage statistics by dividing each row's values by the totals for each metric.
   UPDATE db2perf_dynsql SET (
        pct_of_total_elapsed,
        pct_of_total_CPU,
        pct_of_total_data_preads,
        pct_of_total_index_preads,
        pct_of_total_rows_read,
        pct_of_total_sorts,
        pct_of_total_spilled_sorts ) = (

          -- This statement's execution time as a % of total for all statements
          ((CAST(total_exec_time AS FLOAT) + CAST(total_exec_time_ms AS FLOAT) / 1000000.0) * 100 /
             (SELECT CAST(sum(CAST(total_exec_time as BIGINT)) as FLOAT) +
                     CAST(sum(CAST(total_exec_time_ms as BIGINT)) AS FLOAT)/1000000.0 + 1 from db2perf_dynsql)),

          -- This statement's CPU time as a % of total for all statements
          ((CAST(total_usr_time AS FLOAT) + CAST(total_usr_time_ms AS FLOAT) / 1000000.0 +
            CAST(total_sys_time AS FLOAT) + CAST(total_sys_time_ms AS FLOAT) / 1000000.0) * 100 /
             (SELECT CAST(sum(CAST(total_usr_time as BIGINT)) as FLOAT) +
                     CAST(sum(CAST(total_usr_time_ms as BIGINT)) as float)/1000000.0 +
                     CAST(sum(CAST(total_sys_time as BIGINT)) as FLOAT) +
                     CAST(sum(CAST(total_sys_time_ms as BIGINT)) as float)/1000000.0 + 1 from db2perf_dynsql )),

          -- This statement's physical data reads as a % of total for all statements
          (CAST(pool_temp_data_p_reads + pool_data_p_reads AS FLOAT) * 100 /
             (SELECT sum(CAST(pool_temp_data_p_reads + pool_data_p_reads as FLOAT)) + 1 from db2perf_dynsql )),

          -- This statement's physical index reads as a % of total for all statements
          (CAST(pool_temp_index_p_reads + pool_index_p_reads AS FLOAT) * 100 /
             (SELECT sum(CAST(pool_temp_index_p_reads + pool_index_p_reads as FLOAT)) + 1 from db2perf_dynsql )),

          -- This statement's rows read as a % of total for all statements
          (CAST(rows_read AS FLOAT) * 100 /
             (SELECT sum(CAST(rows_read as FLOAT)) + 1 from db2perf_dynsql )),

          -- This statement's sorts as a % of total for all statements
          (CAST(stmt_sorts AS FLOAT) * 100 /
             (SELECT sum(CAST(stmt_sorts as FLOAT)) + 1 from db2perf_dynsql )),

          -- This statement's sort overflows as a % of total for all statements
          (CAST(sort_overflows AS FLOAT) * 100 /
             (SELECT sum(CAST(sort_overflows as FLOAT)) + 1 from db2perf_dynsql ))
        );



echo ************************************************************;
echo Top 10 dynamic SQL statements by execution time;
echo ************************************************************;
echo;


   ----------------------------------------------------------------------
   -- It's easy to calculate the 'top 10' for any metric, but at the end of all this, we
   -- will have a reason to have recorded which statements have what rank for this metric.
   -- We use SELECT (to get the metric and the rank), UPDATE to save the rank, and then
   -- SELECT again to return the results to the user.
   -- We use the row_number() function to determine the number of the row in the innermost result
   -- set.  This is the rank.
   -- Then we use an UPDATE to save that rank back into our snapshot table.
   -- Then we SELECT again, to return the result to the user.

   SELECT
        substr(char(row_num),1,2) as "#","Executions","Exec Time","% of Total","sec / 100","Statement"
   FROM
        OLD TABLE
             ( UPDATE
                  (
                    SELECT
                       CAST(num_executions
                         as INTEGER) as "Executions",
                       CAST(
                           (CAST(total_exec_time
                              as FLOAT) +
                            CAST(total_exec_time_ms
                              as FLOAT)/1000000.0)
                         as DECIMAL(10,3)) as "Exec Time",
                       CAST( pct_of_total_elapsed
                         as SMALLINT) as "% of Total",
                       CAST(
                           (CAST(total_exec_time
                              as FLOAT) +
                            CAST(total_exec_time_ms
                              as FLOAT)/1000000.0) / (num_executions+1) * 100
                         as DECIMAL(10,3)) as "sec / 100",
                       top10_elapsed,
                       row_number() over
                         (ORDER BY (1000000*CAST(total_exec_time as BIGINT) + total_exec_time_ms) DESC)
                         as row_num,
                       substr( stmt_text,1,80 )
                         as "Statement"
                    FROM db2perf_dynsql
                    WHERE total_exec_time + total_exec_time_ms > 0
                    ORDER BY (1000000*total_exec_time + total_exec_time_ms) DESC
                    FETCH FIRST 10 ROWS ONLY )
              SET top10_elapsed = char(row_num) );



echo ************************************************************;
echo Top 10 dynamic SQL statements by CPU time;
echo ************************************************************;
echo;

   ----------------------------------------------------------------------
   -- Very similar to above, except in this case it's CPU time.

   SELECT
        substr(char(row_num),1,2) as "#","Executions","CPU Time","% of Total","sec / 100","Statement"
   FROM
        OLD TABLE
             ( UPDATE
                  ( SELECT
                       CAST(num_executions
                         as INTEGER) as "Executions",
                       CAST(
                           (CAST(total_usr_time
                              as FLOAT) +
                            CAST(total_usr_time_ms
                              as float)/1000000.0)
                         as DECIMAL(10,3)) +
                       CAST(
                           (CAST(total_sys_time
                              as FLOAT) +
                            CAST(total_sys_time_ms
                              as float)/1000000.0)
                         as DECIMAL(10,3)) as "CPU Time",
                       CAST(pct_of_total_cpu
                         as SMALLINT) as "% of Total",
                       CAST(
                           (CAST(total_usr_time
                              as FLOAT) +
                            CAST(total_usr_time_ms
                              as float)/1000000.0) / (num_executions+1)
                         as DECIMAL(10,3)) +
                       CAST(
                           (CAST(total_sys_time
                              as FLOAT) +
                            CAST(total_sys_time_ms
                              as float)/1000000.0) / (num_executions+1) * 100 as DECIMAL(10,3)) as "sec / 100",
                       top10_cpu,
                       row_number() over
                                (ORDER BY (1000000*CAST(total_usr_time as BIGINT) + total_usr_time_ms +
                                           1000000*CAST(total_sys_time as BIGINT) + total_sys_time_ms) DESC) as row_num,
                       substr( stmt_text,1,80 ) as "Statement"
                    FROM db2perf_dynsql
                    WHERE total_usr_time + total_usr_time_ms + total_sys_time + total_sys_time_ms > 0
                    ORDER BY "CPU Time" DESC
                    FETCH FIRST 10 ROWS ONLY )
              SET top10_cpu = char(row_num) );


echo ************************************************************;
echo Top 10 dynamic SQL statements by most physical reads;
echo ************************************************************;
echo;

   SELECT
        substr(char(row_num),1,2) as "#","Executions","Data H/R","% Data phys rd","Index H/R","% Idx phys rd","Total phys rd","Statement"
   FROM
        OLD TABLE
             ( UPDATE
                  (
                    SELECT
                       CAST(num_executions
                         as INTEGER) as "Executions",
                       CAST(
                           (CAST( (pool_data_l_reads+pool_temp_data_l_reads) -
                                  (pool_data_p_reads+pool_temp_data_p_reads)
                              as double))*100.0 /
                            (pool_data_l_reads+pool_temp_data_l_reads+1)
                         as DECIMAL(3,1)) as "Data H/R",
                       CAST(pct_of_total_data_preads
                         as SMALLINT) as "% Data phys rd",
                       CAST(
                           (CAST( (pool_index_l_reads+pool_temp_index_l_reads) -
                                  (pool_index_p_reads+pool_temp_index_p_reads)
                              as double))*100.0 /
                            (pool_index_l_reads+pool_temp_index_l_reads+1)
                         as DECIMAL(3,1)) as "Index H/R",
                       CAST(pct_of_total_index_preads
                         as SMALLINT) as "% Idx phys rd",
                       top10_phys_read,
                       row_number() over (ORDER BY (
                               (pool_data_p_reads  + pool_temp_data_p_reads +
                                pool_index_p_reads + pool_temp_index_p_reads)) DESC ) as row_num,
                       CAST(pool_data_p_reads  + pool_temp_data_p_reads +
                            pool_index_p_reads + pool_temp_index_p_reads
                         as INTEGER) as "Total phys rd",
                       substr( stmt_text,1,80 ) as "Statement"
                    FROM db2perf_dynsql
                    WHERE num_executions > 1
                      AND pool_data_l_reads+pool_temp_data_l_reads+pool_index_l_reads+pool_temp_index_l_reads > 100
                    ORDER BY pool_data_p_reads+pool_temp_data_p_reads+pool_index_p_reads+pool_temp_index_p_reads DESC
                    FETCH FIRST 10 ROWS ONLY )
              SET top10_phys_read = char(row_num) );



echo ************************************************************;
echo Top 10 dynamic SQL statements by most rows read;
echo ************************************************************;
echo;

   SELECT
        substr(char(row_num),1,2) as "#","Executions","Rows read","% of Total","r/r / 100","Statement"
   FROM
        OLD TABLE
             ( UPDATE
                  ( SELECT
                       CAST(num_executions
                         as INTEGER) as "Executions",
                       CAST(rows_read
                         as INTEGER) as "Rows read",
                       CAST(pct_of_total_rows_read
                         as SMALLINT) as "% of Total",
                       100 * CAST(
                                  round(
                                     CAST(rows_read
                                       as FLOAT) / (num_executions+1),0)
                               as INTEGER) as "r/r / 100",
                       top10_rows_read,
                       row_number() over (ORDER BY (rows_read) DESC) as row_num,
                       substr( stmt_text,1,80 ) as "Statement"
                    FROM db2perf_dynsql
                    WHERE rows_read > 0
                    ORDER BY "Rows read" DESC
                    FETCH FIRST 10 ROWS ONLY )
              SET top10_rows_read = char(row_num) );



echo ************************************************************;
echo Top 10 dynamic SQL statements by most sorts;
echo ************************************************************;
echo;

   SELECT
        substr(char(row_num),1,2) as "#","Executions","Sorts","% of Total","sorts / 100","Statement"
   FROM
        OLD TABLE
             ( UPDATE
                  ( SELECT
                       CAST(num_executions
                         as INTEGER) as "Executions",
                       CAST(stmt_sorts
                         as INTEGER) as "Sorts",
                       CAST(pct_of_total_sorts
                         as SMALLINT) as "% of Total",
                       100 * CAST(
                                  round(
                                     CAST(stmt_sorts
                                       as FLOAT) / (num_executions+1),0)
                               as INTEGER) as "sorts / 100",
                       top10_sorts,
                       row_number() over (ORDER BY (stmt_sorts) DESC) as row_num,
                       substr( stmt_text,1,80 ) as "Statement"
                    FROM db2perf_dynsql
                    WHERE stmt_sorts > 0
                    ORDER BY "Sorts" DESC
                    FETCH FIRST 10 ROWS ONLY )
              SET top10_sorts = char(row_num) );



echo ************************************************************;
echo Top 10 dynamic SQL statements by most sort overflows;
echo ************************************************************;
echo;

   SELECT
        substr(char(row_num),1,2) as "#","Executions","Sort Overflows","% of Total","s/s / 100","Statement"
   FROM
        OLD TABLE
             ( UPDATE
                  ( SELECT
                       CAST(num_executions
                         as INTEGER) as "Executions",
                       CAST(sort_overflows
                         as INTEGER) as "Sort Overflows",
                       CAST(pct_of_total_spilled_sorts
                         as SMALLINT) as "% of Total",
                       100 * CAST(
                                  round(
                                     CAST(sort_overflows
                                       as FLOAT) / (num_executions+1),0)
                               as INTEGER) as "s/s / 100",
                       top10_spilled,
                       row_number() over (ORDER BY (sort_overflows) DESC) as row_num,
                       substr( stmt_text,1,80 ) as "Statement"
                    FROM db2perf_dynsql
                    WHERE sort_overflows > 0
                    ORDER BY "Sort Overflows" DESC
                    FETCH FIRST 10 ROWS ONLY )
              SET top10_spilled = char(row_num) );




echo ************************************************************;
echo Combined ranking of top dynamic SQL statements;
echo ************************************************************;
echo;


   ----------------------------------------------------------------------
   -- Here, we pick up all statements that art Top 10 in some (any!) metric
   SELECT
        top10_elapsed           as "Rank elapsed",
        top10_CPU               as "Rank CPU",
        top10_phys_read         as "Rank phys rd",
        top10_rows_read         as "Rank R/R",
        top10_sorts             as "Rank sorts",
        top10_spilled           as "Rank sort ovf",
        substr( stmt_text,1,80) as "Statement"
        FROM db2perf_dynsql
        WHERE top10_elapsed     <> '  ' OR
              top10_CPU         <> '  ' OR
              top10_phys_read   <> '  ' OR
              top10_rows_read   <> '  ' OR
              top10_sorts       <> '  ' OR
              top10_spilled     <> '  ' ;


echo ************************************************************;
echo List of dynamic SQL statements which differ only by literal values;
echo (Good candidates for parameter markers);
echo ************************************************************;
echo;

   ----------------------------------------------------------------------
   -- Last but not least, we go through the statements looking for ones which
   -- use only literals, no parameter markers.
   -- We call a UDF which replaces the literals with parameter markers in our snapshot database table,
   -- and then we see which statements 'match up'.
   -- Note that our main target is lightweight statements, so for compatibility with the UDF, we go
   -- for statements up to 3000 characters.
   -- We only get statements that have been executed once, and we only display cases where 10 or more
   -- statements could collapse into one.:w


   UPDATE db2perf_dynsql
        SET compressed_statement = db2perf_RmLiterals( translate( CAST( substr(stmt_text,1,3000) as varchar(3000) ) ) )
        WHERE length(stmt_text) < 3000
          AND CAST( substr(stmt_text,1,3000) as varchar(3000) ) NOT LIKE '%?%';

   SELECT
        count(*) as "Count" , substr(compressed_statement,1,120) as "Statement without literals"
        FROM db2perf_dynsql
        WHERE
                substr( translate( ltrim( compressed_statement ) ),1,6 ) IN ( 'SELECT', 'INSERT', 'UPDATE' )
            AND num_executions = 1
        GROUP BY substr( compressed_statement,1,120 )
        HAVING count(*) > 10;
