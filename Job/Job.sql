BEGIN
    DBMS_SCHEDULER.CREATE_JOB (
        job_name        => 'JOB_EJUD_MPM',
        job_type        => 'PLSQL_BLOCK',
        job_action      => 'BEGIN p_ejud_mpm; END;',
        start_date      => TRUNC(SYSDATE) + 1, -- Starts at midnight tomorrow
        repeat_interval => 'FREQ=DAILY; BYHOUR=0; BYMINUTE=0; BYSECOND=0',
        enabled         => TRUE,
        comments        => 'Job para ser executado todo dia meia noite'
    );
END;


BEGIN
   DBMS_SCHEDULER.DROP_JOB(job_name => 'JOB_EJUD_MPM_BASEDEDADOS');
END;
/

SELECT JOB_NAME, STATE, LAST_START_DATE, NEXT_RUN_DATE
FROM USER_SCHEDULER_JOBS
WHERE JOB_NAME = 'JOB_EJUD_MPM_BASEDEDADOS';

SELECT JOB_NAME, STATUS, ACTUAL_START_DATE, RUN_DURATION
FROM USER_SCHEDULER_JOB_RUN_DETAILS
WHERE JOB_NAME = 'JOB_EJUD_MPM_BASEDEDADOS'
ORDER BY ACTUAL_START_DATE DESC;

SELECT OWNER, JOB_NAME, STATE, ENABLED, LAST_START_DATE, NEXT_RUN_DATE
FROM ALL_SCHEDULER_JOBS WHERE JOB_NAME = 'JOB_EJUD_MPM_BASEDEDADOS'
ORDER BY OWNER, JOB_NAME;

