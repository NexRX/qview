SET default_transaction_read_only = OFF;

CREATE OR REPLACE FUNCTION nuarpublication.fn_createforeigndatawrappers(
    server_name TEXT,
    schema_suffix TEXT,
    host TEXT,
    dbname TEXT,
    username TEXT,
    password TEXT
)
    RETURNS VOID AS
$$
DECLARE
    schemas       TEXT[] := ARRAY [
        'nuarcodelists_datamanagement',
        'nuarcodelists_platform',
        'nuarcodelists_transformation',
        'nuarcustombackdrop',
        'nuardata',
        'nuarorganisations',
        'nuarpublication',
        'nuarsubmissions',
        'nuarusercreateddata',
        'nuarversion'
        ];
    schema_name   TEXT;
    foreign_table RECORD;
BEGIN
    -- Create the foreign server
    EXECUTE format($f$
        CREATE SERVER IF NOT EXISTS %I
        FOREIGN DATA WRAPPER postgres_fdw
        OPTIONS (host %L, port '5432', dbname %L)
    $f$, server_name, host, dbname);

    -- Create user mapping
    EXECUTE format($f$
        CREATE USER MAPPING IF NOT EXISTS FOR %I
        SERVER %I
        OPTIONS (user %L, password %L)
    $f$, username, server_name, username, password);

    -- Loop through schemas to create and import
    FOREACH schema_name IN ARRAY schemas
        LOOP
            -- Create schema if not exists
            EXECUTE format($f$
            CREATE SCHEMA IF NOT EXISTS %I;
        $f$, schema_name || schema_suffix);

            -- Drop existing foreign tables before import
            FOR foreign_table IN
                SELECT foreign_table_name
                FROM information_schema.foreign_tables
                WHERE foreign_table_schema = schema_name || schema_suffix
                LOOP
                    EXECUTE format($f$
                DROP FOREIGN TABLE IF EXISTS %I.%I CASCADE;
            $f$, schema_name || schema_suffix, foreign_table.foreign_table_name);
                END LOOP;

            -- Import foreign schema
            EXECUTE format($f$
            IMPORT FOREIGN SCHEMA %I FROM SERVER %I INTO %I;
        $f$, schema_name, server_name, schema_name || schema_suffix);
        END LOOP;
END;
$$ LANGUAGE plpgsql;

ALTER FUNCTION nuarpublication.fn_createforeigndatawrappers(
    server_name TEXT,
    schema_suffix TEXT,
    host TEXT,
    dbname TEXT,
    username TEXT,
    password TEXT
    ) OWNER TO nuar_admin;


CREATE OR REPLACE FUNCTION nuarpublication.fn_recordcountvalidationrun_v211(runname character varying,
                                                                            sourceschemasuffix character varying,
                                                                            fdwservername character varying DEFAULT ''::character varying,
                                                                            fdwdbhostname character varying DEFAULT ''::character varying,
                                                                            fdwdbport character varying DEFAULT ''::character varying,
                                                                            fdwdbname character varying DEFAULT ''::character varying,
                                                                            fdwdbusername character varying DEFAULT ''::character varying,
                                                                            fdwdbpassword character varying DEFAULT ''::character varying,
                                                                            createanddropfdwserver boolean DEFAULT false,
                                                                            createanddropfdwschemas boolean DEFAULT false,
                                                                            publishablesubmissionsonly boolean DEFAULT false) RETURNS integer
    LANGUAGE plpgsql
AS
$$

DECLARE
    submissionid               character varying;
    submissionidsarray         character varying[];
    targetorganisationidsarray character varying[];
    tablesarray                character varying[];
    tablename                  character varying;
BEGIN

    TRUNCATE TABLE nuarvalidation.validationrunsource;
    TRUNCATE TABLE nuarvalidation.validationruntarget;
    TRUNCATE TABLE nuarvalidation.recordcountdifferences;
    TRUNCATE TABLE nuarvalidation.recordcounts;
    TRUNCATE TABLE nuarvalidation.filterorganisationdifferences;
    TRUNCATE TABLE nuarvalidation.missingsubmissions;

    DROP VIEW IF EXISTS targetsubmissions;
    DROP VIEW IF EXISTS filterorganisationids;
    DROP VIEW IF EXISTS targetorganisationids;
    DROP VIEW IF EXISTS sourcesubmissionids;

    IF (createanddropfdwserver)
    THEN
        -- Create the FDW Server and User Mapping
        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwserver(
			servername => ''%s'',
			fdwname => ''postgres_fdw'',
			hostname => ''%s'',
			port => ''%s'',
			databasename => ''%s'',
			username => ''%s'',
			userpassword => ''%s'');
	', fdwservername, fdwdbhostname, fdwdbport, fdwdbname, fdwdbusername, fdwdbpassword);
    END IF;

    IF (createanddropfdwschemas)
    THEN
        -- Create and import the FDW schemas
        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarcodelists_datamanagement_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarcodelists_datamanagement'');
	', sourceschemasuffix, fdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarcodelists_platform_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarcodelists_platform'');
	', sourceschemasuffix, fdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarcodelists_transformation_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarcodelists_transformation'');
	', sourceschemasuffix, fdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarcustombackdrop_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarcustombackdrop'');
	', sourceschemasuffix, fdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuardata_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuardata'');
	', sourceschemasuffix, fdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarorganisations_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarorganisations'');
	', sourceschemasuffix, fdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarsubmissions_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarsubmissions'');
	', sourceschemasuffix, fdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarversion_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarversion'');
	', sourceschemasuffix, fdwservername);

    END IF;

    CREATE TEMPORARY VIEW targetsubmissions AS
    SELECT systemid
    FROM nuarsubmissions.nuarsubmissionevent;

    SELECT ARRAY_AGG(systemid)
    FROM targetsubmissions
    INTO submissionidsarray;

    CREATE TEMPORARY VIEW filterorganisationids AS
    SELECT organisationid_fk
    FROM nuarpublication.nuarpublicationorganisationfilter;

    CREATE TEMPORARY VIEW targetorganisationids AS
    SELECT systemid, name
    FROM nuarorganisations.nuaractor;

    SELECT ARRAY_AGG(systemid)
    FROM targetorganisationids
    INTO targetorganisationidsarray;

    IF (publishablesubmissionsonly)
    THEN
        EXECUTE FORMAT('
	CREATE TEMPORARY VIEW sourcesubmissionids AS
	SELECT systemid, eventname, status, result, lifecyclestatus, dataproviderid_fk
	FROM nuarsubmissions_%s.nuarsubmissionevent
	WHERE dataproviderid_fk = ANY (''%s'')
	AND result = ''Success''
	AND status = ''Completed''
	AND lifecyclestatus = ''Publishable''
	AND NOT EXISTS (
	   SELECT systemid
	   FROM targetsubmissions
	   WHERE systemid = nuarsubmissions_%s.nuarsubmissionevent.systemid
	);
	', sourceschemasuffix, targetorganisationidsarray, sourceschemasuffix);
    ELSE
        EXECUTE FORMAT('
	CREATE TEMPORARY VIEW sourcesubmissionids AS
	SELECT systemid, eventname, status, result, lifecyclestatus, dataproviderid_fk
	FROM nuarsubmissions_%s.nuarsubmissionevent
	WHERE dataproviderid_fk = ANY (%L)
	AND NOT EXISTS (
	   SELECT systemid
	   FROM targetsubmissions
	   WHERE systemid = nuarsubmissions_%s.nuarsubmissionevent.systemid
	);
	', sourceschemasuffix, targetorganisationidsarray, sourceschemasuffix);
    END IF;

-- We record NUARActor records that are missing or additional relative to the filter list
    INSERT INTO nuarvalidation.filterorganisationdifferences
        (systemid, runname, rundate, organisationname, status, organisationid_fk)
    SELECT gen_random_uuid()::character varying,
           runname,
           NOW(),
           NULL,
           'In Filter List not in Target NUARActor Table',
           organisationid_fk
    FROM filterorganisationids filterids
    WHERE NOT EXISTS (SELECT systemid
                      FROM targetorganisationids
                      WHERE systemid = filterids.organisationid_fk);

    INSERT INTO nuarvalidation.filterorganisationdifferences
        (systemid, runname, rundate, organisationname, status, organisationid_fk)
    SELECT gen_random_uuid()::character varying,
           runname,
           NOW(),
           name,
           'In Target NUARActor table not in Filter List',
           systemid
    FROM targetorganisationids targetids
    WHERE NOT EXISTS (SELECT organisationid_fk
                      FROM filterorganisationids
                      WHERE organisationid_fk = targetids.systemid);

-- We record Submissions in the source and not in the target for the NUARActor records that we hold
    INSERT INTO nuarvalidation.missingsubmissions
    (systemid, runname, rundate, submissionname, sourcesubmissionstatus, sourcesubmissionresult, organisationid_fk,
     submissionid_fk, sourcelifecyclestatus)
    SELECT gen_random_uuid()::character varying,
           runname,
           NOW(),
           eventname,
           status,
           result,
           dataproviderid_fk,
           systemid,
           lifecyclestatus
    FROM sourcesubmissionids sourcesubmissions;

    CREATE TEMPORARY TABLE tablenames
    (
        tablenamestring character varying
    );

    CREATE TEMPORARY TABLE recordcountswithduplicates
    (
        runname           character varying,
        rundate           timestamp with time zone,
        sourceschemaname  character varying,
        targetschemaname  character varying,
        sourcetablename   character varying,
        targettablename   character varying,
        sourcerecordcount bigint,
        targetrecordcount bigint
    );

    TRUNCATE tablenames;
    INSERT INTO tablenames
    SELECT table_name::character varying as tablenamestring
    FROM information_schema.tables
    WHERE table_schema = 'nuarcodelists_datamanagement';

    tablesarray := '{}';
    SELECT ARRAY_AGG(tablenamestring) FROM tablenames INTO tablesarray;

    FOREACH tablename IN ARRAY tablesarray
        LOOP

            EXECUTE FORMAT('
		INSERT INTO nuarvalidation.validationruntarget
		SELECT ''%s'' as runname, NOW() as rundate, NULL as submissionid, ''nuarcodelists_datamanagement'' as schemaname, ''%s'' as tablename, COUNT(systemid) as recordcount
		FROM nuarcodelists_datamanagement.%s;
	', runname, tablename, tablename);

            EXECUTE FORMAT('
		INSERT INTO nuarvalidation.validationrunsource
		SELECT ''%s'' as runname, NOW() as rundate, NULL as submissionid, ''nuarcodelists_datamanagement_%s'' as schemaname, ''%s'' as tablename, COUNT(systemid) as recordcount
		FROM nuarcodelists_datamanagement_%s.%s;
	', runname, sourceschemasuffix, tablename, sourceschemasuffix, tablename);

            EXECUTE FORMAT('
		INSERT INTO recordcountswithduplicates
		SELECT ''%s'' as runname, NOW() as rundate, ''nuarcodelists_datamanagement_%s'' as sourceschemaname, ''nuarcodelists_datamanagement'' as targetschemaname, ''%s'' as sourcetablename, ''%s'' as targettablename, (SELECT COALESCE (SUM (recordcount), 0) AS sourcerecordcount FROM nuarvalidation.validationrunsource WHERE tablename = ''%s''), (SELECT COALESCE (SUM (recordcount), 0) AS targetrecordcount FROM nuarvalidation.validationruntarget WHERE tablename = ''%s'');
	', runname, sourceschemasuffix, tablename, tablename, tablename, tablename);

        END LOOP;

    TRUNCATE tablenames;
    INSERT INTO tablenames
    SELECT table_name::character varying as tablenamestring
    FROM information_schema.tables
    WHERE table_schema = 'nuarcodelists_platform';

    tablesarray := '{}';
    SELECT ARRAY_AGG(tablenamestring) FROM tablenames INTO tablesarray;

    FOREACH tablename IN ARRAY tablesarray
        LOOP

            EXECUTE FORMAT('
		INSERT INTO nuarvalidation.validationruntarget
		SELECT ''%s'' as runname, NOW() as rundate, NULL as submissionid, ''nuarcodelists_platform'' as schemaname, ''%s'' as tablename, COUNT(systemid) as recordcount
		FROM nuarcodelists_platform.%s;
	', runname, tablename, tablename);

            EXECUTE FORMAT('
		INSERT INTO nuarvalidation.validationrunsource
		SELECT ''%s'' as runname, NOW() as rundate, NULL as submissionid, ''nuarcodelists_platform_%s'' as schemaname, ''%s'' as tablename, COUNT(systemid) as recordcount
		FROM nuarcodelists_platform_%s.%s;
	', runname, sourceschemasuffix, tablename, sourceschemasuffix, tablename);

            EXECUTE FORMAT('
		INSERT INTO recordcountswithduplicates
		SELECT ''%s'' as runname, NOW() as rundate, ''nuarcodelists_platform_%s'' as sourceschemaname, ''nuarcodelists_platform'' as targetschemaname, ''%s'' as sourcetablename, ''%s'' as targettablename, (SELECT COALESCE (SUM (recordcount), 0) AS sourcerecordcount FROM nuarvalidation.validationrunsource WHERE tablename = ''%s''), (SELECT COALESCE (SUM (recordcount), 0) AS targetrecordcount FROM nuarvalidation.validationruntarget WHERE tablename = ''%s'');
	', runname, sourceschemasuffix, tablename, tablename, tablename, tablename);

        END LOOP;

    TRUNCATE tablenames;
    INSERT INTO tablenames
    SELECT table_name::character varying as tablenamestring
    FROM information_schema.tables
    WHERE table_schema = 'nuarcodelists_transformation';

    tablesarray := '{}';
    SELECT ARRAY_AGG(tablenamestring) FROM tablenames INTO tablesarray;

    FOREACH tablename IN ARRAY tablesarray
        LOOP

            EXECUTE FORMAT('
		INSERT INTO nuarvalidation.validationruntarget
		SELECT ''%s'' as runname, NOW() as rundate, NULL as submissionid, ''nuarcodelists_transformation'' as schemaname, ''%s'' as tablename, COUNT(systemid) as recordcount
		FROM nuarcodelists_transformation.%s;
	', runname, tablename, tablename);

            EXECUTE FORMAT('
		INSERT INTO nuarvalidation.validationrunsource
		SELECT ''%s'' as runname, NOW() as rundate, NULL as submissionid, ''nuarcodelists_transformation_%s'' as schemaname, ''%s'' as tablename, COUNT(systemid) as recordcount
		FROM nuarcodelists_transformation_%s.%s;
	', runname, sourceschemasuffix, tablename, sourceschemasuffix, tablename);

            EXECUTE FORMAT('
		INSERT INTO recordcountswithduplicates
		SELECT ''%s'' as runname, NOW() as rundate, ''nuarcodelists_transformation_%s'' as sourceschemaname, ''nuarcodelists_transformation'' as targetschemaname, ''%s'' as sourcetablename, ''%s'' as targettablename, (SELECT COALESCE (SUM (recordcount), 0) AS sourcerecordcount FROM nuarvalidation.validationrunsource WHERE tablename = ''%s''), (SELECT COALESCE (SUM (recordcount), 0) AS targetrecordcount FROM nuarvalidation.validationruntarget WHERE tablename = ''%s'');
	', runname, sourceschemasuffix, tablename, tablename, tablename, tablename);

        END LOOP;

    TRUNCATE tablenames;
    INSERT INTO tablenames
    SELECT table_name::character varying as tablenamestring
    FROM information_schema.columns
    WHERE column_name = 'submissioneventid_fk'
      AND table_schema = 'nuarcustombackdrop';

    tablesarray := '{}';
    SELECT ARRAY_AGG(tablenamestring) FROM tablenames INTO tablesarray;

    FOREACH tablename IN ARRAY tablesarray
        LOOP

            EXECUTE FORMAT('
		INSERT INTO nuarvalidation.validationruntarget
		SELECT ''%s'' as runname, NOW() as rundate, submissioneventid_fk as submissionid, ''nuarcustombackdrop'' as schemaname, ''%s'' as tablename, COUNT(submissioneventid_fk) as recordcount
		FROM nuarcustombackdrop.%s
		GROUP BY submissioneventid_fk;
	', runname, tablename, tablename);

            EXECUTE FORMAT('
		INSERT INTO nuarvalidation.validationrunsource
		SELECT ''%s'' as runname, NOW() as rundate, submissioneventid_fk as submissionid, ''nuarcustombackdrop_%s'' as schemaname, ''%s'' as tablename, COUNT(submissioneventid_fk) as recordcount
		FROM nuarcustombackdrop_%s.%s
		GROUP BY submissioneventid_fk;
	', runname, sourceschemasuffix, tablename, sourceschemasuffix, tablename);

            EXECUTE FORMAT('
		INSERT INTO recordcountswithduplicates
		SELECT ''%s'' as runname, NOW() as rundate, ''nuarcustombackdrop_%s'' as sourceschemaname, ''nuarcustombackdrop'' as targetschemaname, ''%s'' as sourcetablename, ''%s'' as targettablename, (SELECT COALESCE (SUM (recordcount), 0) AS sourcerecordcount FROM nuarvalidation.validationrunsource WHERE tablename = ''%s''), (SELECT COALESCE (SUM (recordcount), 0) AS targetrecordcount FROM nuarvalidation.validationruntarget WHERE tablename = ''%s'');
	', runname, sourceschemasuffix, tablename, tablename, tablename, tablename);

        END LOOP;

-- For nuardata, only get the tables with a submissioneventid_fk column
    TRUNCATE tablenames;
    INSERT INTO tablenames
    SELECT table_name::character varying as tablenamestring
    FROM information_schema.columns
    WHERE column_name = 'submissioneventid_fk'
      AND table_schema = 'nuardata';

    tablesarray := '{}';
    SELECT ARRAY_AGG(tablenamestring) FROM tablenames INTO tablesarray;

    FOREACH tablename IN ARRAY tablesarray
        LOOP

            EXECUTE FORMAT('
		INSERT INTO nuarvalidation.validationruntarget
		SELECT ''%s'' as runname, NOW() as rundate, submissioneventid_fk as submissionid, ''nuardata'' as schemaname, ''%s'' as tablename, COUNT(submissioneventid_fk) as recordcount
		FROM nuardata.%s
		GROUP BY submissioneventid_fk;
	', runname, tablename, tablename);

            EXECUTE FORMAT('
		INSERT INTO nuarvalidation.validationrunsource
		SELECT ''%s'' as runname, NOW() as rundate, submissioneventid_fk as submissionid, ''nuardata_%s'' as schemaname, ''%s'' as tablename, COUNT(submissioneventid_fk) as recordcount
		FROM nuardata_%s.%s
		GROUP BY submissioneventid_fk;
	', runname, sourceschemasuffix, tablename, sourceschemasuffix, tablename);

            EXECUTE FORMAT('
		INSERT INTO recordcountswithduplicates
		SELECT ''%s'' as runname, NOW() as rundate, ''nuardata_%s'' as sourceschemaname, ''nuardata'' as targetschemaname, ''%s'' as sourcetablename, ''%s'' as targettablename, (SELECT COALESCE (SUM (recordcount), 0) AS sourcerecordcount FROM nuarvalidation.validationrunsource WHERE tablename = ''%s''), (SELECT COALESCE (SUM (recordcount), 0) AS targetrecordcount FROM nuarvalidation.validationruntarget WHERE tablename = ''%s'');
	', runname, sourceschemasuffix, tablename, tablename, tablename, tablename);

        END LOOP;

-- For nuarorganisations, only get the tables with a submissioneventid_fk column
    TRUNCATE tablenames;
    INSERT INTO tablenames
    SELECT table_name::character varying as tablenamestring
    FROM information_schema.columns
    WHERE column_name = 'submissioneventid_fk'
      AND table_schema = 'nuarorganisations';

    tablesarray := '{}';
    SELECT ARRAY_AGG(tablenamestring) FROM tablenames INTO tablesarray;

    FOREACH tablename IN ARRAY tablesarray
        LOOP

            EXECUTE FORMAT('
		INSERT INTO nuarvalidation.validationruntarget
		SELECT ''%s'' as runname, NOW() as rundate, submissioneventid_fk as submissionid, ''nuarorganisations'' as schemaname, ''%s'' as tablename, COUNT(submissioneventid_fk) as recordcount
		FROM nuarorganisations.%s
		GROUP BY submissioneventid_fk;
	', runname, tablename, tablename);

            EXECUTE FORMAT('
		INSERT INTO nuarvalidation.validationrunsource
		SELECT ''%s'' as runname, NOW() as rundate, submissioneventid_fk as submissionid, ''nuarorganisations_%s'' as schemaname, ''%s'' as tablename, COUNT(submissioneventid_fk) as recordcount
		FROM nuarorganisations_%s.%s
		GROUP BY submissioneventid_fk;
	', runname, sourceschemasuffix, tablename, sourceschemasuffix, tablename);

            EXECUTE FORMAT('
		INSERT INTO recordcountswithduplicates
		SELECT ''%s'' as runname, NOW() as rundate, ''nuarorganisations_%s'' as sourceschemaname, ''nuarorganisations'' as targetschemaname, ''%s'' as sourcetablename, ''%s'' as targettablename, (SELECT COALESCE (SUM (recordcount), 0) AS sourcerecordcount FROM nuarvalidation.validationrunsource WHERE tablename = ''%s''), (SELECT COALESCE (SUM (recordcount), 0) AS targetrecordcount FROM nuarvalidation.validationruntarget WHERE tablename = ''%s'');
	', runname, sourceschemasuffix, tablename, tablename, tablename, tablename);

        END LOOP;

-- nuardda and nuardis are "specials" - no submissioneventid_fk
    TRUNCATE tablenames;
    INSERT INTO tablenames(tablenamestring)
    VALUES ('nuardda');
    INSERT INTO tablenames(tablenamestring)
    VALUES ('nuardis');

    tablesarray := '{}';
    SELECT ARRAY_AGG(tablenamestring) FROM tablenames INTO tablesarray;

    FOREACH tablename IN ARRAY tablesarray
        LOOP

            EXECUTE FORMAT('
		INSERT INTO nuarvalidation.validationruntarget
		SELECT ''%s'' as runname, NOW() as rundate, NULL as submissionid, ''nuarorganisations'' as schemaname, ''%s'' as tablename, COUNT(systemid) as recordcount
		FROM nuarorganisations.%s;
	', runname, tablename, tablename);

            EXECUTE FORMAT('
		INSERT INTO nuarvalidation.validationrunsource
		SELECT ''%s'' as runname, NOW() as rundate, NULL as submissionid, ''nuarorganisations_%s'' as schemaname, ''%s'' as tablename, COUNT(systemid) as recordcount
		FROM nuarorganisations_%s.%s;
	', runname, sourceschemasuffix, tablename, sourceschemasuffix, tablename);

            EXECUTE FORMAT('
		INSERT INTO recordcountswithduplicates
		SELECT ''%s'' as runname, NOW() as rundate, ''nuarorganisations_%s'' as sourceschemaname, ''nuarorganisations'' as targetschemaname, ''%s'' as sourcetablename, ''%s'' as targettablename, (SELECT COALESCE (SUM (recordcount), 0) AS sourcerecordcount FROM nuarvalidation.validationrunsource WHERE tablename = ''%s''), (SELECT COALESCE (SUM (recordcount), 0) AS targetrecordcount FROM nuarvalidation.validationruntarget WHERE tablename = ''%s'');
	', runname, sourceschemasuffix, tablename, tablename, tablename, tablename);

        END LOOP;

    TRUNCATE tablenames;
    INSERT INTO tablenames
    SELECT table_name::character varying as tablenamestring
    FROM information_schema.tables
    WHERE table_schema = 'nuarusercreateddata';

    tablesarray := '{}';
    SELECT ARRAY_AGG(tablenamestring) FROM tablenames INTO tablesarray;

    FOREACH tablename IN ARRAY tablesarray
        LOOP

            EXECUTE FORMAT('
		INSERT INTO nuarvalidation.validationruntarget
		SELECT ''%s'' as runname, NOW() as rundate, NULL as submissionid, ''nuarusercreateddata'' as schemaname, ''%s'' as tablename, COUNT(systemid) as recordcount
		FROM nuarusercreateddata.%s;
	', runname, tablename, tablename);

            EXECUTE FORMAT('
		INSERT INTO nuarvalidation.validationrunsource
		SELECT ''%s'' as runname, NOW() as rundate, NULL as submissionid, ''nuarusercreateddata_%s'' as schemaname, ''%s'' as tablename, COUNT(systemid) as recordcount
		FROM nuarusercreateddata_%s.%s;
	', runname, sourceschemasuffix, tablename, sourceschemasuffix, tablename);

            EXECUTE FORMAT('
		INSERT INTO recordcountswithduplicates
		SELECT ''%s'' as runname, NOW() as rundate, ''nuarusercreateddata_%s'' as sourceschemaname, ''nuarusercreateddata'' as targetschemaname, ''%s'' as sourcetablename, ''%s'' as targettablename, (SELECT COALESCE (SUM (recordcount), 0) AS sourcerecordcount FROM nuarvalidation.validationrunsource WHERE tablename = ''%s''), (SELECT COALESCE (SUM (recordcount), 0) AS targetrecordcount FROM nuarvalidation.validationruntarget WHERE tablename = ''%s'');
	', runname, sourceschemasuffix, tablename, tablename, tablename, tablename);

        END LOOP;


    TRUNCATE tablenames;
    INSERT INTO tablenames
    SELECT table_name::character varying as tablenamestring
    FROM information_schema.tables
    WHERE table_schema = 'nuarversion';

    tablesarray := '{}';
    SELECT ARRAY_AGG(tablenamestring) FROM tablenames INTO tablesarray;

    FOREACH tablename IN ARRAY tablesarray
        LOOP

            EXECUTE FORMAT('
		INSERT INTO nuarvalidation.validationruntarget
		SELECT ''%s'' as runname, NOW() as rundate, NULL as submissionid, ''nuarversion'' as schemaname, ''%s'' as tablename, COUNT(*) as recordcount
		FROM nuarversion.%s;
	', runname, tablename, tablename);

            EXECUTE FORMAT('
		INSERT INTO nuarvalidation.validationrunsource
		SELECT ''%s'' as runname, NOW() as rundate, NULL as submissionid, ''nuarversion_%s'' as schemaname, ''%s'' as tablename, COUNT(*) as recordcount
		FROM nuarversion_%s.%s;
	', runname, sourceschemasuffix, tablename, sourceschemasuffix, tablename);

            EXECUTE FORMAT('
		INSERT INTO recordcountswithduplicates
		SELECT ''%s'' as runname, NOW() as rundate, ''nuarversion_%s'' as sourceschemaname, ''nuarversion'' as targetschemaname, ''%s'' as sourcetablename, ''%s'' as targettablename, (SELECT COALESCE (SUM (recordcount), 0) AS sourcerecordcount FROM nuarvalidation.validationrunsource WHERE tablename = ''%s''), (SELECT COALESCE (SUM (recordcount), 0) AS targetrecordcount FROM nuarvalidation.validationruntarget WHERE tablename = ''%s'');
	', runname, sourceschemasuffix, tablename, tablename, tablename, tablename);

        END LOOP;

    INSERT INTO nuarvalidation.recordcountdifferences
    (systemid, runname, rundate, sourcetableschema, sourcetablename, targettableschema, targettablename,
     sourcerecordcount, targetrecordcount, submissionid_fk)
    SELECT gen_random_uuid()::character varying,
           source.runname,
           source.rundate,
           source.schemaname   as sourcetableschema,
           source.tablename    as sourcetablename,
           target.schemaname   as targettableschema,
           target.tablename    as targettablename,
           source.recordcount  as sourcerecordcount,
           target.recordcount  as targetrecordcount,
           source.submissionid as submissionid_fk
    FROM nuarvalidation.validationrunsource as source,
         nuarvalidation.validationruntarget as target
    WHERE source.submissionid = target.submissionid
      AND source.tablename = target.tablename
      AND source.runname = target.runname
      AND source.recordcount <> target.recordcount;

    INSERT INTO nuarvalidation.recordcounts
    (systemid, runname, rundate, sourcetableschema, sourcetablename, targettableschema, targettablename,
     sourcerecordcount, targetrecordcount)
    SELECT DISTINCT gen_random_uuid()::character varying,
                    recordcountswithduplicates.runname,
                    recordcountswithduplicates.rundate,
                    recordcountswithduplicates.sourceschemaname,
                    recordcountswithduplicates.sourcetablename,
                    recordcountswithduplicates.targetschemaname,
                    recordcountswithduplicates.targettablename,
                    recordcountswithduplicates.sourcerecordcount,
                    recordcountswithduplicates.targetrecordcount
    FROM recordcountswithduplicates;

    DROP TABLE IF EXISTS tablenames;
    DROP TABLE IF EXISTS recordcountswithduplicates;
    DROP VIEW IF EXISTS sourcesubmissionids;
    DROP VIEW IF EXISTS targetsubmissions;
    DROP VIEW IF EXISTS filterorganisationids;
    DROP VIEW IF EXISTS targetorganisationids;

    IF (createanddropfdwschemas)
    THEN
        -- Drop the FDW schemas
        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarcodelists_datamanagement_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarcodelists_platform_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarcodelists_transformation_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarcustombackdrop_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuardata_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarorganisations_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarsubmissions_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarversion_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

    END IF;

    IF (createanddropfdwserver)
    THEN
        -- Drop the FDW Server
        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropfdwserver(
			servername => ''%s'');
	', fdwservername);

    END IF;

    RETURN 1;
END;
$$;


-- TOC entry 1891 (class 1255 OID 71821440)
-- Name: fn_calculatesubordinatenetworkrelationshipsforsubevent_v213(character varying, character varying); Type: FUNCTION; Schema: nuarpublication; Owner: nuar_owner


CREATE OR REPLACE FUNCTION nuarpublication.fn_calculatesubordinatenetworkrelationshipsforsubevent_v213(dataproviderid character varying, submissioneventid character varying) RETURNS integer
    LANGUAGE plpgsql
AS $$

DECLARE
    subordinatenetworkdefinitionidsarray character varying[];
    subordinatenetworkdefinitionid       character varying;
    currentnetworkid                     text;
    currentnetworktable                  text;
    currentmemberfeaturestable           text;
    currentmembershipcriteria            text;

BEGIN

    DROP TABLE IF EXISTS tempsubordinatenetworkdefinitionids;

-- We create a temporary table to hold the subordinate network definitions for the specified data provider
    CREATE TEMPORARY TABLE tempsubordinatenetworkdefinitionids
    (
        systemid            character varying(38),
        linkednetworkid     text,
        linkednetworktable  text,
        memberfeaturestable text,
        membershipcriteria  text
    );

-- Populate the temporary table with subordinate network definitions for the specified data provider
    EXECUTE FORMAT('
	INSERT INTO tempsubordinatenetworkdefinitionids
	SELECT systemid, linkednetworkid, linkednetworktable, memberfeaturestable, membershipcriteria
	FROM nuardata.nuarsubordinatenetworkdefinition
	WHERE dataproviderid_fk = ''%s'';
', dataproviderid);

-- Put the subordinate network definition ids into an array so we can loop through them
    SELECT ARRAY_AGG(systemid)
    FROM tempsubordinatenetworkdefinitionids
    INTO subordinatenetworkdefinitionidsarray;

-- Loop through the subordinate network definitions
    FOREACH subordinatenetworkdefinitionid IN ARRAY subordinatenetworkdefinitionidsarray
        LOOP

            -- Put the field values into variables so we can easily use them in queries on individual tables
            currentnetworkid := '';
            currentnetworktable := '';
            currentmemberfeaturestable := '';
            currentmembershipcriteria := '';

            EXECUTE FORMAT('
		SELECT linkednetworkid, linkednetworktable, memberfeaturestable, membershipcriteria
		FROM tempsubordinatenetworkdefinitionids
		WHERE systemid=''%s''
	',
                           subordinatenetworkdefinitionid) INTO currentnetworkid, currentnetworktable, currentmemberfeaturestable, currentmembershipcriteria;


            -- Create a temporary table to hold the ids of features in a given table that belong to the current subordinate network
            DROP TABLE IF EXISTS tempsubordinatenetworkmemberfeatures;

            CREATE TEMPORARY TABLE tempsubordinatenetworkmemberfeatures
            (
                linkednetworkid     text,
                linkednetworktable  text,
                memberfeaturestable text,
                featureid           text
            );

            -- If there are membership criteria specified for the subordinate network append them to the feature query
            -- otherwise just select on data provider and submission
            IF (COALESCE(currentmembershipcriteria, '') <> '')
            THEN
                -- Populate the temporary table with dataproviderassigneduniqueid values for the features in the table
                -- that belong to the subordinate network
                EXECUTE FORMAT('
			INSERT INTO tempsubordinatenetworkmemberfeatures
			(
				linkednetworkid,
				linkednetworktable,
				memberfeaturestable,
				featureid
			)
			SELECT
				''%s'' AS linkednetworkid,
				''%s'' AS linkednetworktable,
				''%s'' AS memberfeaturestable,
				dataproviderassigneduniqueid AS featureid
			FROM %s
			WHERE dataproviderid_fk=''%s''
			AND submissioneventid_fk=''%s''
			AND %s
		', currentnetworkid, currentnetworktable, currentmemberfeaturestable, currentmemberfeaturestable,
                               dataproviderid, submissioneventid, currentmembershipcriteria);
            ELSE
                -- In the absence of specific membership criteria, populate the temporary table with
                -- dataproviderassigneduniqueid values for all the features in the table for the data provider and submission
                EXECUTE FORMAT('
			INSERT INTO tempsubordinatenetworkmemberfeatures
			(
				linkednetworkid,
				linkednetworktable,
				memberfeaturestable,
				featureid
			)
			SELECT
				''%s'' AS linkednetworkid,
				''%s'' AS linkednetworktable,
				''%s'' AS memberfeaturestable,
				dataproviderassigneduniqueid AS featureid
			FROM %s
			WHERE dataproviderid_fk=''%s''
			AND submissioneventid_fk=''%s''
		', currentnetworkid, currentnetworktable, currentmemberfeaturestable, currentmemberfeaturestable,
                               dataproviderid, submissioneventid);

            END IF;

            -- Populate the relationship_networktonetworkconveyance table with details of the network and the features
            -- belonging to it
            EXECUTE FORMAT('
		INSERT INTO nuardata.relationship_networktonetworkconveyance
		(
			systemid,
			datelastupdated,
			systemloaddate,
			dateoflastlifecyclestatuschange,
			linkednetworkid,
			linkednetworktable,
			linkedconveyanceid,
			linkedconveyancetable,
			lifecyclestatus,
			nuarversion,
			dataproviderid_fk,
			submissioneventid_fk
		)
		SELECT
			gen_random_uuid ()::character varying AS systemid,
			NOW() AS datelastupdated,
			NOW() AS systemloaddate,
			NOW() AS dateoflastlifecyclestatuschange,
			linkednetworkid AS linkednetworkid,
			linkednetworktable AS linkednetworktable,
			featureid linkedconveyanceid,
			memberfeaturestable AS linkedconveyancetable,
			''Submitted'' AS lifecyclestatus,
			NULL AS nuarversion,
			''%s'' AS dataproviderid_fk,
			''%s'' AS submissioneventid_fk
		FROM tempsubordinatenetworkmemberfeatures
	', dataproviderid, submissioneventid);

            DROP TABLE IF EXISTS tempsubordinatenetworkmemberfeatures;

        END LOOP;


    DROP TABLE IF EXISTS tempsubordinatenetworkdefinitionids;


    RETURN 1;
END;
$$;

--
-- TOC entry 1892 (class 1255 OID 71821441)
-- Name: fn_calculatesubordinatenetworkrelationshipsforsubmission_v213(character varying, character varying); Type: FUNCTION; Schema: nuarpublication; Owner: nuar_owner
--

CREATE OR REPLACE FUNCTION nuarpublication.fn_calculatesubordinatenetworkrelationshipsforsubmission_v213(dataproviderid character varying, submissionid character varying) RETURNS integer
    LANGUAGE plpgsql
AS $$

DECLARE
    submissioneventidsarray character varying[];
    submissioneventid       character varying;

BEGIN

    DROP TABLE IF EXISTS tempsubmissioneventids;

-- We create a temporary table to hold the individual submission event ids for the specified data provider and submission
    CREATE TEMPORARY TABLE tempsubmissioneventids
    (
        systemid character varying(38)
    );

-- Populate the temporary table with systemids for submissionevents related to the dataprovider and submission
    EXECUTE FORMAT('
	INSERT INTO tempsubmissioneventids
	SELECT systemid
	FROM nuarsubmissions.nuarsubmissionevent
	WHERE dataproviderid_fk = ''%s''
	AND submissionid = ''%s'';
', dataproviderid, submissionid);

-- Put the submission event ids into an array so we can loop through them
    SELECT ARRAY_AGG(systemid)
    FROM tempsubmissioneventids
    INTO submissioneventidsarray;

-- Loop through the submission event ids
    FOREACH submissioneventid IN ARRAY submissioneventidsarray
        LOOP

            PERFORM nuarpublication.fn_calculatesubordinatenetworkrelationshipsforsubevent_v213(
                    dataproviderid => dataproviderid,
                    submissioneventid => submissioneventid
                    );


        END LOOP;


    DROP TABLE IF EXISTS tempsubmissioneventids;


    RETURN 1;
END;
$$;

--
-- TOC entry 1904 (class 1255 OID 71821463)
-- Name: fn_createforeigndatawrappers(text, text, text, text, text, text); Type: FUNCTION; Schema: nuarpublication; Owner: nuar_admin
--

CREATE OR REPLACE FUNCTION nuarpublication.fn_createforeigndatawrappers(server_name text, schema_suffix text, host text, dbname text, username text, password text) RETURNS void
    LANGUAGE plpgsql
AS $_$
DECLARE
    schemas       TEXT[] := ARRAY [
        'nuarcodelists_datamanagement',
        'nuarcodelists_platform',
        'nuarcodelists_transformation',
        'nuarcustombackdrop',
        'nuardata',
        'nuarorganisations',
        'nuarpublication',
        'nuarsubmissions',
        'nuarusercreateddata',
        'nuarversion'
        ];
    schema_name   TEXT;
    foreign_table RECORD;
BEGIN
    -- Create the foreign server
    EXECUTE format($f$
        CREATE SERVER IF NOT EXISTS %I
        FOREIGN DATA WRAPPER postgres_fdw
        OPTIONS (host %L, port '5432', dbname %L)
    $f$, server_name, host, dbname);

    -- Create user mapping
    EXECUTE format($f$
        CREATE USER MAPPING IF NOT EXISTS FOR %I
        SERVER %I
        OPTIONS (user %L, password %L)
    $f$, username, server_name, username, password);

    -- Loop through schemas to create and import
    FOREACH schema_name IN ARRAY schemas
        LOOP
            -- Create schema if not exists
            EXECUTE format($f$
            CREATE SCHEMA IF NOT EXISTS %I;
        $f$, schema_name || schema_suffix);

            -- Drop existing foreign tables before import
            FOR foreign_table IN
                SELECT foreign_table_name
                FROM information_schema.foreign_tables
                WHERE foreign_table_schema = schema_name || schema_suffix
                LOOP
                    EXECUTE format($f$
                DROP FOREIGN TABLE IF EXISTS %I.%I CASCADE;
            $f$, schema_name || schema_suffix, foreign_table.foreign_table_name);
                END LOOP;

            -- Import foreign schema
            EXECUTE format($f$
            IMPORT FOREIGN SCHEMA %I FROM SERVER %I INTO %I;
        $f$, schema_name, server_name, schema_name || schema_suffix);
        END LOOP;
END;
$_$;

--
-- TOC entry 1876 (class 1255 OID 50851813)
-- Name: fn_dtitoholding(character varying, character varying, character varying, character varying, character varying, character varying, character varying, integer[], boolean, boolean, boolean); Type: FUNCTION; Schema: nuarpublication; Owner: nuar_admin
--

CREATE OR REPLACE FUNCTION nuarpublication.fn_dtitoholding(sourceschemasuffix character varying DEFAULT 'dti'::character varying, fdwservername character varying DEFAULT 'dti_source'::character varying, fdwdbhostname character varying DEFAULT ''::character varying, fdwdbport character varying DEFAULT ''::character varying, fdwdbname character varying DEFAULT ''::character varying, fdwdbusername character varying DEFAULT ''::character varying, fdwdbpassword character varying DEFAULT ''::character varying, process_sequences integer[] DEFAULT '{}'::integer[], createanddropfdwserver boolean DEFAULT false, createanddropfdwschemas boolean DEFAULT false, runvalidation boolean DEFAULT false) RETURNS character varying
    LANGUAGE plpgsql
AS $$

DECLARE
    publicationeventid character varying;

BEGIN

    SELECT nuarpublication.fn_dtitoholding_v211(
                   sourceschemasuffix => sourceschemasuffix,
                   fdwservername => fdwservername,
                   fdwdbhostname => fdwdbhostname,
                   fdwdbport => fdwdbport,
                   fdwdbname => fdwdbname,
                   fdwdbusername => fdwdbusername,
                   fdwdbpassword => fdwdbpassword,
                   process_sequences => process_sequences,
                   createanddropfdwserver => createanddropfdwserver,
                   createanddropfdwschemas => createanddropfdwschemas,
                   runvalidation => runvalidation)
    INTO publicationeventid;

    RETURN publicationeventid;
END;
$$;

--
-- TOC entry 1869 (class 1255 OID 45552150)
-- Name: fn_dtitoholding_upsertsubmissionsonly_v210(character varying, character varying, character varying, character varying, character varying, character varying, character varying, boolean, boolean); Type: FUNCTION; Schema: nuarpublication; Owner: nuar_admin
--

CREATE OR REPLACE FUNCTION nuarpublication.fn_dtitoholding_upsertsubmissionsonly_v210(sourceschemasuffix character varying DEFAULT 'dti'::character varying, fdwservername character varying DEFAULT 'dti_source'::character varying, fdwdbhostname character varying DEFAULT ''::character varying, fdwdbport character varying DEFAULT ''::character varying, fdwdbname character varying DEFAULT ''::character varying, fdwdbusername character varying DEFAULT ''::character varying, fdwdbpassword character varying DEFAULT ''::character varying, createanddropfdwserver boolean DEFAULT false, createanddropfdwschemas boolean DEFAULT false) RETURNS integer
    LANGUAGE plpgsql
AS $$

DECLARE
    publicationstage                  character varying;
    existingholdingsubmissionidsarray character varying[];

BEGIN

    DROP VIEW IF EXISTS existingholdingsubmissions;


-- First we see if we are allowed to start
    IF (SELECT nuarpublication.fn_checkifpublicationstagecanstart(stagenamevalue => 'dti-holding'))
    THEN
        publicationstage := 'DTI to Holding';
    ELSE
        RAISE NOTICE 'Dropping out - unable to run given current publication status';

        PERFORM nuarpublication.fn_logpublicationmessage(
                publicationstage => 'DTI to Holding',
                messagetext => 'Dropping out - unable to run given current publication status',
                logdatetime => NOW()
                );

        -- Set our status to Completed
        PERFORM nuarpublication.fn_recordpublicationjobstatus(
                stagenamevalue => 'dti-holding',
                statusvalue => 'Completed',
                startdatetimevalue => NOW(),
                enddatetimevalue => NOW(),
                droppingout => true
                );

        RETURN 0;
    END IF;

    IF (createanddropfdwserver)
    THEN
        -- Create the FDW Server and User Mapping
        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwserver(
			servername => ''%s'',
			fdwname => ''postgres_fdw'',
			hostname => ''%s'',
			port => ''%s'',
			databasename => ''%s'',
			username => ''%s'',
			userpassword => ''%s'');
	', fdwservername, fdwdbhostname, fdwdbport, fdwdbname, fdwdbusername, fdwdbpassword);
    END IF;

    IF (createanddropfdwschemas)
    THEN

        -- Create and import the FDW schemas
        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarcodelists_datamanagement_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarcodelists_datamanagement'');
	', sourceschemasuffix, fdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarcodelists_platform_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarcodelists_platform'');
	', sourceschemasuffix, fdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarcodelists_transformation_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarcodelists_transformation'');
	', sourceschemasuffix, fdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarcustombackdrop_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarcustombackdrop'');
	', sourceschemasuffix, fdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuardata_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuardata'');
	', sourceschemasuffix, fdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarorganisations_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarorganisations'');
	', sourceschemasuffix, fdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarsubmissions_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarsubmissions'');
	', sourceschemasuffix, fdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarversion_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarversion'');
	', sourceschemasuffix, fdwservername);

    END IF;

    -- For all the Submissions that we currently hold, do an Upsert from Holding to make sure that any status updates etc. are captured
    CREATE TEMPORARY VIEW existingholdingsubmissions AS
    SELECT systemid
    FROM nuarsubmissions.nuarsubmissionevent;

    IF EXISTS (SELECT * FROM existingholdingsubmissions)
    THEN
        SELECT ARRAY_AGG(systemid)
        FROM existingholdingsubmissions
        INTO existingholdingsubmissionidsarray;

        PERFORM nuarpublication.fn_upsertnuarsubmissionevents_bulk_v21x(
                sourceschemaname => 'nuarsubmissions_dti',
                sourcetablename => 'nuarsubmissionevent',
                targetschemaname => 'nuarsubmissions',
                targettablename => 'nuarsubmissionevent',
                sourcesystemids => existingholdingsubmissionidsarray);

        PERFORM nuarpublication.fn_logpublicationmessage(
                publicationstage => publicationstage,
                messagetext => 'Updated existing Submissions in Holding from DTI',
                logdatetime => NOW()
                );
    END IF;

    DROP VIEW IF EXISTS existingholdingsubmissions;

    IF (createanddropfdwschemas)
    THEN
        -- Drop the FDW schemas
        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarcodelists_datamanagement_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarcodelists_platform_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarcodelists_transformation_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarcustombackdrop_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuardata_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarorganisations_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarsubmissions_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarversion_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

    END IF;

    IF (createanddropfdwserver)
    THEN

        -- Drop the FDW Server
        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropfdwserver(
			servername => ''%s'');
	', fdwservername);

    END IF;

    RETURN 1;
END;
$$;

--
-- TOC entry 1893 (class 1255 OID 71821443)
-- Name: fn_dtitoholding_v211(character varying, character varying, character varying, character varying, character varying, character varying, character varying, integer[], boolean, boolean, boolean); Type: FUNCTION; Schema: nuarpublication; Owner: nuar_admin
--

CREATE OR REPLACE FUNCTION nuarpublication.fn_dtitoholding_v211(sourceschemasuffix character varying DEFAULT 'dti'::character varying, fdwservername character varying DEFAULT 'dti_source'::character varying, fdwdbhostname character varying DEFAULT ''::character varying, fdwdbport character varying DEFAULT ''::character varying, fdwdbname character varying DEFAULT ''::character varying, fdwdbusername character varying DEFAULT ''::character varying, fdwdbpassword character varying DEFAULT ''::character varying, process_sequences integer[] DEFAULT '{}'::integer[], createanddropfdwserver boolean DEFAULT false, createanddropfdwschemas boolean DEFAULT false, runvalidation boolean DEFAULT false) RETURNS character varying
    LANGUAGE plpgsql
AS $$

DECLARE
    publicationstage                  character varying;
    organisationfilteridsarray        character varying[];
    submissionidsarray                character varying[];
    nuaractoridsarray                 character varying[];
    backdropgeometrytablesarray       character varying[];
    existingholdingsubmissionidsarray character varying[];
    noerrorstateset                   boolean;
    publicationeventid                character varying;

BEGIN

    DROP VIEW IF EXISTS submissionids;
    DROP VIEW IF EXISTS nuaractorids;
    DROP VIEW IF EXISTS newsubmissions;
    DROP VIEW IF EXISTS existingholdingsubmissions;
    DROP VIEW IF EXISTS organisationfilterids;
    DROP VIEW IF EXISTS geometrytablenames;
    DROP VIEW IF EXISTS relationshiptablenames;
    DROP VIEW IF EXISTS backdropgeometrytablenames;


-- First we see if we are allowed to start
    IF (SELECT nuarpublication.fn_checkifpublicationstagecanstart(stagenamevalue => 'dti-holding'))
    THEN
        publicationstage := 'DTI to Holding';
        noerrorstateset := true;
    ELSE
        RAISE NOTICE 'Dropping out - unable to run given current publication status';

        PERFORM nuarpublication.fn_logpublicationmessage(
                publicationstage => 'DTI to Holding',
                messagetext => 'Dropping out - unable to run given current publication status',
                logdatetime => NOW()
                );

        -- Set our status to Completed
        PERFORM nuarpublication.fn_recordpublicationjobstatus(
                stagenamevalue => 'dti-holding',
                statusvalue => 'Completed',
                startdatetimevalue => NOW(),
                enddatetimevalue => NOW(),
                droppingout => true
                );

        RETURN '';
    END IF;

    IF (createanddropfdwserver)
    THEN
        -- Create the FDW Server and User Mapping
        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwserver(
			servername => ''%s'',
			fdwname => ''postgres_fdw'',
			hostname => ''%s'',
			port => ''%s'',
			databasename => ''%s'',
			username => ''%s'',
			userpassword => ''%s'');
	', fdwservername, fdwdbhostname, fdwdbport, fdwdbname, fdwdbusername, fdwdbpassword);
    END IF;

    IF (createanddropfdwschemas)
    THEN

        -- Create and import the FDW schemas
        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarcodelists_datamanagement_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarcodelists_datamanagement'');
	', sourceschemasuffix, fdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarcodelists_platform_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarcodelists_platform'');
	', sourceschemasuffix, fdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarcodelists_transformation_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarcodelists_transformation'');
	', sourceschemasuffix, fdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarcustombackdrop_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarcustombackdrop'');
	', sourceschemasuffix, fdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuardata_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuardata'');
	', sourceschemasuffix, fdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarorganisations_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarorganisations'');
	', sourceschemasuffix, fdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarsubmissions_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarsubmissions'');
	', sourceschemasuffix, fdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarversion_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarversion'');
	', sourceschemasuffix, fdwservername);

    END IF;

    -- For all the Submissions that we currently hold, do an Upsert from Holding to make sure that any status updates etc. are captured
    CREATE TEMPORARY VIEW existingholdingsubmissions AS
    SELECT systemid
    FROM nuarsubmissions.nuarsubmissionevent;

    IF EXISTS (SELECT * FROM existingholdingsubmissions)
    THEN
        SELECT ARRAY_AGG(systemid)
        FROM existingholdingsubmissions
        INTO existingholdingsubmissionidsarray;

        PERFORM nuarpublication.fn_upsertnuarsubmissionevents_bulk_v21x(
                sourceschemaname => 'nuarsubmissions_dti',
                sourcetablename => 'nuarsubmissionevent',
                targetschemaname => 'nuarsubmissions',
                targettablename => 'nuarsubmissionevent',
                sourcesystemids => existingholdingsubmissionidsarray);

        PERFORM nuarpublication.fn_logpublicationmessage(
                publicationstage => publicationstage,
                messagetext => 'Updated existing Submissions in Holding from DTI',
                logdatetime => NOW()
                );
    END IF;

-- We pick up the list of NUARActor systemid values (if any) which will constrain the data that we copy over
    EXECUTE FORMAT('
CREATE TEMPORARY VIEW organisationfilterids AS
SELECT filteredorganisationid
FROM nuarpublication.fn_getorganisationfilterids(
	schemaname => ''nuarpublication'',
	process_sequences => ''%s'');
', process_sequences);

    IF NOT EXISTS (SELECT * FROM organisationfilterids)
    THEN
        --CREATE TEMPORARY VIEW newsubmissions AS SELECT submissionsystemid, nuaractorsystemid FROM nuarpublication.fn_getnewsubmissions(sourceschemaname =>'nuarsubmissions_dti', sourcetablename => 'nuarsubmissionevent', targetschemaname => 'nuarsubmissions', targettablename => 'nuarsubmissionevent', submissionresult => 'Success', submissionstatus => 'Completed');
        CREATE TEMPORARY VIEW newsubmissions AS
        SELECT submissionsystemid, nuaractorsystemid
        FROM
            nuarpublication.fn_getallnewsubmissions(
                    sourceschemaname => 'nuarsubmissions_dti',
                    sourcetablename => 'nuarsubmissionevent',
                    targetschemaname => 'nuarsubmissions',
                    targettablename => 'nuarsubmissionevent');
    ELSE
        --CREATE TEMPORARY VIEW newsubmissions AS SELECT submissionsystemid, nuaractorsystemid FROM nuarpublication.fn_getnewsubmissionsfilteredbyorganisation(sourceschemaname => 'nuarsubmissions_dti', sourcetablename => 'nuarsubmissionevent', targetschemaname => 'nuarsubmissions', targettablename => 'nuarsubmissionevent', submissionresult => 'Success', submissionstatus => 'Completed', organisationids => (SELECT ARRAY_AGG(filteredorganisationid) FROM organisationfilterids));
        CREATE TEMPORARY VIEW newsubmissions AS
        SELECT submissionsystemid, nuaractorsystemid
        FROM
            nuarpublication.fn_getallnewsubmissionsfilteredbyorganisation(
                    sourceschemaname => 'nuarsubmissions_dti',
                    sourcetablename => 'nuarsubmissionevent',
                    targetschemaname => 'nuarsubmissions',
                    targettablename => 'nuarsubmissionevent',
                    organisationids => (SELECT ARRAY_AGG(filteredorganisationid) FROM organisationfilterids));

        -- If we're filtering by organisation id, we should make sure that all these organisations are in the target nuaractor table
        -- Create an array of the filter organisation ids
        SELECT ARRAY_AGG(filteredorganisationid)
        FROM organisationfilterids
        INTO organisationfilteridsarray;

        -- Do an upsert for all these organisation ids into the target nuaractor table
        PERFORM nuarpublication.fn_upsertnuaractors_v210(
                sourceschemaname => 'nuarorganisations_dti',
                sourcetablename => 'nuaractor',
                targetschemaname => 'nuarorganisations',
                targettablename => 'nuaractor',
                sourcesystemids => organisationfilteridsarray);

        PERFORM nuarpublication.fn_logpublicationmessage(
                publicationstage => publicationstage,
                messagetext => 'Upserted NUARActor records for filter organisation ids',
                logdatetime => NOW()
                );

    END IF;

    CREATE TEMPORARY VIEW submissionids AS
    SELECT DISTINCT submissionsystemid
    FROM newsubmissions;

    CREATE TEMPORARY VIEW nuaractorids AS
    SELECT DISTINCT nuaractorsystemid
    FROM newsubmissions;

    -- Is either the list of Submissions or the list of NUARActors empty?
-- If so, we just drop out
    IF (
        NOT EXISTS (SELECT FROM submissionids) OR
        NOT EXISTS (SELECT FROM nuaractorids)
        )
    THEN
        RAISE NOTICE 'No submissions to bring into Holding';
        PERFORM nuarpublication.fn_logpublicationmessage(
                publicationstage => publicationstage,
                messagetext => 'No submissions to bring into Holding',
                logdatetime => NOW()
                );

        IF (runvalidation)
        THEN
            PERFORM nuarpublication.fn_recordcountvalidationrun_v211(
                    runname => 'dti-holding',
                    sourceschemasuffix => 'dti',
                    createanddropfdwserver => false,
                    createanddropfdwschemas => false,
                    publishablesubmissionsonly => false -- Set true for checking standby against holding, otherwise false
                    );

            -- Check the results and set an Error State if appropriate
            IF (SELECT nuarpublication.fn_checkvalidationresults(stagenamevalue => 'dti-holding'))
            THEN
                noerrorstateset := true;
            ELSE
                noerrorstateset := false;
            END IF;

        END IF;

        IF (createanddropfdwschemas)
        THEN
            -- Drop the FDW schemas
            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarcodelists_datamanagement_%s'',
				shouldcascade => true);
		', sourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarcodelists_platform_%s'',
				shouldcascade => true);
		', sourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarcodelists_transformation_%s'',
				shouldcascade => true);
		', sourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarcustombackdrop_%s'',
				shouldcascade => true);
		', sourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuardata_%s'',
				shouldcascade => true);
		', sourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarorganisations_%s'',
				shouldcascade => true);
		', sourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarsubmissions_%s'',
				shouldcascade => true);
		', sourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarversion_%s'',
				shouldcascade => true);
		', sourceschemasuffix);

        END IF;

        IF (createanddropfdwserver)
        THEN

            -- Drop the FDW Server
            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropfdwserver(
				servername => ''%s'');
		', fdwservername);

        END IF;

        IF (noerrorstateset)
        THEN
            -- The last thing we do is set our status to Completed
            PERFORM nuarpublication.fn_recordpublicationjobstatus(
                    stagenamevalue => 'dti-holding',
                    statusvalue => 'Completed',
                    startdatetimevalue => NOW(),
                    enddatetimevalue => NOW()
                    );
        END IF;

        RETURN '';
    END IF;

    SELECT ARRAY_AGG(submissionsystemid)
    FROM submissionids
    INTO submissionidsarray;

    SELECT ARRAY_AGG(nuaractorsystemid)
    FROM nuaractorids
    INTO nuaractoridsarray;

    -- We get a list of table names from the current database (HOLDING).
-- We are assuming that table names in the source and the destination match.
-- Get tables with geometry
    CREATE TEMPORARY VIEW geometrytablenames AS
    SELECT table_name::character varying as tablenamestring
    FROM information_schema.columns
    WHERE column_name = 'geometry'
      AND table_schema = 'nuardata';

    -- Get relationship_tables from nuardata
    CREATE TEMPORARY VIEW relationshiptablenames AS
    SELECT table_name::character varying as tablenamestring
    FROM information_schema.tables
    WHERE (
        table_name LIKE 'relationship_%'
            OR
        table_name = 'nuarsubordinatenetworkdefinition'
        )
      AND table_schema = 'nuardata';

    -- Get backdrop tables with geometry
    CREATE TEMPORARY VIEW backdropgeometrytablenames AS
    SELECT table_name::character varying as tablenamestring
    FROM information_schema.columns
    WHERE column_name = 'geometry'
      AND table_schema = 'nuarcustombackdrop';

-- Upsert NUARActor records
    PERFORM nuarpublication.fn_upsertnuaractors_v210(
            sourceschemaname => 'nuarorganisations_dti',
            sourcetablename => 'nuaractor',
            targetschemaname => 'nuarorganisations',
            targettablename => 'nuaractor',
            sourcesystemids => nuaractoridsarray);

    PERFORM nuarpublication.fn_logpublicationmessage(
            publicationstage => publicationstage,
            messagetext => 'Upserted NUARActor records',
            logdatetime => NOW()
            );

-- Upsert NUARSubmissionEvent records
    PERFORM nuarpublication.fn_upsertnuarsubmissionevents_v21x(
            sourceschemaname => 'nuarsubmissions_dti',
            sourcetablename => 'nuarsubmissionevent',
            targetschemaname => 'nuarsubmissions',
            targettablename => 'nuarsubmissionevent',
            sourcesystemids => submissionidsarray);

    PERFORM nuarpublication.fn_logpublicationmessage(
            publicationstage => publicationstage,
            messagetext => 'Upserted NUARSubmissionEvent records',
            logdatetime => NOW()
            );

-- Upsert NUARActor related records for these Submissions
    PERFORM nuarpublication.fn_upsertnuaractorrelateddata_v211(
            sourceschemaname => 'nuarorganisations_dti',
            sourcecontactdetailstablename => 'nuarcontactdetails',
            sourceactivityproximityruletablename => 'nuaractivityproximityrule',
            sourcecontactdetailsrelationshiptablename => 'relationship_actortocontactdetails',
            sourceactivityproximityrulerelationshiptablename => 'relationship_actortorule',
            sourceservicearearelationshiptablename => 'relationship_actortoservicearea',
            sourcedomainrelationshiptablename => 'relationship_actortosubmissiondomain',
            sourceserviceproviderrelationshiptablename => 'relationship_serviceprovidertoorganisation',
            targetschemaname => 'nuarorganisations',
            targetcontactdetailstablename => 'nuarcontactdetails',
            targetactivityproximityruletablename => 'nuaractivityproximityrule',
            targetcontactdetailsrelationshiptablename => 'relationship_actortocontactdetails',
            targetactivityproximityrulerelationshiptablename => 'relationship_actortorule',
            targetservicearearelationshiptablename => 'relationship_actortoservicearea',
            targetdomainrelationshiptablename => 'relationship_actortosubmissiondomain',
            targetserviceproviderrelationshiptablename => 'relationship_serviceprovidertoorganisation',
            sourcesystemids => submissionidsarray);

    PERFORM nuarpublication.fn_logpublicationmessage(
            publicationstage => publicationstage,
            messagetext => 'Upserted NUARActor related records',
            logdatetime => NOW()
            );

-- Upsert NUARActor DDA and DIS related records for these Organisations
    PERFORM nuarpublication.fn_upsertnuaractorlegalagreementsdata_v21x(
            sourceschemaname => 'nuarorganisations_dti',
            sourcenuaractortablename => 'nuaractor',
            sourceddatablename => 'nuardda',
            sourcedistablename => 'nuardis',
            sourceddarelationshiptablename => 'relationship_actortodda',
            sourcedisrelationshiptablename => 'relationship_actortodis',
            targetschemaname => 'nuarorganisations',
            targetddatablename => 'nuardda',
            targetdistablename => 'nuardis',
            targetddarelationshiptablename => 'relationship_actortodda',
            targetdisrelationshiptablename => 'relationship_actortodis',
            sourcesystemids => nuaractoridsarray);

    PERFORM nuarpublication.fn_logpublicationmessage(
            publicationstage => publicationstage,
            messagetext => 'Upserted NUARActor DDA and DIS related records',
            logdatetime => NOW()
            );

-- COPY Geometry tables
    PERFORM nuarpublication.fn_copysubmissiontablesfromsource(
            sourceschemaname => 'nuardata_dti',
            targetschemaname => 'nuardata',
            submissionids => submissionidsarray,
            tablenames => (SELECT ARRAY_AGG(tablenamestring) FROM geometrytablenames),
            publicationstage => publicationstage);

-- COPY Relationship tables
    PERFORM nuarpublication.fn_copysubmissiontablesfromsource(
            sourceschemaname => 'nuardata_dti',
            targetschemaname => 'nuardata',
            submissionids => submissionidsarray,
            tablenames => (SELECT ARRAY_AGG(tablenamestring) FROM relationshiptablenames),
            publicationstage => publicationstage);

-- COPY Backdrop Geometry tables
    SELECT ARRAY_AGG(tablenamestring) FROM backdropgeometrytablenames INTO backdropgeometrytablesarray;
    PERFORM nuarpublication.fn_copysubmissiontablesfromsource(
            sourceschemaname => 'nuarcustombackdrop_dti',
            targetschemaname => 'nuarcustombackdrop',
            submissionids => submissionidsarray,
            tablenames => backdropgeometrytablesarray,
            publicationstage => publicationstage);

-- COPY NUARVersion tables
    PERFORM nuarpublication.fn_upsertnuarversion(
            sourceschemaname => 'nuarversion_dti',
            targetschemaname => 'nuarversion');

    PERFORM nuarpublication.fn_logpublicationmessage(
            publicationstage => publicationstage,
            messagetext => 'Upserted NUARVersion records',
            logdatetime => NOW()
            );

-- COPY the tables in the different Codelist schemas
    PERFORM nuarpublication.fn_upsertnuarcodelists(
            sourceschemaname => 'nuarcodelists_datamanagement_dti',
            targetschemaname => 'nuarcodelists_datamanagement');

    PERFORM nuarpublication.fn_upsertnuarcodelists(
            sourceschemaname => 'nuarcodelists_platform_dti',
            targetschemaname => 'nuarcodelists_platform');

    PERFORM nuarpublication.fn_upsertnuarcodelists(
            sourceschemaname => 'nuarcodelists_transformation_dti',
            targetschemaname => 'nuarcodelists_transformation');

    PERFORM nuarpublication.fn_logpublicationmessage(
            publicationstage => publicationstage,
            messagetext => 'Upserted NUARCodelist records',
            logdatetime => NOW()
            );

    -- Now we've done all our copies, we need to process any "No Change" submissions.
-- Each of these specifies a "predecessor" submission.
-- We update the datelastupdated value to the dateofsubmission of the No Change submission
-- for records in every table which have a submissioneventid_fk value equal to the predecessor Submission
    PERFORM nuarpublication.fn_processnochangesubmissionevents_v211(
            submissionsschemaname => 'nuarsubmissions',
            submissionstablename => 'nuarsubmissionevent',
            submissionids => submissionidsarray
            );

-- Now we need to update the NUARPublicationEvent table
    SELECT nuarpublication.fn_recordpublicationdetails(
                   targetschemaname => 'nuarpublication',
                   publicationeventtablename => 'nuarpublicationevent',
                   publicationsubmissionrelationshiptablename => 'relationship_publicationtosubmission',
                   datamodelversion => '2.1.1',
                   submissionids => submissionidsarray)
    INTO publicationeventid;

    PERFORM nuarpublication.fn_logpublicationmessage(
            publicationstage => publicationstage,
            messagetext => 'Updated NUARPublicationEvent records',
            logdatetime => NOW()
            );

    DROP VIEW IF EXISTS submissionids;
    DROP VIEW IF EXISTS nuaractorids;
    DROP VIEW IF EXISTS newsubmissions;
    DROP VIEW IF EXISTS existingholdingsubmissions;
    DROP VIEW IF EXISTS organisationfilterids;
    DROP VIEW IF EXISTS geometrytablenames;
    DROP VIEW IF EXISTS relationshiptablenames;
    DROP VIEW IF EXISTS backdropgeometrytablenames;

    IF (runvalidation)
    THEN
        PERFORM nuarpublication.fn_recordcountvalidationrun_v211(
                runname => 'dti-holding',
                sourceschemasuffix => 'dti',
                createanddropfdwserver => false,
                createanddropfdwschemas => false,
                publishablesubmissionsonly => false -- Set true for checking standby against holding, otherwise false
                );

        -- Check the results and set an Error State if appropriate
        IF (SELECT nuarpublication.fn_checkvalidationresults(stagenamevalue => 'dti-holding'))
        THEN
            noerrorstateset := true;
        ELSE
            noerrorstateset := false;
        END IF;

    END IF;

    IF (createanddropfdwschemas)
    THEN
        -- Drop the FDW schemas
        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarcodelists_datamanagement_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarcodelists_platform_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarcodelists_transformation_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarcustombackdrop_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuardata_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarorganisations_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarsubmissions_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarversion_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

    END IF;

    IF (createanddropfdwserver)
    THEN

        -- Drop the FDW Server
        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropfdwserver(
			servername => ''%s'');
	', fdwservername);

    END IF;

    IF (noerrorstateset)
    THEN
        -- The last thing we do is set our status to Completed
        PERFORM nuarpublication.fn_recordpublicationjobstatus(
                stagenamevalue => 'dti-holding',
                statusvalue => 'Completed',
                startdatetimevalue => NOW(),
                enddatetimevalue => NOW()
                );
    END IF;

    RETURN publicationeventid;
END;
$$;


--
-- TOC entry 1883 (class 1255 OID 59424762)
-- Name: fn_dtitoholding_v212(character varying, character varying, character varying, character varying, character varying, character varying, character varying, integer[], boolean, boolean, boolean); Type: FUNCTION; Schema: nuarpublication; Owner: nuar_admin
--

CREATE OR REPLACE FUNCTION nuarpublication.fn_dtitoholding_v212(sourceschemasuffix character varying DEFAULT 'dti'::character varying, fdwservername character varying DEFAULT 'dti_source'::character varying, fdwdbhostname character varying DEFAULT ''::character varying, fdwdbport character varying DEFAULT ''::character varying, fdwdbname character varying DEFAULT ''::character varying, fdwdbusername character varying DEFAULT ''::character varying, fdwdbpassword character varying DEFAULT ''::character varying, process_sequences integer[] DEFAULT '{}'::integer[], createanddropfdwserver boolean DEFAULT false, createanddropfdwschemas boolean DEFAULT false, runvalidation boolean DEFAULT false) RETURNS character varying
    LANGUAGE plpgsql
AS $$

DECLARE
    publicationstage character varying;
    organisationfilteridsarray character varying[];
    submissionidsarray character varying[];
    nuaractoridsarray character varying[];
    backdropgeometrytablesarray character varying[];
    existingholdingsubmissionidsarray character varying[];
    noerrorstateset boolean;
    publicationeventid character varying;

BEGIN

    DROP VIEW IF EXISTS submissionids;
    DROP VIEW IF EXISTS nuaractorids;
    DROP VIEW IF EXISTS newsubmissions;
    DROP VIEW IF EXISTS existingholdingsubmissions;
    DROP VIEW IF EXISTS organisationfilterids;
    DROP VIEW IF EXISTS geometrytablenames;
    DROP VIEW IF EXISTS relationshiptablenames;
    DROP VIEW IF EXISTS backdropgeometrytablenames;


-- First we see if we are allowed to start
    IF (SELECT nuarpublication.fn_checkifpublicationstagecanstart (stagenamevalue => 'dti-holding'))
    THEN
        publicationstage := 'DTI to Holding';
        noerrorstateset := true;
    ELSE
        RAISE NOTICE 'Dropping out - unable to run given current publication status';

        PERFORM nuarpublication.fn_logpublicationmessage(
                publicationstage => 'DTI to Holding',
                messagetext => 'Dropping out - unable to run given current publication status',
                logdatetime => NOW()
                );

        -- Set our status to Completed
        PERFORM nuarpublication.fn_recordpublicationjobstatus(
                stagenamevalue => 'dti-holding',
                statusvalue => 'Completed',
                startdatetimevalue => NOW(),
                enddatetimevalue => NOW(),
                droppingout => true
                );

        RETURN '';
    END IF;

    IF (createanddropfdwserver)
    THEN
        -- Create the FDW Server and User Mapping
        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwserver(
			servername => ''%s'',
			fdwname => ''postgres_fdw'',
			hostname => ''%s'',
			port => ''%s'',
			databasename => ''%s'',
			username => ''%s'',
			userpassword => ''%s'');
	', fdwservername, fdwdbhostname, fdwdbport, fdwdbname, fdwdbusername, fdwdbpassword);
    END IF;

    IF (createanddropfdwschemas)
    THEN

        -- Create and import the FDW schemas
        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarcodelists_datamanagement_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarcodelists_datamanagement'');
	', sourceschemasuffix, fdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarcodelists_platform_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarcodelists_platform'');
	', sourceschemasuffix, fdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarcodelists_transformation_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarcodelists_transformation'');
	', sourceschemasuffix, fdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarcustombackdrop_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarcustombackdrop'');
	', sourceschemasuffix, fdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuardata_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuardata'');
	', sourceschemasuffix, fdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarorganisations_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarorganisations'');
	', sourceschemasuffix, fdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarsubmissions_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarsubmissions'');
	', sourceschemasuffix, fdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarversion_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarversion'');
	', sourceschemasuffix, fdwservername);

    END IF;

    -- For all the Submissions that we currently hold, do an Upsert from Holding to make sure that any status updates etc. are captured
    CREATE TEMPORARY VIEW existingholdingsubmissions AS
    SELECT systemid
    FROM nuarsubmissions.nuarsubmissionevent;

    IF EXISTS (SELECT * FROM existingholdingsubmissions)
    THEN
        SELECT ARRAY_AGG(systemid)
        FROM existingholdingsubmissions
        INTO existingholdingsubmissionidsarray;

        PERFORM nuarpublication.fn_upsertnuarsubmissionevents_bulk_v21x(
                sourceschemaname => 'nuarsubmissions_dti',
                sourcetablename => 'nuarsubmissionevent',
                targetschemaname => 'nuarsubmissions',
                targettablename => 'nuarsubmissionevent',
                sourcesystemids => existingholdingsubmissionidsarray);

        PERFORM nuarpublication.fn_logpublicationmessage(
                publicationstage => publicationstage,
                messagetext => 'Updated existing Submissions in Holding from DTI',
                logdatetime => NOW()
                );
    END IF;

-- We pick up the list of NUARActor systemid values (if any) which will constrain the data that we copy over
    EXECUTE FORMAT('
CREATE TEMPORARY VIEW organisationfilterids AS
SELECT filteredorganisationid
FROM nuarpublication.fn_getorganisationfilterids(
	schemaname => ''nuarpublication'',
	process_sequences => ''%s'');
', process_sequences);

    IF NOT EXISTS (SELECT * FROM organisationfilterids)
    THEN
        --CREATE TEMPORARY VIEW newsubmissions AS SELECT submissionsystemid, nuaractorsystemid FROM nuarpublication.fn_getnewsubmissions(sourceschemaname =>'nuarsubmissions_dti', sourcetablename => 'nuarsubmissionevent', targetschemaname => 'nuarsubmissions', targettablename => 'nuarsubmissionevent', submissionresult => 'Success', submissionstatus => 'Completed');
        CREATE TEMPORARY VIEW newsubmissions AS SELECT submissionsystemid, nuaractorsystemid
                                                FROM
                                                    nuarpublication.fn_getallnewsubmissions(
                                                            sourceschemaname => 'nuarsubmissions_dti',
                                                            sourcetablename => 'nuarsubmissionevent',
                                                            targetschemaname => 'nuarsubmissions',
                                                            targettablename => 'nuarsubmissionevent');
    ELSE
        --CREATE TEMPORARY VIEW newsubmissions AS SELECT submissionsystemid, nuaractorsystemid FROM nuarpublication.fn_getnewsubmissionsfilteredbyorganisation(sourceschemaname => 'nuarsubmissions_dti', sourcetablename => 'nuarsubmissionevent', targetschemaname => 'nuarsubmissions', targettablename => 'nuarsubmissionevent', submissionresult => 'Success', submissionstatus => 'Completed', organisationids => (SELECT ARRAY_AGG(filteredorganisationid) FROM organisationfilterids));
        CREATE TEMPORARY VIEW newsubmissions AS SELECT submissionsystemid, nuaractorsystemid
                                                FROM
                                                    nuarpublication.fn_getallnewsubmissionsfilteredbyorganisation(
                                                            sourceschemaname => 'nuarsubmissions_dti',
                                                            sourcetablename => 'nuarsubmissionevent',
                                                            targetschemaname => 'nuarsubmissions',
                                                            targettablename => 'nuarsubmissionevent',
                                                            organisationids => (SELECT ARRAY_AGG(filteredorganisationid) FROM organisationfilterids));

        -- If we're filtering by organisation id, we should make sure that all these organisations are in the target nuaractor table
        -- Create an array of the filter organisation ids
        SELECT ARRAY_AGG(filteredorganisationid)
        FROM organisationfilterids
        INTO organisationfilteridsarray;

        -- Do an upsert for all these organisation ids into the target nuaractor table
        PERFORM nuarpublication.fn_upsertnuaractors_v212(
                sourceschemaname => 'nuarorganisations_dti',
                sourcetablename => 'nuaractor',
                targetschemaname => 'nuarorganisations',
                targettablename => 'nuaractor',
                sourcesystemids => organisationfilteridsarray);

        PERFORM nuarpublication.fn_logpublicationmessage(
                publicationstage => publicationstage,
                messagetext => 'Upserted NUARActor records for filter organisation ids',
                logdatetime => NOW()
                );

    END IF;

    CREATE TEMPORARY VIEW submissionids AS
    SELECT DISTINCT submissionsystemid
    FROM newsubmissions;

    CREATE TEMPORARY VIEW nuaractorids AS
    SELECT DISTINCT nuaractorsystemid
    FROM newsubmissions;

    -- Is either the list of Submissions or the list of NUARActors empty?
-- If so, we just drop out
    IF (
        NOT EXISTS (SELECT FROM submissionids) OR
        NOT EXISTS (SELECT FROM nuaractorids)
        )
    THEN
        RAISE NOTICE 'No submissions to bring into Holding';
        PERFORM nuarpublication.fn_logpublicationmessage(
                publicationstage => publicationstage,
                messagetext => 'No submissions to bring into Holding',
                logdatetime => NOW()
                );

        IF (runvalidation)
        THEN
            PERFORM nuarpublication.fn_recordcountvalidationrun_v211(
                    runname => 'dti-holding',
                    sourceschemasuffix => 'dti',
                    createanddropfdwserver => false,
                    createanddropfdwschemas => false,
                    publishablesubmissionsonly => false -- Set true for checking standby against holding, otherwise false
                    );

            -- Check the results and set an Error State if appropriate
            IF (SELECT nuarpublication.fn_checkvalidationresults (stagenamevalue => 'dti-holding'))
            THEN
                noerrorstateset := true;
            ELSE
                noerrorstateset := false;
            END IF;

        END IF;

        IF (createanddropfdwschemas)
        THEN
            -- Drop the FDW schemas
            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarcodelists_datamanagement_%s'',
				shouldcascade => true);
		', sourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarcodelists_platform_%s'',
				shouldcascade => true);
		', sourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarcodelists_transformation_%s'',
				shouldcascade => true);
		', sourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarcustombackdrop_%s'',
				shouldcascade => true);
		', sourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuardata_%s'',
				shouldcascade => true);
		', sourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarorganisations_%s'',
				shouldcascade => true);
		', sourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarsubmissions_%s'',
				shouldcascade => true);
		', sourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarversion_%s'',
				shouldcascade => true);
		', sourceschemasuffix);

        END IF;

        IF (createanddropfdwserver)
        THEN

            -- Drop the FDW Server
            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropfdwserver(
				servername => ''%s'');
		', fdwservername);

        END IF;

        IF (noerrorstateset)
        THEN
            -- The last thing we do is set our status to Completed
            PERFORM nuarpublication.fn_recordpublicationjobstatus(
                    stagenamevalue => 'dti-holding',
                    statusvalue => 'Completed',
                    startdatetimevalue => NOW(),
                    enddatetimevalue => NOW()
                    );
        END IF;

        RETURN '';
    END IF;

    SELECT ARRAY_AGG(submissionsystemid)
    FROM submissionids
    INTO submissionidsarray;

    SELECT ARRAY_AGG(nuaractorsystemid)
    FROM nuaractorids
    INTO nuaractoridsarray;

    -- We get a list of table names from the current database (HOLDING).
-- We are assuming that table names in the source and the destination match.
-- Get tables with geometry
    CREATE TEMPORARY VIEW geometrytablenames AS
    SELECT table_name::character varying as tablenamestring
    FROM information_schema.columns
    WHERE column_name = 'geometry'
      AND table_schema = 'nuardata';

    -- Get relationship_tables from nuardata
    CREATE TEMPORARY VIEW relationshiptablenames AS
    SELECT table_name::character varying as tablenamestring
    FROM information_schema.tables
    WHERE
        (
            table_name LIKE 'relationship_%'
                OR
            table_name='nuarsubordinatenetworkdefinition'
            )
      AND table_schema = 'nuardata';

    -- Get backdrop tables with geometry
    CREATE TEMPORARY VIEW backdropgeometrytablenames AS
    SELECT table_name::character varying as tablenamestring
    FROM information_schema.columns
    WHERE column_name = 'geometry'
      AND table_schema = 'nuarcustombackdrop';

-- Upsert NUARActor records
    PERFORM nuarpublication.fn_upsertnuaractors_v212(
            sourceschemaname => 'nuarorganisations_dti',
            sourcetablename => 'nuaractor',
            targetschemaname => 'nuarorganisations',
            targettablename => 'nuaractor',
            sourcesystemids => nuaractoridsarray);

    PERFORM nuarpublication.fn_logpublicationmessage(
            publicationstage => publicationstage,
            messagetext => 'Upserted NUARActor records',
            logdatetime => NOW()
            );

-- Upsert NUARSubmissionEvent records
    PERFORM nuarpublication.fn_upsertnuarsubmissionevents_v21x(
            sourceschemaname => 'nuarsubmissions_dti',
            sourcetablename => 'nuarsubmissionevent',
            targetschemaname => 'nuarsubmissions',
            targettablename => 'nuarsubmissionevent',
            sourcesystemids => submissionidsarray);

    PERFORM nuarpublication.fn_logpublicationmessage(
            publicationstage => publicationstage,
            messagetext => 'Upserted NUARSubmissionEvent records',
            logdatetime => NOW()
            );

-- Upsert NUARActor related records for these Submissions
    PERFORM nuarpublication.fn_upsertnuaractorrelateddata_v212(
            sourceschemaname => 'nuarorganisations_dti',
            sourcecontactdetailstablename => 'nuarcontactdetails',
            sourceactivityproximityruletablename => 'nuaractivityproximityrule',
            sourcecontactdetailsrelationshiptablename => 'relationship_actortocontactdetails',
            sourceactivityproximityrulerelationshiptablename => 'relationship_actortorule',
            sourceservicearearelationshiptablename => 'relationship_actortoservicearea',
            sourcedomainrelationshiptablename => 'relationship_actortosubmissiondomain',
            sourceserviceproviderrelationshiptablename => 'relationship_serviceprovidertoorganisation',
            targetschemaname => 'nuarorganisations',
            targetcontactdetailstablename => 'nuarcontactdetails',
            targetactivityproximityruletablename => 'nuaractivityproximityrule',
            targetcontactdetailsrelationshiptablename => 'relationship_actortocontactdetails',
            targetactivityproximityrulerelationshiptablename => 'relationship_actortorule',
            targetservicearearelationshiptablename => 'relationship_actortoservicearea',
            targetdomainrelationshiptablename => 'relationship_actortosubmissiondomain',
            targetserviceproviderrelationshiptablename => 'relationship_serviceprovidertoorganisation',
            sourcesystemids => submissionidsarray);

    PERFORM nuarpublication.fn_logpublicationmessage(
            publicationstage => publicationstage,
            messagetext => 'Upserted NUARActor related records',
            logdatetime => NOW()
            );

-- Upsert NUARActor DDA and DIS related records for these Organisations
    PERFORM nuarpublication.fn_upsertnuaractorlegalagreementsdata_v21x(
            sourceschemaname => 'nuarorganisations_dti',
            sourcenuaractortablename => 'nuaractor',
            sourceddatablename => 'nuardda',
            sourcedistablename => 'nuardis',
            sourceddarelationshiptablename => 'relationship_actortodda',
            sourcedisrelationshiptablename => 'relationship_actortodis',
            targetschemaname => 'nuarorganisations',
            targetddatablename => 'nuardda',
            targetdistablename => 'nuardis',
            targetddarelationshiptablename => 'relationship_actortodda',
            targetdisrelationshiptablename => 'relationship_actortodis',
            sourcesystemids => nuaractoridsarray);

    PERFORM nuarpublication.fn_logpublicationmessage(
            publicationstage => publicationstage,
            messagetext => 'Upserted NUARActor DDA and DIS related records',
            logdatetime => NOW()
            );

-- COPY Geometry tables
    PERFORM nuarpublication.fn_copysubmissiontablesfromsource(
            sourceschemaname => 'nuardata_dti',
            targetschemaname => 'nuardata',
            submissionids => submissionidsarray,
            tablenames => (SELECT ARRAY_AGG(tablenamestring) FROM geometrytablenames),
            publicationstage => publicationstage);

-- COPY Relationship tables
    PERFORM nuarpublication.fn_copysubmissiontablesfromsource(
            sourceschemaname => 'nuardata_dti',
            targetschemaname => 'nuardata',
            submissionids => submissionidsarray,
            tablenames => (SELECT ARRAY_AGG(tablenamestring) FROM relationshiptablenames),
            publicationstage => publicationstage);

-- COPY Backdrop Geometry tables
    SELECT ARRAY_AGG(tablenamestring) FROM backdropgeometrytablenames INTO backdropgeometrytablesarray;
    PERFORM nuarpublication.fn_copysubmissiontablesfromsource(
            sourceschemaname => 'nuarcustombackdrop_dti',
            targetschemaname => 'nuarcustombackdrop',
            submissionids => submissionidsarray,
            tablenames => backdropgeometrytablesarray,
            publicationstage => publicationstage);

-- COPY NUARVersion tables
    PERFORM nuarpublication.fn_upsertnuarversion(
            sourceschemaname => 'nuarversion_dti',
            targetschemaname => 'nuarversion');

    PERFORM nuarpublication.fn_logpublicationmessage(
            publicationstage => publicationstage,
            messagetext => 'Upserted NUARVersion records',
            logdatetime => NOW()
            );

-- COPY the tables in the different Codelist schemas
    PERFORM nuarpublication.fn_upsertnuarcodelists(
            sourceschemaname => 'nuarcodelists_datamanagement_dti',
            targetschemaname => 'nuarcodelists_datamanagement');

    PERFORM nuarpublication.fn_upsertnuarcodelists(
            sourceschemaname => 'nuarcodelists_platform_dti',
            targetschemaname => 'nuarcodelists_platform');

    PERFORM nuarpublication.fn_upsertnuarcodelists(
            sourceschemaname => 'nuarcodelists_transformation_dti',
            targetschemaname => 'nuarcodelists_transformation');

    PERFORM nuarpublication.fn_logpublicationmessage(
            publicationstage => publicationstage,
            messagetext => 'Upserted NUARCodelist records',
            logdatetime => NOW()
            );

    -- Now we've done all our copies, we need to process any "No Change" submissions.
-- Each of these specifies a "predecessor" submission.
-- We update the datelastupdated value to the dateofsubmission of the No Change submission
-- for records in every table which have a submissioneventid_fk value equal to the predecessor Submission
    PERFORM nuarpublication.fn_processnochangesubmissionevents_v211(
            submissionsschemaname => 'nuarsubmissions',
            submissionstablename => 'nuarsubmissionevent',
            submissionids => submissionidsarray
            );

-- Now we need to update the NUARPublicationEvent table
    SELECT nuarpublication.fn_recordpublicationdetails(
                   targetschemaname => 'nuarpublication',
                   publicationeventtablename => 'nuarpublicationevent',
                   publicationsubmissionrelationshiptablename => 'relationship_publicationtosubmission',
                   datamodelversion => '2.1.1',
                   submissionids => submissionidsarray)
    INTO publicationeventid;

    PERFORM nuarpublication.fn_logpublicationmessage(
            publicationstage => publicationstage,
            messagetext => 'Updated NUARPublicationEvent records',
            logdatetime => NOW()
            );

    DROP VIEW IF EXISTS submissionids;
    DROP VIEW IF EXISTS nuaractorids;
    DROP VIEW IF EXISTS newsubmissions;
    DROP VIEW IF EXISTS existingholdingsubmissions;
    DROP VIEW IF EXISTS organisationfilterids;
    DROP VIEW IF EXISTS geometrytablenames;
    DROP VIEW IF EXISTS relationshiptablenames;
    DROP VIEW IF EXISTS backdropgeometrytablenames;

    IF (runvalidation)
    THEN
        PERFORM nuarpublication.fn_recordcountvalidationrun_v211(
                runname => 'dti-holding',
                sourceschemasuffix => 'dti',
                createanddropfdwserver => false,
                createanddropfdwschemas => false,
                publishablesubmissionsonly => false -- Set true for checking standby against holding, otherwise false
                );

        -- Check the results and set an Error State if appropriate
        IF (SELECT nuarpublication.fn_checkvalidationresults (stagenamevalue => 'dti-holding'))
        THEN
            noerrorstateset := true;
        ELSE
            noerrorstateset := false;
        END IF;

    END IF;

    IF (createanddropfdwschemas)
    THEN
        -- Drop the FDW schemas
        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarcodelists_datamanagement_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarcodelists_platform_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarcodelists_transformation_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarcustombackdrop_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuardata_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarorganisations_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarsubmissions_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarversion_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

    END IF;

    IF (createanddropfdwserver)
    THEN

        -- Drop the FDW Server
        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropfdwserver(
			servername => ''%s'');
	', fdwservername);

    END IF;

    IF (noerrorstateset)
    THEN
        -- The last thing we do is set our status to Completed
        PERFORM nuarpublication.fn_recordpublicationjobstatus(
                stagenamevalue => 'dti-holding',
                statusvalue => 'Completed',
                startdatetimevalue => NOW(),
                enddatetimevalue => NOW()
                );
    END IF;

    RETURN publicationeventid;
END;
$$;


--
-- TOC entry 1852 (class 1255 OID 20122951)
-- Name: fn_getallnewsubmissions(character varying, character varying, character varying, character varying); Type: FUNCTION; Schema: nuarpublication; Owner: nuar_admin
--

CREATE OR REPLACE FUNCTION nuarpublication.fn_getallnewsubmissions(sourceschemaname character varying, sourcetablename character varying, targetschemaname character varying, targettablename character varying) RETURNS TABLE(submissionsystemid character varying, nuaractorsystemid character varying)
    LANGUAGE plpgsql
AS $$

BEGIN
    DROP VIEW IF EXISTS sourcetable;
    EXECUTE FORMAT('CREATE TEMPORARY VIEW sourcetable AS SELECT systemid, dataproviderid_fk, status FROM %s.%s',
                   sourceSchemaName, sourceTableName);

    RETURN QUERY
        EXECUTE FORMAT('
	SELECT systemid, dataproviderid_fk
	FROM sourcetable src
	WHERE (src.status IS NULL OR src.status = ''Completed'')
	AND	NOT EXISTS (
	   SELECT
	   FROM %s.%s
	   WHERE systemid = src.systemid
	);
', targetschemaname, targettablename);

    DROP VIEW IF EXISTS sourcetable;
END;
$$;


--
-- TOC entry 1853 (class 1255 OID 20122952)
-- Name: fn_getallnewsubmissionsfilteredbyorganisation(character varying, character varying, character varying, character varying, character varying[]); Type: FUNCTION; Schema: nuarpublication; Owner: nuar_admin
--

CREATE OR REPLACE FUNCTION nuarpublication.fn_getallnewsubmissionsfilteredbyorganisation(sourceschemaname character varying, sourcetablename character varying, targetschemaname character varying, targettablename character varying, organisationids character varying[]) RETURNS TABLE(submissionsystemid character varying, nuaractorsystemid character varying)
    LANGUAGE plpgsql
AS $$

BEGIN
    DROP VIEW IF EXISTS sourcetable;
    EXECUTE FORMAT('CREATE TEMPORARY VIEW sourcetable AS SELECT systemid, dataproviderid_fk, status FROM %s.%s',
                   sourceSchemaName, sourceTableName);

    RETURN QUERY
        EXECUTE FORMAT('
SELECT systemid, dataproviderid_fk
FROM sourcetable src
WHERE src.dataproviderid_fk = ANY (''%s'')
AND (src.status IS NULL OR src.status = ''Completed'')
AND NOT EXISTS (
   SELECT
   FROM %s.%s
   WHERE systemid = src.systemid
   );
', organisationids, targetschemaname, targettablename);

    DROP VIEW IF EXISTS sourcetable;
END;
$$;



--
-- TOC entry 1856 (class 1255 OID 20122955)
-- Name: fn_getnewsubmissions(character varying, character varying, character varying, character varying, character varying, character varying); Type: FUNCTION; Schema: nuarpublication; Owner: nuar_admin
--

CREATE OR REPLACE FUNCTION nuarpublication.fn_getnewsubmissions(sourceschemaname character varying, sourcetablename character varying, targetschemaname character varying, targettablename character varying, submissionresult character varying, submissionstatus character varying) RETURNS TABLE(submissionsystemid character varying, nuaractorsystemid character varying)
    LANGUAGE plpgsql
AS $$

BEGIN
    DROP VIEW IF EXISTS sourcetable;
    EXECUTE FORMAT(
            'CREATE TEMPORARY VIEW sourcetable AS SELECT systemid, dataproviderid_fk FROM %s.%s WHERE result = ''%s'' AND status = ''%s''',
            sourceSchemaName, sourceTableName, submissionresult, submissionstatus);

    RETURN QUERY
        EXECUTE FORMAT('
SELECT systemid, dataproviderid_fk
FROM sourcetable src
WHERE NOT EXISTS (
   SELECT
   FROM   %s.%s
   WHERE  systemid = src.systemid
   );
', targetschemaname, targettablename);

    DROP VIEW IF EXISTS sourcetable;
END;
$$;


--
-- TOC entry 1857 (class 1255 OID 20122956)
-- Name: fn_getnewsubmissionsfilteredbyorganisation(character varying, character varying, character varying, character varying, character varying, character varying, character varying[]); Type: FUNCTION; Schema: nuarpublication; Owner: nuar_admin
--

CREATE OR REPLACE FUNCTION nuarpublication.fn_getnewsubmissionsfilteredbyorganisation(sourceschemaname character varying, sourcetablename character varying, targetschemaname character varying, targettablename character varying, submissionresult character varying, submissionstatus character varying, organisationids character varying[]) RETURNS TABLE(submissionsystemid character varying, nuaractorsystemid character varying)
    LANGUAGE plpgsql
AS $$

BEGIN
    DROP VIEW IF EXISTS sourcetable;
    EXECUTE FORMAT(
            'CREATE TEMPORARY VIEW sourcetable AS SELECT systemid, dataproviderid_fk FROM %s.%s WHERE result = ''%s'' AND status = ''%s''',
            sourceSchemaName, sourceTableName, submissionresult, submissionstatus);

    RETURN QUERY
        EXECUTE FORMAT('
SELECT systemid, dataproviderid_fk
FROM   sourcetable src
WHERE dataprovider_fk = ANY (''%s'')
AND NOT EXISTS (
   SELECT
   FROM   %s.%s
   WHERE  systemid = src.systemid
   );
', organisationids, targetschemaname, targettablename);

    DROP VIEW IF EXISTS sourcetable;
END;
$$;




--
-- TOC entry 1873 (class 1255 OID 50630522)
-- Name: fn_processnochangesubmissionevents_v211(character varying, character varying, character varying[]); Type: FUNCTION; Schema: nuarpublication; Owner: nuar_admin
--

CREATE OR REPLACE FUNCTION nuarpublication.fn_processnochangesubmissionevents_v211(submissionsschemaname character varying, submissionstablename character varying, submissionids character varying[]) RETURNS integer
    LANGUAGE plpgsql
AS $$

BEGIN

    DECLARE
        nochangesubmissionid       character varying;
        nochangesubmissionidsarray character varying[];
        targettablenamesarray      character varying[];
        targettablename            character varying;
        selectcount                integer;

    BEGIN

        DROP TABLE IF EXISTS nochangesubmissions;
        DROP VIEW IF EXISTS targettablenames;

        -- We create a temporary table containing systemid, dateofsubmission, nochangetosubmissionid
        -- WHERE nuarsubmissionevent.systemid = ANY(submissionids)
        -- AND nuarsubmissionevent.nochange=true
        CREATE TEMPORARY TABLE nochangesubmissions
        (
            systemid               character varying,
            dateofsubmission       timestamp with time zone,
            nochangetosubmissionid character varying
        );

        EXECUTE FORMAT('
		INSERT INTO nochangesubmissions
		SELECT DISTINCT ON (nochangetosubmissionid) systemid, dateofsubmission, nochangetosubmissionid
		FROM %s.%s
		WHERE systemid=ANY (''%s'')
		AND nochange=TRUE
		ORDER BY nochangetosubmissionid, dateofsubmission DESC
	', submissionsschemaname, submissionstablename, submissionids);

        -- Is the table of No Change submissions empty?
        -- If so, we just drop out
        IF (NOT EXISTS (SELECT FROM nochangesubmissions))
        THEN
            RAISE NOTICE 'There are 0 No Change Submissions';
            RETURN 0;
        END IF;

        -- FOR EACH nochangetosubmissionid in the temporary table
        -- Update all tables that contain a submissioneventid_fk column and don't end with '_history' in nuarcustombackdrop, nuardata, nuarorganisations
        -- SET datelastupdated = temptable.dateofsubmission (select TOP 1 dateofsubmission FROM temptable WHERE nochangetosubmissionid='x' ORDER BY dateofsubmission DESC)
        CREATE TEMPORARY VIEW targettablenames AS
        SELECT table_schema::character varying as tableschemastring, table_name::character varying as tablenamestring
        FROM information_schema.columns
        WHERE column_name = 'submissioneventid_fk'
          AND (table_schema = 'nuarcustombackdrop' OR table_schema = 'nuardata' OR table_schema = 'nuarorganisations')
          AND table_name NOT LIKE '%_history';

        SELECT ARRAY_AGG(tableschemastring || '.' || tablenamestring)
        FROM targettablenames
        INTO targettablenamesarray;

        SELECT ARRAY_AGG(nochangetosubmissionid)
        FROM nochangesubmissions
        INTO nochangesubmissionidsarray;

        FOREACH nochangesubmissionid IN ARRAY nochangesubmissionidsarray
            LOOP

                FOREACH targettablename IN ARRAY targettablenamesarray
                    LOOP

                        EXECUTE FORMAT('
				UPDATE %s AS t
				SET datelastupdated = (SELECT dateofsubmission FROM nochangesubmissions WHERE nochangetosubmissionid=''%s'')
				WHERE t.submissioneventid_fk = ''%s''
			', targettablename, nochangesubmissionid, nochangesubmissionid);

                    END LOOP;

            END LOOP;

        PERFORM COUNT(systemid) FROM nochangesubmissions;
        GET DIAGNOSTICS selectcount = ROW_COUNT;

        RAISE NOTICE 'Applied % No Change Submissions', selectcount;

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_logpublicationmessage(
			publicationstage => ''DTI to Holding'',
			messagetext => ''Applied %s No Change Submissions'',
			logdatetime => NOW()
			);
		', selectcount);

        DROP VIEW IF EXISTS targettablenames;
        DROP TABLE IF EXISTS nochangesubmissions;

    END;

    RETURN 0;
END;
$$;




--
-- TOC entry 1877 (class 1255 OID 50851814)
-- Name: fn_submissionvalidationrun(character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, boolean, boolean, boolean); Type: FUNCTION; Schema: nuarpublication; Owner: nuar_admin
--

CREATE OR REPLACE FUNCTION nuarpublication.fn_submissionvalidationrun(runname character varying, sourceschemasuffix character varying, fdwservername character varying DEFAULT ''::character varying, fdwdbhostname character varying DEFAULT ''::character varying, fdwdbport character varying DEFAULT ''::character varying, fdwdbname character varying DEFAULT ''::character varying, fdwdbusername character varying DEFAULT ''::character varying, fdwdbpassword character varying DEFAULT ''::character varying, createanddropfdwserver boolean DEFAULT false, createanddropfdwschemas boolean DEFAULT false, publishablesubmissionsonly boolean DEFAULT false) RETURNS integer
    LANGUAGE plpgsql
AS $$

BEGIN

    PERFORM nuarpublication.fn_submissionvalidationrun_v211(
            runname => runname,
            sourceschemasuffix => sourceschemasuffix,
            fdwservername => fdwservername,
            fdwdbhostname => fdwdbhostname,
            fdwdbport => fdwdbport,
            fdwdbname => fdwdbname,
            fdwdbusername => fdwdbusername,
            fdwdbpassword => fdwdbpassword,
            createanddropfdwserver => createanddropfdwserver,
            createanddropfdwschemas => createanddropfdwschemas,
            publishablesubmissionsonly => publishablesubmissionsonly);

    RETURN 1;
END;
$$;


--
-- TOC entry 1895 (class 1255 OID 71821448)
-- Name: fn_synchroniseserviceareasubmissionchanges(); Type: FUNCTION; Schema: nuarpublication; Owner: nuar_owner
--

CREATE OR REPLACE FUNCTION nuarpublication.fn_synchroniseserviceareasubmissionchanges() RETURNS trigger
    LANGUAGE plpgsql
AS $$

BEGIN

    DECLARE
        sourcerecordsystemid       character varying;
        columnnamesstring          text;
        targetrecordsystemid       character varying;
        targetrecorddataproviderid character varying;
        nuaractorname              character varying;

    BEGIN

        sourcerecordsystemid = NEW.systemid;

        DROP TABLE IF EXISTS columnnames;

        CREATE TEMPORARY TABLE columnnames
        (
            column_name      character varying,
            ordinal_position integer
        );

        TRUNCATE columnnames;
        columnnamesstring := '';
        -- Get the column names from the target table ordered by ordinal position so that the INSERT will work
        INSERT INTO columnnames
        SELECT column_name, ordinal_position
        FROM information_schema.columns
        WHERE table_schema = 'nuarsupportdata'
          AND table_name = 'servicearea_ft_platformdb'
          AND column_name <> 'systemid'
          AND column_name <> 'submissioneventid_fk'
        ORDER BY ordinal_position;

        -- Get the column names from the order view as a comma-separated string
        SELECT STRING_AGG(column_name, ', ') AS col INTO columnnamesstring FROM columnnames;

        SELECT fsa.systemid, fsa.dataproviderassigneduniqueid
        INTO targetrecordsystemid, targetrecorddataproviderid
        FROM nuardata_platformdb.servicearea fsa
                 INNER JOIN nuarorganisations_platformdb.relationship_actortoservicearea frelasa
                            ON frelasa.serviceareaid = fsa.dataproviderassigneduniqueid
        WHERE frelasa.actorid = NEW.dataproviderid_fk
          AND frelasa.serviceareapurpose = NEW.serviceareasubtype
          AND ST_SRID(NEW.geometry) = ST_SRID(fsa.geometry);

        -- See if there are any existing Service Area records in platform_db with this purpose for this Asset Owners
        IF (coalesce(targetrecordsystemid, '') <> '')
        THEN
            -- An equivalent record already exists - we're doing an UPDATE
            EXECUTE FORMAT('
			UPDATE nuarsupportdata.servicearea_ft_platformdb as ft
			SET (%s) =
			(SELECT %s FROM nuarsupportdata.servicearea
			WHERE systemid = ''%s'')
			WHERE ft.systemid = ''%s'';
		', columnnamesstring, columnnamesstring, sourcerecordsystemid, targetrecordsystemid);

            -- We also need to update the relationship record
            UPDATE nuarsupportdata.servicearearelationship_ft_platformdb
            SET datelastupdated    = NOW(),
                actorid            = NEW.dataproviderid_fk,
                serviceareaid      = NEW.dataproviderassigneduniqueid,
                serviceareapurpose = NEW.serviceareasubtype,
                serviceareaname    = nuaractorname || ' ' || NEW.serviceareasubtype
            WHERE actorid = NEW.dataproviderid_fk
              AND serviceareaid = targetrecorddataproviderid
              AND serviceareapurpose = NEW.serviceareasubtype;
        ELSE
            SELECT displayname
            INTO nuaractorname
            FROM nuarorganisations.nuaractor
            WHERE systemid = NEW.dataproviderid_fk;

            -- No equivalent record already exists - we're doing an INSERT
            EXECUTE FORMAT('
			INSERT INTO nuarsupportdata.servicearea_ft_platformdb
			(systemid, %s)
			SELECT systemid,%s from nuarsupportdata.servicearea
			WHERE systemid = ''%s'';
		', columnnamesstring, columnnamesstring, sourcerecordsystemid);

            -- We also need to create a relationship record
            INSERT INTO nuarsupportdata.servicearearelationship_ft_platformdb (systemid,
                                                                               lifecyclestatus,
                                                                               datelastupdated,
                                                                               dateoflastlifecyclestatuschange,
                                                                               systemloaddate,
                                                                               actorid,
                                                                               serviceareaid,
                                                                               serviceareapurpose,
                                                                               serviceareaname,
                                                                               dataproviderid_fk)
            VALUES (gen_random_uuid()::character varying,
                    'Submitted',
                    NOW(),
                    NOW(),
                    NOW(),
                    NEW.dataproviderid_fk,
                    NEW.dataproviderassigneduniqueid,
                    NEW.serviceareasubtype,
                    nuaractorname || ' ' || NEW.serviceareasubtype,
                    NEW.dataproviderid_fk);

        END IF;

        DROP TABLE IF EXISTS columnnames;
    END;

    RETURN NEW;

END;

$$;



--
-- TOC entry 1896 (class 1255 OID 71821450)
-- Name: fn_upsertnuaractorrelateddata_singlesubmission_v211(character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying); Type: FUNCTION; Schema: nuarpublication; Owner: nuar_admin
--

CREATE OR REPLACE FUNCTION nuarpublication.fn_upsertnuaractorrelateddata_singlesubmission_v211(sourceschemaname character varying, sourcecontactdetailstablename character varying, sourceactivityproximityruletablename character varying, sourcecontactdetailsrelationshiptablename character varying, sourceactivityproximityrulerelationshiptablename character varying, sourceservicearearelationshiptablename character varying, sourcedomainrelationshiptablename character varying, sourceserviceproviderrelationshiptablename character varying, targetschemaname character varying, targetcontactdetailstablename character varying, targetactivityproximityruletablename character varying, targetcontactdetailsrelationshiptablename character varying, targetactivityproximityrulerelationshiptablename character varying, targetservicearearelationshiptablename character varying, targetdomainrelationshiptablename character varying, targetserviceproviderrelationshiptablename character varying, sourcesystemid character varying) RETURNS integer
    LANGUAGE plpgsql
AS $$

BEGIN

    DECLARE
        contactdetailscolumnnamesstring           text;
        contactdetailshistorycolumnnamesstring    text;
        rulescolumnnamesstring                    text;
        ruleshistorycolumnnamesstring             text;
        contactdetailsrelcolumnnamesstring        text;
        contactdetailsrelhistorycolumnnamesstring text;
        rulesrelcolumnnamesstring                 text;
        rulesrelhistorycolumnnamesstring          text;
        servicearearelcolumnnamesstring           text;
        servicearearelhistorycolumnnamesstring    text;
        domainrelcolumnnamesstring                text;
        serviceproviderrelcolumnnamesstring       text;

    BEGIN

        CREATE TEMPORARY TABLE columnnames
        (
            column_name      character varying,
            ordinal_position integer
        );

        TRUNCATE columnnames;
        -- Get the column names from the target table ordered by ordinal position so that the INSERT will work
        EXECUTE FORMAT('
		INSERT INTO columnnames
		SELECT column_name::character varying, ordinal_position
		FROM information_schema.columns
		WHERE table_name = ''%s'' AND table_schema = ''%s''
		ORDER BY ordinal_position', targetcontactdetailstablename, targetschemaname);

        -- Get the column names from the order view as a comma-separated string
        SELECT STRING_AGG(column_name, ', ') AS col INTO contactdetailscolumnnamesstring FROM columnnames;

        TRUNCATE columnnames;
        -- Get the column names from the target table ordered by ordinal position so that the INSERT will work
        EXECUTE FORMAT('
		INSERT INTO columnnames
		SELECT column_name::character varying, ordinal_position
		FROM information_schema.columns
		WHERE table_name = ''%s_history'' AND table_schema = ''%s''
		ORDER BY ordinal_position', targetcontactdetailstablename, targetschemaname);

        -- Get the column names from the order view as a comma-separated string
        SELECT STRING_AGG(column_name, ', ') AS col INTO contactdetailshistorycolumnnamesstring FROM columnnames;

        TRUNCATE columnnames;
        -- Get the column names from the target table ordered by ordinal position so that the INSERT will work
        EXECUTE FORMAT('
		INSERT INTO columnnames
		SELECT column_name::character varying, ordinal_position
		FROM information_schema.columns
		WHERE table_name = ''%s'' AND table_schema = ''%s''
		ORDER BY ordinal_position', targetactivityproximityruletablename, targetschemaname);

        -- Get the column names from the order view as a comma-separated string
        SELECT STRING_AGG(column_name, ', ') AS col INTO rulescolumnnamesstring FROM columnnames;

        TRUNCATE columnnames;
        -- Get the column names from the target table ordered by ordinal position so that the INSERT will work
        EXECUTE FORMAT('
		INSERT INTO columnnames
		SELECT column_name::character varying, ordinal_position
		FROM information_schema.columns
		WHERE table_name = ''%s_history'' AND table_schema = ''%s''
		ORDER BY ordinal_position', targetactivityproximityruletablename, targetschemaname);

        -- Get the column names from the order view as a comma-separated string
        SELECT STRING_AGG(column_name, ', ') AS col INTO ruleshistorycolumnnamesstring FROM columnnames;

        TRUNCATE columnnames;
        -- Get the column names from the target table ordered by ordinal position so that the INSERT will work
        EXECUTE FORMAT('
		INSERT INTO columnnames
		SELECT column_name::character varying, ordinal_position
		FROM information_schema.columns
		WHERE table_name = ''%s'' AND table_schema = ''%s''
		ORDER BY ordinal_position', targetcontactdetailsrelationshiptablename, targetschemaname);

        -- Get the column names from the order view as a comma-separated string
        SELECT STRING_AGG(column_name, ', ') AS col INTO contactdetailsrelcolumnnamesstring FROM columnnames;

        TRUNCATE columnnames;
        -- Get the column names from the target table ordered by ordinal position so that the INSERT will work
        EXECUTE FORMAT('
		INSERT INTO columnnames
		SELECT column_name::character varying, ordinal_position
		FROM information_schema.columns
		WHERE table_name = ''%s_history'' AND table_schema = ''%s''
		ORDER BY ordinal_position', targetcontactdetailsrelationshiptablename, targetschemaname);

        -- Get the column names from the order view as a comma-separated string
        SELECT STRING_AGG(column_name, ', ') AS col INTO contactdetailsrelhistorycolumnnamesstring FROM columnnames;

        TRUNCATE columnnames;
        -- Get the column names from the target table ordered by ordinal position so that the INSERT will work
        EXECUTE FORMAT('
		INSERT INTO columnnames
		SELECT column_name::character varying, ordinal_position
		FROM information_schema.columns
		WHERE table_name = ''%s'' AND table_schema = ''%s''
		ORDER BY ordinal_position', targetactivityproximityrulerelationshiptablename, targetschemaname);

        -- Get the column names from the order view as a comma-separated string
        SELECT STRING_AGG(column_name, ', ') AS col INTO rulesrelcolumnnamesstring FROM columnnames;

        TRUNCATE columnnames;
        -- Get the column names from the target table ordered by ordinal position so that the INSERT will work
        EXECUTE FORMAT('
		INSERT INTO columnnames
		SELECT column_name::character varying, ordinal_position
		FROM information_schema.columns
		WHERE table_name = ''%s_history'' AND table_schema = ''%s''
		ORDER BY ordinal_position', targetactivityproximityrulerelationshiptablename, targetschemaname);

        -- Get the column names from the order view as a comma-separated string
        SELECT STRING_AGG(column_name, ', ') AS col INTO rulesrelhistorycolumnnamesstring FROM columnnames;

        TRUNCATE columnnames;
        -- Get the column names from the target table ordered by ordinal position so that the INSERT will work
        EXECUTE FORMAT('
		INSERT INTO columnnames
		SELECT column_name::character varying, ordinal_position
		FROM information_schema.columns
		WHERE table_name = ''%s'' AND table_schema = ''%s''
		ORDER BY ordinal_position', targetservicearearelationshiptablename, targetschemaname);

        -- Get the column names from the order view as a comma-separated string
        SELECT STRING_AGG(column_name, ', ') AS col INTO servicearearelcolumnnamesstring FROM columnnames;

        TRUNCATE columnnames;
        -- Get the column names from the target table ordered by ordinal position so that the INSERT will work
        EXECUTE FORMAT('
		INSERT INTO columnnames
		SELECT column_name::character varying, ordinal_position
		FROM information_schema.columns
		WHERE table_name = ''%s_history'' AND table_schema = ''%s''
		ORDER BY ordinal_position', targetservicearearelationshiptablename, targetschemaname);

        -- Get the column names from the order view as a comma-separated string
        SELECT STRING_AGG(column_name, ', ') AS col INTO servicearearelhistorycolumnnamesstring FROM columnnames;

        TRUNCATE columnnames;
        -- Get the column names from the target table ordered by ordinal position so that the INSERT will work
        EXECUTE FORMAT('
		INSERT INTO columnnames
		SELECT column_name::character varying, ordinal_position
		FROM information_schema.columns
		WHERE table_name = ''%s'' AND table_schema = ''%s''
		ORDER BY ordinal_position', targetdomainrelationshiptablename, targetschemaname);

        -- Get the column names from the order view as a comma-separated string
        SELECT STRING_AGG(column_name, ', ') AS col INTO domainrelcolumnnamesstring FROM columnnames;

        TRUNCATE columnnames;
        -- Get the column names from the target table ordered by ordinal position so that the INSERT will work
        EXECUTE FORMAT('
		INSERT INTO columnnames
		SELECT column_name::character varying, ordinal_position
		FROM information_schema.columns
		WHERE table_name = ''%s'' AND table_schema = ''%s''
		ORDER BY ordinal_position', targetserviceproviderrelationshiptablename, targetschemaname);

        -- Get the column names from the order view as a comma-separated string
        SELECT STRING_AGG(column_name, ', ') AS col INTO serviceproviderrelcolumnnamesstring FROM columnnames;


        -- NUARContactDetails
        DROP VIEW IF EXISTS sourcecontactdetailstable;
        EXECUTE FORMAT('
		CREATE TEMPORARY VIEW sourcecontactdetailstable AS
		SELECT %s
		FROM %s.%s
		WHERE submissioneventid_fk=''%s'';
	', contactdetailscolumnnamesstring, sourceschemaname, sourcecontactdetailstablename, sourcesystemid);

        EXECUTE FORMAT('
	INSERT INTO %s.%s AS nc
	SELECT * from sourcecontactdetailstable WHERE submissioneventid_fk = ''%s''
	ON CONFLICT (systemid)
	DO
	UPDATE
		SET systemid=excluded.systemid, datelastupdated=excluded.datelastupdated, systemloaddate=excluded.systemloaddate, organisationname=excluded.organisationname, address_singlelineaddress=excluded.address_singlelineaddress, address_subbuilding=excluded.address_subbuilding, address_buildingname=excluded.address_buildingname, address_buildingnumber=excluded.address_buildingnumber, address_streetname=excluded.address_streetname, address_locality=excluded.address_locality, address_townname=excluded.address_townname, address_postcode=excluded.address_postcode, address_uprn=excluded.address_uprn, contactdetailstype=excluded.contactdetailstype, departmentname=excluded.departmentname, emailaddress=excluded.emailaddress, telephonenumber=excluded.telephonenumber, dataproviderid_fk=excluded.dataproviderid_fk, submissioneventid_fk=excluded.submissioneventid_fk, webform=excluded.webform, dateoflastlifecyclestatuschange=excluded.dateoflastlifecyclestatuschange, lifecyclestatus=excluded.lifecyclestatus, nuarversion=excluded.nuarversion
		WHERE excluded.systemid = nc.systemid;
	', targetschemaname, targetcontactdetailstablename, sourcesystemid);

        -- nuarcontactdetails_history
        DROP VIEW IF EXISTS sourcecontactdetailstable_history;
        EXECUTE FORMAT('
		CREATE TEMPORARY VIEW sourcecontactdetailstable_history AS
		SELECT %s
		FROM %s.%s_history
		WHERE originalsystemid = ANY (SELECT systemid FROM %s.%s);
	', contactdetailshistorycolumnnamesstring, sourceschemaname, sourcecontactdetailstablename, targetschemaname,
                       targetcontactdetailstablename);

        EXECUTE FORMAT('
	INSERT INTO %s.%s_history AS nch
	SELECT * from sourcecontactdetailstable_history
	ON CONFLICT (systemid)
	DO
	UPDATE
		SET systemid=excluded.systemid, lifecyclestatus=excluded.lifecyclestatus, datelastupdated=excluded.datelastupdated, dateoflastlifecyclestatuschange=excluded.dateoflastlifecyclestatuschange, nuarversion=excluded.nuarversion, systemloaddate=excluded.systemloaddate, originalsystemid=excluded.originalsystemid, changeuserid=excluded.changeuserid, dateandtimeofchange=excluded.dateandtimeofchange, fieldschanged=excluded.fieldschanged, organisationname=excluded.organisationname, address_singlelineaddress=excluded.address_singlelineaddress, address_subbuilding=excluded.address_subbuilding, address_buildingname=excluded.address_buildingname, address_buildingnumber=excluded.address_buildingnumber, address_streetname=excluded.address_streetname, address_locality=excluded.address_locality, address_townname=excluded.address_townname, address_postcode=excluded.address_postcode, address_uprn=excluded.address_uprn, contactdetailstype=excluded.contactdetailstype, departmentname=excluded.departmentname, emailaddress=excluded.emailaddress, telephonenumber=excluded.telephonenumber, webform=excluded.webform, dataproviderid=excluded.dataproviderid
		WHERE excluded.systemid = nch.systemid;
	', targetschemaname, targetcontactdetailstablename);

        -- NUARActivityProximityRule
        DROP VIEW IF EXISTS sourceactivityproximityruletable;
        EXECUTE FORMAT('
		CREATE TEMPORARY VIEW sourceactivityproximityruletable AS
		SELECT %s
		FROM %s.%s
		WHERE submissioneventid_fk=''%s''
	', rulescolumnnamesstring, sourceschemaname, sourceactivityproximityruletablename, sourcesystemid);

        EXECUTE FORMAT('
	INSERT INTO %s.%s AS pr
	SELECT * from sourceactivityproximityruletable WHERE submissioneventid_fk = ''%s''
	ON CONFLICT (systemid)
	DO
	UPDATE
		SET systemid=excluded.systemid, datelastupdated=excluded.datelastupdated, systemloaddate=excluded.systemloaddate, name=excluded.name, description=excluded.description, enhancedmeasures=excluded.enhancedmeasures, activitytype=excluded.activitytype, proximity_length=excluded.proximity_length, proximity_unitofmeasure=excluded.proximity_unitofmeasure, dateoflastlifecyclestatuschange=excluded.dateoflastlifecyclestatuschange, lifecyclestatus=excluded.lifecyclestatus, nuarversion=excluded.nuarversion, dataproviderid_fk=excluded.dataproviderid_fk, submissioneventid_fk=excluded.submissioneventid_fk
		WHERE excluded.systemid = pr.systemid;
	', targetschemaname, targetactivityproximityruletablename, sourcesystemid);

        -- nuaractivityproximityrule_history
        DROP VIEW IF EXISTS sourceactivityproximityruletable_history;
        EXECUTE FORMAT('
		CREATE TEMPORARY VIEW sourceactivityproximityruletable_history AS
		SELECT %s
		FROM %s.%s_history
		WHERE originalsystemid = ANY (SELECT systemid FROM %s.%s);
	', ruleshistorycolumnnamesstring, sourceschemaname, sourceactivityproximityruletablename, targetschemaname,
                       targetactivityproximityruletablename);

        EXECUTE FORMAT('
	INSERT INTO %s.%s_history AS prh
	SELECT * from sourceactivityproximityruletable_history
	ON CONFLICT (systemid)
	DO
	UPDATE
		SET systemid=excluded.systemid, lifecyclestatus=excluded.lifecyclestatus, datelastupdated=excluded.datelastupdated, dateoflastlifecyclestatuschange=excluded.dateoflastlifecyclestatuschange, nuarversion=excluded.nuarversion, systemloaddate=excluded.systemloaddate, name=excluded.name, description=excluded.description, enhancedmeasures=excluded.enhancedmeasures, originalsystemid=excluded.originalsystemid, changeuserid=excluded.changeuserid, dateandtimeofchange=excluded.dateandtimeofchange, fieldschanged=excluded.fieldschanged, activitytype=excluded.activitytype, proximity_length=excluded.proximity_length, proximity_unitofmeasure=excluded.proximity_unitofmeasure, submissioneventid_fk=excluded.submissioneventid_fk, dataproviderid_fk=excluded.dataproviderid_fk
		WHERE excluded.systemid = prh.systemid;
	', targetschemaname, targetactivityproximityruletablename);

        -- relationship_actortocontactdetails
        DROP VIEW IF EXISTS sourcecontactdetailsrelationshiptable;
        EXECUTE FORMAT('
		CREATE TEMPORARY VIEW sourcecontactdetailsrelationshiptable AS
		SELECT %s
		FROM %s.%s
		WHERE submissioneventid_fk=''%s''
	', contactdetailsrelcolumnnamesstring, sourceschemaname, sourcecontactdetailsrelationshiptablename, sourcesystemid);

        EXECUTE FORMAT('
	INSERT INTO %s.%s AS rcd
	SELECT * from sourcecontactdetailsrelationshiptable WHERE submissioneventid_fk = ''%s''
	ON CONFLICT (systemid)
	DO
	UPDATE
		SET systemid=excluded.systemid, datelastupdated=excluded.datelastupdated, systemloaddate=excluded.systemloaddate, linkedactorid=excluded.linkedactorid, linkedcontactdetailsid=excluded.linkedcontactdetailsid, dateoflastlifecyclestatuschange=excluded.dateoflastlifecyclestatuschange, lifecyclestatus=excluded.lifecyclestatus, nuarversion=excluded.nuarversion, dataproviderid_fk=excluded.dataproviderid_fk, submissioneventid_fk=excluded.submissioneventid_fk
		WHERE excluded.systemid = rcd.systemid;
	', targetschemaname, targetcontactdetailsrelationshiptablename, sourcesystemid);

        -- relationship_actortocontactdetails_history
        DROP VIEW IF EXISTS sourcecontactdetailsrelationshiptable_history;
        EXECUTE FORMAT('
		CREATE TEMPORARY VIEW sourcecontactdetailsrelationshiptable_history AS
		SELECT %s
		FROM %s.%s_history
		WHERE originalsystemid = ANY (SELECT systemid FROM %s.%s);
	', contactdetailsrelhistorycolumnnamesstring, sourceschemaname, sourcecontactdetailsrelationshiptablename,
                       targetschemaname, targetcontactdetailsrelationshiptablename);

        EXECUTE FORMAT('
	INSERT INTO %s.%s_history AS rcdh
	SELECT * from sourcecontactdetailsrelationshiptable_history
	ON CONFLICT (systemid)
	DO
	UPDATE
		SET systemid=excluded.systemid, lifecyclestatus=excluded.lifecyclestatus, datelastupdated=excluded.datelastupdated, dateoflastlifecyclestatuschange=excluded.dateoflastlifecyclestatuschange, nuarversion=excluded.nuarversion, systemloaddate=excluded.systemloaddate, originalsystemid=excluded.originalsystemid, changeuserid=excluded.changeuserid, dateandtimeofchange=excluded.dateandtimeofchange, fieldschanged=excluded.fieldschanged, linkedactorid=excluded.linkedactorid, linkedcontactdetailsid=excluded.linkedcontactdetailsid, dataproviderid_fk=excluded.dataproviderid_fk, submissioneventid_fk=excluded.submissioneventid_fk
		WHERE excluded.systemid = rcdh.systemid;
	', targetschemaname, targetcontactdetailsrelationshiptablename);

        -- relationship_actortorule
        DROP VIEW IF EXISTS sourceactivityproximityrulerelationshiptable;
        EXECUTE FORMAT('
		CREATE TEMPORARY VIEW sourceactivityproximityrulerelationshiptable AS
		SELECT %s FROM %s.%s
		WHERE submissioneventid_fk=''%s''
	', rulesrelcolumnnamesstring, sourceschemaname, sourceactivityproximityrulerelationshiptablename, sourcesystemid);

        EXECUTE FORMAT('
	INSERT INTO %s.%s AS rpr
	SELECT * from sourceactivityproximityrulerelationshiptable WHERE submissioneventid_fk = ''%s''
	ON CONFLICT (systemid)
	DO
	UPDATE
		SET systemid=excluded.systemid, datelastupdated=excluded.datelastupdated, systemloaddate=excluded.systemloaddate, actorid=excluded.actorid, ruletablename=excluded.ruletablename, ruleid=excluded.ruleid, dateoflastlifecyclestatuschange=excluded.dateoflastlifecyclestatuschange, lifecyclestatus=excluded.lifecyclestatus, nuarversion=excluded.nuarversion, dataproviderid_fk=excluded.dataproviderid_fk, submissioneventid_fk=excluded.submissioneventid_fk
		WHERE excluded.systemid = rpr.systemid;
	', targetschemaname, targetactivityproximityrulerelationshiptablename, sourcesystemid);

        -- relationship_actortorule_history
        DROP VIEW IF EXISTS sourceactivityproximityrulerelationshiptable_history;
        EXECUTE FORMAT('
		CREATE TEMPORARY VIEW sourceactivityproximityrulerelationshiptable_history AS
		SELECT %s
		FROM %s.%s_history
		WHERE originalsystemid = ANY (SELECT systemid FROM %s.%s);
	', rulesrelhistorycolumnnamesstring, sourceschemaname, sourceactivityproximityrulerelationshiptablename,
                       targetschemaname, targetactivityproximityrulerelationshiptablename);

        EXECUTE FORMAT('
	INSERT INTO %s.%s_history AS rprh
	SELECT * from sourceactivityproximityrulerelationshiptable_history
	ON CONFLICT (systemid)
	DO
	UPDATE
		SET systemid=excluded.systemid, lifecyclestatus=excluded.lifecyclestatus, datelastupdated=excluded.datelastupdated, dateoflastlifecyclestatuschange=excluded.dateoflastlifecyclestatuschange, nuarversion=excluded.nuarversion, systemloaddate=excluded.systemloaddate, originalsystemid=excluded.originalsystemid, changeuserid=excluded.changeuserid, dateandtimeofchange=excluded.dateandtimeofchange, fieldschanged=excluded.fieldschanged, actorid=excluded.actorid, ruletablename=excluded.ruletablename, ruleid=excluded.ruleid, dataproviderid_fk=excluded.dataproviderid_fk, submissioneventid_fk=excluded.submissioneventid_fk
		WHERE excluded.systemid = rprh.systemid;
	', targetschemaname, targetactivityproximityrulerelationshiptablename);

        -- relationship_actortoservicearea
        DROP VIEW IF EXISTS sourceservicearearelationshiptable;
        EXECUTE FORMAT('
		CREATE TEMPORARY VIEW sourceservicearearelationshiptable AS
		SELECT %s
		FROM %s.%s
		WHERE submissioneventid_fk=''%s''
	', servicearearelcolumnnamesstring, sourceschemaname, sourceservicearearelationshiptablename, sourcesystemid);

        EXECUTE FORMAT('
	INSERT INTO %s.%s AS rsa
	SELECT * from sourceservicearearelationshiptable WHERE submissioneventid_fk = ''%s''
	ON CONFLICT (systemid)
	DO
	UPDATE
		SET systemid=excluded.systemid, datelastupdated=excluded.datelastupdated, systemloaddate=excluded.systemloaddate, actorid=excluded.actorid, serviceareaid=excluded.serviceareaid, serviceareapurpose=excluded.serviceareapurpose, serviceareaname=excluded.serviceareaname, utilitytype=excluded.utilitytype, dateoflastlifecyclestatuschange=excluded.dateoflastlifecyclestatuschange, lifecyclestatus=excluded.lifecyclestatus, nuarversion=excluded.nuarversion, dataproviderid_fk=excluded.dataproviderid_fk, submissioneventid_fk=excluded.submissioneventid_fk
		WHERE excluded.systemid = rsa.systemid;
	', targetschemaname, targetservicearearelationshiptablename, sourcesystemid);

        -- relationship_actortoservicearea_history
        DROP VIEW IF EXISTS sourceservicearearelationshiptable_history;
        EXECUTE FORMAT('
		CREATE TEMPORARY VIEW sourceservicearearelationshiptable_history AS
		SELECT %s
		FROM %s.%s_history
		WHERE originalsystemid = ANY (SELECT systemid FROM %s.%s);
	', servicearearelhistorycolumnnamesstring, sourceschemaname, sourceservicearearelationshiptablename,
                       targetschemaname, targetservicearearelationshiptablename);

        EXECUTE FORMAT('
	INSERT INTO %s.%s_history AS rsah
	SELECT * from sourceservicearearelationshiptable_history
	ON CONFLICT (systemid)
	DO
	UPDATE
		SET systemid=excluded.systemid, lifecyclestatus=excluded.lifecyclestatus, datelastupdated=excluded.datelastupdated, dateoflastlifecyclestatuschange=excluded.dateoflastlifecyclestatuschange, nuarversion=excluded.nuarversion, systemloaddate=excluded.systemloaddate, originalsystemid=excluded.originalsystemid, changeuserid=excluded.changeuserid, dateandtimeofchange=excluded.dateandtimeofchange, fieldschanged=excluded.fieldschanged, actorid=excluded.actorid, serviceareaid=excluded.serviceareaid, serviceareapurpose=excluded.serviceareapurpose, serviceareaname=excluded.serviceareaname, utilitytype=excluded.utilitytype, dataproviderid_fk=excluded.dataproviderid_fk, submissioneventid_fk=excluded.submissioneventid_fk
		WHERE excluded.systemid = rsah.systemid;
	', targetschemaname, targetservicearearelationshiptablename);


        -- relationship_actortosubmissiondomain
        DROP VIEW IF EXISTS sourcedomainrelationshiptable;
        EXECUTE FORMAT('
		CREATE TEMPORARY VIEW sourcedomainrelationshiptable AS
		SELECT %s
		FROM %s.%s
		WHERE submissioneventid_fk=''%s''
	', domainrelcolumnnamesstring, sourceschemaname, sourcedomainrelationshiptablename, sourcesystemid);

        EXECUTE FORMAT('
	INSERT INTO %s.%s AS rsd
	SELECT * from sourcedomainrelationshiptable WHERE submissioneventid_fk = ''%s''
	ON CONFLICT (systemid)
	DO
	UPDATE
		SET systemid=excluded.systemid, lifecyclestatus=excluded.lifecyclestatus, datelastupdated=excluded.datelastupdated, dateoflastlifecyclestatuschange=excluded.dateoflastlifecyclestatuschange, nuarversion=excluded.nuarversion, systemloaddate=excluded.systemloaddate, linkedactorid=excluded.linkedactorid, submissioneventid_fk=excluded.submissioneventid_fk, dataproviderid_fk=excluded.dataproviderid_fk, linkedsubmissiondomain_fk=excluded.linkedsubmissiondomain_fk
		WHERE excluded.systemid = rsd.systemid;
	', targetschemaname, targetdomainrelationshiptablename, sourcesystemid);


        -- relationship_serviceprovidertoorganisation
        DROP VIEW IF EXISTS sourceserviceproviderrelationshiptable;
        EXECUTE FORMAT('
		CREATE TEMPORARY VIEW sourceserviceproviderrelationshiptable AS
		SELECT %s
		FROM %s.%s
		WHERE submissioneventid_fk=''%s''
	', serviceproviderrelcolumnnamesstring, sourceschemaname, sourceserviceproviderrelationshiptablename,
                       sourcesystemid);

        EXECUTE FORMAT('
	INSERT INTO %s.%s AS rsp
	SELECT * from sourceserviceproviderrelationshiptable WHERE submissioneventid_fk = ''%s''
	ON CONFLICT (systemid)
	DO
	UPDATE
		SET systemid=excluded.systemid, lifecyclestatus=excluded.lifecyclestatus, datelastupdated=excluded.datelastupdated, dateoflastlifecyclestatuschange=excluded.dateoflastlifecyclestatuschange, nuarversion=excluded.nuarversion, systemloaddate=excluded.systemloaddate, linkedserviceproviderorganisationid=excluded.linkedserviceproviderorganisationid, linkedorganisationid=excluded.linkedorganisationid, servicetype=excluded.servicetype, submissioneventid_fk=excluded.submissioneventid_fk, dataproviderid_fk=excluded.dataproviderid_fk
		WHERE excluded.systemid = rsp.systemid;
	', targetschemaname, targetserviceproviderrelationshiptablename, sourcesystemid);


        RAISE NOTICE 'Added NUARActor related data for Submission Id: %', sourcesystemid;

        DROP VIEW IF EXISTS sourcecontactdetailstable;
        DROP VIEW IF EXISTS sourceactivityproximityruletable;
        DROP VIEW IF EXISTS sourcecontactdetailsrelationshiptable;
        DROP VIEW IF EXISTS sourceactivityproximityrulerelationshiptable;
        DROP VIEW IF EXISTS sourceservicearearelationshiptable;
        DROP VIEW IF EXISTS sourcedomainrelationshiptable;
        DROP VIEW IF EXISTS sourceserviceproviderrelationshiptable;

        DROP VIEW IF EXISTS sourcecontactdetailstable_history;
        DROP VIEW IF EXISTS sourceactivityproximityruletable_history;
        DROP VIEW IF EXISTS sourcecontactdetailsrelationshiptable_history;
        DROP VIEW IF EXISTS sourceactivityproximityrulerelationshiptable_history;
        DROP VIEW IF EXISTS sourceservicearearelationshiptable_history;

        DROP TABLE IF EXISTS columnnames;

    END;

    RETURN 1;
END;
$$;



--
-- TOC entry 1897 (class 1255 OID 71821452)
-- Name: fn_upsertnuaractorrelateddata_v211(character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying[]); Type: FUNCTION; Schema: nuarpublication; Owner: nuar_admin
--

CREATE OR REPLACE FUNCTION nuarpublication.fn_upsertnuaractorrelateddata_v211(sourceschemaname character varying, sourcecontactdetailstablename character varying, sourceactivityproximityruletablename character varying, sourcecontactdetailsrelationshiptablename character varying, sourceactivityproximityrulerelationshiptablename character varying, sourceservicearearelationshiptablename character varying, sourcedomainrelationshiptablename character varying, sourceserviceproviderrelationshiptablename character varying, targetschemaname character varying, targetcontactdetailstablename character varying, targetactivityproximityruletablename character varying, targetcontactdetailsrelationshiptablename character varying, targetactivityproximityrulerelationshiptablename character varying, targetservicearearelationshiptablename character varying, targetdomainrelationshiptablename character varying, targetserviceproviderrelationshiptablename character varying, sourcesystemids character varying[]) RETURNS integer
    LANGUAGE plpgsql
AS $$

BEGIN

    DECLARE
        sourcesystemid character varying;

    BEGIN

        FOREACH sourcesystemid IN ARRAY sourcesystemids
            LOOP

                PERFORM nuarpublication.fn_upsertnuaractorrelateddata_singlesubmission_v211(
                        sourceschemaname => sourceschemaname,
                        sourcecontactdetailstablename => sourcecontactdetailstablename,
                        sourceactivityproximityruletablename => sourceactivityproximityruletablename,
                        sourcecontactdetailsrelationshiptablename => sourcecontactdetailsrelationshiptablename,
                        sourceactivityproximityrulerelationshiptablename => sourceactivityproximityrulerelationshiptablename,
                        sourceservicearearelationshiptablename => sourceservicearearelationshiptablename,
                        sourcedomainrelationshiptablename => sourcedomainrelationshiptablename,
                        sourceserviceproviderrelationshiptablename => sourceserviceproviderrelationshiptablename,
                        targetschemaname => targetschemaname,
                        targetcontactdetailstablename => targetcontactdetailstablename,
                        targetactivityproximityruletablename => targetactivityproximityruletablename,
                        targetcontactdetailsrelationshiptablename => targetcontactdetailsrelationshiptablename,
                        targetactivityproximityrulerelationshiptablename => targetactivityproximityrulerelationshiptablename,
                        targetservicearearelationshiptablename => targetservicearearelationshiptablename,
                        targetdomainrelationshiptablename => targetdomainrelationshiptablename,
                        targetserviceproviderrelationshiptablename => targetserviceproviderrelationshiptablename,
                        sourcesystemid => sourcesystemid);

            END LOOP;
    END;

    RETURN 1;
END;
$$;



--
-- TOC entry 1898 (class 1255 OID 71821453)
-- Name: fn_upsertnuaractors_v210(character varying, character varying, character varying, character varying, character varying[]); Type: FUNCTION; Schema: nuarpublication; Owner: nuar_admin
--

CREATE OR REPLACE FUNCTION nuarpublication.fn_upsertnuaractors_v210(sourceschemaname character varying, sourcetablename character varying, targetschemaname character varying, targettablename character varying, sourcesystemids character varying[]) RETURNS integer
    LANGUAGE plpgsql
AS $$

BEGIN

    DECLARE
        sourcesystemid character varying;

    BEGIN

        FOREACH sourcesystemid IN ARRAY sourcesystemids
            LOOP

                PERFORM nuarpublication.fn_upsertsinglenuaractor_v210(
                        sourceschemaname => sourceschemaname,
                        sourcetablename => sourcetablename,
                        targetschemaname => targetschemaname,
                        targettablename => targettablename,
                        sourcesystemid => sourcesystemid);

            END LOOP;

-- Finally for NUARActor, we set displayname to be the same as name if it is blank
        EXECUTE FORMAT('
UPDATE %s.%s
SET displayname=name
WHERE displayname = '''' OR displayname IS NULL;
', targetschemaname, targettablename);

    END;

    RETURN 1;
END;
$$;



-- TOC entry 1899 (class 1255 OID 71821455)
-- Name: fn_upsertsinglenuaractor_v210(character varying, character varying, character varying, character varying, character varying); Type: FUNCTION; Schema: nuarpublication; Owner: nuar_admin
--

CREATE OR REPLACE FUNCTION nuarpublication.fn_upsertsinglenuaractor_v210(sourceschemaname character varying, sourcetablename character varying, targetschemaname character varying, targettablename character varying, sourcesystemid character varying) RETURNS integer
    LANGUAGE plpgsql
AS $$

BEGIN

    DECLARE
        columnnamesstring text;

    BEGIN

        CREATE TEMPORARY TABLE columnnames
        (
            column_name      character varying,
            ordinal_position integer
        );

        TRUNCATE columnnames;
        columnnamesstring := '';
        -- Get the column names from the target table ordered by ordinal position so that the INSERT will work
        EXECUTE FORMAT('
		INSERT INTO columnnames
		SELECT column_name, ordinal_position
		FROM information_schema.columns
		WHERE table_name = ''%s'' AND table_schema = ''%s''
		ORDER BY ordinal_position', targettablename, targetschemaname);

        -- Get the column names from the order view as a comma-separated string
        SELECT STRING_AGG(column_name, ', ') AS col INTO columnnamesstring FROM columnnames;

        DROP VIEW IF EXISTS sourcetable;
        EXECUTE format('
		CREATE TEMPORARY VIEW sourcetable AS
		SELECT %s
		FROM %s.%s
		WHERE systemid=''%s''
		OR systemid = (SELECT parentorganisationid FROM %s.%s WHERE systemid = ''%s'')', columnnamesstring,
                       sourceSchemaName, sourceTableName, sourcesystemid, sourceSchemaName, sourceTableName,
                       sourcesystemid);

        EXECUTE FORMAT('
	INSERT INTO %s.%s AS na
	SELECT * from sourcetable WHERE systemid = ''%s''
	ON CONFLICT (systemid)
	DO
	UPDATE
		SET systemid=excluded.systemid, datelastupdated=excluded.datelastupdated, systemloaddate=excluded.systemloaddate, name=excluded.name, actortype=excluded.actortype, swacode=excluded.swacode, address_buildingname=excluded.address_buildingname, address_buildingnumber=excluded.address_buildingnumber, address_locality=excluded.address_locality, address_postcode=excluded.address_postcode, address_singlelineaddress=excluded.address_singlelineaddress, address_streetname=excluded.address_streetname, address_subbuilding=excluded.address_subbuilding, address_townname=excluded.address_townname, address_uprn=excluded.address_uprn, copyrighttext=excluded.copyrighttext, corporateemaildomains=excluded.corporateemaildomains, disclaimertext=excluded.disclaimertext, displayname=excluded.displayname, organisationtype=excluded.organisationtype, parentorganisationname=excluded.parentorganisationname, websiteurl=excluded.websiteurl, parentorganisationid=excluded.parentorganisationid, reference=excluded.reference, administeredbyparent=excluded.administeredbyparent, dateoflastlifecyclestatuschange=excluded.dateoflastlifecyclestatuschange, lifecyclestatus=excluded.lifecyclestatus, nuarversion=excluded.nuarversion, shortname=excluded.shortname, standardguidance=excluded.standardguidance
		WHERE excluded.systemid = na.systemid;
	', targetschemaname, targettablename, sourcesystemid);

        -- Now do any _history records for this nuaractor record
        TRUNCATE columnnames;
        columnnamesstring := '';
        -- Get the column names from the target table ordered by ordinal position so that the INSERT will work
        EXECUTE FORMAT('
		INSERT INTO columnnames
		SELECT column_name, ordinal_position
		FROM information_schema.columns
		WHERE table_name = ''%s_history'' AND table_schema = ''%s''
		ORDER BY ordinal_position', targettablename, targetschemaname);

        -- Get the column names from the order view as a comma-separated string
        SELECT STRING_AGG(column_name, ', ') AS col INTO columnnamesstring FROM columnnames;

        DROP VIEW IF EXISTS sourcetable_history;
        EXECUTE format('
		CREATE TEMPORARY VIEW sourcetable_history AS
		SELECT %s
		FROM %s.%s_history
		WHERE originalsystemid=''%s''
	', columnnamesstring, sourceSchemaName, sourceTableName, sourcesystemid);

        EXECUTE FORMAT('
	INSERT INTO %s.%s_history AS nah
	SELECT * from sourcetable_history WHERE originalsystemid = ''%s''
	ON CONFLICT (systemid)
	DO
	UPDATE
		SET systemid=excluded.systemid, lifecyclestatus=excluded.lifecyclestatus, datelastupdated=excluded.datelastupdated, dateoflastlifecyclestatuschange=excluded.dateoflastlifecyclestatuschange, nuarversion=excluded.nuarversion, systemloaddate=excluded.systemloaddate, originalsystemid=excluded.originalsystemid, changeuserid=excluded.changeuserid, dateandtimeofchange=excluded.dateandtimeofchange, fieldschanged=excluded.fieldschanged, actortype=excluded.actortype, name=excluded.name, address_singlelineaddress=excluded.address_singlelineaddress, address_subbuilding=excluded.address_subbuilding, address_buildingname=excluded.address_buildingname, address_buildingnumber=excluded.address_buildingnumber, address_streetname=excluded.address_streetname, address_locality=excluded.address_locality, address_townname=excluded.address_townname, address_postcode=excluded.address_postcode, address_uprn=excluded.address_uprn, swacode=excluded.swacode, copyrighttext=excluded.copyrighttext, corporateemaildomains=excluded.corporateemaildomains, organisationtype=excluded.organisationtype, disclaimertext=excluded.disclaimertext, parentorganisationname=excluded.parentorganisationname, displayname=excluded.displayname, reference=excluded.reference, websiteurl=excluded.websiteurl, parentorganisationid=excluded.parentorganisationid, standardguidance=excluded.standardguidance, administeredbyparent=excluded.administeredbyparent, shortname=excluded.shortname
		WHERE excluded.systemid = nah.systemid;
	', targetschemaname, targettablename, sourcesystemid);

        -- Now do any nuarsubmissions.nuarsourcedataset records related to this nuaractor record
        TRUNCATE columnnames;
        columnnamesstring := '';
        -- Get the column names from the target table ordered by ordinal position so that the INSERT will work
        INSERT INTO columnnames
        SELECT column_name, ordinal_position
        FROM information_schema.columns
        WHERE table_name = 'nuarsourcedataset'
          AND table_schema = 'nuarsubmissions'
        ORDER BY ordinal_position;

        -- Get the column names from the order view as a comma-separated string
        SELECT STRING_AGG(column_name, ', ') AS col INTO columnnamesstring FROM columnnames;

        DROP VIEW IF EXISTS sourcetable_nuarsourcedataset;
        EXECUTE format('
		CREATE TEMPORARY VIEW sourcetable_nuarsourcedataset AS
		SELECT %s
		FROM nuarsubmissions.nuarsourcedataset
		WHERE nuaractorid_fk=''%s''
	', columnnamesstring, sourcesystemid);

        EXECUTE FORMAT('
	INSERT INTO nuarsubmissions.nuarsourcedataset AS nsd
	SELECT * from sourcetable_nuarsourcedataset WHERE nuaractorid_fk = ''%s''
	ON CONFLICT (systemid)
	DO
	UPDATE
		SET systemid=excluded.systemid, expectedrefreshperiod_period=excluded.expectedrefreshperiod_period, expectedrefreshperiod_unitoftime=excluded.expectedrefreshperiod_unitoftime, sourcefeatureclassvalue=excluded.sourcefeatureclassvalue, type=excluded.type, externalreference=excluded.externalreference, nuaractorid_fk=excluded.nuaractorid_fk
		WHERE excluded.systemid = nsd.systemid;
	', sourcesystemid);

        DROP TABLE IF EXISTS columnnames;
        DROP VIEW IF EXISTS sourcetable;
        DROP VIEW IF EXISTS sourcetable_history;
        DROP VIEW IF EXISTS sourcetable_nuarsourcedataset;

        RAISE NOTICE 'Added NUARActor Id: %', sourcesystemid;

    END;

    RETURN 1;
END;
$$;

-- Alter the table to add the new columns
ALTER TABLE nuarpublication.nuarpublicationstagestatus
    ADD COLUMN IF NOT EXISTS publisherexception TEXT,
    ADD COLUMN IF NOT EXISTS publisherstacktrace TEXT;

-- Update the function to handle the new columns
DROP FUNCTION IF EXISTS nuarpublication.fn_recordpublicationjobstatus;
CREATE OR REPLACE FUNCTION nuarpublication.fn_recordpublicationjobstatus(
    stagenamevalue character varying,
    statusvalue character varying,
    descriptionvalue character varying DEFAULT ''::character varying,
    startdatetimevalue timestamp with time zone DEFAULT now(),
    enddatetimevalue timestamp with time zone DEFAULT NULL::timestamp with time zone,
    droppingout boolean DEFAULT false,
    publisherexceptionvalue TEXT DEFAULT NULL,
    publisherstacktracevalue TEXT DEFAULT NULL
) RETURNS integer
    LANGUAGE plpgsql
AS $$
BEGIN
    DECLARE
        databasename character varying;
    BEGIN
        databasename := current_database()::character varying;

        DROP VIEW IF EXISTS inprogressjobs;

        EXECUTE FORMAT('
            CREATE TEMPORARY VIEW inprogressjobs AS
            SELECT *
            FROM nuarpublication.nuarpublicationstagestatus
            WHERE stagename = ''%s''
            AND (status = ''In Progress'' OR status = ''Duplicate'');
        ', stagenamevalue);

        IF (statusvalue = 'In Progress' AND (EXISTS (SELECT FROM inprogressjobs))) THEN
            EXECUTE FORMAT ('
                UPDATE nuarpublication.nuarpublicationstagestatus
                SET status=''Duplicate''
                WHERE stagename = ''%s'';
            ', stagenamevalue);

            DROP VIEW IF EXISTS inprogressjobs;
            RETURN 1;
        END IF;

        IF (droppingout AND statusvalue = 'Completed') THEN
            EXECUTE FORMAT ('
                UPDATE nuarpublication.nuarpublicationstagestatus
                SET status=''Completed''
                WHERE stagename = ''%s''
                AND status = ''In Progress'';
            ', stagenamevalue);

            DROP VIEW IF EXISTS inprogressjobs;
            RETURN 1;
        END IF;

        INSERT INTO nuarpublication.nuarpublicationstagestatus AS target(
            systemid, systemloaddate, stagename, status, description, databasename, startdatetime, enddatetime, publisherexception, publisherstacktrace
        ) VALUES (
                     gen_random_uuid ()::character varying, NOW(), stagenamevalue, statusvalue, descriptionvalue, databasename, startdatetimevalue, enddatetimevalue, publisherexceptionvalue, publisherstacktracevalue
                 ) ON CONFLICT (stagename)
            DO UPDATE SET
                          status = excluded.status,
                          description = excluded.description,
                          databasename = excluded.databasename,
                          startdatetime = excluded.startdatetime,
                          enddatetime = excluded.enddatetime,
                          publisherexception = excluded.publisherexception,
                          publisherstacktrace = excluded.publisherstacktrace
        WHERE target.stagename = stagenamevalue AND target.status <> 'Error';

        DROP VIEW IF EXISTS inprogressjobs;
    END;
    RETURN 1;
END;
$$;

-- New function to record publisher job exceptions
-- Drop any existing overloads of nuarpublication.fn_recordpublisherexception
DO
$$
DECLARE f record;
BEGIN
    FOR f IN
        SELECT p.oid::regprocedure::text AS funcsig
        FROM pg_proc p
                 JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'nuarpublication'
          AND p.proname = 'fn_recordpublisherexception'
    LOOP
        EXECUTE 'DROP FUNCTION IF EXISTS ' || f.funcsig;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION nuarpublication.fn_recordpublisherexception(
    stagenamevalue character varying,
    startdatetimevalue timestamp with time zone DEFAULT now(),
    enddatetimevalue timestamp with time zone DEFAULT NULL::timestamp with time zone,
    modifystatus boolean DEFAULT false,
    publisherexceptionvalue TEXT DEFAULT NULL,
    publisherstacktracevalue TEXT DEFAULT NULL
) RETURNS void
    LANGUAGE plpgsql
AS $$
BEGIN
    -- This function checks if a record exists in `nuarpublication.nuarpublicationstagestatus` for the given `stagenamevalue`.
    -- If it does not exist, it calls `fn_recordpublicationjobstatus` to create a new record with the `publisherexceptionvalue` and `publisherstacktracevalue`.
    -- If it does exist, it updates the `publisherexception` and `publisherstacktrace` columns and optionally sets the `status` to `Error` if `modifystatus` is true.
    IF NOT EXISTS (
        SELECT 1
        FROM nuarpublication.nuarpublicationstagestatus
        WHERE stagename = stagenamevalue
    ) THEN
        PERFORM nuarpublication.fn_recordpublicationjobstatus(
                stagenamevalue,
                'Error',
                'Publisher Exception',
                startdatetimevalue,
                enddatetimevalue,
                false,
                publisherexceptionvalue,
                publisherstacktracevalue
                );
    ELSE
        UPDATE nuarpublication.nuarpublicationstagestatus
        SET publisherexception = publisherexceptionvalue,
            publisherstacktrace = publisherstacktracevalue,
            enddatetime = enddatetimevalue,
            status = CASE WHEN modifystatus THEN 'Error' ELSE status END
        WHERE stagename = stagenamevalue;
    END IF;
END;
$$;

-- NEW: No longer want stagename to be unique and want to be able to run batches without getting blocked

DROP FUNCTION IF EXISTS nuarpublication.fn_seterrorifstillinprogress;
CREATE OR REPLACE FUNCTION nuarpublication.fn_seterrorifstillinprogress(stagenamevalue character varying, process_sequences integer[] DEFAULT NULL) RETURNS integer
    LANGUAGE plpgsql
AS
$$

BEGIN

    UPDATE nuarpublication.nuarpublicationstagestatus
    SET status='Error',
        description='Publication Stage did not complete - unknown error'
    WHERE stagename = stagenamevalue
      AND status = 'In Progress'
      AND ((process_sequences IS NULL AND batch IS NULL) OR batch = any (process_sequences));

    RETURN 1;
END;
$$;


ALTER FUNCTION nuarpublication.fn_seterrorifstillinprogress(stagenamevalue character varying, process_sequences integer[]) OWNER TO nuar_admin;

-- Alter the table to add the new columns
ALTER TABLE nuarpublication.nuarpublicationstagestatus
    ADD COLUMN batch    integer,
    ADD COLUMN attempts integer NOT NULL default 1;

-- Old Constraint
ALTER TABLE nuarpublication.nuarpublicationstagestatus
    DROP CONSTRAINT IF EXISTS uk_nuarpublication_nuarpublicationstagestatus_stagename;

ALTER TABLE nuarpublication.nuarpublicationstagestatus
    DROP CONSTRAINT IF EXISTS nuarpublicationstagestatusstagename_unique;

-- New joint contraint
ALTER TABLE nuarpublication.nuarpublicationstagestatus
    ADD CONSTRAINT uk_nuarpublication_nuarpublicationstagestatus_stagename_batch UNIQUE (stagename, batch);

ALTER TABLE ONLY nuarpublication.nuarpublicationstagestatus
    ADD CONSTRAINT nuarpublicationstagestatusstagenamebatch_unique UNIQUE (stagename, batch);


-- Editing existing function to add check for batch attempts. This should still support the old method neil used to run by default
DROP FUNCTION IF EXISTS nuarpublication.fn_checkifpublicationstagecanstart;
CREATE OR REPLACE FUNCTION nuarpublication.fn_checkifpublicationstagecanstart(stagenamevalue character varying,
                                                                              process_sequences integer[] DEFAULT NULL,
                                                                              remoteschemaname character varying DEFAULT ''::character varying,
                                                                              max_attempts integer DEFAULT 1) RETURNS boolean
    LANGUAGE plpgsql
AS
$$

BEGIN
    IF stagenamevalue = 'dti-holding'
    THEN
        RETURN NOT EXISTS (SELECT
                           FROM nuarpublication.nuarpublicationstagestatus
                           WHERE stagename = 'dti-holding'
                             AND status IN ('Error', 'Duplicate')
                             AND (process_sequences IS NULL OR batch = any (process_sequences))
                             AND attempts >= max_attempts);
    ELSE
        IF stagenamevalue = 'holding-standby'
        THEN
            IF remoteschemaname <> ''
            THEN
                -- We're checking the status of Holding as well
                DROP VIEW IF EXISTS holdingstatus;
                EXECUTE FORMAT('
				CREATE TEMPORARY VIEW holdingstatus AS
				SELECT stagename, status
				FROM %s.nuarpublicationstagestatus;
				', remoteschemaname);

                RETURN (
                    NOT EXISTS (SELECT
                                FROM nuarpublication.nuarpublicationstagestatus
                                WHERE stagename = 'holding-standby'
                                  AND (status = 'Error' OR status = 'Duplicate')
                                  AND (process_sequences IS NULL OR batch = ANY (process_sequences))
                                  AND attempts >= max_attempts)
                        AND
                    NOT EXISTS (SELECT
                                FROM nuarpublication.nuarpublicationstagestatus
                                WHERE stagename = 'live-standby'
                                  AND (status = 'Error' OR status = 'In Progress' OR status = 'Duplicate')
                                  AND (process_sequences IS NULL OR batch = ANY (process_sequences))
                                  AND attempts >= max_attempts)
                        AND
                    NOT EXISTS (SELECT
                                FROM holdingstatus
                                WHERE stagename = 'dti-holding'
                                  AND (status = 'Error' OR status = 'In Progress' OR status = 'Duplicate')
                                  AND (process_sequences IS NULL OR batch = ANY (process_sequences))
                                  AND attempts >= max_attempts)
                    );
            ELSE
                -- We're only checking the status of Standby
                RETURN (
                    NOT EXISTS (SELECT
                                FROM nuarpublication.nuarpublicationstagestatus
                                WHERE stagename = 'holding-standby'
                                  AND (status = 'Error' OR status = 'Duplicate')
                                  AND (process_sequences IS NULL OR batch = ANY (process_sequences))
                                  AND attempts >= max_attempts)
                        AND
                    NOT EXISTS (SELECT
                                FROM nuarpublication.nuarpublicationstagestatus
                                WHERE stagename = 'live-standby'
                                  AND (status = 'Error' OR status = 'In Progress' OR status = 'Duplicate')
                                  AND (process_sequences IS NULL OR batch = ANY (process_sequences))
                                  AND attempts >= max_attempts)
                    );
            END IF;
        ELSE
            IF stagenamevalue = 'live-standby'
            THEN
                RETURN (
                    NOT EXISTS (SELECT
                                FROM nuarpublication.nuarpublicationstagestatus
                                WHERE stagename = 'holding-standby'
                                  AND (status = 'Error' OR status = 'In Progress' OR status = 'Duplicate')
                                  AND (process_sequences IS NULL OR batch = ANY (process_sequences))
                                  AND attempts >= max_attempts)
                        AND
                    NOT EXISTS (SELECT
                                FROM nuarpublication.nuarpublicationstagestatus
                                WHERE stagename = 'live-standby'
                                  AND (status = 'Error' OR status = 'Duplicate')
                                  AND (process_sequences IS NULL OR batch = ANY (process_sequences))
                                  AND attempts >= max_attempts)
                    );
            ELSE
                RETURN false;
            END IF;
        END IF;
    END IF;
END;
$$;


ALTER FUNCTION nuarpublication.fn_checkifpublicationstagecanstart(stagenamevalue character varying, process_sequences integer[], remoteschemaname character varying,
    max_attempts integer) OWNER TO nuar_admin;


-- Editing previous script to support new batch column

-- Alter the table to add the new columns
DROP FUNCTION IF EXISTS nuarpublication.fn_recordpublicationjobstatus;
CREATE OR REPLACE FUNCTION nuarpublication.fn_recordpublicationjobstatus(
    stagenamevalue character varying,
    statusvalue character varying,
    process_sequences integer[] DEFAULT '{-1}'::integer[],
    descriptionvalue character varying DEFAULT ''::character varying,
    startdatetimevalue timestamp with time zone DEFAULT now(),
    enddatetimevalue timestamp with time zone DEFAULT NULL::timestamp with time zone,
    droppingout boolean DEFAULT false,
    publisherexceptionvalue TEXT DEFAULT NULL,
    publisherstacktracevalue TEXT DEFAULT NULL
) RETURNS integer
    LANGUAGE plpgsql
AS
$$
BEGIN
    DECLARE
        databasename character varying;
        batch_arg    integer;
    BEGIN
        databasename := current_database()::character varying;

        DROP VIEW IF EXISTS inprogressjobs;

        FOREACH batch_arg IN ARRAY process_sequences
            LOOP
                EXECUTE FORMAT('
                    CREATE TEMPORARY VIEW inprogressjobs AS
                    SELECT *
                    FROM nuarpublication.nuarpublicationstagestatus
                    WHERE stagename = ''%s''
                      AND batch = %L
                      AND (status = ''In Progress'' OR status = ''Duplicate'');
                ', stagenamevalue, batch_arg);

                IF (statusvalue = 'In Progress' AND (EXISTS (SELECT FROM inprogressjobs))) THEN
                    EXECUTE FORMAT('
                    UPDATE nuarpublication.nuarpublicationstagestatus
                    SET status=''Duplicate''
                    WHERE stagename = ''%s''
                      AND batch = %L;
                ', stagenamevalue);

                    DROP VIEW IF EXISTS inprogressjobs;
                    RETURN 1;
                END IF;

                IF (droppingout AND statusvalue = 'Completed') THEN
                    EXECUTE FORMAT('
                    UPDATE nuarpublication.nuarpublicationstagestatus
                    SET status=''Completed''
                    WHERE stagename = ''%s''
                      AND batch = %L
                      AND status = ''In Progress'';
                ', stagenamevalue);

                    DROP VIEW IF EXISTS inprogressjobs;
                    RETURN 1;
                END IF;

                INSERT INTO nuarpublication.nuarpublicationstagestatus AS target(systemid, systemloaddate, stagename, batch, status,
                                                                                 description,
                                                                                 databasename, startdatetime, enddatetime,
                                                                                 publisherexception, publisherstacktrace)
                VALUES (gen_random_uuid()::character varying, NOW(), stagenamevalue, batch_arg, statusvalue, descriptionvalue, databasename,
                        startdatetimevalue, enddatetimevalue, publisherexceptionvalue, publisherstacktracevalue)
                ON CONFLICT (stagename, batch)
                    DO UPDATE SET status              = excluded.status,
                                  description         = excluded.description,
                                  databasename        = excluded.databasename,
                                  startdatetime       = excluded.startdatetime,
                                  enddatetime         = excluded.enddatetime,
                                  publisherexception  = excluded.publisherexception,
                                  publisherstacktrace = excluded.publisherstacktrace
                WHERE target.stagename = stagenamevalue
                  AND target.batch = batch_arg
                  AND target.status <> 'Error';

                DROP VIEW IF EXISTS inprogressjobs;
                RETURN 1;
            END LOOP;
    END;
END;
$$;

DROP FUNCTION IF EXISTS nuarpublication.fn_recordpublisherexception;
CREATE OR REPLACE FUNCTION nuarpublication.fn_recordpublisherexception(
    stagenamevalue character varying,
    batch_arg integer DEFAULT NULL,
    startdatetimevalue timestamp with time zone DEFAULT now(),
    enddatetimevalue timestamp with time zone DEFAULT NULL::timestamp with time zone,
    modifystatus boolean DEFAULT false,
    publisherexceptionvalue TEXT DEFAULT NULL,
    publisherstacktracevalue TEXT DEFAULT NULL
) RETURNS void
    LANGUAGE plpgsql
AS
$$
BEGIN
    -- This function checks if a record exists in `nuarpublication.nuarpublicationstagestatus` for the given `stagenamevalue`.
    -- If it does not exist, it calls `fn_recordpublicationjobstatus` to create a new record with the `publisherexceptionvalue` and `publisherstacktracevalue`.
    -- If it does exist, it updates the `publisherexception` and `publisherstacktrace` columns and optionally sets the `status` to `Error` if `modifystatus` is true.
    IF NOT EXISTS (SELECT 1
                   FROM nuarpublication.nuarpublicationstagestatus
                   WHERE stagename = stagenamevalue
                     AND batch = batch_arg) THEN
        PERFORM nuarpublication.fn_recordpublicationjobstatus(
                stagenamevalue,
                'Error',
                ARRAY [batch_arg],
                'Publisher Exception',
                startdatetimevalue,
                enddatetimevalue,
                false,
                publisherexceptionvalue,
                publisherstacktracevalue
                );
    ELSE
        UPDATE nuarpublication.nuarpublicationstagestatus
        SET publisherexception  = publisherexceptionvalue,
            publisherstacktrace = publisherstacktracevalue,
            enddatetime         = enddatetimevalue,
            status              = CASE WHEN modifystatus THEN 'Error' ELSE status END
        WHERE stagename = stagenamevalue
          AND batch = batch_arg;
    END IF;
END;
$$;

ALTER FUNCTION nuarpublication.fn_recordpublisherexception(
    stagenamevalue character varying,
    batch_arg integer,
    startdatetimevalue timestamp with time zone,
    enddatetimevalue timestamp with time zone,
    modifystatus boolean,
    publisherexceptionvalue TEXT,
    publisherstacktracevalue TEXT
    ) OWNER TO nuar_admin;

-- Validation Function

DROP FUNCTION IF EXISTS nuarpublication.fn_checkvalidationresults;
CREATE OR REPLACE FUNCTION nuarpublication.fn_checkvalidationresults(stagenamevalue character varying, process_sequences integer[] DEFAULT NULL) RETURNS boolean
    LANGUAGE plpgsql
AS
$$

BEGIN

    -- Set an error state in nuarpublicationstagestatus depending on the validation requirements for the current phase
    IF stagenamevalue = 'dti-holding'
    THEN
        IF EXISTS (SELECT FROM nuarvalidation.filterorganisationdifferences)
        THEN
            PERFORM nuarpublication.fn_recordpublicationjobstatus(
                    stagenamevalue => stagenamevalue,
                    process_sequences => process_sequences,
                    statusvalue => 'Error',
                    descriptionvalue => 'There are differences between the Holding NUARActor table and the organisation filter table',
                    startdatetimevalue => NOW(),
                    enddatetimevalue => NOW()
                    );
            RETURN false;
        END IF;

        IF EXISTS (SELECT
                   FROM nuarvalidation.missingsubmissions
                   WHERE (
                             (sourcesubmissionstatus = 'Completed')
                                 OR
                             (sourcesubmissionstatus IS NULL)
                             ))
        THEN
            PERFORM nuarpublication.fn_recordpublicationjobstatus(
                    stagenamevalue => stagenamevalue,
                    process_sequences => process_sequences,
                    statusvalue => 'Error',
                    descriptionvalue => 'There are valid Submissions missing from Holding compared to DTI',
                    startdatetimevalue => NOW(),
                    enddatetimevalue => NOW()
                    );
            RETURN false;
        END IF;

        IF EXISTS (SELECT FROM nuarvalidation.recordcountdifferences)
        THEN
            PERFORM nuarpublication.fn_recordpublicationjobstatus(
                    stagenamevalue => stagenamevalue,
                    process_sequences => process_sequences,
                    statusvalue => 'Error',
                    descriptionvalue => 'There are record count differences between Holding and DTI',
                    startdatetimevalue => NOW(),
                    enddatetimevalue => NOW()
                    );
            RETURN false;
        END IF;
    ELSE
        IF stagenamevalue = 'holding-standby'
        THEN
            IF EXISTS (SELECT
                       FROM nuarvalidation.filterorganisationdifferences
                       WHERE status LIKE 'In Filter List not in Target NUARActor Table')
            THEN
                PERFORM nuarpublication.fn_recordpublicationjobstatus(
                        stagenamevalue => stagenamevalue,
                        process_sequences => process_sequences,
                        statusvalue => 'Error',
                        descriptionvalue => 'There are records in the organisation filter table that are not in the Standby NUARActor table',
                        startdatetimevalue => NOW(),
                        enddatetimevalue => NOW()
                        );
                RETURN false;
            END IF;

            IF EXISTS (SELECT FROM nuarvalidation.missingsubmissions)
            THEN
                PERFORM nuarpublication.fn_recordpublicationjobstatus(
                        stagenamevalue => stagenamevalue,
                        process_sequences => process_sequences,
                        statusvalue => 'Error',
                        descriptionvalue => 'There are publishable Submissions missing from Standby compared to Holding',
                        startdatetimevalue => NOW(),
                        enddatetimevalue => NOW()
                        );
                RETURN false;
            END IF;

            IF EXISTS (SELECT
                       FROM nuarvalidation.recordcountdifferences
                       WHERE sourcetablename <> 'relationship_actortoservicearea')
            THEN
                PERFORM nuarpublication.fn_recordpublicationjobstatus(
                        stagenamevalue => stagenamevalue,
                        process_sequences => process_sequences,
                        statusvalue => 'Error',
                        descriptionvalue => 'There are record count differences between Standby and Holding',
                        startdatetimevalue => NOW(),
                        enddatetimevalue => NOW()
                        );
                RETURN false;
            END IF;
        ELSE
            IF stagenamevalue = 'standby-live'
            THEN
                -- This is checking Standby vs. Live after doing Holding-Standby
                IF EXISTS (SELECT
                           FROM nuarvalidation.recordcounts
                           WHERE targetrecordcount < sourcerecordcount
                             AND ((sourcerecordcount - targetrecordcount) >= (sourcerecordcount * 0.1)))
                THEN
                    PERFORM nuarpublication.fn_recordpublicationjobstatus(
                            stagenamevalue => stagenamevalue,
                            process_sequences => process_sequences,
                            statusvalue => 'Error',
                            descriptionvalue => 'There is a significant drop in some record counts between new Standby and existing Live',
                            startdatetimevalue => NOW(),
                            enddatetimevalue => NOW()
                            );
                    RETURN false;
                END IF;
            ELSE
                IF stagenamevalue = 'live-standby'
                THEN
                    IF EXISTS (SELECT
                               FROM nuarvalidation.filterorganisationdifferences
                               WHERE status LIKE 'In Filter List not in Target NUARActor Table')
                    THEN
                        PERFORM nuarpublication.fn_recordpublicationjobstatus(
                                stagenamevalue => stagenamevalue,
                                process_sequences => process_sequences,
                                statusvalue => 'Error',
                                descriptionvalue => 'There are records in the organisation filter table that are not in the Standby or Live NUARActor table after Live-Standby replication',
                                startdatetimevalue => NOW(),
                                enddatetimevalue => NOW()
                                );
                        RETURN false;
                    END IF;

                    IF EXISTS (SELECT FROM nuarvalidation.missingsubmissions)
                    THEN
                        PERFORM nuarpublication.fn_recordpublicationjobstatus(
                                stagenamevalue => stagenamevalue,
                                process_sequences => process_sequences,
                                statusvalue => 'Error',
                                descriptionvalue => 'There are publishable Submissions missing from Standby compared to Live after Live-Standby replication',
                                startdatetimevalue => NOW(),
                                enddatetimevalue => NOW()
                                );
                        RETURN false;
                    END IF;

                    IF EXISTS (SELECT FROM nuarvalidation.recordcountdifferences)
                    THEN
                        PERFORM nuarpublication.fn_recordpublicationjobstatus(
                                stagenamevalue => stagenamevalue,
                                process_sequences => process_sequences,
                                statusvalue => 'Error',
                                descriptionvalue => 'There are record count differences between Standby and Live after Live-Standby replication',
                                startdatetimevalue => NOW(),
                                enddatetimevalue => NOW()
                                );
                        RETURN false;
                    END IF;
                END IF;
            END IF;
        END IF;
    END IF;

    RETURN true;

END;
$$;


ALTER FUNCTION nuarpublication.fn_checkvalidationresults(stagenamevalue character varying, process_sequences integer[]) OWNER TO nuar_admin;

-- Pull Functions

DO
$$
DECLARE f record;
BEGIN
  FOR f IN
    SELECT p.oid::regprocedure::text AS funcsig
    FROM pg_proc p
         JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname = 'nuarpublication'
      AND p.proname = 'fn_dtitoholding_v211'
  LOOP
    EXECUTE 'DROP FUNCTION IF EXISTS ' || f.funcsig;
  END LOOP;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION nuarpublication.fn_dtitoholding_v211(sourceschemasuffix character varying DEFAULT 'dti'::character varying,
                                                                fdwservername character varying DEFAULT 'dti_source'::character varying,
                                                                fdwdbhostname character varying DEFAULT ''::character varying,
                                                                fdwdbport character varying DEFAULT ''::character varying,
                                                                fdwdbname character varying DEFAULT ''::character varying,
                                                                fdwdbusername character varying DEFAULT ''::character varying,
                                                                fdwdbpassword character varying DEFAULT ''::character varying,
                                                                process_sequences integer[] DEFAULT '{}'::integer[],
                                                                createanddropfdwserver boolean DEFAULT false,
                                                                createanddropfdwschemas boolean DEFAULT false,
                                                                runvalidation boolean DEFAULT false,
                                                                max_attempts integer DEFAULT 1) RETURNS character varying
    LANGUAGE plpgsql
AS
$$

DECLARE
    publicationstage                  character varying;
    organisationfilteridsarray        character varying[];
    submissionidsarray                character varying[];
    nuaractoridsarray                 character varying[];
    backdropgeometrytablesarray       character varying[];
    existingholdingsubmissionidsarray character varying[];
    noerrorstateset                   boolean;
    publicationeventid                character varying;

BEGIN

    DROP VIEW IF EXISTS submissionids;
    DROP VIEW IF EXISTS nuaractorids;
    DROP VIEW IF EXISTS newsubmissions;
    DROP VIEW IF EXISTS existingholdingsubmissions;
    DROP VIEW IF EXISTS organisationfilterids;
    DROP VIEW IF EXISTS geometrytablenames;
    DROP VIEW IF EXISTS relationshiptablenames;
    DROP VIEW IF EXISTS backdropgeometrytablenames;


-- First we see if we are allowed to start
    IF (SELECT nuarpublication.fn_checkifpublicationstagecanstart(stagenamevalue => 'dti-holding',
                                                                  process_sequences => process_sequences,
                                                                  max_attempts => max_attempts))
    THEN
        publicationstage := 'DTI to Holding';
        noerrorstateset := true;
    ELSE
        RAISE NOTICE 'Dropping out - unable to run given current publication status';

        PERFORM nuarpublication.fn_logpublicationmessage(
                publicationstage => 'DTI to Holding',
                messagetext => 'Dropping out - unable to run given current publication status',
                logdatetime => NOW()
                );

        -- Set our status to Completed
        PERFORM nuarpublication.fn_recordpublicationjobstatus(
                stagenamevalue => 'dti-holding',
                process_sequences => process_sequences,
                statusvalue => 'Completed',
                startdatetimevalue => NOW(),
                enddatetimevalue => NOW(),
                droppingout => true
                );

        RETURN '';
    END IF;

    IF (createanddropfdwserver)
    THEN
        -- Create the FDW Server and User Mapping
        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwserver(
			servername => ''%s'',
			fdwname => ''postgres_fdw'',
			hostname => ''%s'',
			port => ''%s'',
			databasename => ''%s'',
			username => ''%s'',
			userpassword => ''%s'');
	', fdwservername, fdwdbhostname, fdwdbport, fdwdbname, fdwdbusername, fdwdbpassword);
    END IF;

    IF (createanddropfdwschemas)
    THEN

        -- Create and import the FDW schemas
        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarcodelists_datamanagement_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarcodelists_datamanagement'');
	', sourceschemasuffix, fdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarcodelists_platform_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarcodelists_platform'');
	', sourceschemasuffix, fdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarcodelists_transformation_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarcodelists_transformation'');
	', sourceschemasuffix, fdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarcustombackdrop_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarcustombackdrop'');
	', sourceschemasuffix, fdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuardata_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuardata'');
	', sourceschemasuffix, fdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarorganisations_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarorganisations'');
	', sourceschemasuffix, fdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarsubmissions_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarsubmissions'');
	', sourceschemasuffix, fdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarversion_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarversion'');
	', sourceschemasuffix, fdwservername);

    END IF;

    -- For all the Submissions that we currently hold, do an Upsert from Holding to make sure that any status updates etc. are captured
    CREATE TEMPORARY VIEW existingholdingsubmissions AS
    SELECT systemid
    FROM nuarsubmissions.nuarsubmissionevent;

    IF EXISTS (SELECT * FROM existingholdingsubmissions)
    THEN
        SELECT ARRAY_AGG(systemid)
        FROM existingholdingsubmissions
        INTO existingholdingsubmissionidsarray;

        PERFORM nuarpublication.fn_upsertnuarsubmissionevents_bulk_v21x(
                sourceschemaname => 'nuarsubmissions_dti',
                sourcetablename => 'nuarsubmissionevent',
                targetschemaname => 'nuarsubmissions',
                targettablename => 'nuarsubmissionevent',
                sourcesystemids => existingholdingsubmissionidsarray);

        PERFORM nuarpublication.fn_logpublicationmessage(
                publicationstage => publicationstage,
                messagetext => 'Updated existing Submissions in Holding from DTI',
                logdatetime => NOW()
                );
    END IF;

-- We pick up the list of NUARActor systemid values (if any) which will constrain the data that we copy over
    EXECUTE FORMAT('
CREATE TEMPORARY VIEW organisationfilterids AS
SELECT filteredorganisationid
FROM nuarpublication.fn_getorganisationfilterids(
	schemaname => ''nuarpublication'',
	process_sequences => ''%s'');
', process_sequences);

    IF NOT EXISTS (SELECT * FROM organisationfilterids)
    THEN
        --CREATE TEMPORARY VIEW newsubmissions AS SELECT submissionsystemid, nuaractorsystemid FROM nuarpublication.fn_getnewsubmissions(sourceschemaname =>'nuarsubmissions_dti', sourcetablename => 'nuarsubmissionevent', targetschemaname => 'nuarsubmissions', targettablename => 'nuarsubmissionevent', submissionresult => 'Success', submissionstatus => 'Completed');
        CREATE TEMPORARY VIEW newsubmissions AS
        SELECT submissionsystemid, nuaractorsystemid
        FROM
            nuarpublication.fn_getallnewsubmissions(
                    sourceschemaname => 'nuarsubmissions_dti',
                    sourcetablename => 'nuarsubmissionevent',
                    targetschemaname => 'nuarsubmissions',
                    targettablename => 'nuarsubmissionevent');
    ELSE
        --CREATE TEMPORARY VIEW newsubmissions AS SELECT submissionsystemid, nuaractorsystemid FROM nuarpublication.fn_getnewsubmissionsfilteredbyorganisation(sourceschemaname => 'nuarsubmissions_dti', sourcetablename => 'nuarsubmissionevent', targetschemaname => 'nuarsubmissions', targettablename => 'nuarsubmissionevent', submissionresult => 'Success', submissionstatus => 'Completed', organisationids => (SELECT ARRAY_AGG(filteredorganisationid) FROM organisationfilterids));
        CREATE TEMPORARY VIEW newsubmissions AS
        SELECT submissionsystemid, nuaractorsystemid
        FROM
            nuarpublication.fn_getallnewsubmissionsfilteredbyorganisation(
                    sourceschemaname => 'nuarsubmissions_dti',
                    sourcetablename => 'nuarsubmissionevent',
                    targetschemaname => 'nuarsubmissions',
                    targettablename => 'nuarsubmissionevent',
                    organisationids => (SELECT ARRAY_AGG(filteredorganisationid) FROM organisationfilterids));

        -- If we're filtering by organisation id, we should make sure that all these organisations are in the target nuaractor table
        -- Create an array of the filter organisation ids
        SELECT ARRAY_AGG(filteredorganisationid)
        FROM organisationfilterids
        INTO organisationfilteridsarray;

        -- Do an upsert for all these organisation ids into the target nuaractor table
        PERFORM nuarpublication.fn_upsertnuaractors_v210(
                sourceschemaname => 'nuarorganisations_dti',
                sourcetablename => 'nuaractor',
                targetschemaname => 'nuarorganisations',
                targettablename => 'nuaractor',
                sourcesystemids => organisationfilteridsarray);

        PERFORM nuarpublication.fn_logpublicationmessage(
                publicationstage => publicationstage,
                messagetext => 'Upserted NUARActor records for filter organisation ids',
                logdatetime => NOW()
                );

    END IF;

    CREATE TEMPORARY VIEW submissionids AS
    SELECT DISTINCT submissionsystemid
    FROM newsubmissions;

    CREATE TEMPORARY VIEW nuaractorids AS
    SELECT DISTINCT nuaractorsystemid
    FROM newsubmissions;

    -- Is either the list of Submissions or the list of NUARActors empty?
-- If so, we just drop out
    IF (
        NOT EXISTS (SELECT FROM submissionids) OR
        NOT EXISTS (SELECT FROM nuaractorids)
        )
    THEN
        RAISE NOTICE 'No submissions to bring into Holding';
        PERFORM nuarpublication.fn_logpublicationmessage(
                publicationstage => publicationstage,
                messagetext => 'No submissions to bring into Holding',
                logdatetime => NOW()
                );

        IF (runvalidation)
        THEN
            PERFORM nuarpublication.fn_recordcountvalidationrun_v211(
                    runname => 'dti-holding',
                    sourceschemasuffix => 'dti',
                    createanddropfdwserver => false,
                    createanddropfdwschemas => false,
                    publishablesubmissionsonly => false -- Set true for checking standby against holding, otherwise false
                    );

            -- Check the results and set an Error State if appropriate
            IF (SELECT nuarpublication.fn_checkvalidationresults(stagenamevalue => 'dti-holding', process_sequences => process_sequences))
            THEN
                noerrorstateset := true;
            ELSE
                noerrorstateset := false;
            END IF;

        END IF;

        IF (createanddropfdwschemas)
        THEN
            -- Drop the FDW schemas
            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarcodelists_datamanagement_%s'',
				shouldcascade => true);
		', sourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarcodelists_platform_%s'',
				shouldcascade => true);
		', sourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarcodelists_transformation_%s'',
				shouldcascade => true);
		', sourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarcustombackdrop_%s'',
				shouldcascade => true);
		', sourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuardata_%s'',
				shouldcascade => true);
		', sourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarorganisations_%s'',
				shouldcascade => true);
		', sourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarsubmissions_%s'',
				shouldcascade => true);
		', sourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarversion_%s'',
				shouldcascade => true);
		', sourceschemasuffix);

        END IF;

        IF (createanddropfdwserver)
        THEN

            -- Drop the FDW Server
            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropfdwserver(
				servername => ''%s'');
		', fdwservername);

        END IF;

        IF (noerrorstateset)
        THEN
            -- The last thing we do is set our status to Completed
            PERFORM nuarpublication.fn_recordpublicationjobstatus(
                    stagenamevalue => 'dti-holding',
                    process_sequences => process_sequences,
                    statusvalue => 'Completed',
                    startdatetimevalue => NOW(),
                    enddatetimevalue => NOW()
                    );
        END IF;

        RETURN '';
    END IF;

    SELECT ARRAY_AGG(submissionsystemid)
    FROM submissionids
    INTO submissionidsarray;

    SELECT ARRAY_AGG(nuaractorsystemid)
    FROM nuaractorids
    INTO nuaractoridsarray;

    -- We get a list of table names from the current database (HOLDING).
-- We are assuming that table names in the source and the destination match.
-- Get tables with geometry
    CREATE TEMPORARY VIEW geometrytablenames AS
    SELECT table_name::character varying as tablenamestring
    FROM information_schema.columns
    WHERE column_name = 'geometry'
      AND table_schema = 'nuardata';

    -- Get relationship_tables from nuardata
    CREATE TEMPORARY VIEW relationshiptablenames AS
    SELECT table_name::character varying as tablenamestring
    FROM information_schema.tables
    WHERE (
        table_name LIKE 'relationship_%'
            OR
        table_name = 'nuarsubordinatenetworkdefinition'
        )
      AND table_schema = 'nuardata';

    -- Get backdrop tables with geometry
    CREATE TEMPORARY VIEW backdropgeometrytablenames AS
    SELECT table_name::character varying as tablenamestring
    FROM information_schema.columns
    WHERE column_name = 'geometry'
      AND table_schema = 'nuarcustombackdrop';

-- Upsert NUARActor records
    PERFORM nuarpublication.fn_upsertnuaractors_v210(
            sourceschemaname => 'nuarorganisations_dti',
            sourcetablename => 'nuaractor',
            targetschemaname => 'nuarorganisations',
            targettablename => 'nuaractor',
            sourcesystemids => nuaractoridsarray);

    PERFORM nuarpublication.fn_logpublicationmessage(
            publicationstage => publicationstage,
            messagetext => 'Upserted NUARActor records',
            logdatetime => NOW()
            );

-- Upsert NUARSubmissionEvent records
    PERFORM nuarpublication.fn_upsertnuarsubmissionevents_v21x(
            sourceschemaname => 'nuarsubmissions_dti',
            sourcetablename => 'nuarsubmissionevent',
            targetschemaname => 'nuarsubmissions',
            targettablename => 'nuarsubmissionevent',
            sourcesystemids => submissionidsarray);

    PERFORM nuarpublication.fn_logpublicationmessage(
            publicationstage => publicationstage,
            messagetext => 'Upserted NUARSubmissionEvent records',
            logdatetime => NOW()
            );

-- Upsert NUARActor related records for these Submissions
    PERFORM nuarpublication.fn_upsertnuaractorrelateddata_v211(
            sourceschemaname => 'nuarorganisations_dti',
            sourcecontactdetailstablename => 'nuarcontactdetails',
            sourceactivityproximityruletablename => 'nuaractivityproximityrule',
            sourcecontactdetailsrelationshiptablename => 'relationship_actortocontactdetails',
            sourceactivityproximityrulerelationshiptablename => 'relationship_actortorule',
            sourceservicearearelationshiptablename => 'relationship_actortoservicearea',
            sourcedomainrelationshiptablename => 'relationship_actortosubmissiondomain',
            sourceserviceproviderrelationshiptablename => 'relationship_serviceprovidertoorganisation',
            targetschemaname => 'nuarorganisations',
            targetcontactdetailstablename => 'nuarcontactdetails',
            targetactivityproximityruletablename => 'nuaractivityproximityrule',
            targetcontactdetailsrelationshiptablename => 'relationship_actortocontactdetails',
            targetactivityproximityrulerelationshiptablename => 'relationship_actortorule',
            targetservicearearelationshiptablename => 'relationship_actortoservicearea',
            targetdomainrelationshiptablename => 'relationship_actortosubmissiondomain',
            targetserviceproviderrelationshiptablename => 'relationship_serviceprovidertoorganisation',
            sourcesystemids => submissionidsarray);

    PERFORM nuarpublication.fn_logpublicationmessage(
            publicationstage => publicationstage,
            messagetext => 'Upserted NUARActor related records',
            logdatetime => NOW()
            );

-- Upsert NUARActor DDA and DIS related records for these Organisations
    PERFORM nuarpublication.fn_upsertnuaractorlegalagreementsdata_v21x(
            sourceschemaname => 'nuarorganisations_dti',
            sourcenuaractortablename => 'nuaractor',
            sourceddatablename => 'nuardda',
            sourcedistablename => 'nuardis',
            sourceddarelationshiptablename => 'relationship_actortodda',
            sourcedisrelationshiptablename => 'relationship_actortodis',
            targetschemaname => 'nuarorganisations',
            targetddatablename => 'nuardda',
            targetdistablename => 'nuardis',
            targetddarelationshiptablename => 'relationship_actortodda',
            targetdisrelationshiptablename => 'relationship_actortodis',
            sourcesystemids => nuaractoridsarray);

    PERFORM nuarpublication.fn_logpublicationmessage(
            publicationstage => publicationstage,
            messagetext => 'Upserted NUARActor DDA and DIS related records',
            logdatetime => NOW()
            );

-- COPY Geometry tables
    PERFORM nuarpublication.fn_copysubmissiontablesfromsource(
            sourceschemaname => 'nuardata_dti',
            targetschemaname => 'nuardata',
            submissionids => submissionidsarray,
            tablenames => (SELECT ARRAY_AGG(tablenamestring) FROM geometrytablenames),
            publicationstage => publicationstage);

-- COPY Relationship tables
    PERFORM nuarpublication.fn_copysubmissiontablesfromsource(
            sourceschemaname => 'nuardata_dti',
            targetschemaname => 'nuardata',
            submissionids => submissionidsarray,
            tablenames => (SELECT ARRAY_AGG(tablenamestring) FROM relationshiptablenames),
            publicationstage => publicationstage);

-- COPY Backdrop Geometry tables
    SELECT ARRAY_AGG(tablenamestring) FROM backdropgeometrytablenames INTO backdropgeometrytablesarray;
    PERFORM nuarpublication.fn_copysubmissiontablesfromsource(
            sourceschemaname => 'nuarcustombackdrop_dti',
            targetschemaname => 'nuarcustombackdrop',
            submissionids => submissionidsarray,
            tablenames => backdropgeometrytablesarray,
            publicationstage => publicationstage);

-- COPY NUARVersion tables
    PERFORM nuarpublication.fn_upsertnuarversion(
            sourceschemaname => 'nuarversion_dti',
            targetschemaname => 'nuarversion');

    PERFORM nuarpublication.fn_logpublicationmessage(
            publicationstage => publicationstage,
            messagetext => 'Upserted NUARVersion records',
            logdatetime => NOW()
            );

-- COPY the tables in the different Codelist schemas
    PERFORM nuarpublication.fn_upsertnuarcodelists(
            sourceschemaname => 'nuarcodelists_datamanagement_dti',
            targetschemaname => 'nuarcodelists_datamanagement');

    PERFORM nuarpublication.fn_upsertnuarcodelists(
            sourceschemaname => 'nuarcodelists_platform_dti',
            targetschemaname => 'nuarcodelists_platform');

    PERFORM nuarpublication.fn_upsertnuarcodelists(
            sourceschemaname => 'nuarcodelists_transformation_dti',
            targetschemaname => 'nuarcodelists_transformation');

    PERFORM nuarpublication.fn_logpublicationmessage(
            publicationstage => publicationstage,
            messagetext => 'Upserted NUARCodelist records',
            logdatetime => NOW()
            );

    -- Now we've done all our copies, we need to process any "No Change" submissions.
-- Each of these specifies a "predecessor" submission.
-- We update the datelastupdated value to the dateofsubmission of the No Change submission
-- for records in every table which have a submissioneventid_fk value equal to the predecessor Submission
    PERFORM nuarpublication.fn_processnochangesubmissionevents_v211(
            submissionsschemaname => 'nuarsubmissions',
            submissionstablename => 'nuarsubmissionevent',
            submissionids => submissionidsarray
            );

-- Now we need to update the NUARPublicationEvent table
    SELECT nuarpublication.fn_recordpublicationdetails(
                   targetschemaname => 'nuarpublication',
                   publicationeventtablename => 'nuarpublicationevent',
                   publicationsubmissionrelationshiptablename => 'relationship_publicationtosubmission',
                   datamodelversion => '2.1.1',
                   submissionids => submissionidsarray)
    INTO publicationeventid;

    PERFORM nuarpublication.fn_logpublicationmessage(
            publicationstage => publicationstage,
            messagetext => 'Updated NUARPublicationEvent records',
            logdatetime => NOW()
            );

    DROP VIEW IF EXISTS submissionids;
    DROP VIEW IF EXISTS nuaractorids;
    DROP VIEW IF EXISTS newsubmissions;
    DROP VIEW IF EXISTS existingholdingsubmissions;
    DROP VIEW IF EXISTS organisationfilterids;
    DROP VIEW IF EXISTS geometrytablenames;
    DROP VIEW IF EXISTS relationshiptablenames;
    DROP VIEW IF EXISTS backdropgeometrytablenames;

    IF (runvalidation)
    THEN
        PERFORM nuarpublication.fn_recordcountvalidationrun_v211(
                runname => 'dti-holding',
                sourceschemasuffix => 'dti',
                createanddropfdwserver => false,
                createanddropfdwschemas => false,
                publishablesubmissionsonly => false -- Set true for checking standby against holding, otherwise false
                );

        -- Check the results and set an Error State if appropriate
        IF (SELECT nuarpublication.fn_checkvalidationresults(stagenamevalue => 'dti-holding', process_sequences => process_sequences))
        THEN
            noerrorstateset := true;
        ELSE
            noerrorstateset := false;
        END IF;

    END IF;

    IF (createanddropfdwschemas)
    THEN
        -- Drop the FDW schemas
        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarcodelists_datamanagement_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarcodelists_platform_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarcodelists_transformation_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarcustombackdrop_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuardata_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarorganisations_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarsubmissions_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarversion_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

    END IF;

    IF (createanddropfdwserver)
    THEN

        -- Drop the FDW Server
        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropfdwserver(
			servername => ''%s'');
	', fdwservername);

    END IF;

    IF (noerrorstateset)
    THEN
        -- The last thing we do is set our status to Completed
        PERFORM nuarpublication.fn_recordpublicationjobstatus(
                stagenamevalue => 'dti-holding',
                process_sequences => process_sequences,
                statusvalue => 'Completed',
                startdatetimevalue => NOW(),
                enddatetimevalue => NOW()
                );
    END IF;

    RETURN publicationeventid;
END;
$$;


ALTER FUNCTION nuarpublication.fn_dtitoholding_v211(sourceschemasuffix character varying, fdwservername character varying, fdwdbhostname character varying, fdwdbport character varying, fdwdbname character varying, fdwdbusername character varying, fdwdbpassword character varying, process_sequences integer[], createanddropfdwserver boolean, createanddropfdwschemas boolean, runvalidation boolean, max_attempts integer) OWNER TO nuar_admin;


-- Standby

DROP FUNCTION IF EXISTS nuarpublication.fn_holdingtostandby;
CREATE OR REPLACE FUNCTION nuarpublication.fn_holdingtostandby(sourceschemasuffix character varying DEFAULT 'holding'::character varying,
                                                               fdwservername character varying DEFAULT 'holding_source'::character varying,
                                                               livesourceschemasuffix character varying DEFAULT 'live'::character varying,
                                                               livefdwservername character varying DEFAULT 'live_source'::character varying,
                                                               fdwdbhostname character varying DEFAULT ''::character varying,
                                                               fdwdbport character varying DEFAULT ''::character varying,
                                                               fdwdbname character varying DEFAULT ''::character varying,
                                                               fdwdbusername character varying DEFAULT ''::character varying,
                                                               fdwdbpassword character varying DEFAULT ''::character varying,
                                                               livefdwdbhostname character varying DEFAULT ''::character varying,
                                                               livefdwdbport character varying DEFAULT ''::character varying,
                                                               livefdwdbname character varying DEFAULT ''::character varying,
                                                               livefdwdbusername character varying DEFAULT ''::character varying,
                                                               livefdwdbpassword character varying DEFAULT ''::character varying,
                                                               process_sequences integer[] DEFAULT '{}'::integer[],
                                                               createanddropfdwserver boolean DEFAULT false,
                                                               createanddropfdwschemas boolean DEFAULT false,
                                                               createanddroplivefdw boolean DEFAULT false,
                                                               runvalidation boolean DEFAULT false,
                                                               errorifmoredeletesthancopies boolean DEFAULT false,
                                                               max_attempts integer DEFAULT 1) RETURNS character varying
    LANGUAGE plpgsql
AS
$$

DECLARE
    publicationeventid character varying;
BEGIN

    -- First we clear down all tables in the nuarusercreateddata schema and refresh them from the Live instance
    PERFORM nuarpublication.fn_refreshnuarusercreateddatafromlive_v21x(
            createanddroplivefdw => createanddroplivefdw,
            livesourceschemasuffix => livesourceschemasuffix,
            livefdwservername => livefdwservername,
            livefdwdbhostname => livefdwdbhostname,
            livefdwdbport => livefdwdbport,
            livefdwdbname => livefdwdbname,
            livefdwdbusername => livefdwdbusername,
            livefdwdbpassword => livefdwdbpassword,
            max_attempts => max_attempts
            );

    -- We also need to do an Upsert from Live to Standby of the following:
--	nuaractor
--	nuarlinkedfile
--	relationship_linkedfile
-- COMMENTED OUT FOR NOW

--SELECT ARRAY_AGG(systemid)
--FROM nuarorganisations_live.nuaractor
--INTO organisationfilteridsarray;

--PERFORM nuarpublication.fn_upsertnuaractors_v212(
--	sourceschemaname => 'nuarorganisations_live',
--	sourcetablename => 'nuaractor',
--	targetschemaname => 'nuarorganisations',
--	targettablename => 'nuaractor',
--	sourcesystemids => organisationfilteridsarray);

--PERFORM nuarpublication.fn_upsertlinkedfiles_v212(
--	sourceschemaname => 'nuarorganisations_live',
--	targetschemaname => 'nuarorganisations');

-- We run fn_holdingtostandby_v212 first for replacement scopes "Organisation" and "None" (with no validation)
    SELECT nuarpublication.fn_holdingtostandby_v212(
                   sourceschemasuffix => sourceschemasuffix,
                   fdwservername => fdwservername,
                   livesourceschemasuffix => livesourceschemasuffix,
                   livefdwservername => livefdwservername,
                   fdwdbhostname => fdwdbhostname,
                   fdwdbport => fdwdbport,
                   fdwdbname => fdwdbname,
                   fdwdbusername => fdwdbusername,
                   fdwdbpassword => fdwdbpassword,
                   livefdwdbhostname => livefdwdbhostname,
                   livefdwdbport => livefdwdbport,
                   livefdwdbname => livefdwdbname,
                   livefdwdbusername => livefdwdbusername,
                   livefdwdbpassword => livefdwdbpassword,
                   process_sequences => process_sequences,
                   createanddropfdwserver => createanddropfdwserver,
                   createanddropfdwschemas => createanddropfdwschemas,
                   createanddroplivefdw => createanddroplivefdw,
                   runvalidation => false,
                   replacementscopes => '{"Organisation", "None"}',
                   errorifmoredeletesthancopies => errorifmoredeletesthancopies,
                   max_attempts => max_attempts)
    INTO publicationeventid;

    -- We run fn_holdingtostandby_v212 next for replacement scopes "Submission", "Domain", "Dataset", "Area", "Feature"
-- with validation as specified and passing on the previously-generated publication id so these submissions get related to that
    PERFORM nuarpublication.fn_holdingtostandby_v212(
            sourceschemasuffix => sourceschemasuffix,
            fdwservername => fdwservername,
            livesourceschemasuffix => livesourceschemasuffix,
            livefdwservername => livefdwservername,
            fdwdbhostname => fdwdbhostname,
            fdwdbport => fdwdbport,
            fdwdbname => fdwdbname,
            fdwdbusername => fdwdbusername,
            fdwdbpassword => fdwdbpassword,
            livefdwdbhostname => livefdwdbhostname,
            livefdwdbport => livefdwdbport,
            livefdwdbname => livefdwdbname,
            livefdwdbusername => livefdwdbusername,
            livefdwdbpassword => livefdwdbpassword,
            process_sequences => process_sequences,
            createanddropfdwserver => createanddropfdwserver,
            createanddropfdwschemas => createanddropfdwschemas,
            createanddroplivefdw => createanddroplivefdw,
            runvalidation => runvalidation,
            replacementscopes => '{"Submission", "Domain", "Dataset", "Area", "Feature"}',
            existingpublicationeventid => publicationeventid,
            errorifmoredeletesthancopies => errorifmoredeletesthancopies,
            max_attempts => max_attempts);

    RETURN publicationeventid;
END;
$$;


ALTER FUNCTION nuarpublication.fn_holdingtostandby(sourceschemasuffix character varying, fdwservername character varying, livesourceschemasuffix character varying, livefdwservername character varying, fdwdbhostname character varying, fdwdbport character varying, fdwdbname character varying, fdwdbusername character varying, fdwdbpassword character varying, livefdwdbhostname character varying, livefdwdbport character varying, livefdwdbname character varying, livefdwdbusername character varying, livefdwdbpassword character varying, process_sequences integer[], createanddropfdwserver boolean, createanddropfdwschemas boolean, createanddroplivefdw boolean, runvalidation boolean, errorifmoredeletesthancopies boolean, max_attempts integer) OWNER TO nuar_admin;


DROP FUNCTION IF EXISTS nuarpublication.fn_refreshnuarusercreateddatafromlive_v21x;
CREATE OR REPLACE FUNCTION nuarpublication.fn_refreshnuarusercreateddatafromlive_v21x(createanddroplivefdw boolean DEFAULT false,
                                                                                      livesourceschemasuffix character varying DEFAULT 'live'::character varying,
                                                                                      livefdwservername character varying DEFAULT 'live_source'::character varying,
                                                                                      livefdwdbhostname character varying DEFAULT ''::character varying,
                                                                                      livefdwdbport character varying DEFAULT ''::character varying,
                                                                                      livefdwdbname character varying DEFAULT ''::character varying,
                                                                                      livefdwdbusername character varying DEFAULT ''::character varying,
                                                                                      livefdwdbpassword character varying DEFAULT ''::character varying,
                                                                                      process_sequences integer[] DEFAULT NULL,
                                                                                      max_attempts integer DEFAULT 1) RETURNS integer
    LANGUAGE plpgsql
AS
$$

DECLARE
    publicationstage character varying;
    noerrorstateset  boolean;

BEGIN

    -- First, see if we are allowed to start by checking the status of jobs running in Standby only
    IF (SELECT nuarpublication.fn_checkifpublicationstagecanstart(stagenamevalue => 'holding-standby',
                                                                  process_sequences => process_sequences,
                                                                  max_attempts => max_attempts))
    THEN
        publicationstage := 'Holding to Standby';
        noerrorstateset := true;
    ELSE
        RAISE NOTICE 'Dropping out - unable to run given current publication status';

        PERFORM nuarpublication.fn_logpublicationmessage(
                publicationstage => 'Holding to Standby',
                messagetext => 'Dropping out - unable to run given current publication status',
                logdatetime => NOW()
                );

        -- Set our status to Completed
        PERFORM nuarpublication.fn_recordpublicationjobstatus(
                stagenamevalue => 'holding-standby',
                process_sequences => process_sequences,
                statusvalue => 'Completed',
                startdatetimevalue => NOW(),
                enddatetimevalue => NOW(),
                droppingout => true
                );

        RETURN 0;
    END IF;

    IF (createanddroplivefdw)
    THEN
        -- We need to set up the FDW connection to live if required
        -- Create the FDW Server and User Mapping
        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwserver(
			servername => ''%s'',
			fdwname => ''postgres_fdw'',
			hostname => ''%s'',
			port => ''%s'',
			databasename => ''%s'',
			username => ''%s'',
			userpassword => ''%s'');
	', livefdwservername, livefdwdbhostname, livefdwdbport, livefdwdbname, livefdwdbusername, livefdwdbpassword);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarusercreateddata_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarusercreateddata'');
	', livesourceschemasuffix, livefdwservername);

    END IF;


-- We need to clear down all tables in our nuarusercreateddata schema and refresh them from the "live" instance
    PERFORM nuarpublication.fn_clearallschematables(
            schemaname => 'nuarusercreateddata'
            );

    EXECUTE FORMAT('
	SELECT nuarpublication.fn_copyallschematables(
		sourceschemaname => ''nuarusercreateddata_%s'',
		targetschemaname => ''nuarusercreateddata'',
		publicationstage => ''Holding to Standby'');
', livesourceschemasuffix);

    PERFORM nuarpublication.fn_logpublicationmessage(
            publicationstage => publicationstage,
            messagetext => 'Refreshed nuarusercreateddata tables from the live instance',
            logdatetime => NOW()
            );


    IF (createanddroplivefdw)
    THEN
        -- Drop the Live FDW schemas
        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarusercreateddata_%s'',
			shouldcascade => true);
	', livesourceschemasuffix);

        -- Drop the Live FDW Server
        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropfdwserver(
			servername => ''%s'');
	', livefdwservername);
    END IF;

    RETURN 1;
END;
$$;


ALTER FUNCTION nuarpublication.fn_refreshnuarusercreateddatafromlive_v21x(createanddroplivefdw boolean, livesourceschemasuffix character varying, livefdwservername character varying, livefdwdbhostname character varying, livefdwdbport character varying, livefdwdbname character varying, livefdwdbusername character varying, livefdwdbpassword character varying, process_sequences integer[], max_attempts integer) OWNER TO nuar_admin;


DROP FUNCTION IF EXISTS nuarpublication.fn_holdingtostandby_v212;
CREATE OR REPLACE FUNCTION nuarpublication.fn_holdingtostandby_v212(sourceschemasuffix character varying DEFAULT 'holding'::character varying,
                                                                    fdwservername character varying DEFAULT 'holding_source'::character varying,
                                                                    livesourceschemasuffix character varying DEFAULT 'live'::character varying,
                                                                    livefdwservername character varying DEFAULT 'live_source'::character varying,
                                                                    fdwdbhostname character varying DEFAULT ''::character varying,
                                                                    fdwdbport character varying DEFAULT ''::character varying,
                                                                    fdwdbname character varying DEFAULT ''::character varying,
                                                                    fdwdbusername character varying DEFAULT ''::character varying,
                                                                    fdwdbpassword character varying DEFAULT ''::character varying,
                                                                    livefdwdbhostname character varying DEFAULT ''::character varying,
                                                                    livefdwdbport character varying DEFAULT ''::character varying,
                                                                    livefdwdbname character varying DEFAULT ''::character varying,
                                                                    livefdwdbusername character varying DEFAULT ''::character varying,
                                                                    livefdwdbpassword character varying DEFAULT ''::character varying,
                                                                    process_sequences integer[] DEFAULT '{}'::integer[],
                                                                    createanddropfdwserver boolean DEFAULT false,
                                                                    createanddropfdwschemas boolean DEFAULT false,
                                                                    createanddroplivefdw boolean DEFAULT false,
                                                                    runvalidation boolean DEFAULT false,
                                                                    replacementscopes character varying[] DEFAULT '{}'::character varying[],
                                                                    existingpublicationeventid character varying DEFAULT ''::character varying,
                                                                    errorifmoredeletesthancopies boolean DEFAULT false,
                                                                    max_attempts integer DEFAULT 1) RETURNS character varying
    LANGUAGE plpgsql
AS
$$

DECLARE
    publicationstage            character varying;
    organisationfilteridsarray  character varying[];
    submissionidsarray          character varying[];
    nuaractoridsarray           character varying[];
    backdropgeometrytablesarray character varying[];
    noerrorstateset             boolean;
    publicationeventid          character varying;
    submissionidstodeletearray  character varying[];

BEGIN

    DROP VIEW IF EXISTS submissionids;
    DROP VIEW IF EXISTS nuaractorids;
    DROP VIEW IF EXISTS newsubmissions;
    DROP VIEW IF EXISTS organisationfilterids;
    DROP VIEW IF EXISTS othernuartablenames;
    DROP VIEW IF EXISTS serviceareatablename;
    DROP VIEW IF EXISTS backdropgeometrytablenames;
    DROP VIEW IF EXISTS relationshiptablenames;
    DROP VIEW IF EXISTS geometrytablenames;
    DROP VIEW IF EXISTS finalsubmissionlists;

-- First, see if we are allowed to start by checking the status of jobs running in Standby only
    IF (SELECT nuarpublication.fn_checkifpublicationstagecanstart(stagenamevalue => 'holding-standby',
                                                                  process_sequences => process_sequences,
                                                                  max_attempts => max_attempts))
    THEN
        publicationstage := 'Holding to Standby';
        noerrorstateset := true;
    ELSE
        RAISE NOTICE 'Dropping out - unable to run given current publication status';

        PERFORM nuarpublication.fn_logpublicationmessage(
                publicationstage => 'Holding to Standby',
                messagetext => 'Dropping out - unable to run given current publication status',
                logdatetime => NOW()
                );

        -- Set our status to Completed
        PERFORM nuarpublication.fn_recordpublicationjobstatus(
                stagenamevalue => 'holding-standby',
                process_sequences => process_sequences,
                statusvalue => 'Completed',
                startdatetimevalue => NOW(),
                enddatetimevalue => NOW(),
                droppingout => true
                );

        RETURN '';
    END IF;

    IF (createanddropfdwserver)
    THEN
        -- Create the FDW Server and User Mapping
        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwserver(
			servername => ''%s'',
			fdwname => ''postgres_fdw'',
			hostname => ''%s'',
			port => ''%s'',
			databasename => ''%s'',
			username => ''%s'',
			userpassword => ''%s'');
	', fdwservername, fdwdbhostname, fdwdbport, fdwdbname, fdwdbusername, fdwdbpassword);
    END IF;

    IF (createanddropfdwschemas)
    THEN

        -- Create and import the FDW schemas
        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarcodelists_datamanagement_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarcodelists_datamanagement'');
	', sourceschemasuffix, fdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarcodelists_platform_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarcodelists_platform'');
	', sourceschemasuffix, fdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarcodelists_transformation_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarcodelists_transformation'');
	', sourceschemasuffix, fdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarcustombackdrop_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarcustombackdrop'');
	', sourceschemasuffix, fdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuardata_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuardata'');
	', sourceschemasuffix, fdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarorganisations_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarorganisations'');
	', sourceschemasuffix, fdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarpublication_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarpublication'');
	', sourceschemasuffix, fdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarsubmissions_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarsubmissions'');
	', sourceschemasuffix, fdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarversion_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarversion'');
	', sourceschemasuffix, fdwservername);

    END IF;

    IF (createanddroplivefdw)
    THEN
        -- We need to set up the FDW connection to live if required
        -- Create the FDW Server and User Mapping
        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwserver(
			servername => ''%s'',
			fdwname => ''postgres_fdw'',
			hostname => ''%s'',
			port => ''%s'',
			databasename => ''%s'',
			username => ''%s'',
			userpassword => ''%s'');
	', livefdwservername, livefdwdbhostname, livefdwdbport, livefdwdbname, livefdwdbusername, livefdwdbpassword);

        -- Create and import the Live FDW schemas
        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarcodelists_datamanagement_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarcodelists_datamanagement'');
	', livesourceschemasuffix, livefdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarcodelists_platform_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarcodelists_platform'');
	', livesourceschemasuffix, livefdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarcodelists_transformation_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarcodelists_transformation'');
	', livesourceschemasuffix, livefdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarcustombackdrop_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarcustombackdrop'');
	', livesourceschemasuffix, livefdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuardata_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuardata'');
	', livesourceschemasuffix, livefdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarorganisations_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarorganisations'');
	', livesourceschemasuffix, livefdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarpublication_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarpublication'');
	', livesourceschemasuffix, livefdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarsubmissions_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarsubmissions'');
	', livesourceschemasuffix, livefdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarversion_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarversion'');
	', livesourceschemasuffix, livefdwservername);
    END IF;

-- See if we are allowed to start (we need to have created the FDW schemas first, as we need to check the Holding db status as well)
    IF (SELECT nuarpublication.fn_checkifpublicationstagecanstart(stagenamevalue => 'holding-standby',
                                                                  process_sequences => process_sequences,
                                                                  max_attempts => max_attempts,
                                                                  remoteschemaname => 'nuarpublication_holding'))
    THEN
        publicationstage := 'Holding to Standby';
        noerrorstateset := true;
    ELSE
        RAISE NOTICE 'Dropping out - unable to run given current publication status';

        -- We also need to remove the FDW Schemas and Server before dropping out, if directed to
        IF (createanddropfdwschemas)
        THEN
            -- Drop the FDW schemas
            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarcodelists_datamanagement_%s'',
				shouldcascade => true);
		', sourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarcodelists_platform_%s'',
				shouldcascade => true);
		', sourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarcodelists_transformation_%s'',
				shouldcascade => true);
		', sourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarcustombackdrop_%s'',
				shouldcascade => true);
		', sourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuardata_%s'',
				shouldcascade => true);
		', sourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarorganisations_%s'',
				shouldcascade => true);
		', sourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarpublication_%s'',
				shouldcascade => true);
		', sourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarsubmissions_%s'',
				shouldcascade => true);
		', sourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarversion_%s'',
				shouldcascade => true);
		', sourceschemasuffix);

        END IF;

        IF (createanddropfdwserver)
        THEN
            -- Drop the FDW Server
            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropfdwserver(
				servername => ''%s'');
		', fdwservername);
        END IF;

        IF (createanddroplivefdw)
        THEN
            -- Drop the Live FDW schemas
            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarcodelists_datamanagement_%s'',
				shouldcascade => true);
		', livesourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarcodelists_platform_%s'',
				shouldcascade => true);
		', livesourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarcodelists_transformation_%s'',
				shouldcascade => true);
		', livesourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarcustombackdrop_%s'',
				shouldcascade => true);
		', livesourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuardata_%s'',
				shouldcascade => true);
		', livesourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarorganisations_%s'',
				shouldcascade => true);
		', livesourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarpublication_%s'',
				shouldcascade => true);
		', livesourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarsubmissions_%s'',
				shouldcascade => true);
		', livesourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarversion_%s'',
				shouldcascade => true);
		', livesourceschemasuffix);

            -- Drop the Live FDW Server
            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropfdwserver(
				servername => ''%s'');
		', livefdwservername);
        END IF;

        PERFORM nuarpublication.fn_logpublicationmessage(
                publicationstage => 'Holding to Standby',
                messagetext => 'Dropping out - unable to run given current publication status',
                logdatetime => NOW()
                );

        -- Set our status to Completed
        PERFORM nuarpublication.fn_recordpublicationjobstatus(
                stagenamevalue => 'holding-standby',
                process_sequences => process_sequences,
                statusvalue => 'Completed',
                startdatetimevalue => NOW(),
                enddatetimevalue => NOW(),
                droppingout => true
                );

        RETURN '';
    END IF;

    -- Clearing down and repopulating of all tables in our nuarusercreateddata schema from the Live instance is now done as a separate function

-- We pick up the list of NUARActor systemid values (if any) which will constrain the data that we copy over
    EXECUTE FORMAT('
CREATE TEMPORARY VIEW organisationfilterids AS
SELECT filteredorganisationid
FROM nuarpublication.fn_getorganisationfilterids(
	schemaname => ''nuarpublication'',
	process_sequences => ''%s'');
', process_sequences);

-- The following functions retrieve ALL publishable submissions for Asset Owners which have unpublished Submissions
    IF NOT EXISTS (SELECT * FROM organisationfilterids)
    THEN
        EXECUTE FORMAT('
		CREATE TEMPORARY VIEW newsubmissions AS SELECT submissionsystemid, nuaractorsystemid
		FROM
		nuarpublication.fn_getallunpublishedsubmissions(
			submissionsschemaname => ''nuarsubmissions_holding'',
			submissionstablename => ''nuarsubmissionevent'',
			publicationsschemaname => ''nuarpublication'',
			publicationstablename => ''nuarpublicationevent'',
			publicationssubmissionsreltablename => ''relationship_publicationtosubmission'',
			submissionresult => ''Success'',
			submissionstatus => ''Completed'',
			publishablestatus => ''Publishable'',
			replacementscopes => ''%s'');
		', replacementscopes);
    ELSE
        -- Create an array of the filter organisation ids
        SELECT ARRAY_AGG(filteredorganisationid)
        FROM organisationfilterids
        INTO organisationfilteridsarray;

        EXECUTE FORMAT('
		CREATE TEMPORARY VIEW newsubmissions AS SELECT submissionsystemid, nuaractorsystemid
		FROM
		nuarpublication.fn_getallunpublishedsubmissionsfilteredbyorganisation(
			submissionsschemaname => ''nuarsubmissions_holding'',
			submissionstablename => ''nuarsubmissionevent'',
			publicationsschemaname => ''nuarpublication'',
			publicationstablename => ''nuarpublicationevent'',
			publicationssubmissionsreltablename => ''relationship_publicationtosubmission'',
			submissionresult => ''Success'',
			submissionstatus => ''Completed'',
			publishablestatus => ''Publishable'',
			organisationids => (SELECT ARRAY_AGG(filteredorganisationid) FROM organisationfilterids),
			replacementscopes => ''%s'');
		', replacementscopes);

        -- If we're filtering by organisation id, we should make sure that all these organisations are in the target nuaractor table
        -- Do an upsert for all these organisation ids into the target nuaractor table
        PERFORM nuarpublication.fn_upsertnuaractors_v212(
                sourceschemaname => 'nuarorganisations_holding',
                sourcetablename => 'nuaractor',
                targetschemaname => 'nuarorganisations',
                targettablename => 'nuaractor',
                sourcesystemids => organisationfilteridsarray);

        PERFORM nuarpublication.fn_logpublicationmessage(
                publicationstage => publicationstage,
                messagetext => 'Upserted NUARActor records for filter organisation ids',
                logdatetime => NOW()
                );

    END IF;

    CREATE TEMPORARY VIEW submissionids AS
    SELECT DISTINCT submissionsystemid
    FROM newsubmissions;

    CREATE TEMPORARY VIEW nuaractorids AS
    SELECT DISTINCT nuaractorsystemid
    FROM newsubmissions;

    -- Is either the list of Submissions or the list of NUARActors empty?
-- If so, we just drop out
    IF (
        NOT EXISTS (SELECT FROM submissionids) OR
        NOT EXISTS (SELECT FROM nuaractorids)
        )
    THEN
        RAISE NOTICE 'No submissions to publish';

        PERFORM nuarpublication.fn_logpublicationmessage(
                publicationstage => publicationstage,
                messagetext => 'No submissions to publish',
                logdatetime => NOW()
                );

        IF (runvalidation)
        THEN
            -- Do the check against Holding first, check the results, then run the check against Live
            PERFORM nuarpublication.fn_recordcountvalidationrun_v211(
                    runname => 'holding-standby',
                    sourceschemasuffix => 'holding',
                    createanddropfdwserver => false,
                    createanddropfdwschemas => false,
                    publishablesubmissionsonly => true -- Set true for checking standby against holding, otherwise false
                    );

            -- Check the results of the validation against Holding first.
            IF (SELECT nuarpublication.fn_checkvalidationresults(stagenamevalue => 'holding-standby'))
            THEN
                noerrorstateset := true;
            ELSE
                noerrorstateset := false;
            END IF;

            IF (noerrorstateset)
            THEN
                -- If there are no error conditions, we then check against Live
                PERFORM nuarpublication.fn_recordcountvalidationrun_v211(
                        runname => 'standby-live',
                        sourceschemasuffix => 'live',
                        createanddropfdwserver => false,
                        createanddropfdwschemas => false,
                        publishablesubmissionsonly => false -- Set true for checking standby against holding, otherwise false
                        );

                -- Check the results of the validation against Live.
                IF (SELECT nuarpublication.fn_checkvalidationresults(stagenamevalue => 'standby-live'))
                THEN
                    noerrorstateset := true;
                ELSE
                    noerrorstateset := false;
                END IF;

            END IF;

        END IF;

        IF (createanddropfdwschemas)
        THEN
            -- Drop the FDW schemas
            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarcodelists_datamanagement_%s'',
				shouldcascade => true);
		', sourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarcodelists_platform_%s'',
				shouldcascade => true);
		', sourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarcodelists_transformation_%s'',
				shouldcascade => true);
		', sourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarcustombackdrop_%s'',
				shouldcascade => true);
		', sourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuardata_%s'',
				shouldcascade => true);
		', sourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarorganisations_%s'',
				shouldcascade => true);
		', sourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarpublication_%s'',
				shouldcascade => true);
		', sourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarsubmissions_%s'',
				shouldcascade => true);
		', sourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarversion_%s'',
				shouldcascade => true);
		', sourceschemasuffix);

        END IF;

        IF (createanddropfdwserver)
        THEN
            -- Drop the FDW Server
            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropfdwserver(
				servername => ''%s'');
		', fdwservername);
        END IF;

        IF (createanddroplivefdw)
        THEN
            -- Drop the Live FDW schemas
            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarcodelists_datamanagement_%s'',
				shouldcascade => true);
		', livesourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarcodelists_platform_%s'',
				shouldcascade => true);
		', livesourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarcodelists_transformation_%s'',
				shouldcascade => true);
		', livesourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarcustombackdrop_%s'',
				shouldcascade => true);
		', livesourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuardata_%s'',
				shouldcascade => true);
		', livesourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarorganisations_%s'',
				shouldcascade => true);
		', livesourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarpublication_%s'',
				shouldcascade => true);
		', livesourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarsubmissions_%s'',
				shouldcascade => true);
		', livesourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarversion_%s'',
				shouldcascade => true);
		', livesourceschemasuffix);

            -- Drop the Live FDW Server
            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropfdwserver(
				servername => ''%s'');
		', livefdwservername);
        END IF;

        IF (noerrorstateset)
        THEN
            -- The last thing we do is set our status to Completed
            PERFORM nuarpublication.fn_recordpublicationjobstatus(
                    stagenamevalue => 'holding-standby',
                    process_sequences => process_sequences,
                    statusvalue => 'Completed',
                    startdatetimevalue => NOW(),
                    enddatetimevalue => NOW()
                    );
        END IF;

        RETURN '';
    END IF;

    SELECT ARRAY_AGG(submissionsystemid)
    FROM submissionids
    INTO submissionidsarray;

    SELECT ARRAY_AGG(nuaractorsystemid)
    FROM nuaractorids
    INTO nuaractoridsarray;

    RAISE NOTICE 'Initial submission selection: %', submissionidsarray;

    -- Now we need to get the set of Submission Event Ids that we want to delete
-- and the set of Submission Event Ids that we're going to publish
    EXECUTE FORMAT('
	CREATE TEMPORARY VIEW finalsubmissionlists AS SELECT finalpublicationlist, deletelist
	FROM
	nuarpublication.fn_selectsubmissionsfordeletion_v211(
		sourceschemaname => ''nuarsubmissions_holding'',
		targetschemaname => ''nuarsubmissions'',
		publicationlist => ''%s'');
', submissionidsarray);

-- Recreate the submission ids array as it may have been added to
    submissionidsarray := '{}';
    IF (ARRAY_LENGTH((SELECT finalpublicationlist FROM finalsubmissionlists), 1) IS NOT NULL)
    THEN
        SELECT finalpublicationlist
        FROM finalsubmissionlists
        INTO submissionidsarray;
    END IF;

    RAISE NOTICE 'Submissions to copy: %', submissionidsarray;

    PERFORM nuarpublication.fn_logpublicationmessage(
            publicationstage => 'Holding to Standby',
            messagetext => FORMAT('Submission Events to copy: %s', submissionidsarray)
            );

    submissionidstodeletearray := '{}';
    IF (ARRAY_LENGTH((SELECT deletelist FROM finalsubmissionlists), 1) IS NOT NULL)
    THEN
        SELECT deletelist
        FROM finalsubmissionlists
        INTO submissionidstodeletearray;
    END IF;

    RAISE NOTICE 'Submissions to be deleted: %', submissionidstodeletearray;

    PERFORM nuarpublication.fn_logpublicationmessage(
            publicationstage => 'Holding to Standby',
            messagetext => FORMAT('Submission Events to delete: %s', submissionidstodeletearray)
            );

    DROP TABLE IF EXISTS submissionsfordeletionreturntable;

-- Raise an error if the number of submission events to delete is greater than the number to copy
    IF (errorifmoredeletesthancopies)
    THEN
        IF
            (
                (submissionidstodeletearray <> '{}' AND submissionidsarray = '{}')
                    OR
                (ARRAY_LENGTH(submissionidstodeletearray, 1) > ARRAY_LENGTH(submissionidsarray, 1))
                )
        THEN
            RAISE NOTICE 'WE ARE DELETING MORE SUBMISSIONS THAN WE ARE COPYING - DROPPING OUT';

            PERFORM nuarpublication.fn_logpublicationmessage(
                    publicationstage => 'Holding to Standby',
                    messagetext => 'Dropping out - we are deleting more submissions than we are copying',
                    logdatetime => NOW()
                    );

            PERFORM nuarpublication.fn_recordpublicationjobstatus(
                    stagenamevalue => 'Holding to Standby',
                    process_sequences => process_sequences,
                    statusvalue => 'Error',
                    descriptionvalue => 'We are deleting more submissions than we are copying',
                    startdatetimevalue => NOW(),
                    enddatetimevalue => NOW()
                    );

            RETURN '';
        END IF;
    END IF;


    -- ****
-- NOTE: we no longer do this general update of all Submission Events.
-- Now that the update is split across two transactions covering different replacement scopes
-- (Organisation level first then all other), this general update meant that submissions in the second transaction
-- would be set as "Retired" prematurely and therefore not included in the list of Submissions to be deleted.
-- Instead, we update only the Submissions that are going to be retained or deleted during this run.
-- ****
-- Now we have the set of Submissions that we're going to delete, for all the Submissions that we currently hold
-- do an Upsert from Holding to make sure that any status updates etc. are captured
--existingsubmissionidsarray := '{}';
--SELECT ARRAY_AGG(systemid)
--FROM nuarsubmissions.nuarsubmissionevent
--INTO existingsubmissionidsarray;

--IF (ARRAY_LENGTH(existingsubmissionidsarray, 1) IS NOT NULL)
--THEN
--	PERFORM nuarpublication.fn_upsertnuarsubmissionevents_bulk_v21x(
--		sourceschemaname => 'nuarsubmissions_holding',
--		sourcetablename => 'nuarsubmissionevent',
--		targetschemaname => 'nuarsubmissions',
--		targettablename => 'nuarsubmissionevent',
--		sourcesystemids => existingsubmissionidsarray);

--	PERFORM nuarpublication.fn_logpublicationmessage(
--		publicationstage => publicationstage,
--		messagetext => 'Updated existing Submissions in Standby from Holding',
--		logdatetime => NOW()
--	);
--END IF;

    -- We get a list of table names from the current database (STANDBY).
-- We are assuming that table names in the source and the destination match.
-- Get tables with geometry
    CREATE TEMPORARY VIEW geometrytablenames AS
    SELECT table_name::character varying as tablenamestring
    FROM information_schema.columns
    WHERE column_name = 'geometry'
      AND table_schema = 'nuardata_holding';

    -- Get relationship_tables from nuardata
    CREATE TEMPORARY VIEW relationshiptablenames AS
    SELECT table_name::character varying as tablenamestring
    FROM information_schema.tables
    WHERE (
        table_name LIKE 'relationship_%'
            OR
        table_name = 'nuarsubordinatenetworkdefinition'
        )
      AND table_schema = 'nuardata_holding';

    -- We'll also need to delete for this NUARActor from:
-- > nuarcontactdetails
-- > relationship_actortocontactdetails
-- > relationship_actortoservicearea
--
-- Tables that won't be cleared down before the copies (as they are not anticipated to be populated/managed via DT&I) are:
-- > nuaractivityproximityrule
-- > relationship_actortodda
-- > relationship_actortodis
-- > relationship_actortorule
-- > relationship_actortosubmissiondomain
-- > relationship_serviceprovidertoorganisation
--
-- The following tables do not have a direct relationship with NUARActor or NUARSubmissionEvent so won't be cleared down
-- > nuardda
-- > nuardis

    CREATE TEMPORARY VIEW othernuartablenames AS
    SELECT table_name::character varying as tablenamestring
    FROM information_schema.tables
    WHERE table_schema = 'nuarorganisations_holding'
      AND (
        table_name = 'nuarcontactdetails'
            OR
        table_name = 'relationship_actortocontactdetails'
            OR
        table_name = 'relationship_actortoservicearea'
        );


    PERFORM nuarpublication.fn_deletesubmissionrecordsfromtables(
            targetschemaname => 'nuarorganisations',
            submissionids => submissionidstodeletearray,
            tablenames => (SELECT ARRAY_AGG(tablenamestring) FROM othernuartablenames));

    -- Get backdrop tables with geometry
    CREATE TEMPORARY VIEW backdropgeometrytablenames AS
    SELECT table_name::character varying as tablenamestring
    FROM information_schema.columns
    WHERE column_name = 'geometry'
      AND table_schema = 'nuarcustombackdrop_holding';

-- Upsert NUARActor records
    PERFORM nuarpublication.fn_upsertnuaractors_V212(
            sourceschemaname => 'nuarorganisations_holding',
            sourcetablename => 'nuaractor',
            targetschemaname => 'nuarorganisations',
            targettablename => 'nuaractor',
            sourcesystemids => nuaractoridsarray);

    PERFORM nuarpublication.fn_logpublicationmessage(
            publicationstage => publicationstage,
            messagetext => 'Upserted NUARActor records',
            logdatetime => NOW()
            );

-- Upsert NUARSubmissionEvent records for the Submissions that we are going to copy
    PERFORM nuarpublication.fn_upsertnuarsubmissionevents_v21x(
            sourceschemaname => 'nuarsubmissions_holding',
            sourcetablename => 'nuarsubmissionevent',
            targetschemaname => 'nuarsubmissions',
            targettablename => 'nuarsubmissionevent',
            sourcesystemids => submissionidsarray);

-- Upsert NUARSubmissionEvent records for the Submissions that we are going to delete
    PERFORM nuarpublication.fn_upsertnuarsubmissionevents_v21x(
            sourceschemaname => 'nuarsubmissions_holding',
            sourcetablename => 'nuarsubmissionevent',
            targetschemaname => 'nuarsubmissions',
            targettablename => 'nuarsubmissionevent',
            sourcesystemids => submissionidstodeletearray);

    PERFORM nuarpublication.fn_logpublicationmessage(
            publicationstage => publicationstage,
            messagetext => 'Upserted NUARSubmissionEvent records',
            logdatetime => NOW()
            );

-- Upsert NUARActor related records for these Submissions
    PERFORM nuarpublication.fn_upsertnuaractorrelateddata_v212(
            sourceschemaname => 'nuarorganisations_holding',
            sourcecontactdetailstablename => 'nuarcontactdetails',
            sourceactivityproximityruletablename => 'nuaractivityproximityrule',
            sourcecontactdetailsrelationshiptablename => 'relationship_actortocontactdetails',
            sourceactivityproximityrulerelationshiptablename => 'relationship_actortorule',
            sourceservicearearelationshiptablename => 'relationship_actortoservicearea',
            sourcedomainrelationshiptablename => 'relationship_actortosubmissiondomain',
            sourceserviceproviderrelationshiptablename => 'relationship_serviceprovidertoorganisation',
            targetschemaname => 'nuarorganisations',
            targetcontactdetailstablename => 'nuarcontactdetails',
            targetactivityproximityruletablename => 'nuaractivityproximityrule',
            targetcontactdetailsrelationshiptablename => 'relationship_actortocontactdetails',
            targetactivityproximityrulerelationshiptablename => 'relationship_actortorule',
            targetservicearearelationshiptablename => 'relationship_actortoservicearea',
            targetdomainrelationshiptablename => 'relationship_actortosubmissiondomain',
            targetserviceproviderrelationshiptablename => 'relationship_serviceprovidertoorganisation',
            sourcesystemids => submissionidsarray);

    PERFORM nuarpublication.fn_logpublicationmessage(
            publicationstage => publicationstage,
            messagetext => 'Upserted NUARActor related records',
            logdatetime => NOW()
            );

-- Upsert NUARActor DDA and DIS related records for these Organisations
    PERFORM nuarpublication.fn_upsertnuaractorlegalagreementsdata_v21x(
            sourceschemaname => 'nuarorganisations_holding',
            sourcenuaractortablename => 'nuaractor',
            sourceddatablename => 'nuardda',
            sourcedistablename => 'nuardis',
            sourceddarelationshiptablename => 'relationship_actortodda',
            sourcedisrelationshiptablename => 'relationship_actortodis',
            targetschemaname => 'nuarorganisations',
            targetddatablename => 'nuardda',
            targetdistablename => 'nuardis',
            targetddarelationshiptablename => 'relationship_actortodda',
            targetdisrelationshiptablename => 'relationship_actortodis',
            sourcesystemids => nuaractoridsarray);

    PERFORM nuarpublication.fn_logpublicationmessage(
            publicationstage => publicationstage,
            messagetext => 'Upserted NUARActor DDA and DIS related records',
            logdatetime => NOW()
            );

    -- Before copying new records, we delete records for the specified Submissions
-- Delete from Geometry tables
    PERFORM nuarpublication.fn_deletesubmissionrecordsfromtables(
            targetschemaname => 'nuardata',
            submissionids => submissionidstodeletearray,
            tablenames => (SELECT ARRAY_AGG(tablenamestring) FROM geometrytablenames));

-- Delete from Relationship tables
    PERFORM nuarpublication.fn_deletesubmissionrecordsfromtables(
            targetschemaname => 'nuardata',
            submissionids => submissionidstodeletearray,
            tablenames => (SELECT ARRAY_AGG(tablenamestring) FROM relationshiptablenames));

-- Delete from Backdrop Geometry tables
    SELECT ARRAY_AGG(tablenamestring) FROM backdropgeometrytablenames INTO backdropgeometrytablesarray;
    PERFORM nuarpublication.fn_deletesubmissionrecordsfromtables(
            targetschemaname => 'nuarcustombackdrop',
            submissionids => submissionidstodeletearray,
            tablenames => backdropgeometrytablesarray);

-- COPY Geometry tables
    PERFORM nuarpublication.fn_copysubmissiontablesfromsource(
            sourceschemaname => 'nuardata_holding',
            targetschemaname => 'nuardata',
            submissionids => submissionidsarray,
            tablenames => (SELECT ARRAY_AGG(tablenamestring) FROM geometrytablenames),
            publicationstage => publicationstage);

-- COPY Relationship tables
    PERFORM nuarpublication.fn_copysubmissiontablesfromsource(
            sourceschemaname => 'nuardata_holding',
            targetschemaname => 'nuardata',
            submissionids => submissionidsarray,
            tablenames => (SELECT ARRAY_AGG(tablenamestring) FROM relationshiptablenames),
            publicationstage => publicationstage);

-- COPY Backdrop Geometry tables
    PERFORM nuarpublication.fn_copysubmissiontablesfromsource(
            sourceschemaname => 'nuarcustombackdrop_holding',
            targetschemaname => 'nuarcustombackdrop',
            submissionids => submissionidsarray,
            tablenames => backdropgeometrytablesarray,
            publicationstage => publicationstage);

-- Fix Geometry tables
    PERFORM nuarpublication.fn_fixgeometriesforsubmissions(
            publicationschemaname => 'nuarpublication',
            audittablename => 'nuarpublicationgeometryfixaudit',
            targetschemaname => 'nuardata',
            submissionids => submissionidsarray,
            tablenames => (SELECT ARRAY_AGG(tablenamestring) FROM geometrytablenames));

-- Fix Backdrop Geometry tables
    PERFORM nuarpublication.fn_fixgeometriesforsubmissions(
            publicationschemaname => 'nuarpublication',
            audittablename => 'nuarpublicationgeometryfixaudit',
            targetschemaname => 'nuarcustombackdrop',
            submissionids => submissionidsarray,
            tablenames => backdropgeometrytablesarray);

-- COPY NUARVersion tables
    PERFORM nuarpublication.fn_upsertnuarversion(
            sourceschemaname => 'nuarversion_holding',
            targetschemaname => 'nuarversion');

    PERFORM nuarpublication.fn_logpublicationmessage(
            publicationstage => publicationstage,
            messagetext => 'Upserted NUARVersion records',
            logdatetime => NOW()
            );

-- COPY the tables in the different Codelist schemas
    PERFORM nuarpublication.fn_upsertnuarcodelists(
            sourceschemaname => 'nuarcodelists_datamanagement_holding',
            targetschemaname => 'nuarcodelists_datamanagement');

    PERFORM nuarpublication.fn_upsertnuarcodelists(
            sourceschemaname => 'nuarcodelists_platform_holding',
            targetschemaname => 'nuarcodelists_platform');

    PERFORM nuarpublication.fn_upsertnuarcodelists(
            sourceschemaname => 'nuarcodelists_transformation_holding',
            targetschemaname => 'nuarcodelists_transformation');

    PERFORM nuarpublication.fn_logpublicationmessage(
            publicationstage => publicationstage,
            messagetext => 'Upserted NUARCodelist records',
            logdatetime => NOW()
            );

-- Now copy the nuarpublicationclippinggeometry table from Holding to Standby
    PERFORM nuarpublication.fn_upsertclippinggeometry(
            sourceschemaname => 'nuarpublication_holding',
            sourcetablename => 'nuarpublicationclippinggeometry',
            targetschemaname => 'nuarpublication',
            targettablename => 'nuarpublicationclippinggeometry');

    PERFORM nuarpublication.fn_logpublicationmessage(
            publicationstage => publicationstage,
            messagetext => 'Upserted clipping geometries table',
            logdatetime => NOW()
            );

-- Now we need to update the NUARPublicationEvent table
    SELECT nuarpublication.fn_recordpublicationdetails(
                   targetschemaname => 'nuarpublication',
                   publicationeventtablename => 'nuarpublicationevent',
                   publicationsubmissionrelationshiptablename => 'relationship_publicationtosubmission',
                   datamodelversion => '2.1.1',
                   submissionids => submissionidsarray,
                   existingpublicationeventid => existingpublicationeventid)
    INTO publicationeventid;

    PERFORM nuarpublication.fn_logpublicationmessage(
            publicationstage => publicationstage,
            messagetext => 'Updated NUARPublicationEvent records',
            logdatetime => NOW()
            );

-- Clip all records in the Standby ServiceArea table to a filter set of geometries
    PERFORM nuarpublication.fn_clipfeaturesintableforsubmissions(
            schemaname => 'nuardata',
            tablename => 'servicearea',
            geometrycolumnname => 'geometry',
            clippingschemaname => 'nuarpublication',
            clippingtablename => 'nuarpublicationclippinggeometry',
            clippinggeometrycolumnname => 'geometry',
            submissionids => submissionidsarray);

    PERFORM nuarpublication.fn_logpublicationmessage(
            publicationstage => publicationstage,
            messagetext => 'Clipped geometries in table nuardata.servicearea',
            logdatetime => NOW()
            );

    -- Replicate Operations Coverage service areas across parent and child organisations for all organisations that we hold
-- This ensures that users assigned to any organisational level can mark up AOIs
    PERFORM nuarpublication.fn_serviceareareplication_v210();

    PERFORM nuarpublication.fn_logpublicationmessage(
            publicationstage => publicationstage,
            messagetext => 'Replicated Operations Coverage Service Areas across Parent and Child Organisations',
            logdatetime => NOW()
            );

    DROP VIEW IF EXISTS submissionids;
    DROP VIEW IF EXISTS nuaractorids;
    DROP VIEW IF EXISTS newsubmissions;
    DROP VIEW IF EXISTS organisationfilterids;
    DROP VIEW IF EXISTS othernuartablenames;
    DROP VIEW IF EXISTS serviceareatablename;
    DROP VIEW IF EXISTS backdropgeometrytablenames;
    DROP VIEW IF EXISTS relationshiptablenames;
    DROP VIEW IF EXISTS geometrytablenames;
    DROP VIEW IF EXISTS finalsubmissionlists;

    IF (runvalidation)
    THEN
        -- Do the check against Holding first, check the results, then run the check against Live
        PERFORM nuarpublication.fn_recordcountvalidationrun_v211(
                runname => 'holding-standby',
                sourceschemasuffix => 'holding',
                createanddropfdwserver => false,
                createanddropfdwschemas => false,
                publishablesubmissionsonly => true -- Set true for checking standby against holding, otherwise false
                );

        -- Check the results of the validation against Holding first.
        IF (SELECT nuarpublication.fn_checkvalidationresults(stagenamevalue => 'holding-standby'))
        THEN
            noerrorstateset := true;
        ELSE
            noerrorstateset := false;
        END IF;

        IF (noerrorstateset)
        THEN
            -- If there are no error conditions, we then check against Live
            PERFORM nuarpublication.fn_recordcountvalidationrun_v211(
                    runname => 'standby-live',
                    sourceschemasuffix => 'live',
                    createanddropfdwserver => false,
                    createanddropfdwschemas => false,
                    publishablesubmissionsonly => false -- Set true for checking standby against holding, otherwise false
                    );

            -- Check the results of the validation against Live.
            IF (SELECT nuarpublication.fn_checkvalidationresults(stagenamevalue => 'standby-live'))
            THEN
                noerrorstateset := true;
            ELSE
                noerrorstateset := false;
            END IF;

        END IF;

    END IF;

    IF (createanddropfdwschemas)
    THEN
        -- Drop the FDW schemas
        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarcodelists_datamanagement_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarcodelists_platform_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarcodelists_transformation_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarcustombackdrop_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuardata_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarorganisations_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarpublication_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarsubmissions_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarversion_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

    END IF;

    IF (createanddropfdwserver)
    THEN
        -- Drop the FDW Server
        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropfdwserver(
			servername => ''%s'');
	', fdwservername);
    END IF;

    IF (createanddroplivefdw)
    THEN
        -- Drop the Live FDW schemas
        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarcodelists_datamanagement_%s'',
			shouldcascade => true);
	', livesourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarcodelists_platform_%s'',
			shouldcascade => true);
	', livesourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarcodelists_transformation_%s'',
			shouldcascade => true);
	', livesourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarcustombackdrop_%s'',
			shouldcascade => true);
	', livesourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuardata_%s'',
			shouldcascade => true);
	', livesourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarorganisations_%s'',
			shouldcascade => true);
	', livesourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarpublication_%s'',
			shouldcascade => true);
	', livesourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarsubmissions_%s'',
			shouldcascade => true);
	', livesourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarversion_%s'',
			shouldcascade => true);
	', livesourceschemasuffix);

        -- Drop the Live FDW Server
        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropfdwserver(
			servername => ''%s'');
	', livefdwservername);
    END IF;

    IF (noerrorstateset)
    THEN
        -- The last thing we do is set our status to Completed
        PERFORM nuarpublication.fn_recordpublicationjobstatus(
                stagenamevalue => 'holding-standby',
                process_sequences => process_sequences,
                statusvalue => 'Completed',
                startdatetimevalue => NOW(),
                enddatetimevalue => NOW()
                );
    END IF;

    RETURN publicationeventid;
END;
$$;


ALTER FUNCTION nuarpublication.fn_holdingtostandby_v212(sourceschemasuffix character varying, fdwservername character varying, livesourceschemasuffix character varying, livefdwservername character varying, fdwdbhostname character varying, fdwdbport character varying, fdwdbname character varying, fdwdbusername character varying, fdwdbpassword character varying, livefdwdbhostname character varying, livefdwdbport character varying, livefdwdbname character varying, livefdwdbusername character varying, livefdwdbpassword character varying, process_sequences integer[], createanddropfdwserver boolean, createanddropfdwschemas boolean, createanddroplivefdw boolean, runvalidation boolean, replacementscopes character varying[], existingpublicationeventid character varying, errorifmoredeletesthancopies boolean, max_attempts integer) OWNER TO nuar_admin;

-- live

--  Fixing FORMAT calls as they were missing the batch argument
DROP FUNCTION IF EXISTS nuarpublication.fn_recordpublicationjobstatus;
CREATE OR REPLACE FUNCTION nuarpublication.fn_recordpublicationjobstatus(
    stagenamevalue character varying,
    statusvalue character varying,
    process_sequences integer[] DEFAULT '{-999}'::integer[],
    descriptionvalue character varying DEFAULT ''::character varying,
    startdatetimevalue timestamp with time zone DEFAULT now(),
    enddatetimevalue timestamp with time zone DEFAULT NULL::timestamp with time zone,
    droppingout boolean DEFAULT false,
    publisherexceptionvalue TEXT DEFAULT NULL,
    publisherstacktracevalue TEXT DEFAULT NULL
) RETURNS integer
    LANGUAGE plpgsql
AS
$$
BEGIN
    DECLARE
        databasename character varying;
        batch_arg    integer;
    BEGIN
        databasename := current_database()::character varying;

        DROP VIEW IF EXISTS inprogressjobs;

        FOREACH batch_arg IN ARRAY process_sequences
            LOOP
                EXECUTE FORMAT('
                    CREATE TEMPORARY VIEW inprogressjobs AS
                    SELECT *
                    FROM nuarpublication.nuarpublicationstagestatus
                    WHERE stagename = ''%s''
                      AND batch = %L
                      AND (status = ''In Progress'' OR status = ''Duplicate'');
                ', stagenamevalue, batch_arg);

                IF (statusvalue = 'In Progress' AND (EXISTS (SELECT FROM inprogressjobs))) THEN
                    EXECUTE FORMAT('
                    UPDATE nuarpublication.nuarpublicationstagestatus
                    SET status=''Duplicate''
                    WHERE stagename = ''%s''
                      AND batch = %L;
                ', stagenamevalue, batch_arg);

                    DROP VIEW IF EXISTS inprogressjobs;
                    RETURN 1;
                END IF;

                IF (droppingout AND statusvalue = 'Completed') THEN
                    EXECUTE FORMAT('
                    UPDATE nuarpublication.nuarpublicationstagestatus
                    SET status=''Completed''
                    WHERE stagename = ''%s''
                      AND batch = %L
                      AND status = ''In Progress'';
                ', stagenamevalue, batch_arg);

                    DROP VIEW IF EXISTS inprogressjobs;
                    RETURN 1;
                END IF;

                INSERT INTO nuarpublication.nuarpublicationstagestatus AS target(systemid, systemloaddate, stagename, batch, status,
                                                                                 description,
                                                                                 databasename, startdatetime, enddatetime,
                                                                                 publisherexception, publisherstacktrace)
                VALUES (gen_random_uuid()::character varying, NOW(), stagenamevalue, batch_arg, statusvalue, descriptionvalue, databasename,
                        startdatetimevalue, enddatetimevalue, publisherexceptionvalue, publisherstacktracevalue)
                ON CONFLICT (stagename, batch)
                    DO UPDATE SET status              = excluded.status,
                                  description         = excluded.description,
                                  databasename        = excluded.databasename,
                                  startdatetime       = excluded.startdatetime,
                                  enddatetime         = excluded.enddatetime,
                                  publisherexception  = excluded.publisherexception,
                                  publisherstacktrace = excluded.publisherstacktrace
                WHERE target.stagename = stagenamevalue
                  AND target.batch = batch_arg
                  AND target.status <> 'Error';

                DROP VIEW IF EXISTS inprogressjobs;
                RETURN 1;
            END LOOP;
    END;
END;
$$;

-- 1. Delete existing nulls
DELETE FROM nuarpublication.nuarpublicationstagestatus
WHERE batch IS NULL;

-- 2. Set default value to -999
ALTER TABLE nuarpublication.nuarpublicationstagestatus
    ALTER COLUMN batch SET DEFAULT -999;

-- 3. Set column to NOT NULL
ALTER TABLE nuarpublication.nuarpublicationstagestatus
    ALTER COLUMN batch SET NOT NULL;


-- 1.  Add column to table that flags if checkifpublicationstagecanstart should be tried again even if in error status
ALTER TABLE nuarpublication.nuarpublicationstagestatus
    ADD COLUMN retry boolean DEFAULT false;


-- 2.  Upsert updates to status correctly
DROP FUNCTION IF EXISTS nuarpublication.fn_recordpublicationjobstatus;
CREATE OR REPLACE FUNCTION nuarpublication.fn_recordpublicationjobstatus(
    stagenamevalue character varying,
    statusvalue character varying,
    process_sequences integer[] DEFAULT '{-1}'::integer[],
    descriptionvalue character varying DEFAULT ''::character varying,
    startdatetimevalue timestamp with time zone DEFAULT now(),
    enddatetimevalue timestamp with time zone DEFAULT NULL::timestamp with time zone,
    droppingout boolean DEFAULT false,
    publisherexceptionvalue TEXT DEFAULT NULL,
    publisherstacktracevalue TEXT DEFAULT NULL
) RETURNS integer
    LANGUAGE plpgsql
AS
$$
BEGIN
    DECLARE
        databasename character varying;
        batch_arg    integer;
    BEGIN
        databasename := current_database()::character varying;

        DROP VIEW IF EXISTS inprogressjobs;

        FOREACH batch_arg IN ARRAY process_sequences
            LOOP
                EXECUTE FORMAT('
                    CREATE TEMPORARY VIEW inprogressjobs AS
                    SELECT *
                    FROM nuarpublication.nuarpublicationstagestatus
                    WHERE stagename = ''%s''
                      AND batch = %L
                      AND (status = ''In Progress'' OR status = ''Duplicate'');
                ', stagenamevalue, batch_arg);

                IF (statusvalue = 'In Progress' AND (EXISTS (SELECT FROM inprogressjobs))) THEN
                    EXECUTE FORMAT('
                    UPDATE nuarpublication.nuarpublicationstagestatus
                    SET status=''Duplicate'', retry = false
                    WHERE stagename = ''%s''
                      AND batch = %L;
                ', stagenamevalue, batch_arg);

                    DROP VIEW IF EXISTS inprogressjobs;
                    RETURN 1;
                END IF;

                IF (droppingout AND statusvalue = 'Completed') THEN
                    EXECUTE FORMAT('
                    UPDATE nuarpublication.nuarpublicationstagestatus
                    SET status=''Completed'', retry = false
                    WHERE stagename = ''%s''
                      AND batch = %L
                      AND status = ''In Progress'';
                ', stagenamevalue, batch_arg);

                    DROP VIEW IF EXISTS inprogressjobs;
                    RETURN 1;
                END IF;

                INSERT INTO nuarpublication.nuarpublicationstagestatus AS target(systemid, systemloaddate, stagename, batch, status,
                                                                                 description,
                                                                                 databasename, startdatetime, enddatetime,
                                                                                 publisherexception, publisherstacktrace)
                VALUES (gen_random_uuid()::character varying, NOW(), stagenamevalue, batch_arg, statusvalue, descriptionvalue, databasename,
                        startdatetimevalue, enddatetimevalue, publisherexceptionvalue, publisherstacktracevalue)
                ON CONFLICT (stagename, batch)
                    DO UPDATE SET status              = excluded.status,
                                  description         = excluded.description,
                                  databasename        = excluded.databasename,
                                  startdatetime       = excluded.startdatetime,
                                  enddatetime         = excluded.enddatetime,
                                  publisherexception  = excluded.publisherexception,
                                  publisherstacktrace = excluded.publisherstacktrace,
                                  retry               = false,
                                  attempts            = CASE
                                                            WHEN target.status IN ('In Progress', 'Error')
                                                                THEN target.attempts + 1
                                                            WHEN excluded.status = 'Completed'
                                                                THEN 1
                                                            ELSE target.attempts
                                                        END
                WHERE target.stagename = stagenamevalue
                  AND target.batch = batch_arg;

                DROP VIEW IF EXISTS inprogressjobs;
                RETURN 1;
            END LOOP;
    END;
END;
$$;

ALTER FUNCTION nuarpublication.fn_recordpublicationjobstatus(stagenamevalue character varying,
    statusvalue character varying,
    process_sequences integer[],
    descriptionvalue character varying,
    startdatetimevalue timestamp with time zone,
    enddatetimevalue timestamp with time zone,
    droppingout boolean,
    publisherexceptionvalue TEXT,
    publisherstacktracevalue TEXT) OWNER TO nuar_admin;

-- 3. Change function to check  `retry` to determine if the stage can start
DROP FUNCTION IF EXISTS nuarpublication.fn_checkifpublicationstagecanstart;
CREATE OR REPLACE FUNCTION nuarpublication.fn_checkifpublicationstagecanstart(
    stagenamevalue character varying,
    process_sequences integer[] DEFAULT NULL,
    remoteschemaname character varying DEFAULT ''::character varying,
    max_attempts integer DEFAULT 1
) RETURNS boolean
    LANGUAGE plpgsql
AS
$$
BEGIN
    IF stagenamevalue = 'dti-holding' THEN
        RETURN NOT EXISTS (
            SELECT 1
            FROM nuarpublication.nuarpublicationstagestatus
            WHERE stagename = 'dti-holding'
              AND status IN ('Error', 'Duplicate')
              AND (process_sequences IS NULL OR batch = ANY (process_sequences))
              AND attempts >= max_attempts
              AND retry = false
        );
    ELSE
        IF stagenamevalue = 'holding-standby' THEN
            IF remoteschemaname <> '' THEN
                DROP VIEW IF EXISTS holdingstatus;
                EXECUTE FORMAT('
                    CREATE TEMPORARY VIEW holdingstatus AS
                    SELECT stagename, status, attempts, batch, retry
                    FROM %s.nuarpublicationstagestatus;
                ', remoteschemaname);

                RETURN (
                    NOT EXISTS (
                        SELECT 1
                        FROM nuarpublication.nuarpublicationstagestatus
                        WHERE stagename = 'holding-standby'
                          AND (status = 'Error' OR status = 'Duplicate')
                          AND (process_sequences IS NULL OR batch = ANY (process_sequences))
                          AND attempts >= max_attempts
                          AND retry = false
                    )
                        AND
                    NOT EXISTS (
                        SELECT 1
                        FROM nuarpublication.nuarpublicationstagestatus
                        WHERE stagename = 'live-standby'
                          AND (status = 'Error' OR status = 'In Progress' OR status = 'Duplicate')
                          AND (process_sequences IS NULL OR batch = ANY (process_sequences))
                          AND attempts >= max_attempts
                          AND retry = false
                    )
                        AND
                    NOT EXISTS (
                        SELECT 1
                        FROM holdingstatus
                        WHERE stagename = 'dti-holding'
                          AND (status = 'Error' OR status = 'In Progress' OR status = 'Duplicate')
                          AND (process_sequences IS NULL OR batch = ANY (process_sequences))
                          AND attempts >= max_attempts
                          AND retry = false
                    )
                    );
            ELSE
                RETURN (
                    NOT EXISTS (
                        SELECT 1
                        FROM nuarpublication.nuarpublicationstagestatus
                        WHERE stagename = 'holding-standby'
                          AND (status = 'Error' OR status = 'Duplicate')
                          AND (process_sequences IS NULL OR batch = ANY (process_sequences))
                          AND attempts >= max_attempts
                          AND retry = false
                    )
                        AND
                    NOT EXISTS (
                        SELECT 1
                        FROM nuarpublication.nuarpublicationstagestatus
                        WHERE stagename = 'live-standby'
                          AND (status = 'Error' OR status = 'In Progress' OR status = 'Duplicate')
                          AND (process_sequences IS NULL OR batch = ANY (process_sequences))
                          AND attempts >= max_attempts
                          AND retry = false
                    )
                    );
            END IF;
        ELSE
            IF stagenamevalue = 'live-standby' THEN
                RETURN (
                    NOT EXISTS (
                        SELECT 1
                        FROM nuarpublication.nuarpublicationstagestatus
                        WHERE stagename = 'holding-standby'
                          AND (status = 'Error' OR status = 'In Progress' OR status = 'Duplicate')
                          AND (process_sequences IS NULL OR batch = ANY (process_sequences))
                          AND attempts >= max_attempts
                          AND retry = false
                    )
                        AND
                    NOT EXISTS (
                        SELECT 1
                        FROM nuarpublication.nuarpublicationstagestatus
                        WHERE stagename = 'live-standby'
                          AND (status = 'Error' OR status = 'Duplicate')
                          AND (process_sequences IS NULL OR batch = ANY (process_sequences))
                          AND attempts >= max_attempts
                          AND retry = false
                    )
                    );
            ELSE
                RETURN false;
            END IF;
        END IF;
    END IF;
END;
$$;

ALTER FUNCTION nuarpublication.fn_checkifpublicationstagecanstart(
    stagenamevalue character varying,
    process_sequences integer[],
    remoteschemaname character varying,
    max_attempts integer
    ) OWNER TO nuar_admin;

-- Added this because it was bugging me that batch was at the end of the table when querying in pgadmin
-- Step 0: Drop constraints
ALTER TABLE IF EXISTS nuarpublication.nuarpublicationstagestatus
    DROP CONSTRAINT IF EXISTS nuarpublicationstagestatussystemid_pkey;

ALTER TABLE IF EXISTS nuarpublication.nuarpublicationstagestatus
    DROP CONSTRAINT IF EXISTS nuarpublicationstagestatusstagenamebatch_unique;

ALTER TABLE IF EXISTS nuarpublication.nuarpublicationstagestatus
    DROP CONSTRAINT IF EXISTS uk_nuarpublication_nuarpublicationstagestatus_stagename_batch;

-- Step 1: Create new table with desired column order
CREATE TABLE nuarpublication.nuarpublicationstagestatus_new
(
    systemid character varying(38) COLLATE pg_catalog."default" NOT NULL,
    systemloaddate timestamp with time zone NOT NULL,
    stagename text COLLATE pg_catalog."default" NOT NULL,
    batch integer NOT NULL DEFAULT '-999'::integer,         -- moved up here
    status text COLLATE pg_catalog."default",
    attempts integer NOT NULL DEFAULT 1,
    retry boolean NOT NULL DEFAULT false,
    description text COLLATE pg_catalog."default",
    databasename text COLLATE pg_catalog."default",
    startdatetime timestamp with time zone NOT NULL,
    enddatetime timestamp with time zone,
    publisherexception text COLLATE pg_catalog."default",
    publisherstacktrace text COLLATE pg_catalog."default",
    CONSTRAINT nuarpublicationstagestatussystemid_pkey PRIMARY KEY (systemid),
    CONSTRAINT nuarpublicationstagestatusstagenamebatch_unique UNIQUE (stagename, batch),
    CONSTRAINT uk_nuarpublication_nuarpublicationstagestatus_stagename_batch UNIQUE (stagename, batch)
)
    TABLESPACE pg_default;

-- Step 2: Copy data from old table to new table
INSERT INTO nuarpublication.nuarpublicationstagestatus_new (
    systemid,
    systemloaddate,
    stagename,
    batch,
    status,
    attempts,
    retry,
    description,
    databasename,
    startdatetime,
    enddatetime,
    publisherexception,
    publisherstacktrace
)
SELECT
    systemid,
    systemloaddate,
    stagename,
    batch,
    status,
    attempts,
    retry,
    description,
    databasename,
    startdatetime,
    enddatetime,
    publisherexception,
    publisherstacktrace
FROM nuarpublication.nuarpublicationstagestatus;

-- Step 3: Drop old table
DROP TABLE nuarpublication.nuarpublicationstagestatus;

-- Step 4: Rename new table to old table name & Reapply ownership and privileges
ALTER TABLE nuarpublication.nuarpublicationstagestatus_new
    RENAME TO nuarpublicationstagestatus;

ALTER TABLE nuarpublication.nuarpublicationstagestatus
    OWNER TO nuar_admin;

REVOKE ALL ON TABLE nuarpublication.nuarpublicationstagestatus FROM nuar_svc;
GRANT DELETE, UPDATE, INSERT, SELECT ON TABLE nuarpublication.nuarpublicationstagestatus TO nuar_svc;
GRANT ALL ON TABLE nuarpublication.nuarpublicationstagestatus TO nuar_admin;



DROP FUNCTION IF EXISTS nuarpublication.fn_dtitoholding_v211;
CREATE OR REPLACE FUNCTION nuarpublication.fn_dtitoholding_v211(sourceschemasuffix character varying DEFAULT 'dti'::character varying,
                                                                fdwservername character varying DEFAULT 'dti_source'::character varying,
                                                                fdwdbhostname character varying DEFAULT ''::character varying,
                                                                fdwdbport character varying DEFAULT ''::character varying,
                                                                fdwdbname character varying DEFAULT ''::character varying,
                                                                fdwdbusername character varying DEFAULT ''::character varying,
                                                                fdwdbpassword character varying DEFAULT ''::character varying,
                                                                process_sequences integer[] DEFAULT '{}'::integer[],
                                                                createanddropfdwserver boolean DEFAULT false,
                                                                createanddropfdwschemas boolean DEFAULT false,
                                                                runvalidation boolean DEFAULT false,
                                                                max_attempts integer DEFAULT 1,
                                                                update_held boolean DEFAULT false) RETURNS character varying
    LANGUAGE plpgsql
AS
$$

DECLARE
    publicationstage                  character varying;
    organisationfilteridsarray        character varying[];
    submissionidsarray                character varying[];
    nuaractoridsarray                 character varying[];
    backdropgeometrytablesarray       character varying[];
    existingholdingsubmissionidsarray character varying[];
    noerrorstateset                   boolean;
    publicationeventid                character varying;

BEGIN

    DROP VIEW IF EXISTS submissionids;
    DROP VIEW IF EXISTS nuaractorids;
    DROP VIEW IF EXISTS newsubmissions;
    DROP VIEW IF EXISTS existingholdingsubmissions;
    DROP VIEW IF EXISTS organisationfilterids;
    DROP VIEW IF EXISTS geometrytablenames;
    DROP VIEW IF EXISTS relationshiptablenames;
    DROP VIEW IF EXISTS backdropgeometrytablenames;


-- First we see if we are allowed to start
    IF (SELECT nuarpublication.fn_checkifpublicationstagecanstart(stagenamevalue => 'dti-holding',
                                                                  process_sequences => process_sequences,
                                                                  max_attempts => max_attempts))
    THEN
        publicationstage := 'DTI to Holding';
        noerrorstateset := true;
    ELSE
        RAISE NOTICE 'Dropping out - unable to run given current publication status';

        PERFORM nuarpublication.fn_logpublicationmessage(
                publicationstage => 'DTI to Holding',
                messagetext => 'Dropping out - unable to run given current publication status',
                logdatetime => NOW()
                );

        -- Set our status to Completed
        PERFORM nuarpublication.fn_recordpublicationjobstatus(
                stagenamevalue => 'dti-holding',
                process_sequences => process_sequences,
                statusvalue => 'Completed',
                startdatetimevalue => NOW(),
                enddatetimevalue => NOW(),
                droppingout => true
                );

        RETURN '';
    END IF;

    IF (createanddropfdwserver)
    THEN
        -- Create the FDW Server and User Mapping
        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwserver(
			servername => ''%s'',
			fdwname => ''postgres_fdw'',
			hostname => ''%s'',
			port => ''%s'',
			databasename => ''%s'',
			username => ''%s'',
			userpassword => ''%s'');
	', fdwservername, fdwdbhostname, fdwdbport, fdwdbname, fdwdbusername, fdwdbpassword);
    END IF;

    IF (createanddropfdwschemas)
    THEN

        -- Create and import the FDW schemas
        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarcodelists_datamanagement_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarcodelists_datamanagement'');
	', sourceschemasuffix, fdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarcodelists_platform_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarcodelists_platform'');
	', sourceschemasuffix, fdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarcodelists_transformation_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarcodelists_transformation'');
	', sourceschemasuffix, fdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarcustombackdrop_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarcustombackdrop'');
	', sourceschemasuffix, fdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuardata_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuardata'');
	', sourceschemasuffix, fdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarorganisations_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarorganisations'');
	', sourceschemasuffix, fdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarsubmissions_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarsubmissions'');
	', sourceschemasuffix, fdwservername);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_createfdwschema(
			localschemaname => ''nuarversion_%s'',
			foreignservername => ''%s'',
			foreignschemaname => ''nuarversion'');
	', sourceschemasuffix, fdwservername);

    END IF;

    -- For all the Submissions that we currently hold, do an Upsert from Holding to make sure that any status updates etc. are captured
    IF (update_held)
    THEN
        PERFORM nuarpublication.fn_updateheldsubmissions(sourceschemaname => 'nuarsubmissions_dti',
                                                         publicationstage => 'DTI to Holding');
    END IF;

-- We pick up the list of NUARActor systemid values (if any) which will constrain the data that we copy over
    EXECUTE FORMAT('
CREATE TEMPORARY VIEW organisationfilterids AS
SELECT filteredorganisationid
FROM nuarpublication.fn_getorganisationfilterids(
	schemaname => ''nuarpublication'',
	process_sequences => ''%s'');
', process_sequences);

    IF NOT EXISTS (SELECT * FROM organisationfilterids)
    THEN
        --CREATE TEMPORARY VIEW newsubmissions AS SELECT submissionsystemid, nuaractorsystemid FROM nuarpublication.fn_getnewsubmissions(sourceschemaname =>'nuarsubmissions_dti', sourcetablename => 'nuarsubmissionevent', targetschemaname => 'nuarsubmissions', targettablename => 'nuarsubmissionevent', submissionresult => 'Success', submissionstatus => 'Completed');
        CREATE TEMPORARY VIEW newsubmissions AS
        SELECT submissionsystemid, nuaractorsystemid
        FROM
            nuarpublication.fn_getallnewsubmissions(
                    sourceschemaname => 'nuarsubmissions_dti',
                    sourcetablename => 'nuarsubmissionevent',
                    targetschemaname => 'nuarsubmissions',
                    targettablename => 'nuarsubmissionevent');
    ELSE
        --CREATE TEMPORARY VIEW newsubmissions AS SELECT submissionsystemid, nuaractorsystemid FROM nuarpublication.fn_getnewsubmissionsfilteredbyorganisation(sourceschemaname => 'nuarsubmissions_dti', sourcetablename => 'nuarsubmissionevent', targetschemaname => 'nuarsubmissions', targettablename => 'nuarsubmissionevent', submissionresult => 'Success', submissionstatus => 'Completed', organisationids => (SELECT ARRAY_AGG(filteredorganisationid) FROM organisationfilterids));
        CREATE TEMPORARY VIEW newsubmissions AS
        SELECT submissionsystemid, nuaractorsystemid
        FROM
            nuarpublication.fn_getallnewsubmissionsfilteredbyorganisation(
                    sourceschemaname => 'nuarsubmissions_dti',
                    sourcetablename => 'nuarsubmissionevent',
                    targetschemaname => 'nuarsubmissions',
                    targettablename => 'nuarsubmissionevent',
                    organisationids => (SELECT ARRAY_AGG(filteredorganisationid) FROM organisationfilterids));

        -- If we're filtering by organisation id, we should make sure that all these organisations are in the target nuaractor table
        -- Create an array of the filter organisation ids
        SELECT ARRAY_AGG(filteredorganisationid)
        FROM organisationfilterids
        INTO organisationfilteridsarray;

        -- Do an upsert for all these organisation ids into the target nuaractor table
        PERFORM nuarpublication.fn_upsertnuaractors_v210(
                sourceschemaname => 'nuarorganisations_dti',
                sourcetablename => 'nuaractor',
                targetschemaname => 'nuarorganisations',
                targettablename => 'nuaractor',
                sourcesystemids => organisationfilteridsarray);

        PERFORM nuarpublication.fn_logpublicationmessage(
                publicationstage => publicationstage,
                messagetext => 'Upserted NUARActor records for filter organisation ids',
                logdatetime => NOW()
                );

    END IF;

    CREATE TEMPORARY VIEW submissionids AS
    SELECT DISTINCT submissionsystemid
    FROM newsubmissions;

    CREATE TEMPORARY VIEW nuaractorids AS
    SELECT DISTINCT nuaractorsystemid
    FROM newsubmissions;

    -- Is either the list of Submissions or the list of NUARActors empty?
-- If so, we just drop out
    IF (
        NOT EXISTS (SELECT FROM submissionids) OR
        NOT EXISTS (SELECT FROM nuaractorids)
        )
    THEN
        RAISE NOTICE 'No submissions to bring into Holding';
        PERFORM nuarpublication.fn_logpublicationmessage(
                publicationstage => publicationstage,
                messagetext => 'No submissions to bring into Holding',
                logdatetime => NOW()
                );

        IF (runvalidation)
        THEN
            PERFORM nuarpublication.fn_recordcountvalidationrun_v211(
                    runname => 'dti-holding',
                    sourceschemasuffix => 'dti',
                    createanddropfdwserver => false,
                    createanddropfdwschemas => false,
                    publishablesubmissionsonly => false -- Set true for checking standby against holding, otherwise false
                    );

            -- Check the results and set an Error State if appropriate
            IF (SELECT nuarpublication.fn_checkvalidationresults(stagenamevalue => 'dti-holding', process_sequences => process_sequences))
            THEN
                noerrorstateset := true;
            ELSE
                noerrorstateset := false;
            END IF;

        END IF;

        IF (createanddropfdwschemas)
        THEN
            -- Drop the FDW schemas
            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarcodelists_datamanagement_%s'',
				shouldcascade => true);
		', sourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarcodelists_platform_%s'',
				shouldcascade => true);
		', sourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarcodelists_transformation_%s'',
				shouldcascade => true);
		', sourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarcustombackdrop_%s'',
				shouldcascade => true);
		', sourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuardata_%s'',
				shouldcascade => true);
		', sourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarorganisations_%s'',
				shouldcascade => true);
		', sourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarsubmissions_%s'',
				shouldcascade => true);
		', sourceschemasuffix);

            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropschema(
				localschemaname => ''nuarversion_%s'',
				shouldcascade => true);
		', sourceschemasuffix);

        END IF;

        IF (createanddropfdwserver)
        THEN

            -- Drop the FDW Server
            EXECUTE FORMAT('
			SELECT nuarpublication.fn_dropfdwserver(
				servername => ''%s'');
		', fdwservername);

        END IF;

        IF (noerrorstateset)
        THEN
            -- The last thing we do is set our status to Completed
            PERFORM nuarpublication.fn_recordpublicationjobstatus(
                    stagenamevalue => 'dti-holding',
                    process_sequences => process_sequences,
                    statusvalue => 'Completed',
                    startdatetimevalue => NOW(),
                    enddatetimevalue => NOW()
                    );
        END IF;

        RETURN '';
    END IF;

    SELECT ARRAY_AGG(submissionsystemid)
    FROM submissionids
    INTO submissionidsarray;

    SELECT ARRAY_AGG(nuaractorsystemid)
    FROM nuaractorids
    INTO nuaractoridsarray;

    -- We get a list of table names from the current database (HOLDING).
-- We are assuming that table names in the source and the destination match.
-- Get tables with geometry
    CREATE TEMPORARY VIEW geometrytablenames AS
    SELECT table_name::character varying as tablenamestring
    FROM information_schema.columns
    WHERE column_name = 'geometry'
      AND table_schema = 'nuardata';

    -- Get relationship_tables from nuardata
    CREATE TEMPORARY VIEW relationshiptablenames AS
    SELECT table_name::character varying as tablenamestring
    FROM information_schema.tables
    WHERE (
        table_name LIKE 'relationship_%'
            OR
        table_name = 'nuarsubordinatenetworkdefinition'
        )
      AND table_schema = 'nuardata';

    -- Get backdrop tables with geometry
    CREATE TEMPORARY VIEW backdropgeometrytablenames AS
    SELECT table_name::character varying as tablenamestring
    FROM information_schema.columns
    WHERE column_name = 'geometry'
      AND table_schema = 'nuarcustombackdrop';

-- Upsert NUARActor records
    PERFORM nuarpublication.fn_upsertnuaractors_v210(
            sourceschemaname => 'nuarorganisations_dti',
            sourcetablename => 'nuaractor',
            targetschemaname => 'nuarorganisations',
            targettablename => 'nuaractor',
            sourcesystemids => nuaractoridsarray);

    PERFORM nuarpublication.fn_logpublicationmessage(
            publicationstage => publicationstage,
            messagetext => 'Upserted NUARActor records',
            logdatetime => NOW()
            );

-- Upsert NUARSubmissionEvent records
    PERFORM nuarpublication.fn_upsertnuarsubmissionevents_v21x(
            sourceschemaname => 'nuarsubmissions_dti',
            sourcetablename => 'nuarsubmissionevent',
            targetschemaname => 'nuarsubmissions',
            targettablename => 'nuarsubmissionevent',
            sourcesystemids => submissionidsarray);

    PERFORM nuarpublication.fn_logpublicationmessage(
            publicationstage => publicationstage,
            messagetext => 'Upserted NUARSubmissionEvent records',
            logdatetime => NOW()
            );

-- Upsert NUARActor related records for these Submissions
    PERFORM nuarpublication.fn_upsertnuaractorrelateddata_v211(
            sourceschemaname => 'nuarorganisations_dti',
            sourcecontactdetailstablename => 'nuarcontactdetails',
            sourceactivityproximityruletablename => 'nuaractivityproximityrule',
            sourcecontactdetailsrelationshiptablename => 'relationship_actortocontactdetails',
            sourceactivityproximityrulerelationshiptablename => 'relationship_actortorule',
            sourceservicearearelationshiptablename => 'relationship_actortoservicearea',
            sourcedomainrelationshiptablename => 'relationship_actortosubmissiondomain',
            sourceserviceproviderrelationshiptablename => 'relationship_serviceprovidertoorganisation',
            targetschemaname => 'nuarorganisations',
            targetcontactdetailstablename => 'nuarcontactdetails',
            targetactivityproximityruletablename => 'nuaractivityproximityrule',
            targetcontactdetailsrelationshiptablename => 'relationship_actortocontactdetails',
            targetactivityproximityrulerelationshiptablename => 'relationship_actortorule',
            targetservicearearelationshiptablename => 'relationship_actortoservicearea',
            targetdomainrelationshiptablename => 'relationship_actortosubmissiondomain',
            targetserviceproviderrelationshiptablename => 'relationship_serviceprovidertoorganisation',
            sourcesystemids => submissionidsarray);

    PERFORM nuarpublication.fn_logpublicationmessage(
            publicationstage => publicationstage,
            messagetext => 'Upserted NUARActor related records',
            logdatetime => NOW()
            );

-- Upsert NUARActor DDA and DIS related records for these Organisations
    PERFORM nuarpublication.fn_upsertnuaractorlegalagreementsdata_v21x(
            sourceschemaname => 'nuarorganisations_dti',
            sourcenuaractortablename => 'nuaractor',
            sourceddatablename => 'nuardda',
            sourcedistablename => 'nuardis',
            sourceddarelationshiptablename => 'relationship_actortodda',
            sourcedisrelationshiptablename => 'relationship_actortodis',
            targetschemaname => 'nuarorganisations',
            targetddatablename => 'nuardda',
            targetdistablename => 'nuardis',
            targetddarelationshiptablename => 'relationship_actortodda',
            targetdisrelationshiptablename => 'relationship_actortodis',
            sourcesystemids => nuaractoridsarray);

    PERFORM nuarpublication.fn_logpublicationmessage(
            publicationstage => publicationstage,
            messagetext => 'Upserted NUARActor DDA and DIS related records',
            logdatetime => NOW()
            );

-- COPY Geometry tables
    PERFORM nuarpublication.fn_copysubmissiontablesfromsource(
            sourceschemaname => 'nuardata_dti',
            targetschemaname => 'nuardata',
            submissionids => submissionidsarray,
            tablenames => (SELECT ARRAY_AGG(tablenamestring) FROM geometrytablenames),
            publicationstage => publicationstage);

-- COPY Relationship tables
    PERFORM nuarpublication.fn_copysubmissiontablesfromsource(
            sourceschemaname => 'nuardata_dti',
            targetschemaname => 'nuardata',
            submissionids => submissionidsarray,
            tablenames => (SELECT ARRAY_AGG(tablenamestring) FROM relationshiptablenames),
            publicationstage => publicationstage);

-- COPY Backdrop Geometry tables
    SELECT ARRAY_AGG(tablenamestring) FROM backdropgeometrytablenames INTO backdropgeometrytablesarray;
    PERFORM nuarpublication.fn_copysubmissiontablesfromsource(
            sourceschemaname => 'nuarcustombackdrop_dti',
            targetschemaname => 'nuarcustombackdrop',
            submissionids => submissionidsarray,
            tablenames => backdropgeometrytablesarray,
            publicationstage => publicationstage);

-- COPY NUARVersion tables
    PERFORM nuarpublication.fn_upsertnuarversion(
            sourceschemaname => 'nuarversion_dti',
            targetschemaname => 'nuarversion');

    PERFORM nuarpublication.fn_logpublicationmessage(
            publicationstage => publicationstage,
            messagetext => 'Upserted NUARVersion records',
            logdatetime => NOW()
            );

-- COPY the tables in the different Codelist schemas
    PERFORM nuarpublication.fn_upsertnuarcodelists(
            sourceschemaname => 'nuarcodelists_datamanagement_dti',
            targetschemaname => 'nuarcodelists_datamanagement');

    PERFORM nuarpublication.fn_upsertnuarcodelists(
            sourceschemaname => 'nuarcodelists_platform_dti',
            targetschemaname => 'nuarcodelists_platform');

    PERFORM nuarpublication.fn_upsertnuarcodelists(
            sourceschemaname => 'nuarcodelists_transformation_dti',
            targetschemaname => 'nuarcodelists_transformation');

    PERFORM nuarpublication.fn_logpublicationmessage(
            publicationstage => publicationstage,
            messagetext => 'Upserted NUARCodelist records',
            logdatetime => NOW()
            );

    -- Now we've done all our copies, we need to process any "No Change" submissions.
-- Each of these specifies a "predecessor" submission.
-- We update the datelastupdated value to the dateofsubmission of the No Change submission
-- for records in every table which have a submissioneventid_fk value equal to the predecessor Submission
    PERFORM nuarpublication.fn_processnochangesubmissionevents_v211(
            submissionsschemaname => 'nuarsubmissions',
            submissionstablename => 'nuarsubmissionevent',
            submissionids => submissionidsarray
            );

-- Now we need to update the NUARPublicationEvent table
    SELECT nuarpublication.fn_recordpublicationdetails(
                   targetschemaname => 'nuarpublication',
                   publicationeventtablename => 'nuarpublicationevent',
                   publicationsubmissionrelationshiptablename => 'relationship_publicationtosubmission',
                   datamodelversion => '2.1.1',
                   submissionids => submissionidsarray)
    INTO publicationeventid;

    PERFORM nuarpublication.fn_logpublicationmessage(
            publicationstage => publicationstage,
            messagetext => 'Updated NUARPublicationEvent records',
            logdatetime => NOW()
            );

    DROP VIEW IF EXISTS submissionids;
    DROP VIEW IF EXISTS nuaractorids;
    DROP VIEW IF EXISTS newsubmissions;
    DROP VIEW IF EXISTS existingholdingsubmissions;
    DROP VIEW IF EXISTS organisationfilterids;
    DROP VIEW IF EXISTS geometrytablenames;
    DROP VIEW IF EXISTS relationshiptablenames;
    DROP VIEW IF EXISTS backdropgeometrytablenames;

    IF (runvalidation)
    THEN
        PERFORM nuarpublication.fn_recordcountvalidationrun_v211(
                runname => 'dti-holding',
                sourceschemasuffix => 'dti',
                createanddropfdwserver => false,
                createanddropfdwschemas => false,
                publishablesubmissionsonly => false -- Set true for checking standby against holding, otherwise false
                );

        -- Check the results and set an Error State if appropriate
        IF (SELECT nuarpublication.fn_checkvalidationresults(stagenamevalue => 'dti-holding', process_sequences => process_sequences))
        THEN
            noerrorstateset := true;
        ELSE
            noerrorstateset := false;
        END IF;

    END IF;

    IF (createanddropfdwschemas)
    THEN
        -- Drop the FDW schemas
        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarcodelists_datamanagement_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarcodelists_platform_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarcodelists_transformation_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarcustombackdrop_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuardata_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarorganisations_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarsubmissions_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropschema(
			localschemaname => ''nuarversion_%s'',
			shouldcascade => true);
	', sourceschemasuffix);

    END IF;

    IF (createanddropfdwserver)
    THEN

        -- Drop the FDW Server
        EXECUTE FORMAT('
		SELECT nuarpublication.fn_dropfdwserver(
			servername => ''%s'');
	', fdwservername);

    END IF;

    IF (noerrorstateset)
    THEN
        -- The last thing we do is set our status to Completed
        PERFORM nuarpublication.fn_recordpublicationjobstatus(
                stagenamevalue => 'dti-holding',
                process_sequences => process_sequences,
                statusvalue => 'Completed',
                startdatetimevalue => NOW(),
                enddatetimevalue => NOW()
                );
    END IF;

    RETURN publicationeventid;
END;
$$;


ALTER FUNCTION nuarpublication.fn_dtitoholding_v211(sourceschemasuffix character varying, fdwservername character varying, fdwdbhostname character varying, fdwdbport character varying, fdwdbname character varying, fdwdbusername character varying, fdwdbpassword character varying, process_sequences integer[], createanddropfdwserver boolean, createanddropfdwschemas boolean, runvalidation boolean, max_attempts integer, update_held boolean) OWNER TO nuar_admin;




DROP FUNCTION IF EXISTS nuarpublication.fn_updateheldsubmissions;
CREATE OR REPLACE FUNCTION nuarpublication.fn_updateheldsubmissions(sourceschemaname character varying, publicationstage character varying) RETURNS boolean
    LANGUAGE plpgsql
AS
$$

DECLARE
    existingholdingsubmissionidsarray character varying[];
BEGIN
    -- For all the Submissions that we currently hold, do an Upsert from Holding to make sure that any status updates etc. are captured
    CREATE TEMPORARY VIEW existingholdingsubmissions AS
    SELECT systemid
    FROM nuarsubmissions.nuarsubmissionevent;

    IF EXISTS (SELECT * FROM existingholdingsubmissions)
    THEN
        SELECT ARRAY_AGG(systemid)
        FROM existingholdingsubmissions
        INTO existingholdingsubmissionidsarray;

        PERFORM nuarpublication.fn_upsertnuarsubmissionevents_bulk_v21x(
                sourceschemaname => sourceschemaname,
                sourcetablename => 'nuarsubmissionevent',
                targetschemaname => 'nuarsubmissions',
                targettablename => 'nuarsubmissionevent',
                sourcesystemids => existingholdingsubmissionidsarray);

        PERFORM nuarpublication.fn_logpublicationmessage(
                publicationstage => publicationstage,
                messagetext => FORMAT('Updated existing Submissions from source %s', sourceschemaname),
                logdatetime => NOW()
                );
        RETURN true;
    END IF;
    RETURN false;
END;
$$;


ALTER FUNCTION nuarpublication.fn_updateheldsubmissions(sourceschemaname character varying, publicationstage character varying) OWNER TO nuar_admin;


-- 3. Change function to check  `retry` to determine if the stage can start
DROP FUNCTION IF EXISTS nuarpublication.fn_checkifpublicationstagecanstart;
CREATE OR REPLACE FUNCTION nuarpublication.fn_checkifpublicationstagecanstart(
    stagenamevalue character varying,
    process_sequences integer[] DEFAULT NULL,
    remoteschemaname character varying DEFAULT ''::character varying,
    max_attempts integer DEFAULT 1
) RETURNS boolean
    LANGUAGE plpgsql
AS
$$
BEGIN
    IF stagenamevalue = 'dti-holding' THEN
        RETURN NOT EXISTS (
            SELECT 1
            FROM nuarpublication.nuarpublicationstagestatus
            WHERE stagename = 'dti-holding'
              AND status IN ('Error', 'Duplicate')
              AND (process_sequences IS NULL OR batch = ANY (process_sequences))
              AND attempts >= max_attempts
              AND retry = false
        );
    ELSE
        IF stagenamevalue = 'holding-standby' THEN
            RETURN (
                NOT EXISTS (
                    SELECT 1
                    FROM nuarpublication.nuarpublicationstagestatus
                    WHERE stagename = 'holding-standby'
                      AND (status = 'Error' OR status = 'Duplicate')
                      AND (process_sequences IS NULL OR batch = ANY (process_sequences))
                      AND attempts >= max_attempts
                      AND retry = false
                )
                    AND
                NOT EXISTS (
                    SELECT 1
                    FROM nuarpublication.nuarpublicationstagestatus
                    WHERE stagename = 'live-standby'
                      AND (status = 'Error' OR status = 'In Progress' OR status = 'Duplicate')
                      AND (process_sequences IS NULL OR batch = ANY (process_sequences))
                      AND attempts >= max_attempts
                      AND retry = false
                )
                );
        ELSE
            IF stagenamevalue = 'live-standby' THEN
                RETURN (
                    NOT EXISTS (
                        SELECT 1
                        FROM nuarpublication.nuarpublicationstagestatus
                        WHERE stagename = 'holding-standby'
                          AND (status = 'Error' OR status = 'In Progress' OR status = 'Duplicate')
                          AND (process_sequences IS NULL OR batch = ANY (process_sequences))
                          AND attempts >= max_attempts
                          AND retry = false
                    )
                        AND
                    NOT EXISTS (
                        SELECT 1
                        FROM nuarpublication.nuarpublicationstagestatus
                        WHERE stagename = 'live-standby'
                          AND (status = 'Error' OR status = 'Duplicate')
                          AND (process_sequences IS NULL OR batch = ANY (process_sequences))
                          AND attempts >= max_attempts
                          AND retry = false
                    )
                    );
            ELSE
                RETURN false;
            END IF;
        END IF;
    END IF;
END;
$$;

ALTER FUNCTION nuarpublication.fn_checkifpublicationstagecanstart(
    stagenamevalue character varying,
    process_sequences integer[],
    remoteschemaname character varying,
    max_attempts integer
    ) OWNER TO nuar_admin;



-- Drop V1 un-needed columns
ALTER TABLE nuarpublication.nuarpublicationstagestatus
       DROP COLUMN IF EXISTS batch,
       DROP COLUMN IF EXISTS attempts,
       DROP COLUMN IF EXISTS retry;

-- Drop V1 Constraints
ALTER TABLE nuarpublication.nuarpublicationstagestatus
    DROP CONSTRAINT IF EXISTS uk_nuarpublication_nuarpublicationstagestatus_stagename;


ALTER TABLE nuarpublication.nuarpublicationstagestatus
    DROP CONSTRAINT IF EXISTS nuarpublicationstagestatusstagename_unique;

ALTER TABLE nuarpublication.nuarpublicationstagestatus
    DROP CONSTRAINT IF EXISTS uk_nuarpublication_nuarpublicationstagestatus_stagename_batch;
ALTER TABLE ONLY nuarpublication.nuarpublicationstagestatus
    DROP CONSTRAINT IF EXISTS nuarpublicationstagestatusstagenamebatch_unique;

-- V2 Additions / keep needed v1 columns
ALTER TABLE nuarpublication.nuarpublicationstagestatus
    ADD COLUMN IF NOT EXISTS publisherexception  TEXT,
    ADD COLUMN IF NOT EXISTS publisherstacktrace TEXT,
    ADD COLUMN IF NOT EXISTS batches             TEXT default '*';

-- V2 constraint
ALTER TABLE nuarpublication.nuarpublicationstagestatus
    ADD CONSTRAINT uk_nuarpublication_nuarpublicationstagestatus_stagename_batches UNIQUE (stagename, batches);

ALTER TABLE ONLY nuarpublication.nuarpublicationstagestatus
    ADD CONSTRAINT nuarpublicationstagestatusstagenamebatches_unique UNIQUE (stagename, batches);
CREATE TABLE IF NOT EXISTS nuarpublication.nuarpublicationorganisationstatus (
    systemid character varying(38) PRIMARY KEY,
    dataproviderid_fk character varying(38) NOT NULL,
    pulled boolean NOT NULL,
    dateoflastchange timestamp without time zone NOT NULL,
    CONSTRAINT nuarpublication_organisationstatus_dataproviderid_fk_unique UNIQUE (dataproviderid_fk)
);

ALTER TABLE nuarpublication.nuarpublicationorganisationstatus OWNER TO nuar_admin;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_foreign_server WHERE srvname = 'holding_source') THEN
        EXECUTE $cte$
        CREATE FOREIGN TABLE IF NOT EXISTS nuarpublication_holding.nuarpublicationorganisationstatus (
            systemid character varying(38) NOT NULL,
            dataproviderid_fk character varying(38) NOT NULL,
            pulled boolean NOT NULL,
            dateoflastchange timestamp without time zone NOT NULL
        )
        SERVER holding_source
        OPTIONS (schema_name 'nuarpublication', table_name 'nuarpublicationorganisationstatus');
        $cte$;
    END IF;
END
$$;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_foreign_server WHERE srvname = 'live_source') THEN
        EXECUTE $cte$
        CREATE FOREIGN TABLE IF NOT EXISTS nuarpublication_live.nuarpublicationorganisationstatus (
            systemid character varying(38) NOT NULL,
            dataproviderid_fk character varying(38) NOT NULL,
            pulled boolean NOT NULL,
            dateoflastchange timestamp without time zone NOT NULL
        )
        SERVER live_source
        OPTIONS (schema_name 'nuarpublication', table_name 'nuarpublicationorganisationstatus');
        $cte$;
    END IF;
END
$$;



DROP TABLE IF EXISTS nuarpublication.flyway_schema_history;

CREATE TABLE IF NOT EXISTS nuarpublication.flyway_schema_history
(
    installed_rank integer NOT NULL,
    version character varying(50) COLLATE pg_catalog."default",
    description character varying(200) COLLATE pg_catalog."default" NOT NULL,
    type character varying(20) COLLATE pg_catalog."default" NOT NULL,
    script character varying(1000) COLLATE pg_catalog."default" NOT NULL,
    checksum integer,
    installed_by character varying(100) COLLATE pg_catalog."default" NOT NULL,
    installed_on timestamp without time zone NOT NULL DEFAULT now(),
    execution_time integer NOT NULL,
    success boolean NOT NULL,
    CONSTRAINT flyway_schema_history_pk PRIMARY KEY (installed_rank)
);

ALTER TABLE IF EXISTS nuarpublication.flyway_schema_history
    OWNER to nuar_admin;
-- Index: flyway_schema_history_s_idx

-- DROP INDEX IF EXISTS nuarpublication.flyway_schema_history_s_idx;

CREATE INDEX IF NOT EXISTS flyway_schema_history_s_idx
    ON nuarpublication.flyway_schema_history USING btree
    (success ASC NULLS LAST)
    WITH (fillfactor=100, deduplicate_items=True)
    TABLESPACE pg_default;

SELECT * FROM nuarpublication.flyway_schema_history;

DELETE FROM nuarpublication.flyway_schema_history;

INSERT INTO nuarpublication.flyway_schema_history
(installed_rank, version, description, type, script, checksum, installed_by, installed_on, execution_time, success)
VALUES
(1, '1', '<< Flyway Baseline >>', 'BASELINE', '<< Flyway Baseline >>', NULL, 'nuar_admin', '2025-10-05 08:02:26.270505', 0, TRUE),
(2, '2025.02.13.101300', 'prerequisites', 'SQL', 'V2025.02.13_101300__prerequisites.sql', -981878695, 'nuar_admin', '2025-10-05 08:02:26.317163', 8, TRUE),
(3, '2025.02.13.101311', 'baseline publication functions', 'SQL', 'V2025.02.13_101311__baseline_publication_functions.sql', -1568401492, 'nuar_admin', '2025-10-05 08:02:26.33626', 120, TRUE),
(4, '2025.02.20.120135', 'fn foreign data wrappers', 'SQL', 'V2025.02.20_120135__fn_foreign_data_wrappers.sql', -482709948, 'nuar_admin', '2025-10-05 08:02:26.600299', 3, TRUE),
(5, '2025.02.24.131050', 'validation fix in fn recordcountvalidationrun v211', 'SQL', 'V2025.02.24_131050__validation_fix_in_fn_recordcountvalidationrun_v211.sql', -1322701348, 'nuar_admin', '2025-10-05 08:02:26.613113', 5, TRUE),
(6, '2025.03.21.101311', 'baseline publication functions holding specific', 'SQL', 'V2025.03.21_101311__baseline_publication_functions_holding_specific.sql', -1805022596, 'nuar_admin', '2025-10-05 08:02:26.631409', 30, TRUE),
(7, '2025.04.04.101526', 'job publication exception recording', 'SQL', 'V2025.04.04_101526__job_publication_exception_recording.sql', 1026228965, 'nuar_admin', '2025-10-05 08:02:26.697372', 5, TRUE),
(8, '2025.04.29.153650', 'optional can start check', 'SQL', 'V2025.04.29_153650__optional_can_start_check.sql', 1825612919, 'nuar_admin', '2025-10-05 08:02:26.714116', 43, TRUE),
(9, '2025.05.29.163353', 'fix fn recordpublicationstatus', 'SQL', 'V2025.05.29_163353__fix_fn_recordpublicationstatus.sql', 112379267, 'nuar_admin', '2025-10-05 08:02:26.792533', 3, TRUE),
(10, '2025.05.30.101030', 'disallow duplicate null batch stagestatus', 'SQL', 'V2025.05.30_101030__disallow_duplicate_null_batch_stagestatus.sql', -1055254711, 'nuar_admin', '2025-10-05 08:02:26.80448', 3, TRUE),
(11, '2025.06.02.130308', 'attempts corrections', 'SQL', 'V2025.06.02_130308__attempts_corrections.sql', 1790756912, 'nuar_admin', '2025-10-05 08:02:26.81577', 6, TRUE),
(12, '2025.06.02.150315', 'reorder stagestatus columns', 'SQL', 'V2025.06.02_150315__reorder_stagestatus_columns.sql', -774309816, 'nuar_admin', '2025-10-05 08:02:26.833064', 25, TRUE),
(13, '2025.08.01.095121', 'update held submissions refactor', 'SQL', 'V2025.08.01_095121__update_held_submissions_refactor.sql', -510506003, 'nuar_admin', '2025-10-05 08:02:26.877893', 6, TRUE);

SELECT * FROM nuarpublication.flyway_schema_history;
