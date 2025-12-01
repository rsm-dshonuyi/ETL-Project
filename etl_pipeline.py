import snowflake.connector
import os
import subprocess
import psycopg2
from pathlib import Path

# Snowflake connection configuration
conn = snowflake.connector.connect(
    user='MY_USERNAME',
    password='MY_PASSWORD',
    account='MY_ACCOUNT',
    warehouse="MY_FIRST_WAREHOUSE",
    database="PROJECTDB",
    schema="PROJECTSCHEMA"
)

cs = conn.cursor()

cs.execute("CREATE DATABASE IF NOT EXISTS projectdb")

cs.execute("CREATE SCHEMA IF NOT EXISTS projectschema")

with conn.cursor() as cs:
    cs.execute("CREATE STAGE IF NOT EXISTS pgstage")

# ==============================================================================
# SECTION 1: Load XML Data (Supplier Transactions)
# ==============================================================================

XML_FILE = "/home/jovyan/Downloads/rsm-ict/python/DataForSnowflakeProj/Supplier Transactions XML.xml"

with conn.cursor() as cs:
    cs.execute("USE DATABASE projectdb")
    cs.execute("USE SCHEMA projectschema")
    cs.execute("CREATE FILE FORMAT IF NOT EXISTS xml_fmt TYPE=XML")
    cs.execute("CREATE TABLE IF NOT EXISTS supplier_transactions_raw (doc VARIANT)")

    # upload the local XML (no gzip), then load it
    cs.execute(f"PUT 'file://{XML_FILE}' @pgstage AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
    cs.execute("COPY INTO supplier_transactions_raw "
               "FROM @pgstage FILE_FORMAT=(FORMAT_NAME=xml_fmt) "
               "PATTERN='.*Supplier Transactions XML\\.xml'")

print("XML loaded into projectdb.projectschema.supplier_transactions_raw")

# Create view for supplier transactions
with conn.cursor() as cs:
    cs.execute("USE DATABASE projectdb")
    cs.execute("USE SCHEMA projectschema")

    cs.execute("""
        CREATE OR REPLACE VIEW supplier_transactions AS
        WITH row_nodes AS (
          SELECT XMLGET(s.doc, 'row', f.value::NUMBER) AS v
          FROM supplier_transactions_raw s,
               LATERAL FLATTEN(input => TO_ARRAY(GET(s.doc, 'row'))) f
        )
        SELECT
          TRY_TO_NUMBER(TO_VARCHAR(XMLGET(v,'SupplierTransactionID'):"$")) AS supplier_transaction_id,
          TRY_TO_NUMBER(TO_VARCHAR(XMLGET(v,'SupplierID'):"$"))            AS supplier_id,
          TRY_TO_NUMBER(TO_VARCHAR(XMLGET(v,'TransactionTypeID'):"$"))     AS transaction_type_id,
          TRY_TO_NUMBER(TO_VARCHAR(XMLGET(v,'PurchaseOrderID'):"$"))       AS purchase_order_id,
          TRY_TO_NUMBER(TO_VARCHAR(XMLGET(v,'PaymentMethodID'):"$"))       AS payment_method_id,
          TO_VARCHAR(XMLGET(v,'SupplierInvoiceNumber'):"$")                AS supplier_invoice_number,
          TRY_TO_DATE(TO_VARCHAR(XMLGET(v,'TransactionDate'):"$"))         AS transaction_date,
          TRY_TO_NUMBER(TO_VARCHAR(XMLGET(v,'AmountExcludingTax'):"$"))    AS amount_excluding_tax,
          TRY_TO_NUMBER(TO_VARCHAR(XMLGET(v,'TaxAmount'):"$"))             AS tax_amount,
          TRY_TO_NUMBER(TO_VARCHAR(XMLGET(v,'TransactionAmount'):"$"))     AS transaction_amount,
          TRY_TO_NUMBER(TO_VARCHAR(XMLGET(v,'OutstandingBalance'):"$"))    AS outstanding_balance,
          TRY_TO_DATE(TO_VARCHAR(XMLGET(v,'FinalizationDate'):"$"))        AS finalization_date,
          (TRY_TO_NUMBER(TO_VARCHAR(XMLGET(v,'IsFinalized'):"$")) = 1)     AS is_finalized,
          TRY_TO_NUMBER(TO_VARCHAR(XMLGET(v,'LastEditedBy'):"$"))          AS last_edited_by,
          TRY_TO_TIMESTAMP_NTZ(
            TO_VARCHAR(XMLGET(v,'LastEditedWhen'):"$"),
            'YYYY-MM-DD HH24:MI:SS.FF7'
          ) AS last_edited_when
        FROM row_nodes
        WHERE XMLGET(v,'SupplierTransactionID') IS NOT NULL
    """)

    # sanity peek
    cs.execute("SELECT COUNT(*) FROM supplier_transactions")
    print("rows:", cs.fetchone()[0])
    cs.execute("""
        SELECT supplier_transaction_id, supplier_id, transaction_amount, transaction_date
        FROM supplier_transactions
        WHERE supplier_transaction_id IS NOT NULL
        ORDER BY supplier_transaction_id
        LIMIT 10
    """)
    print(cs.fetchall())


# ==============================================================================
# SECTION 2: Load ZCTA 2021 Data (Tab-delimited TXT)
# ==============================================================================

TXT_FILE = "/home/jovyan/Downloads/rsm-ict/python/DataForSnowflakeProj/2021_Gaz_zcta_national.txt"

with conn.cursor() as cs:
    cs.execute("USE DATABASE projectdb")
    cs.execute("USE SCHEMA projectschema")

    # Tab-delimited with header
    cs.execute("""
        CREATE OR REPLACE FILE FORMAT zcta_fmt
        TYPE=CSV
        FIELD_DELIMITER='\\t'
        PARSE_HEADER=TRUE
        TRIM_SPACE=TRUE
        EMPTY_FIELD_AS_NULL=TRUE
        NULL_IF=('','\\N','NULL')
    """)

    # Upload the local file (no gzip)
    cs.execute(f"PUT 'file://{TXT_FILE}' @pgstage AUTO_COMPRESS=FALSE OVERWRITE=TRUE")

    # Create table from the single staged file's inferred schema
    cs.execute("""
        CREATE OR REPLACE TABLE zcta_2021_raw USING TEMPLATE (
          SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
          FROM TABLE(
            INFER_SCHEMA(
              LOCATION=>'@pgstage/2021_Gaz_zcta_national.txt',
              FILE_FORMAT=>'zcta_fmt'
            )
          )
        )
    """)

    # Load the data (match by column names from the header)
    cs.execute("""
        COPY INTO zcta_2021_raw
        FROM @pgstage/2021_Gaz_zcta_national.txt
        FILE_FORMAT=(FORMAT_NAME=zcta_fmt)
        MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
    """)

    cs.execute("SELECT COUNT(*) FROM zcta_2021_raw")
    print("Rows loaded:", cs.fetchone()[0])

print("Done.")

# Create ZCTA 2021 view
with conn.cursor() as cs:
    cs.execute("USE DATABASE projectdb")
    cs.execute("USE SCHEMA projectschema")
    cs.execute("""
        CREATE OR REPLACE VIEW zcta_2021 AS
        SELECT
            GEOID::STRING                            AS zcta5,
            GEOID::STRING                            AS geoid,
            ALAND                                     AS land_area_m2,   -- already NUMBER
            AWATER                                    AS water_area_m2,  -- already NUMBER
            ALAND_SQMI::FLOAT                         AS land_area_sqmi,
            AWATER_SQMI::FLOAT                        AS water_area_sqmi,
            TRY_TO_DOUBLE(NULLIF(TRIM(TO_VARCHAR(INTPTLAT)),  ''))  AS centroid_lat,
            TRY_TO_DOUBLE(NULLIF(TRIM(TO_VARCHAR(INTPTLONG)), ''))  AS centroid_lon,
            (ALAND_SQMI::FLOAT + AWATER_SQMI::FLOAT)  AS total_area_sqmi
        FROM zcta_2021_raw
    """)


# ==============================================================================
# SECTION 3: Load Monthly PO Data (CSV Files)
# ==============================================================================

CSV_DIR = "/home/jovyan/Downloads/rsm-ict/python/DataForSnowflakeProj/Monthly PO Data"

with conn.cursor() as cs:
    # scope & one-time objects
    cs.execute("USE DATABASE projectdb")
    cs.execute("USE SCHEMA projectschema")
    cs.execute("CREATE STAGE IF NOT EXISTS pgstage")

    # 1) File format for COPY (header row)
    cs.execute("""
        CREATE OR REPLACE FILE FORMAT po_csv_fmt
        TYPE=CSV
        PARSE_HEADER=TRUE
        FIELD_OPTIONALLY_ENCLOSED_BY='"'
        TRIM_SPACE=TRUE
        EMPTY_FIELD_AS_NULL=TRUE
        NULL_IF=('','\\N','NULL')
    """)

    # 2) Upload all CSVs from the folder to a subdir on the stage
    cs.execute(f"PUT 'file://{CSV_DIR}/*.csv' @pgstage/po_monthly AUTO_COMPRESS=FALSE OVERWRITE=TRUE")

    # 3) Create a single table from inferred schema across ALL staged CSVs
    cs.execute("""
        CREATE OR REPLACE TABLE po_monthly USING TEMPLATE (
          SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
          FROM TABLE(
            INFER_SCHEMA(
              LOCATION=>'@pgstage/po_monthly',
              FILE_FORMAT=>'po_csv_fmt'
            )
          )
        )
    """)

    # 4) Load everything into that table (this "aggregates" all files)
    cs.execute("""
        COPY INTO po_monthly
        FROM @pgstage/po_monthly
        FILE_FORMAT=(FORMAT_NAME=po_csv_fmt)
        MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
        ON_ERROR='CONTINUE'
    """)

    # 5) Quick verification
    cs.execute("SELECT COUNT(*) AS total_row_count FROM po_monthly")
    print(cs.fetchall())

    cs.execute("""
        SELECT
          COUNT(DISTINCT FILE_NAME) AS files_loaded,
          SUM(ROW_COUNT)           AS total_row_count
        FROM TABLE(
          INFORMATION_SCHEMA.COPY_HISTORY(
            TABLE_NAME => 'PROJECTDB.PROJECTSCHEMA.PO_MONTHLY',
            START_TIME => DATEADD('year', -1, CURRENT_TIMESTAMP())
          )
        )
    """)
    print(cs.fetchall())


# ==============================================================================
# SECTION 4: Add POAmount Column
# ==============================================================================

with conn.cursor() as cs:
    cs.execute("""
        ALTER TABLE po_monthly
        ADD COLUMN "POAmount" NUMBER(18,2)
    """)


with conn.cursor() as cs:
    cs.execute("""
    UPDATE po_monthly
    SET "POAmount" = ROUND(
            COALESCE("ReceivedOuters", 0)
        * COALESCE("ExpectedUnitPricePerOuter", 0)
        , 2)
    """)


# ==============================================================================
# SECTION 5: Create Supplier Invoice Data Table
# ==============================================================================

with conn.cursor() as cs:
    # 1) Create the final typed table (one row per invoice)
    cs.execute("""
        CREATE OR REPLACE TABLE supplier_invoice_data (
            supplier_transaction_id NUMBER,
            supplier_id             NUMBER,
            transaction_type_id     NUMBER,
            purchase_order_id       NUMBER,
            payment_method_id       NUMBER,
            supplier_invoice_number STRING,
            transaction_date        DATE,
            amount_excluding_tax    NUMBER(18,4),
            tax_amount              NUMBER(18,4),
            transaction_amount      NUMBER(18,4),
            outstanding_balance     NUMBER(18,4),
            finalization_date       DATE,
            is_finalized            BOOLEAN,
            last_edited_by          NUMBER,
            last_edited_when        TIMESTAMP_NTZ
        )
    """)

with conn.cursor() as cs:
    # 2) Shred from RAW (doc VARIANT) into the typed table using INSERT ... SELECT
    cs.execute("""
        INSERT INTO supplier_invoice_data
        SELECT
            TRY_TO_NUMBER(TO_VARCHAR(XMLGET(v,'SupplierTransactionID'):"$")) AS supplier_transaction_id,
            TRY_TO_NUMBER(TO_VARCHAR(XMLGET(v,'SupplierID'):"$"))            AS supplier_id,
            TRY_TO_NUMBER(TO_VARCHAR(XMLGET(v,'TransactionTypeID'):"$"))     AS transaction_type_id,
            TRY_TO_NUMBER(TO_VARCHAR(XMLGET(v,'PurchaseOrderID'):"$"))       AS purchase_order_id,
            TRY_TO_NUMBER(TO_VARCHAR(XMLGET(v,'PaymentMethodID'):"$"))       AS payment_method_id,
            TO_VARCHAR(XMLGET(v,'SupplierInvoiceNumber'):"$")                AS supplier_invoice_number,
            TRY_TO_DATE(TO_VARCHAR(XMLGET(v,'TransactionDate'):"$"))         AS transaction_date,
            TRY_TO_NUMBER(TO_VARCHAR(XMLGET(v,'AmountExcludingTax'):"$"))    AS amount_excluding_tax,
            TRY_TO_NUMBER(TO_VARCHAR(XMLGET(v,'TaxAmount'):"$"))             AS tax_amount,
            TRY_TO_NUMBER(TO_VARCHAR(XMLGET(v,'TransactionAmount'):"$"))     AS transaction_amount,
            TRY_TO_NUMBER(TO_VARCHAR(XMLGET(v,'OutstandingBalance'):"$"))    AS outstanding_balance,
            TRY_TO_DATE(TO_VARCHAR(XMLGET(v,'FinalizationDate'):"$"))        AS finalization_date,
            (TRY_TO_NUMBER(TO_VARCHAR(XMLGET(v,'IsFinalized'):"$")) = 1)     AS is_finalized,
            TRY_TO_NUMBER(TO_VARCHAR(XMLGET(v,'LastEditedBy'):"$"))          AS last_edited_by,
            TRY_TO_TIMESTAMP_NTZ(
              TO_VARCHAR(XMLGET(v,'LastEditedWhen'):"$"),
              'YYYY-MM-DD HH24:MI:SS.FF7'
            ) AS last_edited_when
        FROM supplier_transactions_raw r,
             LATERAL FLATTEN(input => TO_ARRAY(GET(r.doc,'row'))) f,
             LATERAL (SELECT XMLGET(r.doc,'row', f.value::NUMBER)) t(v)
        WHERE XMLGET(v,'SupplierTransactionID') IS NOT NULL
    """)

with conn.cursor() as cs:
   # 3) Quick checks
    cs.execute("SELECT COUNT(*) FROM supplier_invoice_data")
    print("rows:", cs.fetchone()[0])


    cs.execute("""
        SELECT supplier_transaction_id, supplier_id, transaction_amount, transaction_date
        FROM supplier_invoice_data
        ORDER BY supplier_transaction_id
        LIMIT 10
    """)
    print(cs.fetchall())


# ==============================================================================
# SECTION 6: Create Purchase Orders and Invoices View/Table
# ==============================================================================

with conn.cursor() as cs:
    # 5) Using purchases + invoices, compute PO totals and invoiced_vs_quoted
    # Try to create an MV named purchase_orders_and_invoices (base tables only).
    # If MV creation is not supported in your edition, create a TABLE instead.
    try:
        cs.execute("""
            CREATE OR REPLACE MATERIALIZED VIEW purchase_orders_and_invoices AS
            SELECT
                i.supplier_transaction_id,
                i.supplier_id,
                i.purchase_order_id,
                i.transaction_date,
                CAST(
                  SUM(
                    COALESCE(m."POAmount",
                             COALESCE(m."ReceivedOuters", 0) * COALESCE(m."ExpectedUnitPricePerOuter", 0))
                  ) AS NUMBER(18,2)
                ) AS po_total_amount,
                CAST(i.amount_excluding_tax AS NUMBER(18,2)) AS amount_excluding_tax,
                CAST(
                  SUM(
                    COALESCE(m."POAmount",
                             COALESCE(m."ReceivedOuters", 0) * COALESCE(m."ExpectedUnitPricePerOuter", 0))
                  ) - COALESCE(i.amount_excluding_tax, 0)
                  AS NUMBER(18,2)
                ) AS invoiced_vs_quoted
            FROM supplier_invoice_data i
            JOIN po_monthly m
              ON m."PurchaseOrderID" = i.purchase_order_id
             AND m."SupplierID"      = i.supplier_id
            GROUP BY
                i.supplier_transaction_id,
                i.supplier_id,
                i.purchase_order_id,
                i.transaction_date,
                i.amount_excluding_tax
        """)
        print("✓ Created MATERIALIZED VIEW purchase_orders_and_invoices")
    except Exception:
        cs.execute("DROP TABLE IF EXISTS purchase_orders_and_invoices")
        cs.execute("""
            CREATE TABLE purchase_orders_and_invoices AS
            SELECT
                i.supplier_transaction_id,
                i.supplier_id,
                i.purchase_order_id,
                i.transaction_date,
                CAST(
                  SUM(
                    COALESCE(m."POAmount",
                             COALESCE(m."ReceivedOuters", 0) * COALESCE(m."ExpectedUnitPricePerOuter", 0))
                  ) AS NUMBER(18,2)
                ) AS po_total_amount,
                CAST(i.amount_excluding_tax AS NUMBER(18,2)) AS amount_excluding_tax,
                CAST(
                  SUM(
                    COALESCE(m."POAmount",
                             COALESCE(m."ReceivedOuters", 0) * COALESCE(m."ExpectedUnitPricePerOuter", 0))
                  ) - COALESCE(i.amount_excluding_tax, 0)
                  AS NUMBER(18,2)
                ) AS invoiced_vs_quoted
            FROM supplier_invoice_data i
            JOIN po_monthly m
              ON m."PurchaseOrderID" = i.purchase_order_id
             AND m."SupplierID"      = i.supplier_id
            GROUP BY
                i.supplier_transaction_id,
                i.supplier_id,
                i.purchase_order_id,
                i.transaction_date,
                i.amount_excluding_tax
        """)
        print("✓ Created TABLE purchase_orders_and_invoices (fallback)")

with conn.cursor() as cs:
    cs.execute("SELECT COUNT(*) FROM purchase_orders_and_invoices"); print(cs.fetchone())
    cs.execute("""
        SELECT supplier_transaction_id, purchase_order_id, po_total_amount,
               amount_excluding_tax, invoiced_vs_quoted, transaction_date
        FROM purchase_orders_and_invoices
        ORDER BY ABS(purchase_order_id) DESC NULLS LAST
        LIMIT 10
    """); print(cs.fetchall())


# ==============================================================================
# SECTION 7: PostgreSQL Integration - Supplier Case Data
# ==============================================================================

PG = dict(
    host="127.0.0.1",        # or your PG host
    port=8765,
    dbname="rsm-docker",
    user="jovyan",
    password="postgres",
)

SQL_FILE = "/home/jovyan/Downloads/rsm-ict/python/DataForSnowflakeProj/supplier_case.pgsql"

env = os.environ.copy()
env.update({
    "PGHOST": PG["host"],
    "PGPORT": str(PG["port"]),
    "PGUSER": PG["user"],
    "PGDATABASE": PG["dbname"],
    "PGPASSWORD": PG["password"],
})

subprocess.run(["psql", "-f", SQL_FILE], check=True, env=env)
print("✓ Ran supplier_case.pgsql into Postgres.")


CSV_OUT = Path("/home/jovyan/Downloads/rsm-ict/python/DataForSnowflakeProj/supplier_case.csv")

with psycopg2.connect(**PG) as pg_conn:
    with pg_conn.cursor() as cur, open(CSV_OUT, "w", newline="") as f:
        # schema-qualify; default is public unless your script chose another
        cur.copy_expert("COPY public.supplier_case TO STDOUT WITH CSV HEADER", f)

print(f"✓ Exported to {CSV_OUT}")

# Load supplier_case into Snowflake
with conn.cursor() as cs:
    cs.execute("USE DATABASE projectdb")
    cs.execute("USE SCHEMA projectschema")
    cs.execute("CREATE STAGE IF NOT EXISTS pgstage")

    cs.execute("""
        CREATE OR REPLACE FILE FORMAT csv_std
        TYPE=CSV
        PARSE_HEADER=TRUE
        FIELD_OPTIONALLY_ENCLOSED_BY='"'
        TRIM_SPACE=TRUE
        EMPTY_FIELD_AS_NULL=TRUE
        NULL_IF=('','\\N','NULL')
    """)

    # Upload the CSV
    cs.execute(f"PUT 'file://{CSV_OUT}' @pgstage OVERWRITE=TRUE AUTO_COMPRESS=FALSE")

    # Create the table from the file's inferred schema
    cs.execute("""
        CREATE OR REPLACE TABLE supplier_case USING TEMPLATE (
          SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
          FROM TABLE(
            INFER_SCHEMA(
              LOCATION=>'@pgstage/supplier_case.csv',
              FILE_FORMAT=>'csv_std'
            )
          )
        )
    """)

    # Load the data
    cs.execute("""
        COPY INTO supplier_case
        FROM @pgstage/supplier_case.csv
        FILE_FORMAT=(FORMAT_NAME=csv_std)
        MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
        ON_ERROR='CONTINUE'
    """)

    cs.execute("SELECT COUNT(*) FROM supplier_case")
    print("row_count:", cs.fetchone()[0])


# ==============================================================================
# SECTION 8: Weather Pipeline with 5-Digit ZIP Normalization
# ==============================================================================

def _show(cs, sql):
    cs.execute(sql); return cs.fetchall()

def find_relation_fqn(cs, name_upper: str):
    """Find first DB.SCHEMA.OBJECT (TABLE or VIEW) named name_upper (case-insensitive)."""
    for db in [r[1] for r in _show(cs, "SHOW DATABASES")]:
        try:
            for sch in [r[1] for r in _show(cs, f"SHOW SCHEMAS IN DATABASE {db}")]:
                try:
                    for t in [r[1] for r in _show(cs, f"SHOW TABLES IN SCHEMA {db}.{sch}")]:
                        if t.upper() == name_upper.upper():
                            return f"{db}.{sch}.{t}"
                except: pass
                try:
                    for v in [r[1] for r in _show(cs, f"SHOW VIEWS IN SCHEMA {db}.{sch}")]:
                        if v.upper() == name_upper.upper():
                            return f"{db}.{sch}.{v}"
                except: pass
        except: pass
    return None

def desc_relation(cs, fqn: str):
    """DESC TABLE first; fallback to DESC VIEW."""
    try:
        cs.execute(f"DESC TABLE {fqn}"); return cs.fetchall()
    except:
        cs.execute(f"DESC VIEW {fqn}");  return cs.fetchall()

def pick_col(cs, fqn: str, candidates):
    cols = {r[0].upper() for r in desc_relation(cs, fqn)}
    for c in candidates:
        if c in cols: return c
    return next(iter(cols))  # fallback to any column to fail fast later

# --- REBUILD WEATHER PIPELINE WITH 5-DIGIT ZIP NORMALIZATION ---

with conn.cursor() as cs:
    # 7a) Supplier ZIPs (strict 5-digit from any ZIP+4 or messy strings)
    cs.execute("""
        CREATE OR REPLACE VIEW VW_SUPPLIER_ZIPS AS
        SELECT DISTINCT
            LPAD(SUBSTR(REGEXP_REPLACE(TRIM("postalpostalcode"), '[^0-9]', ''), 1, 5), 5, '0') AS ZIP
        FROM supplier_case
        WHERE "postalpostalcode" IS NOT NULL
          AND REGEXP_REPLACE(TRIM("postalpostalcode"), '[^0-9]', '') <> ''
    """)

    # ZCTA centroids unchanged
    cs.execute("""
        CREATE OR REPLACE VIEW VW_ZIP_LATLON AS
        SELECT
          LPAD(TRIM(zcta5), 5, '0')        AS ZIP,
          TRY_TO_DOUBLE(centroid_lat)      AS ZIP_LAT,
          TRY_TO_DOUBLE(centroid_lon)      AS ZIP_LON
        FROM zcta_2021
        WHERE zcta5 IS NOT NULL
    """)

    # Nearest station per ZIP
    stations_fqn   = find_relation_fqn(cs, "NOAA_WEATHER_STATION_INDEX")
    timeseries_fqn = find_relation_fqn(cs, "NOAA_WEATHER_METRICS_TIMESERIES")
    if not stations_fqn or not timeseries_fqn:
        raise RuntimeError("NOAA Marketplace objects not found/authorized. Ensure subscription & privileges, then re-run.")

    date_col = pick_col(cs, timeseries_fqn, ("DATE","OBSERVATION_DATE","DAY","OBS_DATE"))
    high_col = pick_col(cs, timeseries_fqn, ("TMAX","MAX_TEMP","DAILY_MAX_TEMPERATURE","TMAX_F","TMAX_C"))

    cs.execute("""
        CREATE OR REPLACE VIEW VW_ZIP_STATION_NEAREST AS
        WITH cand AS (
          SELECT
            zl.ZIP,
            si.NOAA_WEATHER_STATION_ID,
            2 * 6371 * ASIN(SQRT(
              POWER(SIN(RADIANS((zl.ZIP_LAT - TRY_TO_DOUBLE(si.LATITUDE))/2)), 2) +
              COS(RADIANS(zl.ZIP_LAT)) * COS(RADIANS(TRY_TO_DOUBLE(si.LATITUDE))) *
              POWER(SIN(RADIANS((zl.ZIP_LON - TRY_TO_DOUBLE(si.LONGITUDE))/2)), 2)
            )) AS DIST_KM
          FROM VW_SUPPLIER_ZIPS sz
          JOIN VW_ZIP_LATLON zl ON zl.ZIP = sz.ZIP
          JOIN {stations_fqn} si
            ON si.LATITUDE IS NOT NULL AND si.LONGITUDE IS NOT NULL
        )
        SELECT ZIP, NOAA_WEATHER_STATION_ID
        FROM (
          SELECT ZIP, NOAA_WEATHER_STATION_ID, DIST_KM,
                 ROW_NUMBER() OVER (PARTITION BY ZIP ORDER BY DIST_KM) AS rn
          FROM cand
        )
        WHERE rn = 1
    """.format(stations_fqn=stations_fqn))

    # Rebuild the weather MV/table to carry 5-digit ZIPs
    try:
        cs.execute("DROP MATERIALIZED VIEW IF EXISTS supplier_zip_code_weather")
    except Exception:
        pass
    cs.execute("DROP TABLE IF EXISTS supplier_zip_code_weather")

    try:
        cs.execute(f"""
            CREATE MATERIALIZED VIEW supplier_zip_code_weather AS
            SELECT
              z.ZIP,
              ts.{date_col}::DATE AS WX_DATE,
              ts.{high_col}       AS HIGH_TEMP
            FROM VW_ZIP_STATION_NEAREST z
            JOIN {timeseries_fqn} ts
              ON ts.NOAA_WEATHER_STATION_ID = z.NOAA_WEATHER_STATION_ID
            WHERE ts.{high_col} IS NOT NULL
        """)
    except Exception:
        cs.execute(f"""
            CREATE TABLE supplier_zip_code_weather AS
            SELECT
              z.ZIP,
              ts.{date_col}::DATE AS WX_DATE,
              ts.{high_col}       AS HIGH_TEMP
            FROM VW_ZIP_STATION_NEAREST z
            JOIN {timeseries_fqn} ts
              ON ts.NOAA_WEATHER_STATION_ID = z.NOAA_WEATHER_STATION_ID
            WHERE ts.{high_col} IS NOT NULL
        """)

    # Quick overlap sanity check (optional)
    cs.execute("SELECT MIN(WX_DATE), MAX(WX_DATE), COUNT(*) FROM supplier_zip_code_weather")
    print("weather date range & rows:", cs.fetchone())


# ==============================================================================
# SECTION 9: Final Join - PO, Invoice, Supplier, and Weather Data
# ==============================================================================

with conn.cursor() as cs:
    # 5-digit ZIP for suppliers (ZIP+4 safe)
    cs.execute("""
        CREATE OR REPLACE VIEW VW_SUPPLIER_ZIP5 AS
        SELECT
            "supplierid" AS supplier_id,
            LPAD(SUBSTR(REGEXP_REPLACE(TRIM("postalpostalcode"),'[^0-9]',''),1,5),5,'0') AS zip5
        FROM supplier_case
        WHERE "postalpostalcode" IS NOT NULL AND TRIM("postalpostalcode") <> ''
    """)

    cs.execute("DROP TABLE IF EXISTS FINAL_PO_INV_SUPPLIER_WEATHER")
    cs.execute("""
        CREATE TABLE FINAL_PO_INV_SUPPLIER_WEATHER AS
        SELECT
            p.purchase_order_id,
            p.supplier_id,
            z.ZIP5                     AS zip_code,
            p.transaction_date         AS wx_date,
            p.po_total_amount,
            p.amount_excluding_tax,
            p.invoiced_vs_quoted,
            w.HIGH_TEMP
        FROM purchase_orders_and_invoices p
        JOIN VW_SUPPLIER_ZIP5 z
          ON z.supplier_id = p.supplier_id
        JOIN supplier_zip_code_weather w
          ON w.ZIP = z.ZIP5
         AND w.WX_DATE = p.transaction_date
    """)

    cs.execute("SELECT COUNT(*) FROM FINAL_PO_INV_SUPPLIER_WEATHER")
    print("final joined rows:", cs.fetchone()[0])

    cs.execute("""
        SELECT purchase_order_id, supplier_id, zip_code, wx_date,
               po_total_amount, amount_excluding_tax, invoiced_vs_quoted, HIGH_TEMP
        FROM FINAL_PO_INV_SUPPLIER_WEATHER
        ORDER BY purchase_order_id
        LIMIT 10
    """)
    print(cs.fetchall())
