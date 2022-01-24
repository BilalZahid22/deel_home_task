contracts_raw = """CREATE TABLE IF NOT EXISTS public.CONTRACTS_RAW
(
    ID serial PRIMARY KEY,
    CONTRACT_ID VARCHAR(1000),
    CLIENT_ID varchar(1000),
    CONTRACT_CREATED_AT varchar(1000),
    STATUS varchar(1000),
    COMPLETION_DATE varchar(1000),
    IS_DELETED varchar(1000),
    RECEIVED_AT varchar(1000)
);"""


invoices_raw = """CREATE TABLE IF NOT EXISTS public.INVOICES_RAW
(
    ID serial PRIMARY KEY,
    INVOICE_ID varchar(1000),
    CONTRACT_ID varchar(1000),
    AMOUNT varchar(1000),
    CURRENCY varchar(10),
    IS_EARLY_PAID varchar(1000),
    IS_DELETED varchar(1000),
    RECEIVED_AT varchar(1000)
);"""


contracts = """CREATE TABLE IF NOT EXISTS public.CONTRACTS
(
    ID serial PRIMARY KEY,
    CONTRACT_ID varchar(100),
    CLIENT_ID varchar(100),
    CONTRACT_CREATED_AT timestamp with time zone,
    STATUS varchar(100),
    COMPLETION_DATE timestamp with time zone,
    IS_DELETED boolean DEFAULT false,
    RECEIVED_AT timestamp with time zone,
    IS_CURRENT boolean DEFAULT true,
    CREATED_ON timestamp DEFAULT current_timestamp,
    CREATED_BY varchar(100),
    MODIFIED_ON timestamp DEFAULT current_timestamp,
    MODIFIED_BY varchar(100)
);"""


invoices = """CREATE TABLE IF NOT EXISTS public.INVOICES
(
    ID serial PRIMARY KEY ,
    INVOICE_ID varchar(100),
    CONTRACT_ID varchar(100),
    AMOUNT double precision,
    CURRENCY varchar(10),
    IS_EARLY_PAID boolean DEFAULT false,
    IS_DELETED boolean DEFAULT false,
    RECEIVED_AT timestamp with time zone,
    IS_CURRENT boolean DEFAULT true,
    CREATED_ON timestamp DEFAULT current_timestamp,
    CREATED_BY varchar(100),
    MODIFIED_ON timestamp DEFAULT current_timestamp,
    MODIFIED_BY varchar(100)
);"""


sp_contracts = """create or replace procedure sp_load_dim_contracts()
    language plpgsql
as
$$
begin

    -- insert new records with default is_current to true

    insert into contracts(contract_id, client_id, contract_created_at, status, completion_date, received_at, is_deleted,
                          created_by)
    select nullif(A.contract_id, ''),
           nullif(A.client_id, ''),
           nullif(A.contract_created_at, '')::timestamptz,
           nullif(A.status, ''),
           nullif(A.completion_date, '')::timestamptz,
           nullif(A.received_at, '')::timestamptz,
           A.is_deleted::boolean,
           'ELT.load_dim_contracts()'
    from contracts_raw A
             left join contracts B on A.contract_id = B.contract_id
        and A.received_at <> B.received_at::varchar
    where B.contract_id is NULL;

    -- set current record to true and others to false
    drop table if exists contracts_current_temp;
    create temp table contracts_current_temp as
        (select id,
                row_number()
                over (PARTITION BY contract_id order by received_at desc) as dup_row
         from contracts);


    update contracts
    set is_current  = false,
        modified_by = 'ELT.load_dim_contracts()',
        modified_on = current_timestamp
    where id not in
          (select id from contracts_current_temp where dup_row = 1);

    truncate contracts_raw;
end
$$
;"""


sp_invoices = """create or replace procedure sp_load_invoices()
    language plpgsql
as
$$
begin

    -- insert new records with default is_current to true

    insert into invoices(invoice_id, contract_id, amount, currency, received_at, created_by, is_deleted)

    select nullif(A.invoice_id, ''),
           nullif(A.contract_id, ''),
           nullif(A.amount, '')::double precision,
           nullif(A.currency, ''),
           nullif(A.received_at, '')::timestamptz,
           'ELT.sp_load_invoices()',
           nullif(A.is_deleted, '')::boolean
    from invoices_raw A
             left join invoices B on A.invoice_id = B.invoice_id
        and A.received_at <> B.received_at::varchar
    where B.invoice_id is NULL;

    -- set current record to true and others to false
    drop table if exists invoices_current_temp;
    create temp table invoices_current_temp as
        (select id,
                row_number()
                over (PARTITION BY invoice_id order by received_at desc) as dup_row
         from invoices);


    update invoices
    set is_current  = false,
        modified_by = 'ELT.sp_load_invoices()',
        modified_on = current_timestamp
    where id not in
          (select id from invoices_current_temp where dup_row = 1);

    truncate invoices_raw;
end
$$
;
"""
