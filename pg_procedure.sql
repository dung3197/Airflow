CREATE OR REPLACE FUNCTION public.safe_insert_back_account_map(
    p_customer_code TEXT,
    p_account_code  TEXT,
    p_op_type       TEXT,
    p_current_ts    BIGINT
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO public.t_back_account_map (
        "C_CUSTOMER_CODE",
        "C_ACCOUNT_CODE",
        "op_type",
        "current_ts"
    )
    VALUES (
        p_customer_code,
        p_account_code,
        p_op_type,
        p_current_ts
    )
    ON CONFLICT ("C_CUSTOMER_CODE") DO NOTHING;
END;
$$;

PostgreSQL must have a unique key or primary key:

ALTER TABLE public.t_back_account_map
ADD CONSTRAINT t_back_account_map_customer_code_pk
PRIMARY KEY ("C_CUSTOMER_CODE");
