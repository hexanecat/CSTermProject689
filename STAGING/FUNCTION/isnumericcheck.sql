create or replace function public.is_numeric(p_text text)
returns boolean
language sql
immutable
as $$
    select p_text ~ '^[+-]?[0-9]+(\.[0-9]+)?$'
$$;