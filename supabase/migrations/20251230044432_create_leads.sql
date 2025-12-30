create extension if not exists pgcrypto;

create table if not exists public.leads (
  id uuid primary key default gen_random_uuid(),
  created_at timestamptz not null default now(),

  name text,
  email text,
  phone text,

  -- attribution
  utm_source text,
  utm_medium text,
  utm_campaign text,
  utm_term text,
  utm_content text,

  -- context
  entry_page text,
  referrer text,
  first_seen_at timestamptz
);

create index if not exists leads_created_at_idx on public.leads (created_at desc);
create index if not exists leads_email_idx on public.leads (email);

-- Lock down reads by default. Inserts will be performed server-side.
alter table if exists public.leads enable row level security;

revoke all on table public.leads from anon;
revoke all on table public.leads from public;

grant select on table public.leads to authenticated;

drop policy if exists "authenticated_select_leads" on public.leads;
create policy "authenticated_select_leads"
  on public.leads
  for select
  to authenticated
  using (true);

