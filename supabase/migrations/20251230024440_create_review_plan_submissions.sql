create extension if not exists pgcrypto;

create table if not exists review_plan_submissions (
  id uuid primary key default gen_random_uuid(),
  created_at timestamptz not null default now(),

  email text not null,
  name text,

  selections jsonb not null,
  market_context jsonb,
  client_capital jsonb,
  assessment jsonb,
  listings jsonb,
  sora_snapshot jsonb,

  status text not null default 'accepted'
);

create index if not exists review_plan_submissions_created_at_idx
  on review_plan_submissions (created_at desc);

