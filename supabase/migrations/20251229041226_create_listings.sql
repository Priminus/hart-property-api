-- Listings table + Storage bucket for listing photos

create extension if not exists pgcrypto with schema extensions;

create table public.listings (
  id uuid primary key default gen_random_uuid(),
  external_id text not null unique,
  title text not null,
  address text,
  price text,
  psf text,
  size text,
  property_type text,
  status text not null default 'Active',
  distance text,
  link text,
  listed_at date,
  photos jsonb not null default '[]'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create or replace function public.set_listings_updated_at()
returns trigger as $$
begin
  new.updated_at = now();
  return new;
end;
$$ language plpgsql;

create trigger trg_listings_updated_at
before update on public.listings
for each row execute function public.set_listings_updated_at();

-- Public bucket for listing photos (uploaded by ingest script)
insert into storage.buckets (id, name, public)
values ('listings', 'listings', true)
on conflict (id) do update set public = true;


