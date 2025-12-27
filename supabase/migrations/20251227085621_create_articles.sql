-- Articles table + Storage bucket for article assets

-- Ensure pgcrypto is available for gen_random_uuid()
create extension if not exists pgcrypto with schema extensions;

create table public.articles (
  id uuid primary key default gen_random_uuid(),
  slug text not null unique,
  title text not null,
  excerpt text,
  content_mdx text not null,
  cover_image_url text,
  published_at date,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create or replace function public.set_articles_updated_at()
returns trigger as $$
begin
  new.updated_at = now();
  return new;
end;
$$ language plpgsql;

create trigger trg_articles_updated_at
before update on public.articles
for each row execute function public.set_articles_updated_at();

-- Public bucket for article images (used by ingestion script)
insert into storage.buckets (id, name, public)
values ('articles', 'articles', true)
on conflict (id) do update set public = true;


