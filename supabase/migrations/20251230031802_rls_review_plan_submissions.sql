-- Enable Row Level Security and allow ONLY authenticated users access.
-- Note: This intentionally allows any authenticated user to view/insert/update any row.
-- If you later want per-user scoping, we can tie rows to auth.uid().

alter table if exists public.review_plan_submissions enable row level security;

revoke all on table public.review_plan_submissions from anon;
revoke all on table public.review_plan_submissions from public;

grant select, insert, update on table public.review_plan_submissions to authenticated;

drop policy if exists "authenticated_select_review_plan_submissions" on public.review_plan_submissions;
create policy "authenticated_select_review_plan_submissions"
  on public.review_plan_submissions
  for select
  to authenticated
  using (true);

drop policy if exists "authenticated_insert_review_plan_submissions" on public.review_plan_submissions;
create policy "authenticated_insert_review_plan_submissions"
  on public.review_plan_submissions
  for insert
  to authenticated
  with check (true);

drop policy if exists "authenticated_update_review_plan_submissions" on public.review_plan_submissions;
create policy "authenticated_update_review_plan_submissions"
  on public.review_plan_submissions
  for update
  to authenticated
  using (true)
  with check (true);

