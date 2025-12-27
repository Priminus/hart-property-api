import { Inject, Injectable } from "@nestjs/common";
import type { SupabaseClient } from "@supabase/supabase-js";
import { SUPABASE_CLIENT } from "../supabase/supabase.constants";
import type { ArticleRow } from "./articles.types";

@Injectable()
export class ArticlesService {
  constructor(@Inject(SUPABASE_CLIENT) private readonly supabase: SupabaseClient) {}

  async list() {
    const { data, error } = await this.supabase
      .from("articles")
      .select("slug,title,excerpt,cover_image_url,published_at,created_at,updated_at")
      .order("published_at", { ascending: false, nullsFirst: false });

    if (error) throw new Error(error.message);
    return data ?? [];
  }

  async getBySlug(slug: string): Promise<ArticleRow> {
    const { data, error } = await this.supabase
      .from("articles")
      .select("*")
      .eq("slug", slug)
      .maybeSingle();

    if (error) throw new Error(error.message);
    if (!data) throw new Error(`Article not found: ${slug}`);
    return data as ArticleRow;
  }
}


