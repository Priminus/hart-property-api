export type ArticleRow = {
  id: string;
  slug: string;
  title: string;
  excerpt: string | null;
  content_mdx: string;
  cover_image_url: string | null;
  published_at: string | null;
  created_at: string;
  updated_at: string;
};


