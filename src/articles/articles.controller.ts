import { Controller, Get, Param } from "@nestjs/common";
import { ArticlesService } from "./articles.service";

@Controller("articles")
export class ArticlesController {
  constructor(private readonly articles: ArticlesService) {}

  @Get()
  async list() {
    return this.articles.list();
  }

  @Get(":slug")
  async getBySlug(@Param("slug") slug: string) {
    return this.articles.getBySlug(slug);
  }
}


