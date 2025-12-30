export type LeadAttribution = {
  utm_source?: string;
  utm_medium?: string;
  utm_campaign?: string;
  utm_term?: string;
  utm_content?: string;
};

export type LeadContext = {
  entry_page?: string;
  referrer?: string;
  first_seen_at?: string; // ISO string
};

export type CreateLeadRequest = {
  name?: string;
  email?: string;
  phone?: string;
  utm?: LeadAttribution;
  context?: LeadContext;
};


