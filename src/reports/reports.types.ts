import type { LeadAttribution, LeadContext } from '../leads/leads.types';

export type SendMarketOutlook2026Request = {
  email: string;
  utm?: LeadAttribution;
  context?: LeadContext;
};


