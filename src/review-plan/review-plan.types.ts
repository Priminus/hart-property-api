export type ReviewPlanSelections = {
  buyerType: string;
  ageRange: string;
  isFirstPurchase: string;
  cashRange: string;
  cpfRange: string;
  bufferRange: string;
  priceRange: string;
  locationPref: string;
  launchType: string;
  isGoodDecision: string;
  holdingPeriod: string;
  exitOption: string;
  targetBuyer: string;
  stability: string;
};

export type ReviewPlanRequest = {
  email: string;
  name?: string;
  selections: ReviewPlanSelections;
};


