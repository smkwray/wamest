import { createContext, useContext, useEffect, useState, type ReactNode } from "react";

export interface SiteData {
  hero: {
    sectors_covered: string;
    quarters: number;
    snapshot_quarter: string;
    published_rows: number;
    data_sources: number;
  };
  snapshot: Array<{
    sector: string;
    sector_key: string;
    bill_share: number | null;
    maturity: number | null;
    bill_share_lower: number | null;
    bill_share_upper: number | null;
    maturity_lower: number | null;
    maturity_upper: number | null;
    level_tier: string;
    maturity_tier: string;
    method: string;
    high_confidence: boolean;
    point_estimate_origin: string;
    interval_origin: string;
    fallback_peer_group: string;
    fallback_reason: string;
    maturity_low_identification: boolean;
    maturity_low_identification_reason: string;
    bill_share_low_information: boolean;
    bill_share_interval_width: number | null;
    concept_risk: string;
    revaluation_source_observed: boolean;
    sector_family: string;
  }>;
  time_series: Record<string, {
    dates: string[];
    bill_share: (number | null)[];
    maturity: (number | null)[];
    short_share: (number | null)[];
  }>;
  fed_comparison: {
    dates: string[];
    inferred_maturity: (number | null)[];
    exact_maturity: (number | null)[];
    inferred_bill_share: (number | null)[];
    exact_bill_share: (number | null)[];
  };
  evidence_tiers: Record<string, number>;
  inventory: Array<{
    sector: string;
    sector_key: string;
    level_tier: string;
    maturity_tier: string;
    has_bills_series: boolean;
    publication_start: string;
    publication_end: string;
    source_level_status: string;
  }>;
  soma_exact: {
    dates: string[];
    wam_years: (number | null)[];
    duration_years: (number | null)[];
    bill_share: (number | null)[];
    holdings_trillions: (number | null)[];
  };
  validation?: {
    fed_calibration: {
      dates: string[];
      bill_share_abs_error: (number | null)[];
      maturity_abs_error: (number | null)[];
      summary: {
        bill_share_median_ae: number;
        bill_share_p90_ae: number;
        bill_share_max_ae: number;
        maturity_median_ae: number;
        maturity_p90_ae: number;
        maturity_max_ae: number;
      };
    };
  };
  build_info: {
    schema_version: string;
    snapshot_quarter: string;
    source: string;
  };
}

const Ctx = createContext<SiteData | null>(null);

export function DataProvider({ children }: { children: ReactNode }) {
  const [data, setData] = useState<SiteData | null>(null);

  useEffect(() => {
    fetch(import.meta.env.BASE_URL + "data/site_data.json")
      .then((r) => r.json())
      .then(setData)
      .catch((e) => console.error("Failed to load site data:", e));
  }, []);

  return <Ctx.Provider value={data}>{children}</Ctx.Provider>;
}

export function useSiteData() {
  return useContext(Ctx);
}
