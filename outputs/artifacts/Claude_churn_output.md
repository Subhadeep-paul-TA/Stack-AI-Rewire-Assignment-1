
❯ /churn-feature-pipeline data_dictionary.md, data/customers.csv,
data/transactions.db, data/transactions_monthly_agg.parquet, requirements.txt,
and my confirmed feature_spec.md are already in the repo root. Start at Step 0.

Environment and housekeeping:
- The Python environment already exists: conda env churn-pipeline, with
requirements.txt installed. Your shell starts in conda base, so run every
Python command as `conda run --no-capture-output -n churn-pipeline python ...`,
and pytest as `conda run --no-capture-output -n churn-pipeline python -m
pytest ...`. Don't create a .venv, and don't install or add any dependency.
- The inputs are feature_spec.md and data_dictionary.md. Don't edit either of
them, requirements.txt, or anything in data/. If something in them looks wrong,
note it in ARTIFACT_NOTES.md and carry on.
- These files are my own notes and evidence, not inputs. Don't follow or edit
them: PLAN.md, STEP5_BUILD_README.md,
outputs/artifacts/learner_verification_log.md,
outputs/artifacts/spec_review_response.md,
outputs/artifacts/feature_spec_pre_review.md and outputs/artifacts/stage0_*.md.
- The submission rule requires the reports in outputs/artifacts/, not the repo
root. Write FEATURE_LINEAGE.md, VERSION_COMPARISON.md, PARITY_CHECK.md,
ARTIFACT_NOTES.md and review_log.md there. Save the full output of your final
pytest run as outputs/artifacts/pytest_output.txt.
- Both scripts' --as-of default is the spec's 2026-05-02. Ignore the scaffold's
2026-08-01: that is the label date, and features computed there leak the
label.
- Don't commit or push. I'll review the result and commit it myself.

● I'll start by surveying the repo and reading the inputs in full.

  Read 2 files, ran 1 shell command, recalled 2 memories (ctrl+o to expand)

● The spec is detailed and concrete. Now the existing scaffold code.

  Ran 1 shell command (ctrl+o to expand)

● The feature store is empty, so this will be a first build. I'll now load all
  three sources and verify the counts the spec claims a test must assert.

  Ran 1 shell command (ctrl+o to expand)

● I've confirmed the key facts underpinning the spec: population counts,
  parquet/DB coverage gaps (all isolated to 2026-07, with 0 mismatches in the 12
  months features use), boundary cases, the cap value (197.10), and dtype
  quirks like pandas 3's string handling. Before writing code, I'll run a few
  more environment checks on pandas 3 behavior and the data size needed for the
  mutation-check scratch copy.

  Ran 1 shell command (ctrl+o to expand)

● I'll make ingest explicitly cast dates to [ns] since pandas 3 parses to [us]
  while keeping nanosecond strings at [ns]. Starting with the schema code now,
  which holds the spec's metadata as data.

● Write(features/schema.py)
  ⎿  Added 421 lines, removed 8 lines
        2  Versioned output feature schema: the authoritative list of eve
           ry feature this pipeline
        3  produces, with dtype, nullability, source(s), and feature-set 
           version.
        4  
        5 -This should be generated/maintained from `feature_spec.md`, an
          -d is consumed by                                              
        6 -`FEATURE_LINEAGE.md` generation and by the feature store regis
          -tration step.                                                 
        5 +This is maintained from `feature_spec.md` and is consumed by `
          +features/build.py` (which                                     
        6 +parameters each transform gets), by `scripts/build_training_fe
          +atures.py` (which versions to                                 
        7 +register, with what metadata) and by `scripts/generate_lineage
          +.py` (FEATURE_LINEAGE.md).                                    
        8 +                                                              
        9 +Every tunable value lives here, never in a transform and never
          + recomputed from a batch:                                     
       10 +- `fixed` holds a feature's constants that are part of its def
          +inition (e.g. the 90-day                                      
       11 +  window baked into `txn_count_90d`'s name). They are not regi
          +stry parameters.                                              
       12 +- each `FeatureVersion.parameters` is exactly what is written 
          +to the registry's                                             
       13 +  `parameters` column: the tunable values plus the `definition
          +` revision.                                                   
       14  """
       15  
        9 -from dataclasses import dataclass                             
       16 +from __future__ import annotations                            
       17  
       18 +import copy                                                   
       19 +from dataclasses import dataclass, field                      
       20 +from typing import Callable                                   
       21  
       12 -@dataclass                                                    
       22 +FEATURE_SET_VERSION = "1.0"                                   
       23 +                                                              
       24 +# Every version defined below is definition revision 1. A chan
          +ge of computation is a new                                    
       25 +# definition revision, and so a new version (feature_spec.md, 
          +"What forces a version bump").                                
       26 +CURRENT_DEFINITION = 1                                        
       27 +                                                              
       28 +POPULATION_RULE = (                                           
       29 +    "Customers in data/customers.csv with signup_date < as_of 
          +(as_of read as 00:00:00 naive "                               
       30 +    "local time). One row per customer."                      
       31 +)                                                             
       32 +                                                              
       33 +# QC4 bounds that are fixed constants rather than version para
          +meters (feature_spec.md QC4).                                 
       34 +TXN_COUNT_UPPER_BOUND = 100                                   
       35 +UNCAPPED_MONTHLY_SPEND_UPPER_BOUND = 2000.0                   
       36 +                                                              
       37 +                                                              
       38 +@dataclass(frozen=True)                                       
       39 +class DayWindow:                                              
       40 +    """Half-open window [as_of - days, as_of) on full timestam
          +ps. days=None: everything                                     
       41 +    before as_of."""                                          
       42 +                                                              
       43 +    label: str                                                
       44 +    days: int | None                                          
       45 +                                                              
       46 +                                                              
       47 +@dataclass(frozen=True)                                       
       48 +class MonthWindow:                                            
       49 +    """`months` complete calendar months ending `offset` month
          +s before as_of's month."""                                    
       50 +                                                              
       51 +    label: str                                                
       52 +    months: int                                               
       53 +    offset: int = 0                                           
       54 +                                                              
       55 +                                                              
       56 +@dataclass(frozen=True)                                       
       57 +class FeatureVersion:                                         
       58 +    version: int                                              
       59 +    parameters: dict                                          
       60 +    change_reason: str                                        
       61 +    effect_at_spec_as_of: str = ""                            
       62 +                                                              
       63 +                                                              
       64 +@dataclass(frozen=True)                                       
       65  class FeatureSpec:
       66      name: str
       67      dtype: str
       68      nullable: bool
       69      source: str
       70      feature_set_version: str
       71 +    spec_id: str                                              
       72 +    family: str                                               
       73 +    source_columns: str                                       
       74 +    description: str                                          
       75 +    rationale: str                                            
       76 +    zero_activity_default: int | float | None                 
       77 +    default_note: str                                         
       78 +    computation: Callable[[dict, dict], str]                  
       79 +    windows: Callable[[dict, dict], tuple]                    
       80 +    versions: tuple[FeatureVersion, ...]                      
       81 +    fixed: dict = field(default_factory=dict)                 
       82 +    parameter_notes: str = ""                                 
       83 +    accepted_limitation: str = ""                             
       84 +    comparison: dict | None = None                            
       85  
       86 +    @property                                                 
       87 +    def latest(self) -> FeatureVersion:                       
       88 +        return self.versions[-1]                              
       89  
       21 -# TODO: populate with one FeatureSpec entry per candidate feat
          -ure in your confirmed                                         
       22 -# feature_spec.md. `nullable` should be False for every featur
          -e once your zero-activity                                     
       23 -# default is correctly applied.                               
       24 -FEATURE_SCHEMA: list[FeatureSpec] = []                        
       90 +    @property                                                 
       91 +    def is_versioned(self) -> bool:                           
       92 +        return len(self.versions) >= 2                        
       93 +                                                              
       94 +    def version(self, number: int) -> FeatureVersion:         
       95 +        for v in self.versions:                               
       96 +            if v.version == number:                           
       97 +                return v                                      
       98 +        raise KeyError(f"{self.name} has no schema version {nu
          +mber}")                                                       
       99 +                                                              
      100 +    def transform_kwargs(self, parameters: dict) -> dict:     
      101 +        """Keyword arguments for this feature's transform: fix
          +ed constants plus the                                         
      102 +        version's tunable parameters (the `definition` revisio
          +n is not a transform input)."""                               
      103 +        tunable = {k: v for k, v in parameters.items() if k !=
          + "definition"}                                                
      104 +        return {**self.fixed, **tunable}                      
      105 +                                                              
      106 +    def transform_summary(self, parameters: dict) -> str:     
      107 +        return self.computation(self.fixed, parameters)       
      108 +                                                              
      109 +                                                              
      110 +def _cap_text(cap: float | None) -> str:                      
      111 +    return "no cap" if cap is None else f"{cap:.2f}"          
      112 +                                                              
      113 +                                                              
      114 +FEATURE_SCHEMA: list[FeatureSpec] = [                         
      115 +    FeatureSpec(                                              
      116 +        name="tenure_days",                                   
      117 +        dtype="int64",                                        
      118 +        nullable=False,                                       
      119 +        source="customers.csv",                               
      120 +        feature_set_version=FEATURE_SET_VERSION,              
      121 +        spec_id="F1",                                         
      122 +        family="Profile",                                     
      123 +        source_columns="data/customers.csv:customer_id,signup_
          +date",                                                        
      124 +        description="Whole days from the customer's signup_dat
          +e to as_of. At least 1 for "                                  
      125 +        "everyone in the population.",                        
      126 +        rationale=(                                           
      127 +            "Customers who joined recently have short, patchy 
          +histories while they decide "                                 
      128 +            "whether the store is worth returning to; tenure l
          +ets a model tell 'habit not "                                 
      129 +            "formed yet' apart from an established customer wh
          +ose habit is breaking."                                       
      130 +        ),                                                    
      131 +        zero_activity_default=None,                           
      132 +        default_note="Not needed: every customer has a signup_
          +date, with or without purchases.",                            
      133 +        computation=lambda fixed, p: "(as_of - signup_date) in
          + whole days.",                                                
      134 +        windows=lambda fixed, p: (),                          
      135 +        versions=(                                            
      136 +            FeatureVersion(1, {"definition": 1}, "First versio
          +n."),                                                         
      137 +        ),                                                    
      138 +        accepted_limitation=(                                 
      139 +            "Measures account age, not buying age: first purch
          +ases come a median of 162 days "                              
      140 +            "after signup (data_dictionary.md DQ8)."          
      141 +        ),                                                    
      142 +    ),                                                        
      143 +    FeatureSpec(                                              
      144 +        name="days_since_last_txn",                           
      145 +        dtype="int64",                                        
      146 +        nullable=False,                                       
      147 +        source="transactions.db",                             
      148 +        feature_set_version=FEATURE_SET_VERSION,              
      149 +        spec_id="F2",                                         
      150 +        family="Recency",                                     
      151 +        source_columns="data/transactions.db:transactions.cust
          +omer_id,txn_timestamp",                                       
      152 +        description="Whole days (rounded down) from the custom
          +er's latest transaction before "                              
      153 +        "as_of to as_of, capped at 365. Never-buyers get 365."
          +,                                                             
      154 +        rationale=(                                           
      155 +            "Customers who have gone longer since their last p
          +urchase have often already moved "                            
      156 +            "that spending elsewhere. Recency measures where t
          +he customer stands at the cutoff, "                           
      157 +            "not their average pace; 120 days out is past a re
          +peat buyer's own usual rhythm "                               
      158 +            "(median personal gap 26 days, p90 57)."          
      159 +        ),                                                    
      160 +        zero_activity_default=365,                            
      161 +        default_note="365 (= cap_days) for customers with no t
          +ransaction before as_of. At "                                 
      162 +        "2026-05-02 no buyer exceeds 289 days, so only never-b
          +uyers hold 365; "                                             
      163 +        "has_ever_purchased marks them explicitly.",          
      164 +        computation=lambda fixed, p: (                        
      165 +            f"min(floor((as_of - max(txn_timestamp before as_o
          +f)) / 1 day), {p['cap_days']})."                              
      166 +        ),                                                    
      167 +        windows=lambda fixed, p: (DayWindow("all transactions 
          +before as_of", None),),                                       
      168 +        versions=(                                            
      169 +            FeatureVersion(1, {"cap_days": 365, "definition": 
          +1}, "First version."),                                        
      170 +        ),                                                    
      171 +        parameter_notes="cap_days = 365 is a fixed constant (o
          +ne year), also used as the "                                  
      172 +        "never-buyer default.",                               
      173 +    ),                                                        
      174 +    FeatureSpec(                                              
      175 +        name="has_ever_purchased",                            
      176 +        dtype="int64",                                        
      177 +        nullable=False,                                       
      178 +        source="transactions.db",                             
      179 +        feature_set_version=FEATURE_SET_VERSION,              
      180 +        spec_id="F3",                                         
      181 +        family="Activation",                                  
      182 +        source_columns="data/transactions.db:transactions.cust
          +omer_id,txn_timestamp",                                       
      183 +        description="1 if the customer has at least one transa
          +ction before as_of, else 0.",                                 
      184 +        rationale=(                                           
      185 +            "Customers who have never bought are an activation
          + case, not a lapse: they had no "                             
      186 +            "habit to lose. The flag also stops the other feat
          +ures' defaults (365, 0, 1.0) "                                
      187 +            "from being read as real behaviour."              
      188 +        ),                                                    
      189 +        zero_activity_default=0,                              
      190 +        default_note="0. The feature is itself the zero-activi
          +ty indicator.",                                               
      191 +        computation=lambda fixed, p: "1 if any transaction has
          + txn_timestamp < as_of, else 0.",                             
      192 +        windows=lambda fixed, p: (DayWindow("all transactions 
          +before as_of", None),),                                       
      193 +        versions=(                                            
      194 +            FeatureVersion(1, {"definition": 1}, "First versio
          +n."),                                                         
      195 +        ),                                                    
      196 +    ),                                                        
      197 +    FeatureSpec(                                              
      198 +        name="txn_count_90d",                                 
      199 +        dtype="int64",                                        
      200 +        nullable=False,                                       
      201 +        source="transactions.db",                             
      202 +        feature_set_version=FEATURE_SET_VERSION,              
      203 +        spec_id="F4",                                         
      204 +        family="Frequency",                                   
      205 +        source_columns="data/transactions.db:transactions.cust
          +omer_id,txn_timestamp",                                       
      206 +        description="Number of transactions with as_of - 90 da
          +ys <= txn_timestamp < as_of. "                                
      207 +        "Returned purchases still count.",                    
      208 +        rationale=(                                           
      209 +            "Customers buying less often in the months right b
          +efore the cutoff tend to be "                                 
      210 +            "partway out the door. Read alongside spend_trend_
          +3m_vs_prior_3m and "                                          
      211 +            "active_month_share, it separates a slowdown from 
          +a habitually infrequent buyer. "                              
      212 +            "90 days covers about three typical purchase cycle
          +s (median gap 25 days)."                                      
      213 +        ),                                                    
      214 +        zero_activity_default=0,                              
      215 +        default_note="0 for customers with no transaction in t
          +he window, including never-buyers.",                          
      216 +        computation=lambda fixed, p: (                        
      217 +            f"COUNT(*) of transactions with as_of - {fixed['wi
          +ndow_days']} days <= "                                        
      218 +            "txn_timestamp < as_of (is_returned ignored)."    
      219 +        ),                                                    
      220 +        windows=lambda fixed, p: (DayWindow("transaction windo
          +w", fixed["window_days"]),),                                  
      221 +        versions=(                                            
      222 +            FeatureVersion(1, {"definition": 1}, "First versio
          +n."),                                                         
      223 +        ),                                                    
      224 +        fixed={"window_days": 90},                            
      225 +    ),                                                        
      226 +    FeatureSpec(                                              
      227 +        name="avg_spend_per_active_month_6m",                 
      228 +        dtype="float64",                                      
      229 +        nullable=False,                                       
      230 +        source="transactions_monthly_agg.parquet",            
      231 +        feature_set_version=FEATURE_SET_VERSION,              
      232 +        spec_id="F5",                                         
      233 +        family="Monetary",                                    
      234 +        source_columns="data/transactions_monthly_agg.parquet:
          +customer_id,txn_month,total_amount",                          
      235 +        description="Average monthly spend over the months wit
          +h a purchase among the 6 "                                    
      236 +        "complete calendar months before as_of's month, each m
          +onth capped at monthly_cap.",                                 
      237 +        rationale=(                                           
      238 +            "Customers whose usual monthly basket here is smal
          +l probably spend most of their "                              
      239 +            "money elsewhere, so leaving costs them little. Av
          +eraging only over active months "                             
      240 +            "keeps it apart from purchase frequency (the all-m
          +onths average contained the "                                 
      241 +            "frequency count by construction)."               
      242 +        ),                                                    
      243 +        zero_activity_default=0.0,                            
      244 +        default_note="0.0 for customers with no parquet row in
          + the window: never-buyers and "                               
      245 +        "customers who bought only before the window.",       
      246 +        computation=lambda fixed, p: (                        
      247 +            f"Over the {fixed['window_months']} complete calen
          +dar months before as_of's month: "                            
      248 +            f"cap each month's total_amount at {_cap_text(p['m
          +onthly_cap'])}, sum the capped "                              
      249 +            "totals, divide by the number of those months with
          + a parquet row. Gross of returns."                            
      250 +        ),                                                    
      251 +        windows=lambda fixed, p: (MonthWindow("spend window", 
          +fixed["window_months"]),),                                    
      252 +        versions=(                                            
      253 +            FeatureVersion(                                   
      254 +                1,                                            
      255 +                {"monthly_cap": None, "definition": 1},       
      256 +                "First version: uncapped.",                   
      257 +                "Baseline.",                                  
      258 +            ),                                                
      259 +            FeatureVersion(                                   
      260 +                2,                                            
      261 +                {"monthly_cap": 197.10, "definition": 1},     
      262 +                "Monthly spend is right-skewed (one customer-m
          +onth reaches 1,753.23). Without a "                           
      263 +                "cap, a single unusually large month makes an 
          +occasional buyer look like a big "                            
      264 +                "regular. The cap is per month because the par
          +quet holds no order amounts and a "                           
      265 +                "feature never mixes sources; it keeps the fea
          +ture about typical monthly basket "                           
      266 +                "size.",                                      
      267 +                "157 customers change; max falls from 501.94 t
          +o 197.10; rank correlation 0.997.",                           
      268 +            ),                                                
      269 +        ),                                                    
      270 +        fixed={"window_months": 6},                           
      271 +        parameter_notes="monthly_cap = 197.10 is the 95th perc
          +entile (linear interpolation) of "                            
      272 +        "the 5,900 parquet customer-months with txn_month befo
          +re 2026-05 (197.098, rounded), "                              
      273 +        "fixed as a constant and never recomputed during a bui
          +ld.",                                                         
      274 +        comparison={"kind": "top_decile"},                    
      275 +    ),                                                        
      276 +    FeatureSpec(                                              
      277 +        name="spend_trend_3m_vs_prior_3m",                    
      278 +        dtype="float64",                                      
      279 +        nullable=False,                                       
      280 +        source="transactions_monthly_agg.parquet",            
      281 +        feature_set_version=FEATURE_SET_VERSION,              
      282 +        spec_id="F6",                                         
      283 +        family="Monetary trend",                              
      284 +        source_columns="data/transactions_monthly_agg.parquet:
          +customer_id,txn_month,total_amount",                          
      285 +        description="Spend in the 3 complete months before as_
          +of's month divided by spend in "                              
      286 +        "the 3 months before those, capped at ratio_cap; 0.0 w
          +hen spending stopped, 1.0 when "                              
      287 +        "there is no earlier spend or no spend at all.",      
      288 +        rationale=(                                           
      289 +            "Customers whose spend is falling against their ow
          +n previous quarter tend to be "                               
      290 +            "trading down or moving purchases to a competitor.
          + Each customer is compared with "                             
      291 +            "their own baseline: a steady low spender sits at 
          +1.0, a big spender who halves "                               
      292 +            "their spend at 0.5."                             
      293 +        ),                                                    
      294 +        zero_activity_default=1.0,                            
      295 +        default_note="1.0 when both quarters have no spend (ne
          +ver-buyers and customers who "                                
      296 +        "stopped before the prior quarter): there is no trend 
          +to measure. 0.0 means only one "                              
      297 +        "thing, spending stopped.",                           
      298 +        computation=lambda fixed, p: (                        
      299 +            "recent = SUM(total_amount) over the "            
      300 +            f"{fixed['window_months']} complete months before 
          +as_of's month; prior = SUM over "                             
      301 +            f"the {fixed['window_months']} months before those
          +. recent>0 and prior>0: "                                     
      302 +            f"min(recent/prior, {p['ratio_cap']}); prior>0, re
          +cent=0: 0.0; recent>0, prior=0: "                             
      303 +            "1.0; both 0: 1.0."                               
      304 +        ),                                                    
      305 +        windows=lambda fixed, p: (                            
      306 +            MonthWindow("recent", fixed["window_months"], 0), 
      307 +            MonthWindow("prior", fixed["window_months"], fixed
          +["window_months"]),                                           
      308 +        ),                                                    
      309 +        versions=(                                            
      310 +            FeatureVersion(                                   
      311 +                1,                                            
      312 +                {"ratio_cap": 10, "definition": 1},           
      313 +                "First version: ratio capped at 10.",         
      314 +                "Baseline.",                                  
      315 +            ),                                                
      316 +            FeatureVersion(                                   
      317 +                2,                                            
      318 +                {"ratio_cap": 3, "definition": 1},            
      319 +                "Ratios above 3 come from a tiny previous quar
          +ter, often a single small order "                             
      320 +                "(largest observed 85.9). For retention target
          +ing the useful signal is on the "                             
      321 +                "decline side (below 1); growth beyond 3x adds
          + no information but would dominate "                          
      322 +                "the feature's scale.",                       
      323 +                "162 customers change; mean falls from 1.51 to
          + 1.14; rank correlation 0.999.",                              
      324 +            ),                                                
      325 +        ),                                                    
      326 +        fixed={"window_months": 3},                           
      327 +        parameter_notes="ratio_cap is a fixed constant per ver
          +sion (10, then 3), never "                                    
      328 +        "recomputed from the batch.",                         
      329 +        comparison={"kind": "cap_ties", "parameter": "ratio_ca
          +p", "unchanged_below": 1.0},                                  
      330 +    ),                                                        
      331 +    FeatureSpec(                                              
      332 +        name="active_month_share",                            
      333 +        dtype="float64",                                      
      334 +        nullable=False,                                       
      335 +        source="transactions_monthly_agg.parquet + customers.c
          +sv",                                                          
      336 +        feature_set_version=FEATURE_SET_VERSION,              
      337 +        spec_id="F7",                                         
      338 +        family="Engagement consistency",                      
      339 +        source_columns=(                                      
      340 +            "data/transactions_monthly_agg.parquet:customer_id
          +,txn_month; "                                                 
      341 +            "data/customers.csv:customer_id,signup_date"      
      342 +        ),                                                    
      343 +        description="Share of eligible months (window months o
          +n or after the signup month) "                                
      344 +        "with a purchase, over the lookback_months complete mo
          +nths before as_of's month.",                                  
      345 +        rationale=(                                           
      346 +            "Customers who buy in most of their available mont
          +hs have a habit one quiet month "                             
      347 +            "won't break; customers who buy in only a few can 
          +drift away unnoticed. Counting "                              
      348 +            "only months since signup keeps it from just measu
          +ring tenure."                                                 
      349 +        ),                                                    
      350 +        zero_activity_default=0.0,                            
      351 +        default_note="0.0 for customers with no parquet row in
          + the window, and for customers "                              
      352 +        "with 0 eligible months (signed up in as_of's own mont
          +h; 1 customer at 2026-05-02).",                               
      353 +        computation=lambda fixed, p: (                        
      354 +            f"Over the {p['lookback_months']} complete calenda
          +r months before as_of's month: "                              
      355 +            "active = number of months with a parquet row for 
          +the customer; eligible = number "                             
      356 +            "of those months on or after the customer's signup
          + month; value = active / "                                    
      357 +            "eligible (0.0 if eligible = 0)."                 
      358 +        ),                                                    
      359 +        windows=lambda fixed, p: (MonthWindow("lookback", p["l
          +ookback_months"]),),                                          
      360 +        versions=(                                            
      361 +            FeatureVersion(                                   
      362 +                1,                                            
      363 +                {"lookback_months": 12, "definition": 1},     
      364 +                "First version: 12-month lookback.",          
      365 +                "Baseline.",                                  
      366 +            ),                                                
      367 +            FeatureVersion(                                   
      368 +                2,                                            
      369 +                {"lookback_months": 6, "definition": 1},      
      370 +                "A 12-month window still credits a habit that 
          +ended months ago. With a median "                             
      371 +                "gap of 25 days between purchases, 6 months st
          +ill covers about 7 purchase "                                 
      372 +                "cycles, enough to judge consistency while ref
          +lecting the current habit.",                                  
      373 +                "890 customers change; mean rises from 0.333 t
          +o 0.387; rank correlation 0.849.",                            
      374 +            ),                                                
      375 +        ),                                                    
      376 +        parameter_notes="lookback_months is the versioned para
          +meter (12, then 6). The feature "                             
      377 +        "was renamed from active_months_12m because the lookba
          +ck differs between versions.",                                
      378 +        accepted_limitation=(                                 
      379 +            "Eligible months start at signup, and first purcha
          +ses come a median of 162 days "                               
      380 +            "later (DQ8), so newer customers' shares include i
          +dle months before their first "                               
      381 +            "purchase."                                       
      382 +        ),                                                    
      383 +        comparison={                                          
      384 +            "kind": "bottom_decile",                          
      385 +            "subgroup": {"feature": "has_ever_purchased", "val
          +ue": 1},                                                      
      386 +        },                                                    
      387 +    ),                                                        
      388 +]                                                             
      389 +                                                              
      390 +FEATURE_NAMES: list[str] = [f.name for f in FEATURE_SCHEMA]   
      391 +_BY_NAME = {f.name: f for f in FEATURE_SCHEMA}                
      392 +                                                              
      393 +                                                              
      394 +def get_spec(name: str) -> FeatureSpec:                       
      395 +    try:                                                      
      396 +        return _BY_NAME[name]                                 
      397 +    except KeyError:                                          
      398 +        raise KeyError(f"unknown feature {name!r}; known: {FEA
          +TURE_NAMES}") from None                                       
      399 +                                                              
      400 +                                                              
      401 +def latest_parameters() -> dict[str, dict]:                   
      402 +    return {f.name: copy.deepcopy(f.latest.parameters) for f i
          +n FEATURE_SCHEMA}                                             
      403 +                                                              
      404 +                                                              
      405 +def resolve_feature_params(feature_params: dict[str, dict] | N
          +one) -> dict[str, dict]:                                      
      406 +    """Full {feature_name: parameters} mapping: the latest sch
          +ema version of every feature,                                 
      407 +    overridden by `feature_params` for the features it names. 
      408 +                                                              
      409 +    An override must carry exactly the parameter keys the feat
          +ure's versions declare, and a                                 
      410 +    `definition` this code implements, so a typo can never sil
          +ently fall back to a default.                                 
      411 +    """                                                       
      412 +    resolved = latest_parameters()                            
      413 +    for name, params in (feature_params or {}).items():       
      414 +        spec = get_spec(name)                                 
      415 +        expected = set(spec.latest.parameters)                
      416 +        if set(params) != expected:                           
      417 +            raise ValueError(                                 
      418 +                f"parameters for {name} must have exactly the 
          +keys {sorted(expected)}, "                                    
      419 +                f"got {sorted(params)}"                       
      420 +            )                                                 
      421 +        if params["definition"] != CURRENT_DEFINITION:        
      422 +            raise ValueError(                                 
      423 +                f"{name}: this code implements definition {CUR
          +RENT_DEFINITION}, "                                           
      424 +                f"got definition {params['definition']}"      
      425 +            )                                                 
      426 +        resolved[name] = copy.deepcopy(params)                
      427 +    return resolved                                           
      428 +                                                              
      429 +                                                              
      430 +def month_windows_in_effect(params: dict[str, dict]) -> list[M
          +onthWindow]:                                                  
      431 +    """Every month window any feature uses at these parameters
          + (QC6's coverage scope)."""                                   
      432 +    windows = []                                              
      433 +    for spec in FEATURE_SCHEMA:                               
      434 +        windows.extend(                                       
      435 +            w for w in spec.windows(spec.fixed, params[spec.na
          +me]) if isinstance(w, MonthWindow)                            
      436 +        )                                                     
      437 +    return windows                                            

● I'll remove the effect_at_spec_as_of strings I added, since duplicating them
  risks drift from VERSION_COMPARISON.md, which already computes these effects
  from the store and is validated by tests.

● Bash(conda run --no-capture-output -n churn-pipeline python - <<'EOF'
      import re…)
  ⎿  Updated features/schema.py (+0 -7)
      58      version: int
      59      parameters: dict
      60      change_reason: str
      61 -    effect_at_spec_as_of: str = ""                             
      61  
      62  
      63  @dataclass(frozen=True)
     ...
      253                  1,
      254                  {"monthly_cap": None, "definition": 1},
      255                  "First version: uncapped.",
      257 -                "Baseline.",                                  
      256              ),
      257              FeatureVersion(
      258                  2,
     ...
      262                  "regular. The cap is per month because the par
           quet holds no order amounts and a "
      263                  "feature never mixes sources; it keeps the fea
           ture about typical monthly basket "
      264                  "size.",
      267 -                "157 customers change; max falls from 501.94 t
          -o 197.10; rank correlation 0.997.",                           
      265              ),
      266          ),
      267          fixed={"window_months": 6},
     ...
      308                  1,
      309                  {"ratio_cap": 10, "definition": 1},
      310                  "First version: ratio capped at 10.",
      314 -                "Baseline.",                                  
      311              ),
      312              FeatureVersion(
      313                  2,
     ...
      316                  "(largest observed 85.9). For retention target
           ing the useful signal is on the "
      317                  "decline side (below 1); growth beyond 3x adds
            no information but would dominate "
      318                  "the feature's scale.",
      323 -                "162 customers change; mean falls from 1.51 to
          - 1.14; rank correlation 0.999.",                              
      319              ),
      320          ),
      321          fixed={"window_months": 3},
     ...
      357                  1,
      358                  {"lookback_months": 12, "definition": 1},
      359                  "First version: 12-month lookback.",
      365 -                "Baseline.",                                  
      360              ),
     … 9 more lines (ctrl+o to expand)

● Now ingest (pure I/O, read-only DB access) and the transforms.

● Write(features/ingest.py)
  ⎿  Added 66 lines, removed 24 lines
       4  type coercion, so ingestion can be unit-tested and swapped inde
          pendently of
       5  `transforms.py`.
       6  
       7 -Fill in each function per your confirmed `feature_spec.md`.    
       7 +Every timestamp is coerced to datetime64[ns]: pandas 3 parses p
         +lain dates to [us], while                                      
       8 +`txn_timestamp` carries nanoseconds (data_dictionary.md DQ10) t
         +hat must not be truncated.                                     
       9 +The SQLite database is always opened read-only, so a wrong path
         + fails loudly instead of                                       
      10 +creating an empty database.                                    
      11  """
      12  
      13 +from contextlib import closing                                 
      14  from pathlib import Path
      15 +import sqlite3                                                 
      16  
      17  import pandas as pd
      18  
      19 +CUSTOMER_COLUMNS = ["customer_id", "signup_date", "acquisition_
         +channel", "region", "churned"]                                 
      20 +TRANSACTION_COLUMNS = ["customer_id", "txn_timestamp", "categor
         +y", "channel", "amount", "is_returned"]                        
      21 +MONTHLY_COLUMNS = ["customer_id", "txn_month", "txn_count", "to
         +tal_amount", "return_count"]                                   
      22  
      15 -def load_customers(path: str = "data/customers.csv") -> pd.Data
         -Frame:                                                         
      16 -    """Load customers.csv.                                     
      23  
      18 -    TODO:                                                      
      19 -    - Read the CSV with pandas.                                
      20 -    - Parse `signup_date` as a date.                           
      21 -    - Return one row per customer_id, unmodified otherwise.    
      22 -    """                                                        
      23 -    raise NotImplementedError                                  
      24 +def _require_columns(df: pd.DataFrame, expected: list[str], sou
         +rce: str) -> None:                                             
      25 +    missing = [c for c in expected if c not in df.columns]     
      26 +    if missing:                                                
      27 +        raise ValueError(f"{source} is missing columns {missing
         +}")                                                            
      28  
      29  
      26 -def load_transactions(path: str = "data/transactions.db") -> pd
         -.DataFrame:                                                    
      27 -    """Load the row-level `transactions` table from the SQLite 
         -database.                                                      
      30 +def _connect_read_only(path: str | Path) -> sqlite3.Connection:
      31 +    uri = Path(path).resolve().as_uri() + "?mode=ro"           
      32 +    return sqlite3.connect(uri, uri=True)                      
      33  
      29 -    TODO:                                                      
      30 -    - Connect with sqlite3 (or pandas.read_sql) against `path`.
      31 -    - Cast `txn_timestamp` to a real datetime (SQLite stores it
         - as ISO-8601 text).                                            
      32 -    - Cast `is_returned` to a clean 0/1 int.                   
      33 -    - Return the full row-level frame — do not aggregate here. 
      34 +                                                               
      35 +def load_customers(path: str | Path) -> pd.DataFrame:          
      36 +    """Load customers.csv, with `signup_date` parsed as a datet
         +ime64[ns] date."""                                             
      37 +    df = pd.read_csv(path)                                     
      38 +    _require_columns(df, CUSTOMER_COLUMNS, str(path))          
      39 +    df["customer_id"] = df["customer_id"].astype("int64")      
      40 +    df["signup_date"] = pd.to_datetime(df["signup_date"], forma
         +t="%Y-%m-%d").astype("datetime64[ns]")                         
      41 +    return df                                                  
      42 +                                                               
      43 +                                                               
      44 +def load_transactions(path: str | Path) -> pd.DataFrame:       
      45 +    """Load the full row-level `transactions` table, with `txn_
         +timestamp` parsed from its                                     
      46 +    ISO-8601 text and `is_returned` as a validated 0/1 int64. N
         +ot aggregated or filtered."""                                  
      47 +    with closing(_connect_read_only(path)) as con:             
      48 +        df = pd.read_sql_query(f"SELECT {', '.join(TRANSACTION_
         +COLUMNS)} FROM transactions", con)                             
      49 +    df["customer_id"] = df["customer_id"].astype("int64")      
      50 +    df["txn_timestamp"] = pd.to_datetime(df["txn_timestamp"], f
         +ormat="ISO8601").astype("datetime64[ns]")                      
      51 +    df["amount"] = df["amount"].astype("float64")              
      52 +    bad = ~df["is_returned"].isin([0, 1])                      
      53 +    if bad.any():                                              
      54 +        raise ValueError(f"{path}: is_returned has values other
         + than 0/1 in {int(bad.sum())} rows")                           
      55 +    df["is_returned"] = df["is_returned"].astype("int64")      
      56 +    return df                                                  
      57 +                                                               
      58 +                                                               
      59 +def load_customer_ids_with_txn_before(path: str | Path, as_of: 
         +pd.Timestamp) -> set[int]:                                     
      60 +    """Customer ids with at least one transaction strictly befo
         +re `as_of`, queried directly                                   
      61 +    in SQL on the raw text column.                             
      62 +                                                               
      63 +    This is QC3's independent view of who is zero-activity: it 
         +does not go through                                            
      64 +    `load_transactions`' datetime parsing or any transform. The
         + fixed-width                                                   
      65 +    `YYYY-MM-DD HH:MM:SS.fffffffff` text (DQ10) sorts chronolog
         +ically, and a row at exactly                                   
      66 +    `as_of` 00:00:00 compares greater than the bare date, so `<
         +` excludes it.                                                 
      67      """
      35 -    raise NotImplementedError                                  
      68 +    cutoff = pd.Timestamp(as_of).strftime("%Y-%m-%d")          
      69 +    with closing(_connect_read_only(path)) as con:             
      70 +        rows = con.execute(                                    
      71 +            "SELECT DISTINCT customer_id FROM transactions WHER
         +E txn_timestamp < ?", (cutoff,)                                
      72 +        ).fetchall()                                           
      73 +    return {int(r[0]) for r in rows}                           
      74  
      75  
      38 -def load_monthly_agg(path: str = "data/transactions_monthly_agg
         -.parquet") -> pd.DataFrame:                                    
      39 -    """Load the pre-aggregated customer x month parquet extract
         -.                                                              
      76 +def load_monthly_agg(path: str | Path) -> pd.DataFrame:        
      77 +    """Load the customer x month parquet extract, `txn_month` a
         +s datetime64[ns].                                              
      78  
      41 -    TODO:                                                      
      42 -    - Read the parquet file with pandas.                       
      43 -    - Parse `txn_month` as a date (first-of-month).            
      44 -    - Return as-is — remember customers/months with zero activi
         -ty have no row.                                                
      79 +    Returned as-is: months with zero activity have no row (DQ3)
         +.                                                              
      80      """
      46 -    raise NotImplementedError                                  
      81 +    df = pd.read_parquet(path)                                 
      82 +    _require_columns(df, MONTHLY_COLUMNS, str(path))           
      83 +    df["customer_id"] = df["customer_id"].astype("int64")      
      84 +    df["txn_month"] = pd.to_datetime(df["txn_month"]).astype("d
         +atetime64[ns]")                                                
      85 +    not_month_start = df["txn_month"] != df["txn_month"].dt.to_
         +period("M").dt.to_timestamp()                                  
      86 +    if not_month_start.any():                                  
      87 +        raise ValueError(f"{path}: {int(not_month_start.sum())}
         + txn_month values are not a month start")                      
      88 +    return df                                                  

● Write(features/transforms.py)
  ⎿  Added 179 lines, removed 20 lines
        1  """
        2 -One pure function per candidate feature named in your confirme
          -d `feature_spec.md`.                                          
        2 +One pure function per candidate feature in `feature_spec.md` (
          +F1-F7), plus the date                                         
        3 +conventions they share.                                       
        4  
        4 -Each function should:                                         
        5 -- Be independent of any I/O (take already-loaded DataFrames in
          -, return a Series/DataFrame out).                             
        6 -- Handle the zero-activity case explicitly, per the default yo
          -ur feature_spec.md states —                                   
        7 -  not with a bare try/except that swallows the real problem.  
        8 -- Only touch the source(s) feature_spec.md names for that feat
          -ure. If a feature could                                       
        9 -  plausibly come from either `transactions.db` or `transaction
          -s_monthly_agg.parquet`,                                       
       10 -  use the one your spec chose and justified — do not silently 
          -blend both.                                                   
        5 +Every feature function:                                       
        6 +- takes already-loaded frames, the population, `as_of` (a pars
          +ed pd.Timestamp) and its own                                  
        7 +  parameters as explicit arguments — nothing reads the clock, 
          +a hard-coded date or a                                        
        8 +  default parameter;                                          
        9 +- filters its own window relative to `as_of`, so it is correct
          + even on unfiltered input;                                    
       10 +- returns a Series indexed by the population's customer_id, wi
          +th the spec's zero-activity                                   
       11 +  default filled explicitly by reindexing onto the population 
          +(never a bare NaN);                                           
       12 +- reads only the source feature_spec.md assigns it (DB for F2-
          +F4, parquet for F5-F7).                                       
       13  
       12 -This file intentionally contains no implementations. Add one f
          -unction per feature as you                                    
       13 -build against your confirmed spec.                            
       14 +Conventions (feature_spec.md, "Scope and conventions"):       
       15 +- day windows are [as_of - N days, as_of) on full timestamps; 
       16 +- month windows are N complete calendar months before as_of's 
          +month;                                                        
       17 +- recency is whole days, rounded down.                        
       18  """
       19  
       20 +from datetime import date, datetime                           
       21 +import re                                                     
       22 +                                                              
       23  import pandas as pd
       24  
       25 +_DATE_ONLY = re.compile(r"\d{4}-\d{2}-\d{2}")                 
       26  
       19 -def example_feature_placeholder(                              
       20 -    customers: pd.DataFrame,                                  
       21 -    transactions: pd.DataFrame,                               
       22 -    monthly_agg: pd.DataFrame,                                
       23 -    as_of: str,                                               
       24 -) -> pd.Series:                                               
       25 -    """TODO: replace with your first real feature function.   
       27  
       27 -    Rename this function (and this file's contents) to match t
          -he candidate features                                         
       28 -    named in your confirmed feature_spec.md. Keep one function
          - per feature.                                                 
       28 +def parse_as_of(value) -> pd.Timestamp:                       
       29 +    """Parse the pipeline's single cutoff parameter.          
       30 +                                                              
       31 +    Accepts a 'YYYY-MM-DD' string, a datetime.date, or a naive
          + datetime/Timestamp at exactly                                
       32 +    midnight. Anything with a time of day (or a timezone) is r
          +ejected: as_of is a calendar                                  
       33 +    date read as 00:00:00 naive local time.                   
       34      """
       30 -    raise NotImplementedError                                 
       35 +    if isinstance(value, str):                                
       36 +        if not _DATE_ONLY.fullmatch(value):                   
       37 +            raise ValueError(f"as_of must be a calendar date '
          +YYYY-MM-DD' with no time component, got {value!r}")           
       38 +        ts = pd.Timestamp(value)                              
       39 +    elif isinstance(value, datetime):                         
       40 +        ts = pd.Timestamp(value)                              
       41 +        if ts.tzinfo is not None:                             
       42 +            raise ValueError(f"as_of must be naive local time,
          + got timezone-aware {value!r}")                               
       43 +        if ts != ts.normalize():                              
       44 +            raise ValueError(f"as_of must not have a time comp
          +onent, got {value!r}")                                        
       45 +    elif isinstance(value, date):                             
       46 +        ts = pd.Timestamp(value)                              
       47 +    else:                                                     
       48 +        raise TypeError(f"as_of must be a date string, date or
          + Timestamp, got {type(value).__name__}")                      
       49 +    return ts.as_unit("ns")                                   
       50 +                                                              
       51 +                                                              
       52 +def month_start(ts: pd.Timestamp) -> pd.Timestamp:            
       53 +    return ts.to_period("M").to_timestamp().as_unit("ns")     
       54 +                                                              
       55 +                                                              
       56 +def day_window(as_of: pd.Timestamp, days: int) -> tuple[pd.Tim
          +estamp, pd.Timestamp]:                                        
       57 +    """[as_of - days, as_of) as (inclusive start, exclusive en
          +d)."""                                                        
       58 +    return as_of - pd.Timedelta(days=days), as_of             
       59 +                                                              
       60 +                                                              
       61 +def month_window(as_of: pd.Timestamp, months: int, offset: int
          + = 0) -> tuple[pd.Timestamp, pd.Timestamp]:                   
       62 +    """`months` complete calendar months ending `offset` month
          +s before as_of's month, as                                    
       63 +    (first month start, exclusive end month start)."""        
       64 +    current = month_start(as_of)                              
       65 +    end = current - pd.DateOffset(months=offset)              
       66 +    start = current - pd.DateOffset(months=offset + months)   
       67 +    return start, end                                         
       68 +                                                              
       69 +                                                              
       70 +def population(customers: pd.DataFrame, as_of: pd.Timestamp) -
          +> pd.DataFrame:                                               
       71 +    """Customers with signup_date < as_of, indexed by customer
          +_id (sorted), with signup_date."""                            
       72 +    pop = customers.loc[customers["signup_date"] < as_of, ["cu
          +stomer_id", "signup_date"]]                                   
       73 +    return pop.set_index("customer_id").sort_index()          
       74 +                                                              
       75 +                                                              
       76 +def _on_population(values: pd.Series, population: pd.DataFrame
          +, default, dtype: str, name: str) -> pd.Series:               
       77 +    """Reindex per-customer values onto the population, fillin
          +g customers absent from the                                   
       78 +    source with the feature's explicit default."""            
       79 +    out = values.reindex(population.index, fill_value=default)
       80 +    return out.astype(dtype).rename(name)                     
       81 +                                                              
       82 +                                                              
       83 +def _before(transactions: pd.DataFrame, as_of: pd.Timestamp) -
          +> pd.DataFrame:                                               
       84 +    return transactions.loc[transactions["txn_timestamp"] < as
          +_of]                                                          
       85 +                                                              
       86 +                                                              
       87 +def _months_in_window(monthly: pd.DataFrame, as_of: pd.Timesta
          +mp, months: int, offset: int = 0) -> pd.DataFrame:            
       88 +    start, end = month_window(as_of, months, offset)          
       89 +    month = monthly["txn_month"]                              
       90 +    return monthly.loc[(month >= start) & (month < end)]      
       91 +                                                              
       92 +                                                              
       93 +def _month_index(ts):                                         
       94 +    """Months since year 0, for whole-month arithmetic on a Ti
          +mestamp or datetime Series."""                                
       95 +    if isinstance(ts, pd.Series):                             
       96 +        return ts.dt.year * 12 + ts.dt.month - 1              
       97 +    return ts.year * 12 + ts.month - 1                        
       98 +                                                              
       99 +                                                              
      100 +# F1 ---------------------------------------------------------
          +----------------------------                                  
      101 +def tenure_days(population: pd.DataFrame, as_of: pd.Timestamp)
          + -> pd.Series:                                                
      102 +    days = (as_of - population["signup_date"]) // pd.Timedelta
          +(days=1)                                                      
      103 +    return days.astype("int64").rename("tenure_days")         
      104 +                                                              
      105 +                                                              
      106 +# F2 ---------------------------------------------------------
          +----------------------------                                  
      107 +def days_since_last_txn(                                      
      108 +    population: pd.DataFrame, transactions: pd.DataFrame, as_o
          +f: pd.Timestamp, *, cap_days: int                             
      109 +) -> pd.Series:                                               
      110 +    last = _before(transactions, as_of).groupby("customer_id")
          +["txn_timestamp"].max()                                       
      111 +    days = ((as_of - last) // pd.Timedelta(days=1)).clip(upper
          +=cap_days)                                                    
      112 +    return _on_population(days, population, default=cap_days, 
          +dtype="int64", name="days_since_last_txn")                    
      113 +                                                              
      114 +                                                              
      115 +# F3 ---------------------------------------------------------
          +----------------------------                                  
      116 +def has_ever_purchased(population: pd.DataFrame, transactions:
          + pd.DataFrame, as_of: pd.Timestamp) -> pd.Series:             
      117 +    buyers = _before(transactions, as_of)["customer_id"].uniqu
          +e()                                                           
      118 +    flag = pd.Series(1, index=pd.Index(buyers, name="customer_
          +id"))                                                         
      119 +    return _on_population(flag, population, default=0, dtype="
          +int64", name="has_ever_purchased")                            
      120 +                                                              
      121 +                                                              
      122 +# F4 ---------------------------------------------------------
          +----------------------------                                  
      123 +def txn_count_90d(                                            
      124 +    population: pd.DataFrame, transactions: pd.DataFrame, as_o
          +f: pd.Timestamp, *, window_days: int                          
      125 +) -> pd.Series:                                               
      126 +    start, end = day_window(as_of, window_days)               
      127 +    ts = transactions["txn_timestamp"]                        
      128 +    counts = transactions.loc[(ts >= start) & (ts < end)].grou
          +pby("customer_id").size()                                     
      129 +    return _on_population(counts, population, default=0, dtype
          +="int64", name="txn_count_90d")                               
      130 +                                                              
      131 +                                                              
      132 +# F5 ---------------------------------------------------------
          +----------------------------                                  
      133 +def avg_spend_per_active_month_6m(                            
      134 +    population: pd.DataFrame,                                 
      135 +    monthly: pd.DataFrame,                                    
      136 +    as_of: pd.Timestamp,                                      
      137 +    *,                                                        
      138 +    window_months: int,                                       
      139 +    monthly_cap: float | None,                                
      140 +) -> pd.Series:                                               
      141 +    rows = _months_in_window(monthly, as_of, window_months)   
      142 +    amounts = rows["total_amount"] if monthly_cap is None else
          + rows["total_amount"].clip(upper=monthly_cap)                 
      143 +    avg = amounts.groupby(rows["customer_id"]).mean()         
      144 +    return _on_population(avg, population, default=0.0, dtype=
          +"float64", name="avg_spend_per_active_month_6m")              
      145 +                                                              
      146 +                                                              
      147 +# F6 ---------------------------------------------------------
          +----------------------------                                  
      148 +SPEND_STOPPED = 0.0  # prior > 0, recent = 0                  
      149 +NO_PRIOR_SPEND = 1.0  # recent > 0, prior = 0: no earlier spen
          +d to fall from                                                
      150 +NO_SPEND_EITHER = 1.0  # both 0: the zero-activity default, no
          + trend to measure                                             
      151 +                                                              
      152 +                                                              
      153 +def spend_trend_3m_vs_prior_3m(                               
      154 +    population: pd.DataFrame,                                 
      155 +    monthly: pd.DataFrame,                                    
      156 +    as_of: pd.Timestamp,                                      
      157 +    *,                                                        
      158 +    window_months: int,                                       
      159 +    ratio_cap: float,                                         
      160 +) -> pd.Series:                                               
      161 +    def spend(offset: int) -> pd.Series:                      
      162 +        rows = _months_in_window(monthly, as_of, window_months
          +, offset)                                                     
      163 +        return rows.groupby("customer_id")["total_amount"].sum
          +().reindex(population.index, fill_value=0.0)                  
      164 +                                                              
      165 +    recent, prior = spend(0), spend(window_months)            
      166 +    value = pd.Series(NO_SPEND_EITHER, index=population.index,
          + dtype="float64")                                             
      167 +    both = (recent > 0) & (prior > 0)                         
      168 +    value.loc[both] = (recent[both] / prior[both]).clip(upper=
          +ratio_cap)                                                    
      169 +    value.loc[(prior > 0) & (recent == 0)] = SPEND_STOPPED    
      170 +    value.loc[(recent > 0) & (prior == 0)] = NO_PRIOR_SPEND   
      171 +    return value.rename("spend_trend_3m_vs_prior_3m")         
      172 +                                                              
      173 +                                                              
      174 +# F7 ---------------------------------------------------------
          +----------------------------                                  
      175 +def eligible_months(signup_dates: pd.Series, as_of: pd.Timesta
          +mp, lookback_months: int) -> pd.Series:                       
      176 +    """Number of the lookback window's months on or after each
          + customer's signup month."""                                  
      177 +    start, end = month_window(as_of, lookback_months)         
      178 +    first_eligible = _month_index(signup_dates).clip(lower=_mo
          +nth_index(start))                                             
      179 +    return (_month_index(end) - first_eligible).clip(lower=0, 
          +upper=lookback_months).astype("int64")                        
      180 +                                                              
      181 +                                                              
      182 +def active_month_share(                                       
      183 +    population: pd.DataFrame, monthly: pd.DataFrame, as_of: pd
          +.Timestamp, *, lookback_months: int                           
      184 +) -> pd.Series:                                               
      185 +    rows = _months_in_window(monthly, as_of, lookback_months) 
      186 +    active = rows.groupby("customer_id").size().reindex(popula
          +tion.index, fill_value=0)                                     
      187 +    eligible = eligible_months(population["signup_date"], as_o
          +f, lookback_months)                                           
      188 +    share = (active / eligible).where(eligible > 0, 0.0)      
      189 +    return share.astype("float64").rename("active_month_share"
          +)                                                             

● Moving to quality checks now—each returns a structured result and raises
  QualityCheckError on failure. I'll note that QC3 runs its own independent SQL
  query, so it's unaffected by F3 or the pandas timestamp parsing.

● Write(features/quality_checks.py)
  ⎿  Added 437 lines, removed 23 lines
        1  """
        2 -Runnable-standalone data-quality checks (drift / null / outlie
          -r) against a built                                            
        3 -features DataFrame, implementing every check listed in your co
          -nfirmed                                                       
        4 -`feature_spec.md`'s data-quality section.                     
        2 +Data-quality checks QC1-QC8 from `feature_spec.md`'s data-qual
          +ity section.                                                  
        3  
        6 -Each check function should return a clear pass/fail plus which
          - rows/features                                                
        7 -triggered a failure — never silently pass on bad input.       
        4 +Each check is a pure function returning a `CheckResult` with a
          + clear pass/fail, a detail                                    
        5 +line, and the features/customer_ids that triggered a failure. 
          +`raise_on_failure` turns any                                  
        6 +failure into a `QualityCheckError`, so a failing check always 
          +stops the caller (a non-zero                                  
        7 +exit in both scripts) rather than logging a warning.          
        8 +                                                              
        9 +Where the checks run:                                         
       10 +- QC5 and QC6 check the raw inputs, and QC1-QC4 and QC8 check 
          +the full-population output.                                   
       11 +  All of them run inside `features.build.build_features` on ev
          +ery call, before the final                                    
       12 +  customer_ids selection.                                     
       13 +- QC7 needs the feature store. `scripts/build_training_feature
          +s.py` runs it on every built                                  
       14 +  version before registering anything.                        
       15 +                                                              
       16 +Independence: a check never trusts the transform it checks. QC
          +1 and QC3 recompute the                                       
       17 +population from customers.csv, QC3 takes the zero-activity set
          + from a direct SQL query, and                                 
       18 +QC8 recounts the raw source rows.                             
       19 +                                                              
       20 +Standalone use against a features DataFrame (wide: customer_id
          + + feature columns):                                          
       21 +    python -m features.quality_checks                      # t
          +he store's latest snapshot                                    
       22 +    python -m features.quality_checks --features file.parquet 
       23  """
       24  
       25 +from __future__ import annotations                            
       26 +                                                              
       27 +import argparse                                               
       28 +from dataclasses import dataclass, field                      
       29 +import sys                                                    
       30 +                                                              
       31 +import numpy as np                                            
       32  import pandas as pd
       33  
       34 +from features import schema                                   
       35  
       13 -def check_no_unexpected_nulls(features: pd.DataFrame) -> dict:
       14 -    """TODO: fail if any feature column that should never be n
          -ull (per schema.py) is null.                                  
       36 +DRIFT_TOLERANCE = 1e-9                                        
       37 +AMOUNT_TOLERANCE = 0.01                                       
       38 +_MAX_LISTED = 20                                              
       39  
       16 -    Return something like {"passed": bool, "failing_features":
          - [...], "failing_rows": [...]}.                               
       17 -    """                                                       
       18 -    raise NotImplementedError                                 
       40  
       41 +@dataclass(frozen=True)                                       
       42 +class CheckResult:                                            
       43 +    check_id: str                                             
       44 +    name: str                                                 
       45 +    passed: bool                                              
       46 +    detail: str                                               
       47 +    failing_features: tuple[str, ...] = field(default_factory=
          +tuple)                                                        
       48 +    failing_rows: tuple[int, ...] = field(default_factory=tupl
          +e)                                                            
       49  
       21 -def check_outliers(features: pd.DataFrame) -> dict:           
       22 -    """TODO: fail if a monetary/aggregate feature has values o
          -utside a sane range per                                       
       23 -    feature_spec.md's stated thresholds.                      
       24 -    """                                                       
       25 -    raise NotImplementedError                                 
       50 +    def as_dict(self) -> dict:                                
       51 +        return {                                              
       52 +            "check_id": self.check_id,                        
       53 +            "name": self.name,                                
       54 +            "passed": self.passed,                            
       55 +            "detail": self.detail,                            
       56 +            "failing_features": list(self.failing_features),  
       57 +            "failing_rows": list(self.failing_rows),          
       58 +        }                                                     
       59  
       60 +    def line(self) -> str:                                    
       61 +        status = "PASS" if self.passed else "FAIL"            
       62 +        text = f"{status} {self.check_id} {self.name}: {self.d
          +etail}"                                                       
       63 +        if self.failing_features:                             
       64 +            text += f" | features: {', '.join(self.failing_fea
          +tures)}"                                                      
       65 +        if self.failing_rows:                                 
       66 +            shown = ", ".join(str(r) for r in self.failing_row
          +s[:_MAX_LISTED])                                              
       67 +            more = f" (+{len(self.failing_rows) - _MAX_LISTED}
          + more)" if len(self.failing_rows) > _MAX_LISTED else ""       
       68 +            text += f" | customer_ids: {shown}{more}"         
       69 +        return text                                           
       70  
       28 -def check_drift(features: pd.DataFrame, reference: pd.DataFram
          -e | None = None) -> dict:                                     
       29 -    """TODO: compare the current feature distribution against 
          -a reference snapshot                                          
       30 -    (e.g. a prior build) and fail if the shift exceeds what fe
          -ature_spec.md allows.                                         
       31 -    """                                                       
       32 -    raise NotImplementedError                                 
       71  
       72 +class QualityCheckError(Exception):                           
       73 +    def __init__(self, failures: list[CheckResult]):          
       74 +        self.failures = failures                              
       75 +        super().__init__("quality checks failed:\n" + "\n".joi
          +n(f.line() for f in failures))                                
       76  
       35 -def run_all_checks(features: pd.DataFrame) -> dict:           
       36 -    """TODO: run every check above and return a combined pass/
          -fail summary."""                                              
       37 -    raise NotImplementedError                                 
       77 +                                                              
       78 +def raise_on_failure(results: list[CheckResult]) -> None:     
       79 +    failures = [r for r in results if not r.passed]           
       80 +    if failures:                                              
       81 +        raise QualityCheckError(failures)                     
       82 +                                                              
       83 +                                                              
       84 +def format_results(results: list[CheckResult]) -> str:        
       85 +    return "\n".join(r.line() for r in results)               
       86 +                                                              
       87 +                                                              
       88 +def _ids(values) -> tuple[int, ...]:                          
       89 +    return tuple(sorted(int(v) for v in pd.unique(pd.Series(li
          +st(values), dtype="int64"))))                                 
       90 +                                                              
       91 +                                                              
       92 +def _population_ids(customers: pd.DataFrame, as_of: pd.Timesta
          +mp) -> pd.Series:                                             
       93 +    return customers.loc[customers["signup_date"] < as_of, "cu
          +stomer_id"]                                                   
       94 +                                                              
       95 +                                                              
       96 +# QC1 --------------------------------------------------------
          +----------------------------                                  
       97 +def check_population_rows(features: pd.DataFrame, customers: p
          +d.DataFrame, as_of: pd.Timestamp) -> CheckResult:             
       98 +    """One row per population customer (recomputed from custom
          +ers.csv), customer_id unique."""                              
       99 +    expected = set(_population_ids(customers, as_of))         
      100 +    ids = features["customer_id"]                             
      101 +    duplicated = _ids(ids[ids.duplicated()])                  
      102 +    missing = _ids(expected - set(ids))                       
      103 +    extra = _ids(set(ids) - expected)                         
      104 +    passed = len(features) == len(expected) and not (duplicate
          +d or missing or extra)                                        
      105 +    detail = (                                                
      106 +        f"{len(features)} rows for {len(expected)} population 
          +customers (signup_date < "                                    
      107 +        f"{as_of.date()}); duplicated={len(duplicated)}, missi
          +ng={len(missing)}, extra={len(extra)}"                        
      108 +    )                                                         
      109 +    return CheckResult("QC1", "population row count", passed, 
          +detail, failing_rows=duplicated + missing + extra)            
      110 +                                                              
      111 +                                                              
      112 +def check_selection_rows(selected: pd.DataFrame, requested_ids
          +) -> CheckResult:                                             
      113 +    """After the customer_ids selection: exactly one row per r
          +equested id, in request order."""                             
      114 +    requested = [int(i) for i in requested_ids]               
      115 +    got = [int(i) for i in selected["customer_id"]]           
      116 +    passed = got == requested                                 
      117 +    bad = _ids(set(got) ^ set(requested)) if not passed else (
          +)                                                             
      118 +    detail = f"{len(got)} rows selected for {len(requested)} r
          +equested ids"                                                 
      119 +    return CheckResult("QC1", "selected row count", passed, de
          +tail, failing_rows=bad)                                       
      120 +                                                              
      121 +                                                              
      122 +# QC2 --------------------------------------------------------
          +----------------------------                                  
      123 +def check_no_nulls(features: pd.DataFrame, feature_names: list
          +[str] | None = None) -> CheckResult:                          
      124 +    names = feature_names or schema.FEATURE_NAMES             
      125 +    missing_cols = [n for n in names if n not in features.colu
          +mns]                                                          
      126 +    present = [n for n in names if n in features.columns]     
      127 +    nulls = features[present].isna()                          
      128 +    bad_features = tuple(missing_cols + [n for n in present if
          + nulls[n].any()])                                             
      129 +    bad_rows = _ids(features.loc[nulls.any(axis=1), "customer_
          +id"])                                                         
      130 +    passed = not bad_features                                 
      131 +    detail = (                                                
      132 +        f"{int(nulls.to_numpy().sum())} null values across {le
          +n(present)} feature columns"                                  
      133 +        + (f"; missing columns {missing_cols}" if missing_cols
          + else "")                                                     
      134 +    )                                                         
      135 +    return CheckResult("QC2", "no nulls after defaults", passe
          +d, detail, bad_features, bad_rows)                            
      136 +                                                              
      137 +                                                              
      138 +# QC3 --------------------------------------------------------
          +----------------------------                                  
      139 +def _zero_activity_defaults() -> dict[str, float]:            
      140 +    return {f.name: f.zero_activity_default for f in schema.FE
          +ATURE_SCHEMA if f.zero_activity_default is not None}          
      141 +                                                              
      142 +                                                              
      143 +def check_zero_activity_defaults(                             
      144 +    features: pd.DataFrame, customers: pd.DataFrame, buyer_ids
          +: set[int], as_of: pd.Timestamp                               
      145 +) -> CheckResult:                                             
      146 +    """Every population customer with no DB row before as_of (
          +buyer_ids comes from a direct                                 
      147 +    SQL query, not from has_ever_purchased) holds exactly each
          + feature's spec default, and                                  
      148 +    every other population customer has has_ever_purchased = 1
          +."""                                                          
      149 +    pop = set(_population_ids(customers, as_of))              
      150 +    zero_ids = pop - set(buyer_ids)                           
      151 +    by_id = features.set_index("customer_id")                 
      152 +    zero = by_id.loc[by_id.index.isin(zero_ids)]              
      153 +    bad_features, bad_rows = [], set()                        
      154 +    for name, default in _zero_activity_defaults().items():   
      155 +        wrong = zero.index[zero[name].ne(default) | zero[name]
          +.isna()]                                                      
      156 +        if len(wrong):                                        
      157 +            bad_features.append(name)                         
      158 +            bad_rows.update(wrong)                            
      159 +    buyers = by_id.loc[by_id.index.isin(pop - zero_ids), "has_
          +ever_purchased"]                                              
      160 +    unflagged = buyers.index[buyers.ne(1)]                    
      161 +    if len(unflagged):                                        
      162 +        bad_features.append("has_ever_purchased (buyers)")    
      163 +        bad_rows.update(unflagged)                            
      164 +    passed = not bad_features and len(zero) == len(zero_ids)  
      165 +    detail = (                                                
      166 +        f"{len(zero_ids)} zero-activity customers from the DB 
          +query, {len(zero)} found in the "                             
      167 +        f"output; defaults {_zero_activity_defaults()}"       
      168 +    )                                                         
      169 +    return CheckResult("QC3", "zero-activity defaults", passed
          +, detail, tuple(bad_features), _ids(bad_rows))                
      170 +                                                              
      171 +                                                              
      172 +# QC4 --------------------------------------------------------
          +----------------------------                                  
      173 +def range_bounds(params: dict[str, dict]) -> dict[str, tuple[f
          +loat, float]]:                                                
      174 +    """Inclusive [low, high] per feature at the parameters in 
          +effect (feature_spec.md QC4)."""                              
      175 +    cap = params["avg_spend_per_active_month_6m"]["monthly_cap
          +"]                                                            
      176 +    return {                                                  
      177 +        "tenure_days": (1, np.inf),                           
      178 +        "days_since_last_txn": (0, params["days_since_last_txn
          +"]["cap_days"]),                                              
      179 +        "has_ever_purchased": (0, 1),                         
      180 +        "txn_count_90d": (0, schema.TXN_COUNT_UPPER_BOUND),   
      181 +        "avg_spend_per_active_month_6m": (0.0, schema.UNCAPPED
          +_MONTHLY_SPEND_UPPER_BOUND if cap is None else cap),          
      182 +        "spend_trend_3m_vs_prior_3m": (0.0, params["spend_tren
          +d_3m_vs_prior_3m"]["ratio_cap"]),                             
      183 +        "active_month_share": (0.0, 1.0),                     
      184 +    }                                                         
      185 +                                                              
      186 +                                                              
      187 +def check_ranges(features: pd.DataFrame, params: dict[str, dic
          +t]) -> CheckResult:                                           
      188 +    bad_features, bad_rows, notes = [], set(), []             
      189 +    for name, (low, high) in range_bounds(params).items():    
      190 +        values = features[name]                               
      191 +        ok = values.between(low, high)                        
      192 +        if name == "has_ever_purchased":                      
      193 +            ok &= values.isin([0, 1])                         
      194 +        if not ok.all():                                      
      195 +            bad_features.append(name)                         
      196 +            bad_rows.update(features.loc[~ok, "customer_id"]) 
      197 +            notes.append(f"{name} outside [{low}, {high}]: {in
          +t((~ok).sum())}")                                             
      198 +    detail = "; ".join(notes) if notes else "all 7 features wi
          +thin their bounds"                                            
      199 +    return CheckResult("QC4", "range limits", not bad_features
          +, detail, tuple(bad_features), _ids(bad_rows))                
      200 +                                                              
      201 +                                                              
      202 +# QC5 --------------------------------------------------------
          +----------------------------                                  
      203 +def check_no_future_data(                                     
      204 +    transactions_used: pd.DataFrame, monthly_used: pd.DataFram
          +e, as_of: pd.Timestamp                                        
      205 +) -> CheckResult:                                             
      206 +    """The frames handed to the transforms hold nothing at or 
          +after as_of (DB) or as_of's                                   
      207 +    month (parquet)."""                                       
      208 +    cutoff_month = as_of.to_period("M").to_timestamp()        
      209 +    late_txn = transactions_used.loc[transactions_used["txn_ti
          +mestamp"] >= as_of]                                           
      210 +    late_month = monthly_used.loc[monthly_used["txn_month"] >=
          + cutoff_month]                                                
      211 +    passed = late_txn.empty and late_month.empty              
      212 +    detail = (                                                
      213 +        f"{len(late_txn)} DB rows at/after {as_of}, {len(late_
          +month)} parquet rows in/after "                               
      214 +        f"{cutoff_month.strftime('%Y-%m')}"                   
      215 +    )                                                         
      216 +    bad = _ids(pd.concat([late_txn["customer_id"], late_month[
          +"customer_id"]]))                                             
      217 +    failing = tuple(s for s, df in (("transactions", late_txn)
          +, ("monthly_agg", late_month)) if not df.empty)               
      218 +    return CheckResult("QC5", "no future data", passed, detail
          +, failing, bad)                                               
      219 +                                                              
      220 +                                                              
      221 +# QC6 --------------------------------------------------------
          +----------------------------                                  
      222 +def coverage_months(as_of: pd.Timestamp, params: dict[str, dic
          +t]) -> list[pd.Timestamp]:                                    
      223 +    from features.transforms import month_window              
      224 +                                                              
      225 +    months = set()                                            
      226 +    for w in schema.month_windows_in_effect(params):          
      227 +        start, end = month_window(as_of, w.months, w.offset)  
      228 +        months.update(pd.date_range(start, end, freq="MS", inc
          +lusive="left"))                                               
      229 +    return sorted(months)                                     
      230 +                                                              
      231 +                                                              
      232 +def check_parquet_coverage(                                   
      233 +    transactions: pd.DataFrame, monthly: pd.DataFrame, as_of: 
          +pd.Timestamp, params: dict[str, dict]                         
      234 +) -> CheckResult:                                             
      235 +    """For every month any month-based window uses, the parque
          +t equals the DB's monthly                                     
      236 +    group-by for every customer, and no customer-month exists 
          +in only one source."""                                        
      237 +    months = coverage_months(as_of, params)                   
      238 +    month_set = pd.DatetimeIndex(months)                      
      239 +    txn_month = transactions["txn_timestamp"].dt.to_period("M"
          +).dt.to_timestamp().astype("datetime64[ns]")                  
      240 +    in_scope = txn_month.isin(month_set)                      
      241 +    db = (                                                    
      242 +        transactions.loc[in_scope]                            
      243 +        .assign(txn_month=txn_month[in_scope])                
      244 +        .groupby(["customer_id", "txn_month"])                
      245 +        .agg(txn_count=("amount", "size"), total_amount=("amou
          +nt", "sum"), return_count=("is_returned", "sum"))             
      246 +        .reset_index()                                        
      247 +    )                                                         
      248 +    pq = monthly.loc[monthly["txn_month"].isin(month_set), ["c
          +ustomer_id", "txn_month", "txn_count", "total_amount", "return
          +_count"]]                                                     
      249 +    duplicate_keys = pq.loc[pq.duplicated(["customer_id", "txn
          +_month"], keep=False)]                                        
      250 +    merged = pq.merge(db, on=["customer_id", "txn_month"], how
          +="outer", suffixes=("_pq", "_db"), indicator=True)            
      251 +    both = merged["_merge"] == "both"                         
      252 +    mismatch = both & (                                       
      253 +        (merged["txn_count_pq"] != merged["txn_count_db"])    
      254 +        | (merged["return_count_pq"] != merged["return_count_d
          +b"])                                                          
      255 +        | ((merged["total_amount_pq"] - merged["total_amount_d
          +b"]).abs() > AMOUNT_TOLERANCE)                                
      256 +    )                                                         
      257 +    only_one = ~both                                          
      258 +    bad = merged.loc[mismatch | only_one]                     
      259 +    bad_months = sorted({m.strftime("%Y-%m") for m in bad["txn
          +_month"]})                                                    
      260 +    passed = bad.empty and duplicate_keys.empty               
      261 +    span = f"{months[0].strftime('%Y-%m')} to {months[-1].strf
          +time('%Y-%m')}" if months else "no months"                    
      262 +    detail = (                                                
      263 +        f"{len(months)} months checked ({span}), {int(both.sum
          +())} customer-months in both sources; "                       
      264 +        f"{int(mismatch.sum())} differ, {int(only_one.sum())} 
          +in one source only, "                                         
      265 +        f"{len(duplicate_keys)} duplicate parquet keys"       
      266 +        + (f"; failing months {bad_months}" if bad_months else
          + "")                                                          
      267 +    )                                                         
      268 +    rows = _ids(pd.concat([bad["customer_id"], duplicate_keys[
          +"customer_id"]]))                                             
      269 +    return CheckResult("QC6", "parquet coverage", passed, deta
          +il, tuple(bad_months), rows)                                  
      270 +                                                              
      271 +                                                              
      272 +# QC7 --------------------------------------------------------
          +----------------------------                                  
      273 +def check_drift_against_store(                                
      274 +    feature_name: str, values: pd.DataFrame, reference: pd.Dat
          +aFrame | None                                                 
      275 +) -> CheckResult:                                             
      276 +    """Rebuilt values must equal the stored copy of a version 
          +with identical parameters at                                  
      277 +    the same as_of (to within 1e-9). No stored copy: passes, r
          +eporting 'no reference'."""                                   
      278 +    if reference is None:                                     
      279 +        return CheckResult("QC7", f"drift vs store [{feature_n
          +ame}]", True, "no reference")                                 
      280 +    new = values.set_index("customer_id")                     
      281 +    ref = reference.set_index("customer_id")                  
      282 +    new_dates = set(pd.to_datetime(new["as_of_date"]).unique()
          +)                                                             
      283 +    ref_dates = set(pd.to_datetime(ref["as_of_date"]).unique()
          +)                                                             
      284 +    if new_dates != ref_dates:                                
      285 +        return CheckResult(                                   
      286 +            "QC7", f"drift vs store [{feature_name}]", False, 
      287 +            f"as_of_date differs: rebuilt {sorted(new_dates)} 
          +vs stored {sorted(ref_dates)}", (feature_name,),              
      288 +        )                                                     
      289 +    missing, extra = _ids(ref.index.difference(new.index)), _i
          +ds(new.index.difference(ref.index))                           
      290 +    common = new.index.intersection(ref.index)                
      291 +    a = new.loc[common, feature_name].astype("float64")       
      292 +    b = ref.loc[common, feature_name].astype("float64")       
      293 +    changed = common[~np.isclose(a, b, rtol=0.0, atol=DRIFT_TO
          +LERANCE)]                                                     
      294 +    passed = not (missing or extra or len(changed))           
      295 +    detail = (                                                
      296 +        f"{len(changed)} of {len(common)} customers differ fro
          +m the stored copy "                                           
      297 +        f"(stored mean {b.mean():.6g}, rebuilt mean {a.mean():
          +.6g}); missing={len(missing)}, extra={len(extra)}"            
      298 +    )                                                         
      299 +    return CheckResult(                                       
      300 +        "QC7", f"drift vs store [{feature_name}]", passed, det
          +ail,                                                          
      301 +        () if passed else (feature_name,), _ids(list(changed))
          + + missing + extra,                                           
      302 +    )                                                         
      303 +                                                              
      304 +                                                              
      305 +# QC8 --------------------------------------------------------
          +----------------------------                                  
      306 +def _eligible_months_by_enumeration(signup_dates: pd.Series, w
          +indow_months: pd.DatetimeIndex) -> pd.Series:                 
      307 +    """Independent of transforms.eligible_months: count window
          + months whose start is on or                                  
      308 +    after the signup month's start."""                        
      309 +    signup_month = signup_dates.dt.to_period("M").dt.to_timest
          +amp().to_numpy()                                              
      310 +    grid = window_months.to_numpy()[None, :] >= signup_month[:
          +, None]                                                       
      311 +    return pd.Series(grid.sum(axis=1), index=signup_dates.inde
          +x)                                                            
      312 +                                                              
      313 +                                                              
      314 +def check_totals_reconcile(                                   
      315 +    features: pd.DataFrame,                                   
      316 +    customers: pd.DataFrame,                                  
      317 +    transactions: pd.DataFrame,                               
      318 +    monthly: pd.DataFrame,                                    
      319 +    as_of: pd.Timestamp,                                      
      320 +    params: dict[str, dict],                                  
      321 +) -> CheckResult:                                             
      322 +    """Totals behind txn_count_90d and active_month_share equa
          +l raw source row counts."""                                   
      323 +    from features.transforms import day_window, month_window  
      324 +                                                              
      325 +    pop = customers.loc[customers["signup_date"] < as_of].set_
          +index("customer_id")                                          
      326 +    by_id = features.set_index("customer_id")                 
      327 +    problems, bad_features = [], []                           
      328 +                                                              
      329 +    start, end = day_window(as_of, schema.get_spec("txn_count_
          +90d").fixed["window_days"])                                   
      330 +    ts = transactions["txn_timestamp"]                        
      331 +    db_rows = int((transactions["customer_id"].isin(pop.index)
          + & (ts >= start) & (ts < end)).sum())                         
      332 +    feature_total = int(by_id["txn_count_90d"].sum())         
      333 +    if feature_total != db_rows:                              
      334 +        bad_features.append("txn_count_90d")                  
      335 +    problems.append(f"txn_count_90d sum {feature_total} vs {db
          +_rows} DB rows in [{start.date()}, {end.date()})")            
      336 +                                                              
      337 +    lookback = params["active_month_share"]["lookback_months"]
      338 +    m_start, m_end = month_window(as_of, lookback)            
      339 +    window = pd.date_range(m_start, m_end, freq="MS", inclusiv
          +e="left")                                                     
      340 +    eligible = _eligible_months_by_enumeration(pop["signup_dat
          +e"], window)                                                  
      341 +    implied = by_id["active_month_share"].reindex(pop.index) *
          + eligible                                                     
      342 +    rounded = implied.round()                                 
      343 +    pq_rows = int((monthly["customer_id"].isin(pop.index) & mo
          +nthly["txn_month"].isin(window)).sum())                       
      344 +    non_integer = implied.index[(implied - rounded).abs() > 1e
          +-6]                                                           
      345 +    active_total = int(rounded.sum()) if not implied.isna().an
          +y() else -1                                                   
      346 +    if active_total != pq_rows or len(non_integer):           
      347 +        bad_features.append("active_month_share")             
      348 +    problems.append(                                          
      349 +        f"active months implied by active_month_share {active_
          +total} vs {pq_rows} parquet rows in "                         
      350 +        f"{m_start.strftime('%Y-%m')} to {(m_end - pd.DateOffs
          +et(months=1)).strftime('%Y-%m')}"                             
      351 +        + (f"; {len(non_integer)} non-integer share x eligible
          +" if len(non_integer) else "")                                
      352 +    )                                                         
      353 +    return CheckResult("QC8", "totals reconcile", not bad_feat
          +ures, "; ".join(problems), tuple(bad_features), _ids(non_integ
          +er))                                                          
      354 +                                                              
      355 +                                                              
      356 +# Runners ----------------------------------------------------
          +----------------------------                                  
      357 +def run_input_checks(                                         
      358 +    transactions_used: pd.DataFrame,                          
      359 +    monthly_used: pd.DataFrame,                               
      360 +    transactions: pd.DataFrame,                               
      361 +    monthly: pd.DataFrame,                                    
      362 +    as_of: pd.Timestamp,                                      
      363 +    params: dict[str, dict],                                  
      364 +) -> list[CheckResult]:                                       
      365 +    return [                                                  
      366 +        check_no_future_data(transactions_used, monthly_used, 
          +as_of),                                                       
      367 +        check_parquet_coverage(transactions, monthly, as_of, p
          +arams),                                                       
      368 +    ]                                                         
      369 +                                                              
      370 +                                                              
      371 +def run_output_checks(                                        
      372 +    features: pd.DataFrame,                                   
      373 +    *,                                                        
      374 +    customers: pd.DataFrame,                                  
      375 +    transactions: pd.DataFrame,                               
      376 +    monthly: pd.DataFrame,                                    
      377 +    buyer_ids: set[int],                                      
      378 +    as_of: pd.Timestamp,                                      
      379 +    params: dict[str, dict],                                  
      380 +) -> list[CheckResult]:                                       
      381 +    """QC1-QC4 and QC8 against a full-population features fram
          +e (customer_id column)."""                                    
      382 +    return [                                                  
      383 +        check_population_rows(features, customers, as_of),    
      384 +        check_no_nulls(features),                             
      385 +        check_zero_activity_defaults(features, customers, buye
          +r_ids, as_of),                                                
      386 +        check_ranges(features, params),                       
      387 +        check_totals_reconcile(features, customers, transactio
          +ns, monthly, as_of, params),                                  
      388 +    ]                                                         
      389 +                                                              
      390 +                                                              
      391 +# Standalone CLI ---------------------------------------------
          +----------------------------                                  
      392 +def _store_latest_frame(store_dir) -> tuple[pd.DataFrame, dict
          +[str, dict]]:                                                 
      393 +    import json                                               
      394 +                                                              
      395 +    from feature_store import store                           
      396 +                                                              
      397 +    frame, params = None, {}                                  
      398 +    for name in schema.FEATURE_NAMES:                         
      399 +        versions = store.list_versions(name, store_dir=store_d
          +ir)                                                           
      400 +        if versions.empty:                                    
      401 +            raise SystemExit(f"feature store has no versions o
          +f {name}; run scripts/build_training_features.py")            
      402 +        params[name] = json.loads(versions.loc[versions["is_la
          +test"] == 1, "parameters"].iloc[0])                           
      403 +        values = store.get_latest(name, store_dir=store_dir)  
      404 +        frame = values if frame is None else frame.merge(value
          +s, on=["customer_id", "as_of_date"], how="outer")             
      405 +    return frame, params                                      
      406 +                                                              
      407 +                                                              
      408 +def main(argv: list[str] | None = None) -> int:               
      409 +    from features import build, ingest                        
      410 +    from features.transforms import parse_as_of               
      411 +                                                              
      412 +    parser = argparse.ArgumentParser(description="Run QC1-QC4,
          + QC6 and QC8 against a features frame.")                      
      413 +    parser.add_argument("--features", help="Wide features parq
          +uet (customer_id, the 7 features, as_of_date). "              
      414 +                        "Default: the feature store's latest v
          +ersion of every feature.")                                    
      415 +    parser.add_argument("--as-of", help="Required with a --fea
          +tures file that has no as_of_date column.")                   
      416 +    parser.add_argument("--store-dir", default=None, help="Fea
          +ture store root (default: feature_store/).")                  
      417 +    args = parser.parse_args(argv)                            
      418 +                                                              
      419 +    if args.features:                                         
      420 +        frame = pd.read_parquet(args.features)                
      421 +        params = schema.latest_parameters()                   
      422 +    else:                                                     
      423 +        frame, params = _store_latest_frame(args.store_dir)   
      424 +    if "as_of_date" in frame.columns:                         
      425 +        dates = pd.to_datetime(frame["as_of_date"]).unique()  
      426 +        if len(dates) != 1:                                   
      427 +            raise SystemExit(f"features frame mixes as_of date
          +s: {sorted(dates)}")                                          
      428 +        as_of = parse_as_of(pd.Timestamp(dates[0]))           
      429 +        if args.as_of and parse_as_of(args.as_of) != as_of:   
      430 +            raise SystemExit(f"--as-of {args.as_of} does not m
          +atch the frame's as_of_date {as_of.date()}")                  
      431 +    elif args.as_of:                                          
      432 +        as_of = parse_as_of(args.as_of)                       
      433 +    else:                                                     
      434 +        raise SystemExit("--as-of is required when the feature
          +s frame has no as_of_date column")                            
      435 +                                                              
      436 +    paths = build.resolve_source_paths(None)                  
      437 +    customers = ingest.load_customers(paths["customers"])     
      438 +    transactions = ingest.load_transactions(paths["transaction
          +s"])                                                          
      439 +    monthly = ingest.load_monthly_agg(paths["monthly_agg"])   
      440 +    buyer_ids = ingest.load_customer_ids_with_txn_before(paths
          +["transactions"], as_of)                                      
      441 +    results = [check_parquet_coverage(transactions, monthly, a
          +s_of, params)] + run_output_checks(                           
      442 +        frame, customers=customers, transactions=transactions,
          + monthly=monthly,                                             
      443 +        buyer_ids=buyer_ids, as_of=as_of, params=params,      
      444 +    )                                                         
      445 +    print(f"as_of {as_of.date()}, {len(frame)} rows")         
      446 +    print(format_results(results))                            
      447 +    return 0 if all(r.passed for r in results) else 1         
      448 +                                                              
      449 +                                                              
      450 +if __name__ == "__main__":                                    
      451 +    sys.exit(main())                                          

● Write(features/build.py)
  ⎿  Added 132 lines, removed 20 lines
        4  `scripts/score_customer.py` (scoring path: single customer_id,
            same as-of date) must call
        5  this same function — that is the entire train/score parity gua
           rantee for this pipeline.
        6  
        7 -There must be exactly one code path here. If you find yourself
          - writing separate logic for                                   
        8 -"training mode" vs. "scoring mode" beyond which rows they sele
          -ct at the end, that is a                                      
        9 -parity bug — go back and unify it rather than special-casing e
          -ither path.                                                   
        7 +There is exactly one code path. Features are always computed, 
          +and checked, for the full                                     
        8 +population at `as_of`; the only training/scoring difference is
          + which rows the last step                                     
        9 +returns (`customer_ids`).                                     
       10  """
       11  
       12 +from __future__ import annotations                            
       13 +                                                              
       14 +from pathlib import Path                                      
       15 +                                                              
       16  import pandas as pd
       17  
       14 -from features import ingest, transforms  # noqa: F401 (import 
          -when transforms are implemented)                              
       18 +from features import ingest, schema, transforms               
       19 +from features import quality_checks as qc                     
       20  
       21 +REPO_ROOT = Path(__file__).resolve().parents[1]               
       22 +DEFAULT_SOURCE_PATHS = {                                      
       23 +    "customers": REPO_ROOT / "data" / "customers.csv",        
       24 +    "transactions": REPO_ROOT / "data" / "transactions.db",   
       25 +    "monthly_agg": REPO_ROOT / "data" / "transactions_monthly_
          +agg.parquet",                                                 
       26 +}                                                             
       27  
       28 +                                                              
       29 +def resolve_source_paths(source_paths: dict | None) -> dict[st
          +r, Path]:                                                     
       30 +    paths = dict(DEFAULT_SOURCE_PATHS)                        
       31 +    unknown = set(source_paths or {}) - set(paths)            
       32 +    if unknown:                                               
       33 +        raise ValueError(f"unknown source_paths keys {sorted(u
          +nknown)}; expected {sorted(paths)}")                          
       34 +    paths.update({k: Path(v) for k, v in (source_paths or {}).
          +items()})                                                     
       35 +    return paths                                              
       36 +                                                              
       37 +                                                              
       38 +def _compute_features(                                        
       39 +    population: pd.DataFrame,                                 
       40 +    transactions: pd.DataFrame,                               
       41 +    monthly: pd.DataFrame,                                    
       42 +    as_of: pd.Timestamp,                                      
       43 +    params: dict[str, dict],                                  
       44 +) -> pd.DataFrame:                                            
       45 +    def kw(name: str) -> dict:                                
       46 +        return schema.get_spec(name).transform_kwargs(params[n
          +ame])                                                         
       47 +                                                              
       48 +    columns = {                                               
       49 +        "tenure_days": transforms.tenure_days(population, as_o
          +f),                                                           
       50 +        "days_since_last_txn": transforms.days_since_last_txn(
       51 +            population, transactions, as_of, **kw("days_since_
          +last_txn")                                                    
       52 +        ),                                                    
       53 +        "has_ever_purchased": transforms.has_ever_purchased(po
          +pulation, transactions, as_of),                               
       54 +        "txn_count_90d": transforms.txn_count_90d(population, 
          +transactions, as_of, **kw("txn_count_90d")),                  
       55 +        "avg_spend_per_active_month_6m": transforms.avg_spend_
          +per_active_month_6m(                                          
       56 +            population, monthly, as_of, **kw("avg_spend_per_ac
          +tive_month_6m")                                               
       57 +        ),                                                    
       58 +        "spend_trend_3m_vs_prior_3m": transforms.spend_trend_3
          +m_vs_prior_3m(                                                
       59 +            population, monthly, as_of, **kw("spend_trend_3m_v
          +s_prior_3m")                                                  
       60 +        ),                                                    
       61 +        "active_month_share": transforms.active_month_share(  
       62 +            population, monthly, as_of, **kw("active_month_sha
          +re")                                                          
       63 +        ),                                                    
       64 +    }                                                         
       65 +    if list(columns) != schema.FEATURE_NAMES:                 
       66 +        raise RuntimeError(f"build computes {list(columns)} bu
          +t schema.py lists {schema.FEATURE_NAMES}")                    
       67 +    return pd.DataFrame(columns).rename_axis("customer_id").re
          +set_index()                                                   
       68 +                                                              
       69 +                                                              
       70 +def _requested_ids(customer_ids, customers: pd.DataFrame, as_o
          +f: pd.Timestamp) -> list[int]:                                
       71 +    """Validate scoring ids: each must exist and have signed u
          +p before as_of. Raises a                                      
       72 +    ValueError naming the offending id; never lets an empty se
          +lection through."""                                           
       73 +    ids = list(customer_ids)                                  
       74 +    if not ids:                                               
       75 +        raise ValueError("customer_ids is empty; pass None for
          + the full population")                                        
       76 +    signup = customers.set_index("customer_id")["signup_date"]
       77 +    seen = set()                                              
       78 +    for raw in ids:                                           
       79 +        if isinstance(raw, bool) or int(raw) != raw:          
       80 +            raise ValueError(f"customer_id {raw!r} is not an i
          +nteger")                                                      
       81 +        cid = int(raw)                                        
       82 +        if cid in seen:                                       
       83 +            raise ValueError(f"customer_id {cid} is requested 
          +more than once")                                              
       84 +        seen.add(cid)                                         
       85 +        if cid not in signup.index:                           
       86 +            raise ValueError(f"customer_id {cid} does not exis
          +t in customers.csv")                                          
       87 +        if signup[cid] >= as_of:                              
       88 +            raise ValueError(                                 
       89 +                f"customer_id {cid} signed up on {signup[cid].
          +date()}, not before as_of {as_of.date()}"                     
       90 +            )                                                 
       91 +    return [int(i) for i in ids]                              
       92 +                                                              
       93 +                                                              
       94  def build_features(
       18 -    as_of: str,                                               
       95 +    as_of,                                                    
       96      source_paths: dict | None = None,
       97      customer_ids: list[int] | None = None,
       98 +    feature_params: dict[str, dict] | None = None,            
       99  ) -> pd.DataFrame:
       22 -    """Build the full feature set as of `as_of` for either the
          - whole customer base                                          
       23 -    (training path, `customer_ids=None`) or a specific subset 
          -(scoring path, one or more                                    
       24 -    `customer_ids`).                                          
      100 +    """Build all 7 features at `as_of` for the full population
          + (`customer_ids=None`, the                                    
      101 +    training call) or for the requested ids (the scoring call)
          +.                                                             
      102  
       26 -    TODO:                                                     
       27 -    - Default `source_paths` to the standard data/ locations  
       28 -      (customers.csv, transactions.db, transactions_monthly_ag
          -g.parquet) if not provided.                                   
       29 -    - Load all three sources via features/ingest.py — no direc
          -t file I/O here.                                              
       30 -    - Filter to `customer_ids` if provided; otherwise use the 
          -full customer base. This                                      
       31 -      filtering is the ONLY difference allowed between the tra
          -ining and scoring call                                        
       32 -      sites — every transform must run identically either way.
       33 -    - Compute every candidate feature named in your confirmed 
          -feature_spec.md via                                           
       34 -      features/transforms.py, applying the explicit zero-activ
          -ity default per feature.                                      
       35 -    - Return a DataFrame with one row per requested customer_i
          -d, one column per feature,                                    
       36 -      plus `customer_id` and `as_of_date`.                    
      103 +    `as_of` is required and parsed once here. `feature_params`
          + maps a feature name to one                                   
      104 +    version's parameters (used by the training script to rebui
          +ld older versions); features                                  
      105 +    it doesn't name use their latest version in `features/sche
          +ma.py`.                                                       
      106 +                                                              
      107 +    Returns one row per customer: `customer_id`, the 7 feature
          +s in schema order, and                                        
      108 +    `as_of_date`. The check results are attached as `df.attrs[
          +"quality_checks"]`. Raises                                    
      109 +    `QualityCheckError` if any of QC1-QC6 or QC8 fails, whiche
          +ver rows were requested.                                      
      110      """
       38 -    raise NotImplementedError                                 
      111 +    as_of_ts = transforms.parse_as_of(as_of)                  
      112 +    paths = resolve_source_paths(source_paths)                
      113 +    params = schema.resolve_feature_params(feature_params)    
      114 +                                                              
      115 +    customers = ingest.load_customers(paths["customers"])     
      116 +    transactions = ingest.load_transactions(paths["transaction
          +s"])                                                          
      117 +    monthly = ingest.load_monthly_agg(paths["monthly_agg"])   
      118 +    buyer_ids = ingest.load_customer_ids_with_txn_before(paths
          +["transactions"], as_of_ts)                                   
      119 +                                                              
      120 +    # The cutoff: no DB row at or after as_of, and no parquet 
          +month at or after as_of's month,                              
      121 +    # reaches a transform (QC5 verifies these exact frames).  
      122 +    transactions_used = transactions.loc[transactions["txn_tim
          +estamp"] < as_of_ts]                                          
      123 +    monthly_used = monthly.loc[monthly["txn_month"] < transfor
          +ms.month_start(as_of_ts)]                                     
      124 +    results = qc.run_input_checks(transactions_used, monthly_u
          +sed, transactions, monthly, as_of_ts, params)                 
      125 +    qc.raise_on_failure(results)                              
      126 +                                                              
      127 +    population = transforms.population(customers, as_of_ts)   
      128 +    features = _compute_features(population, transactions_used
          +, monthly_used, as_of_ts, params)                             
      129 +    output_results = qc.run_output_checks(                    
      130 +        features, customers=customers, transactions=transactio
          +ns, monthly=monthly,                                          
      131 +        buyer_ids=buyer_ids, as_of=as_of_ts, params=params,   
      132 +    )                                                         
      133 +    results += output_results                                 
      134 +    qc.raise_on_failure(output_results)                       
      135 +                                                              
      136 +    for spec in schema.FEATURE_SCHEMA:                        
      137 +        if str(features[spec.name].dtype) != spec.dtype:      
      138 +            raise TypeError(f"{spec.name} built as {features[s
          +pec.name].dtype}, schema.py says {spec.dtype}")               
      139 +                                                              
      140 +    # The only training/scoring difference: which rows are ret
          +urned.                                                        
      141 +    requested = population.index.tolist() if customer_ids is N
          +one else _requested_ids(customer_ids, customers, as_of_ts)    
      142 +    selected = features.set_index("customer_id").loc[requested
          +].reset_index()                                               
      143 +    selection = qc.check_selection_rows(selected, requested)  
      144 +    results.append(selection)                                 
      145 +    qc.raise_on_failure([selection])                          
      146 +                                                              
      147 +    selected["as_of_date"] = as_of_ts                         
      148 +    selected.attrs["quality_checks"] = [r.as_dict() for r in r
          +esults]                                                       
      149 +    selected.attrs["feature_params"] = params                 
      150 +    return selected                                           

● A quick smoke run of the shared build at the spec date and at the label date,
  before writing the store. The label-date run should fail QC6, because of the
  July ETL gap.

  Ran 2 shell commands (ctrl+o to expand)

● The build finishes in 0.33s with all checks passing, matching population,
  zero-activity, and the expected feature statistics—QC6 correctly flags the 110
  undercounted and 102 absent July customer-months. I'll now fix QC5 and QC6 so
  failing_features lists the actual features affected rather than source or
  month names.

● Bash(conda run --no-capture-output -n churn-pipeline python - <<'EOF'
      p = "features/quality_checks.py"…)
  ⎿  Updated features/quality_checks.py (+15 -5)
      214          f"{cutoff_month.strftime('%Y-%m')}"
      215      )
      216      bad = _ids(pd.concat([late_txn["customer_id"], late_month[
           "customer_id"]]))
      217 -    failing = tuple(s for s, df in (("transactions", late_txn)
          -, ("monthly_agg", late_month)) if not df.empty)               
      218 -    return CheckResult("QC5", "no future data", passed, detail
          -, failing, bad)                                               
      217 +    failing = []                                              
      218 +    if not late_txn.empty:                                    
      219 +        failing += [f.name for f in schema.FEATURE_SCHEMA if f
          +.source == "transactions.db"]                                 
      220 +    if not late_month.empty:                                  
      221 +        failing += [f.name for f in schema.FEATURE_SCHEMA if f
          +.source.startswith("transactions_monthly_agg")]               
      222 +    return CheckResult("QC5", "no future data", passed, detail
          +, tuple(failing), bad)                                        
      223  
      224  
      225  # QC6 --------------------------------------------------------
           ----------------------------
      222 -def coverage_months(as_of: pd.Timestamp, params: dict[str, dic
          -t]) -> list[pd.Timestamp]:                                    
      226 +def coverage_months(as_of: pd.Timestamp, params: dict[str, dic
          +t], only: str | None = None) -> list[pd.Timestamp]:           
      227 +    """Month starts used by any month window at these paramete
          +rs (or by feature `only`)."""                                 
      228      from features.transforms import month_window
      229  
      230      months = set()
      226 -    for w in schema.month_windows_in_effect(params):          
      231 +    for w in schema.month_windows_in_effect(params, only=only)
          +:                                                             
      232          start, end = month_window(as_of, w.months, w.offset)
      233          months.update(pd.date_range(start, end, freq="MS", inc
           lusive="left"))
      234      return sorted(months)
     ...
      271          + (f"; failing months {bad_months}" if bad_months else
            "")
      272      )
      273      rows = _ids(pd.concat([bad["customer_id"], duplicate_keys[
           "customer_id"]]))
      269 -    return CheckResult("QC6", "parquet coverage", passed, deta
          -il, tuple(bad_months), rows)                                  
      274 +    failing_months = set(bad["txn_month"]) | set(duplicate_key
          +s["txn_month"])                                               
      275 +    affected = tuple(                                         
      276 +        f.name for f in schema.FEATURE_SCHEMA                 
      277 +        if any(m in failing_months for m in coverage_months(as
          +_of, {**params, f.name: params[f.name]}, only=f.name))        
      278 +    )                                                         
      279 +    return CheckResult("QC6", "parquet coverage", passed, deta
          +il, affected, rows)                                           
      280  
      281  
      282  # QC7 --------------------------------------------------------
           ----------------------------
  ⎿  Updated features/schema.py (+5 -2)
      420      return resolved
      421  
      422  
      423 -def month_windows_in_effect(params: dict[str, dict]) -> list[M
          -onthWindow]:                                                  
      424 -    """Every month window any feature uses at these parameters
          - (QC6's coverage scope)."""                                   
      423 +def month_windows_in_effect(params: dict[str, dict], only: str
          + | None = None) -> list[MonthWindow]:                         
      424 +    """Every month window any feature (or just feature `only`)
          + uses at these parameters                                     
      425 +    (QC6's coverage scope)."""                                
      426      windows = []
      427      for spec in FEATURE_SCHEMA:
      428 +        if only is not None and spec.name != only:            
      429 +            continue                                          
      430          windows.extend(
      431              w for w in spec.windows(spec.fixed, params[spec.na
           me]) if isinstance(w, MonthWindow)
      432          )

● Update(features/quality_checks.py)
  ⎿  Added 1 line, removed 1 line
      274      failing_months = set(bad["txn_month"]) | set(duplicate_key
           s["txn_month"])
      275      affected = tuple(
      276          f.name for f in schema.FEATURE_SCHEMA
      277 -        if any(m in failing_months for m in coverage_months(as
          -_of, {**params, f.name: params[f.name]}, only=f.name))        
      277 +        if failing_months & set(coverage_months(as_of, params,
          + only=f.name))                                                
      278      )
      279      return CheckResult("QC6", "parquet coverage", passed, deta
           il, affected, rows)
      280  

● Update(features/build.py)
  ⎿  Added 5 lines, removed 2 lines
      76      signup = customers.set_index("customer_id")["signup_date"]
      77      seen = set()
      78      for raw in ids:
      79 -        if isinstance(raw, bool) or int(raw) != raw:           
      79 +        try:                                                   
      80 +            cid = int(raw)                                     
      81 +        except (TypeError, ValueError):                        
      82 +            cid = None                                         
      83 +        if isinstance(raw, bool) or cid is None or cid != raw: 
      84              raise ValueError(f"customer_id {raw!r} is not an in
          teger")
      81 -        cid = int(raw)                                         
      85          if cid in seen:
      86              raise ValueError(f"customer_id {cid} is requested m
          ore than once")
      87          seen.add(cid)

● Update(features/build.py)
  ⎿  Added 1 line, removed 1 line
      91              raise ValueError(
      92                  f"customer_id {cid} signed up on {signup[cid].d
          ate()}, not before as_of {as_of.date()}"
      93              )
      94 -    return [int(i) for i in ids]                               
      94 +    return [int(i) for i in ids]  # validated above            
      95  
      96  
      97  def build_features(

● Update(features/build.py)
  ⎿  Added 1 line, removed 1 line
      91              raise ValueError(
      92                  f"customer_id {cid} signed up on {signup[cid].d
          ate()}, not before as_of {as_of.date()}"
      93              )
      94 -    return [int(i) for i in ids]  # validated above            
      94 +    return [int(i) for i in ids]                               
      95  
      96  
      97  def build_features(
● Looking at the feature store next — the SQLite registry enforces immutability
  via triggers blocking overwrites/deletes, a partial unique index limiting one
  is_latest row per feature, and exclusive-create parquet writes.

● Write(feature_store/store.py)
  ⎿  Added 251 lines, removed 38 lines
        1  """
        2 -Local feature store API: a SQLite metadata/version registry (`
          -feature_store/registry.db`,                                   
        3 -table `feature_registry`) plus one parquet value snapshot per 
          -registered version                                            
        4 -(`feature_store/values/<feature_name>__v<version>.parquet`).  
        2 +Local feature store API: a SQLite metadata/version registry (`
          +registry.db`, table                                           
        3 +`feature_registry`) plus one parquet value snapshot per regist
          +ered version                                                  
        4 +(`values/<feature_name>__v<version>.parquet`), both under one 
          +store root.                                                   
        5  
        6 -Do not add mlflow, feast, or any other feature-store library —
          - this store is SQLite +                                       
        7 -parquet only. See skill.md's Workflow section for the exact re
          -gistry schema this module                                     
        8 -must implement (columns: feature_name, version, created_at, de
          -scription, source_columns,                                    
        9 -transform_summary, business_rationale, parameters, parquet_pat
          -h, is_latest).                                                
        6 +The store root defaults to this module's own directory (`featu
          +re_store/`), resolved from                                    
        7 +the module path rather than the working directory. Every funct
          +ion takes a keyword-only                                      
        8 +`store_dir` to override it; tests always pass a temporary one.
        9  
       11 -Never hardcode a parquet filename outside this module — all re
          -ads must resolve the path                                     
       12 -via the registry.                                             
       10 +Versions are append-only, enforced in the database as well as 
          +in this module:                                               
       11 +- a new version always gets a new registry row and a new parqu
          +et file. The file is opened                                   
       12 +  with exclusive create, so an existing file is never overwrit
          +ten;                                                          
       13 +- triggers reject any UPDATE of a row's content, any DELETE, a
          +nd any attempt to set                                         
       14 +  is_latest on an existing row, so is_latest can only move to 
          +a newly inserted version;                                     
       15 +- a partial unique index allows at most one is_latest row per 
          +feature, and the old latest                                   
       16 +  row is cleared in the same transaction as the insert.       
       17 +                                                              
       18 +Every stored snapshot in one store has the same as_of_date (fe
          +ature_spec.md, "One as_of per                                 
       19 +store"). The registry has no as_of column, so the date is read
          + from the snapshots.                                          
       20 +                                                              
       21 +Never hardcode a parquet filename outside this module — all re
          +ads resolve the path via the                                  
       22 +registry.                                                     
       23  """
       24  
       25 +from __future__ import annotations                            
       26 +                                                              
       27 +from contextlib import closing                                
       28 +from datetime import datetime, timezone                       
       29 +import json                                                   
       30  from pathlib import Path
       31 +import re                                                     
       32 +import sqlite3                                                
       33  
       34  import pandas as pd
       35  
       19 -REGISTRY_DB = "feature_store/registry.db"                     
       20 -VALUES_DIR = "feature_store/values"                           
       36 +DEFAULT_STORE_DIR = Path(__file__).resolve().parent           
       37 +REGISTRY_FILENAME = "registry.db"                             
       38 +VALUES_DIRNAME = "values"                                     
       39 +_FEATURE_NAME = re.compile(r"[a-z][a-z0-9_]*")                
       40  
       41 +_DDL = """                                                    
       42 +CREATE TABLE IF NOT EXISTS feature_registry (                 
       43 +    feature_name       TEXT    NOT NULL,                      
       44 +    version            INTEGER NOT NULL CHECK (version >= 1), 
       45 +    created_at         TEXT    NOT NULL,                      
       46 +    description        TEXT    NOT NULL,                      
       47 +    source_columns     TEXT    NOT NULL,                      
       48 +    transform_summary  TEXT    NOT NULL,                      
       49 +    business_rationale TEXT    NOT NULL,                      
       50 +    parameters         TEXT    NOT NULL,                      
       51 +    parquet_path       TEXT    NOT NULL UNIQUE,               
       52 +    is_latest          INTEGER NOT NULL CHECK (is_latest IN (0
          +, 1)),                                                        
       53 +    PRIMARY KEY (feature_name, version)                       
       54 +);                                                            
       55 +CREATE UNIQUE INDEX IF NOT EXISTS feature_registry_one_latest 
       56 +    ON feature_registry (feature_name) WHERE is_latest = 1;   
       57 +CREATE TRIGGER IF NOT EXISTS feature_registry_rows_immutable  
       58 +    BEFORE UPDATE OF feature_name, version, created_at, descri
          +ption, source_columns,                                        
       59 +                     transform_summary, business_rationale, pa
          +rameters, parquet_path                                        
       60 +    ON feature_registry                                       
       61 +BEGIN                                                         
       62 +    SELECT RAISE(ABORT, 'feature_registry rows are immutable; 
          +register a new version instead');                             
       63 +END;                                                          
       64 +CREATE TRIGGER IF NOT EXISTS feature_registry_latest_only_move
          +s_forward                                                     
       65 +    BEFORE UPDATE OF is_latest ON feature_registry            
       66 +    WHEN NEW.is_latest = 1 AND OLD.is_latest = 0              
       67 +BEGIN                                                         
       68 +    SELECT RAISE(ABORT, 'is_latest is set only by registering 
          +a new version');                                              
       69 +END;                                                          
       70 +CREATE TRIGGER IF NOT EXISTS feature_registry_rows_never_delet
          +ed                                                            
       71 +    BEFORE DELETE ON feature_registry                         
       72 +BEGIN                                                         
       73 +    SELECT RAISE(ABORT, 'feature_registry rows are never delet
          +ed');                                                         
       74 +END;                                                          
       75 +"""                                                           
       76  
       77 +REGISTRY_COLUMNS = [                                          
       78 +    "feature_name", "version", "created_at", "description", "s
          +ource_columns",                                               
       79 +    "transform_summary", "business_rationale", "parameters", "
          +parquet_path", "is_latest",                                   
       80 +]                                                             
       81 +                                                              
       82 +                                                              
       83 +class StoreError(Exception):                                  
       84 +    """A registration the store refuses: it would break one of
          + the store's invariants."""                                   
       85 +                                                              
       86 +                                                              
       87 +def _store_dir(store_dir) -> Path:                            
       88 +    return DEFAULT_STORE_DIR if store_dir is None else Path(st
          +ore_dir).resolve()                                            
       89 +                                                              
       90 +                                                              
       91 +def registry_path(*, store_dir=None) -> Path:                 
       92 +    return _store_dir(store_dir) / REGISTRY_FILENAME          
       93 +                                                              
       94 +                                                              
       95 +def _connect_write(root: Path) -> sqlite3.Connection:         
       96 +    (root / VALUES_DIRNAME).mkdir(parents=True, exist_ok=True)
       97 +    con = sqlite3.connect(root / REGISTRY_FILENAME, isolation_
          +level=None)                                                   
       98 +    con.executescript(_DDL)                                   
       99 +    return con                                                
      100 +                                                              
      101 +                                                              
      102 +def _connect_read(root: Path) -> sqlite3.Connection | None:   
      103 +    """Read-only connection, or None when the store has no reg
          +istry yet. Reads never create                                 
      104 +    files."""                                                 
      105 +    path = root / REGISTRY_FILENAME                           
      106 +    if not path.exists():                                     
      107 +        return None                                           
      108 +    return sqlite3.connect(path.as_uri() + "?mode=ro", uri=Tru
          +e)                                                            
      109 +                                                              
      110 +                                                              
      111 +def _query(root: Path, sql: str, args: tuple = ()) -> pd.DataF
          +rame:                                                         
      112 +    con = _connect_read(root)                                 
      113 +    if con is None:                                           
      114 +        return pd.DataFrame(columns=REGISTRY_COLUMNS)         
      115 +    with closing(con):                                        
      116 +        return pd.read_sql_query(sql, con, params=args)       
      117 +                                                              
      118 +                                                              
      119 +def _resolve(root: Path, parquet_path: str) -> Path:          
      120 +    """parquet_path is stored relative to the store root's par
          +ent, e.g.                                                     
      121 +    'feature_store/values/x__v1.parquet'. Refuse anything outs
          +ide the values directory."""                                  
      122 +    path = (root.parent / parquet_path).resolve()             
      123 +    if path.parent != (root / VALUES_DIRNAME).resolve():      
      124 +        raise StoreError(f"registry parquet_path {parquet_path
          +!r} is outside {root / VALUES_DIRNAME}")                      
      125 +    return path                                               
      126 +                                                              
      127 +                                                              
      128 +def _snapshot_as_of(path: Path) -> set[pd.Timestamp]:         
      129 +    return set(pd.to_datetime(pd.read_parquet(path, columns=["
          +as_of_date"])["as_of_date"]).unique())                        
      130 +                                                              
      131 +                                                              
      132 +def _validate_values(feature_name: str, values_df: pd.DataFram
          +e) -> pd.DataFrame:                                           
      133 +    expected = ["customer_id", feature_name, "as_of_date"]    
      134 +    if sorted(values_df.columns) != sorted(expected):         
      135 +        raise StoreError(f"values_df must have exactly the col
          +umns {expected}, got {list(values_df.columns)}")              
      136 +    df = values_df[expected].reset_index(drop=True)           
      137 +    if df.empty:                                              
      138 +        raise StoreError(f"values_df for {feature_name} is emp
          +ty")                                                          
      139 +    if df["customer_id"].isna().any() or df["customer_id"].dup
          +licated().any():                                              
      140 +        raise StoreError(f"values_df for {feature_name} has nu
          +ll or duplicated customer_id")                                
      141 +    if df[feature_name].isna().any():                         
      142 +        raise StoreError(f"values_df for {feature_name} has nu
          +ll values")                                                   
      143 +    dates = pd.to_datetime(df["as_of_date"]).unique()         
      144 +    if len(dates) != 1 or pd.isna(dates[0]):                  
      145 +        raise StoreError(f"values_df for {feature_name} must h
          +ave exactly one as_of_date, got {list(dates)}")               
      146 +    df["as_of_date"] = pd.to_datetime(df["as_of_date"]).astype
          +("datetime64[ns]")                                            
      147 +    return df.sort_values("customer_id", ignore_index=True)   
      148 +                                                              
      149 +                                                              
      150  def register_version(
      151      feature_name: str,
      152      values_df: pd.DataFrame,
     ...
      155      transform_summary: str,
      156      business_rationale: str,
      157      parameters: dict,
      158 +    *,                                                        
      159 +    store_dir=None,                                           
      160  ) -> int:
       32 -    """Write a new parquet snapshot, insert the registry row, 
          -flip is_latest, return the                                    
       33 -    new version number.                                       
      161 +    """Write a new parquet snapshot, insert the registry row, 
          +flip is_latest, and return the                                
      162 +    new version number (1 for a new feature, then max + 1).   
      163  
       35 -    TODO:                                                     
       36 -    - Determine the next version number for feature_name (star
          -ts at 1, increments).                                         
       37 -    - Write values_df to feature_store/values/<feature_name>__
          -v<version>.parquet —                                          
       38 -      never overwrite an existing version's file.             
       39 -    - Insert a row into feature_registry with all metadata col
          -umns, JSON-encoding                                           
       40 -      `parameters`.                                           
       41 -    - In the same transaction, flip the prior is_latest row (i
          -f any) to 0 and set the                                       
       42 -      new row's is_latest to 1.                               
       43 -    - Return the new version number.                          
      164 +    Raises StoreError if the snapshot's as_of_date differs fro
          +m the store's, or if the                                      
      165 +    version's parquet file already exists (it is never overwri
          +tten).                                                        
      166      """
       45 -    raise NotImplementedError                                 
      167 +    if not _FEATURE_NAME.fullmatch(feature_name):             
      168 +        raise StoreError(f"feature_name {feature_name!r} must 
          +match {_FEATURE_NAME.pattern}")                               
      169 +    if not isinstance(parameters, dict):                      
      170 +        raise StoreError("parameters must be a dict")         
      171 +    params_json = json.dumps(parameters, sort_keys=True)      
      172 +    df = _validate_values(feature_name, values_df)            
      173 +    new_as_of = pd.Timestamp(df["as_of_date"].iloc[0])        
      174  
      175 +    root = _store_dir(store_dir)                              
      176 +    created_file: Path | None = None                          
      177 +    with closing(_connect_write(root)) as con:                
      178 +        con.execute("BEGIN IMMEDIATE")                        
      179 +        try:                                                  
      180 +            stored = [r[0] for r in con.execute("SELECT parque
          +t_path FROM feature_registry")]                               
      181 +            store_dates = set().union(*(_snapshot_as_of(_resol
          +ve(root, p)) for p in stored)) if stored else set()           
      182 +            if store_dates and store_dates != {new_as_of}:    
      183 +                raise StoreError(                             
      184 +                    f"store holds snapshots at {sorted(d.date(
          +) for d in store_dates)}; refusing to "                       
      185 +                    f"register {feature_name} at {new_as_of.da
          +te()} (one as_of per store)"                                  
      186 +                )                                             
      187 +            version = con.execute(                            
      188 +                "SELECT COALESCE(MAX(version), 0) + 1 FROM fea
          +ture_registry WHERE feature_name = ?",                        
      189 +                (feature_name,),                              
      190 +            ).fetchone()[0]                                   
      191 +            relative = f"{root.name}/{VALUES_DIRNAME}/{feature
          +_name}__v{version}.parquet"                                   
      192 +            path = _resolve(root, relative)                   
      193 +            try:                                              
      194 +                handle = open(path, "xb")                     
      195 +            except FileExistsError:                           
      196 +                raise StoreError(                             
      197 +                    f"{relative} already exists but is not reg
          +istered; refusing to overwrite it"                            
      198 +                ) from None                                   
      199 +            created_file = path                               
      200 +            with handle:                                      
      201 +                df.to_parquet(handle, index=False)            
      202 +            con.execute(                                      
      203 +                "UPDATE feature_registry SET is_latest = 0 WHE
          +RE feature_name = ? AND is_latest = 1",                       
      204 +                (feature_name,),                              
      205 +            )                                                 
      206 +            con.execute(                                      
      207 +                f"INSERT INTO feature_registry ({', '.join(REG
          +ISTRY_COLUMNS)}) "                                            
      208 +                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)",      
      209 +                (                                             
      210 +                    feature_name, version, datetime.now(timezo
          +ne.utc).isoformat(timespec="seconds"),                        
      211 +                    description, source_columns, transform_sum
          +mary, business_rationale,                                     
      212 +                    params_json, relative,                    
      213 +                ),                                            
      214 +            )                                                 
      215 +            con.execute("COMMIT")                             
      216 +        except BaseException:                                 
      217 +            con.execute("ROLLBACK")                           
      218 +            if created_file is not None:                      
      219 +                created_file.unlink(missing_ok=True)          
      220 +            raise                                             
      221 +    return int(version)                                       
      222  
       48 -def get_latest(feature_name: str) -> pd.DataFrame:            
       49 -    """Return the DataFrame for the version currently flagged 
          -is_latest = 1.                                                
      223  
       51 -    TODO: look up the latest row in feature_registry, resolve 
          -parquet_path, load it.                                        
       52 -    """                                                       
       53 -    raise NotImplementedError                                 
      224 +def list_versions(feature_name: str, *, store_dir=None) -> pd.
          +DataFrame:                                                    
      225 +    """Every registered version's registry row for feature_nam
          +e, most recent first. Empty                                   
      226 +    (with the registry columns) if the feature has none."""   
      227 +    return _query(                                            
      228 +        _store_dir(store_dir),                                
      229 +        f"SELECT {', '.join(REGISTRY_COLUMNS)} FROM feature_re
          +gistry WHERE feature_name = ? "                               
      230 +        "ORDER BY version DESC",                              
      231 +        (feature_name,),                                      
      232 +    )                                                         
      233  
      234  
       56 -def get_version(feature_name: str, version: int) -> pd.DataFra
          -me:                                                           
       57 -    """Return the DataFrame for a specific (feature_name, vers
          -ion).                                                         
      235 +def list_features(*, store_dir=None) -> list[str]:            
      236 +    df = _query(_store_dir(store_dir), "SELECT DISTINCT featur
          +e_name FROM feature_registry ORDER BY feature_name")          
      237 +    return df["feature_name"].tolist()                        
      238  
       59 -    TODO: look up the exact row in feature_registry, resolve p
          -arquet_path, load it.                                         
       60 -    """                                                       
       61 -    raise NotImplementedError                                 
      239  
      240 +def get_version(feature_name: str, version: int, *, store_dir=
          +None) -> pd.DataFrame:                                        
      241 +    """The snapshot of (feature_name, version), resolved throu
          +gh the registry."""                                           
      242 +    root = _store_dir(store_dir)                              
      243 +    row = _query(                                             
      244 +        root,                                                 
      245 +        "SELECT parquet_path FROM feature_registry WHERE featu
          +re_name = ? AND version = ?",                                 
      246 +        (feature_name, int(version)),                         
      247 +    )                                                         
      248 +    if row.empty:                                             
      249 +        raise KeyError(f"{feature_name} v{version} is not regi
          +stered in {root}")                                            
      250 +    return pd.read_parquet(_resolve(root, row["parquet_path"].
          +iloc[0]))                                                     
      251  
       64 -def list_versions(feature_name: str) -> pd.DataFrame:         
       65 -    """Return every registered version's metadata row for feat
          -ure_name, most recent first.                                  
      252  
       67 -    TODO: query feature_registry, order by version descending.
       68 -    """                                                       
       69 -    raise NotImplementedError                                 
      253 +def latest_version(feature_name: str, *, store_dir=None) -> in
          +t:                                                            
      254 +    """The version flagged is_latest, checked against the high
          +est registered version."""                                    
      255 +    root = _store_dir(store_dir)                              
      256 +    rows = _query(                                            
      257 +        root,                                                 
      258 +        "SELECT (SELECT version FROM feature_registry WHERE fe
          +ature_name = ? AND is_latest = 1) AS latest, "                
      259 +        "(SELECT MAX(version) FROM feature_registry WHERE feat
          +ure_name = ?) AS highest",                                    
      260 +        (feature_name, feature_name),                         
      261 +    )                                                         
      262 +    if rows.empty or pd.isna(rows["highest"].iloc[0]):        
      263 +        raise KeyError(f"{feature_name} has no registered vers
          +ions in {root}")                                              
      264 +    latest, highest = rows["latest"].iloc[0], int(rows["highes
          +t"].iloc[0])                                                  
      265 +    if pd.isna(latest) or int(latest) != highest:             
      266 +        raise StoreError(f"{feature_name}: is_latest points to
          + v{latest} but the highest version is v{highest}")            
      267 +    return highest                                            
      268 +                                                              
      269 +                                                              
      270 +def get_latest(feature_name: str, *, store_dir=None) -> pd.Dat
          +aFrame:                                                       
      271 +    """The snapshot of the version flagged is_latest = 1 (alwa
          +ys the highest version)."""                                   
      272 +    return get_version(feature_name, latest_version(feature_na
          +me, store_dir=store_dir), store_dir=store_dir)                
      273 +                                                              
      274 +                                                              
      275 +def store_as_of(*, store_dir=None) -> pd.Timestamp | None:    
      276 +    """The single as_of_date of every snapshot in the store, o
          +r None if the store is empty."""                              
      277 +    root = _store_dir(store_dir)                              
      278 +    paths = _query(root, "SELECT parquet_path FROM feature_reg
          +istry")["parquet_path"].tolist()                              
      279 +    dates = set().union(*(_snapshot_as_of(_resolve(root, p)) f
          +or p in paths)) if paths else set()                           
      280 +    if len(dates) > 1:                                        
      281 +        raise StoreError(f"store mixes as_of dates {sorted(dat
          +es)}")                                                        
      282 +    return pd.Timestamp(next(iter(dates))) if dates else None 

● Now the three scripts, plus a lineage generator that reads the schema and
  registry.

● Write(scripts/build_training_features.py)
  ⎿  Added 154 lines, removed 22 lines
        1  #!/usr/bin/env python3
        2  """
        3 -Builds features for the full training population as of a given
          - date, writes a versioned                                     
        4 -parquet feature snapshot, and registers every candidate featur
          -e into the feature store.                                     
        3 +Builds features for the full training population as of a given
          + date and registers every                                     
        4 +version of every candidate feature listed in features/schema.p
          +y into the feature store.                                     
        5  
        6 -TODO:                                                         
        7 -- Parse an --as-of argument (default 2026-08-01, per data_dict
          -ionary.md's as-of-date                                        
        8 -  convention).                                                
        9 -- Call the single shared build_features(as_of, ...) entry poin
          -t (features/build.py) —                                       
       10 -  do not duplicate feature logic here.                        
       11 -- Run features/quality_checks.py against the result before wri
          -ting anything; stop (or                                       
       12 -  clearly flag) on failure rather than silently writing bad ou
          -tput.                                                         
       13 -- Register every candidate feature via feature_store/store.py'
          -s register_version().                                         
       14 -- Print a quality-check summary.                              
        6 +Order of work (feature_spec.md, "Registration"):              
        7 +1. Build every schema version through the one shared build_fea
          +tures(as_of, ...) entry                                       
        8 +   point: build k uses each feature's k-th version (or its las
          +t, if it has fewer). Each                                     
        9 +   build runs QC1-QC6 and QC8 inside build_features.          
       10 +2. Plan every (feature, version) against the store, running QC
          +7 on each:                                                    
       11 +   - a stored version with identical parameters: the rebuilt v
          +alues must equal the stored                                   
       12 +     ones (QC7). If they do, the version is reported as "uncha
          +nged — skipped";                                              
       13 +   - otherwise it is new ("no reference"), and its values must
          + differ from the previous                                     
       14 +     schema version's for at least one customer, or it would b
          +e a cosmetic bump.                                            
       15 +3. Only if every check passed: register the new versions, olde
          +st first, so get_latest()                                     
       16 +   returns each feature's highest version. Then regenerate FEA
          +TURE_LINEAGE.md.                                              
       17 +                                                              
       18 +Any failure exits non-zero before anything is written.        
       19  """
       20  
       21 +from __future__ import annotations                            
       22 +                                                              
       23  import argparse
       24 +from dataclasses import dataclass                             
       25 +import json                                                   
       26 +from pathlib import Path                                      
       27 +import sys                                                    
       28  
       29 +sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  
       30  
       20 -def main() -> None:                                           
       21 -    parser = argparse.ArgumentParser(description=__doc__)     
       22 -    parser.add_argument(                                      
       23 -        "--as-of",                                            
       24 -        default="2026-08-01",                                 
       25 -        help="As-of date for feature computation (default: 202
          -6-08-01).",                                                   
       26 -    )                                                         
       27 -    args = parser.parse_args()                                
       31 +import numpy as np                                            
       32 +import pandas as pd                                           
       33  
       29 -    # TODO: implement — see module docstring above.           
       30 -    raise NotImplementedError                                 
       34 +from feature_store import store                               
       35 +from features import schema                                   
       36 +from features.build import REPO_ROOT, build_features          
       37 +from features.quality_checks import (                         
       38 +    CheckResult,                                              
       39 +    QualityCheckError,                                        
       40 +    check_drift_against_store,                                
       41 +    format_results,                                           
       42 +    raise_on_failure,                                         
       43 +)                                                             
       44 +from features.transforms import parse_as_of                   
       45 +from scripts.generate_lineage import DEFAULT_LINEAGE_PATH, wri
          +te_lineage                                                    
       46  
       47 +SPEC_AS_OF = "2026-05-02"                                     
       48  
       49 +                                                              
       50 +class VersioningError(Exception):                             
       51 +    pass                                                      
       52 +                                                              
       53 +                                                              
       54 +@dataclass                                                    
       55 +class PlannedVersion:                                         
       56 +    spec: schema.FeatureSpec                                  
       57 +    schema_version: schema.FeatureVersion                     
       58 +    values: pd.DataFrame                                      
       59 +    qc7: CheckResult                                          
       60 +    stored_version: int | None  # set when an identical-parame
          +ter version is already stored                                 
       61 +                                                              
       62 +    @property                                                 
       63 +    def action(self) -> str:                                  
       64 +        return "skip" if self.stored_version is not None else 
          +"register"                                                    
       65 +                                                              
       66 +                                                              
       67 +def build_all_versions(as_of: pd.Timestamp) -> dict[int, pd.Da
          +taFrame]:                                                     
       68 +    """One full-population build per version index; feature f'
          +s k-th version is read from                                   
       69 +    build k."""                                               
       70 +    n = max(len(f.versions) for f in schema.FEATURE_SCHEMA)   
       71 +    builds = {}                                               
       72 +    for k in range(1, n + 1):                                 
       73 +        params = {f.name: f.versions[min(k, len(f.versions)) -
          + 1].parameters for f in schema.FEATURE_SCHEMA}                
       74 +        builds[k] = build_features(as_of, feature_params=param
          +s)                                                            
       75 +        print(f"build {k}/{n}: {len(builds[k])} customers")   
       76 +        print(format_results([CheckResult(**r) for r in builds
          +[k].attrs["quality_checks"]]))                                
       77 +    return builds                                             
       78 +                                                              
       79 +                                                              
       80 +def _stored_with_parameters(name: str, parameters: dict, store
          +_dir) -> int | None:                                          
       81 +    versions = store.list_versions(name, store_dir=store_dir) 
       82 +    matches = [int(r.version) for r in versions.itertuples() i
          +f json.loads(r.parameters) == parameters]                     
       83 +    return max(matches) if matches else None                  
       84 +                                                              
       85 +                                                              
       86 +def plan_registrations(builds: dict[int, pd.DataFrame], store_
          +dir) -> list[PlannedVersion]:                                 
       87 +    plan, qc7_results, cosmetic = [], [], []                  
       88 +    for spec in schema.FEATURE_SCHEMA:                        
       89 +        previous = None                                       
       90 +        for k, version in enumerate(spec.versions, start=1):  
       91 +            values = builds[k][["customer_id", spec.name, "as_
          +of_date"]].reset_index(drop=True)                             
       92 +            stored = _stored_with_parameters(spec.name, versio
          +n.parameters, store_dir)                                      
       93 +            reference = store.get_version(spec.name, stored, s
          +tore_dir=store_dir) if stored else None                       
       94 +            qc7 = check_drift_against_store(spec.name, values,
          + reference)                                                   
       95 +            qc7_results.append(qc7)                           
       96 +            if stored is None and previous is not None:       
       97 +                a = previous[spec.name].to_numpy(dtype="float6
          +4")                                                           
       98 +                b = values[spec.name].to_numpy(dtype="float64"
          +)                                                             
       99 +                if np.array_equal(a, b):                      
      100 +                    cosmetic.append(f"{spec.name} v{version.ve
          +rsion}: parameters {version.parameters} "                     
      101 +                                    f"give the same values as 
          +v{version.version - 1}")                                      
      102 +            plan.append(PlannedVersion(spec, version, values, 
          +qc7, stored))                                                 
      103 +            previous = values                                 
      104 +    print(format_results(qc7_results))                        
      105 +    raise_on_failure(qc7_results)                             
      106 +    if cosmetic:                                              
      107 +        raise VersioningError("not a real version bump (values
          + unchanged):\n" + "\n".join(cosmetic))                        
      108 +    return plan                                               
      109 +                                                              
      110 +                                                              
      111 +def business_rationale(spec: schema.FeatureSpec, version: sche
          +ma.FeatureVersion) -> str:                                    
      112 +    return f"{spec.rationale} Version {version.version}: {vers
          +ion.change_reason}"                                           
      113 +                                                              
      114 +                                                              
      115 +def register(plan: list[PlannedVersion], store_dir) -> None:  
      116 +    for item in plan:                                         
      117 +        spec, version = item.spec, item.schema_version        
      118 +        label = f"{spec.name} schema v{version.version}"      
      119 +        if item.action == "skip":                             
      120 +            print(f"{label}: unchanged — skipped (stored as v{
          +item.stored_version})")                                       
      121 +            continue                                          
      122 +        registered = store.register_version(                  
      123 +            spec.name,                                        
      124 +            item.values,                                      
      125 +            description=spec.description,                     
      126 +            source_columns=spec.source_columns,               
      127 +            transform_summary=spec.transform_summary(version.p
          +arameters),                                                   
      128 +            business_rationale=business_rationale(spec, versio
          +n),                                                           
      129 +            parameters=version.parameters,                    
      130 +            store_dir=store_dir,                              
      131 +        )                                                     
      132 +        note = "" if registered == version.version else " (sto
          +re numbering differs from schema.py)"                         
      133 +        print(f"{label}: registered as v{registered}{note}, pa
          +rameters {json.dumps(version.parameters)}")                   
      134 +                                                              
      135 +                                                              
      136 +def main(argv: list[str] | None = None) -> int:               
      137 +    parser = argparse.ArgumentParser(description=__doc__, form
          +atter_class=argparse.RawDescriptionHelpFormatter)             
      138 +    parser.add_argument("--as-of", default=SPEC_AS_OF,        
      139 +                        help=f"Feature cutoff date YYYY-MM-DD 
          +(default: {SPEC_AS_OF}, feature_spec.md).")                   
      140 +    parser.add_argument("--store-dir", default=None, help="Fea
          +ture store root (default: feature_store/).")                  
      141 +    parser.add_argument("--lineage-out", default=str(DEFAULT_L
          +INEAGE_PATH),                                                 
      142 +                        help="Where to regenerate FEATURE_LINE
          +AGE.md (default: outputs/artifacts/).")                       
      143 +    args = parser.parse_args(argv)                            
      144 +                                                              
      145 +    try:                                                      
      146 +        as_of = parse_as_of(args.as_of)                       
      147 +        existing = store.store_as_of(store_dir=args.store_dir)
      148 +        if existing is not None and existing != as_of:        
      149 +            raise store.StoreError(                           
      150 +                f"the store holds snapshots at {existing.date(
          +)}; refusing to build at {as_of.date()} "                     
      151 +                "(one as_of per store)"                       
      152 +            )                                                 
      153 +        builds = build_all_versions(as_of)                    
      154 +        plan = plan_registrations(builds, args.store_dir)     
      155 +        register(plan, args.store_dir)                        
      156 +    except (QualityCheckError, VersioningError, store.StoreErr
          +or, ValueError) as exc:                                       
      157 +        print(f"ERROR: {exc}\nNothing was registered by this r
          +un.", file=sys.stderr)                                        
      158 +        return 1                                              
      159 +                                                              
      160 +    out = write_lineage(Path(args.lineage_out), store_dir=args
          +.store_dir)                                                   
      161 +    print(f"lineage written to {out.relative_to(REPO_ROOT) if 
          +out.is_relative_to(REPO_ROOT) else out}")                     
      162 +    return 0                                                  
      163 +                                                              
      164 +                                                              
      165  if __name__ == "__main__":
       34 -    main()                                                    
      166 +    sys.exit(main())                                          

● Write(scripts/score_customer.py)
  ⎿  Added 35 lines, removed 14 lines
       2  """
       3  Builds features for one or more specific customer_ids as of a g
          iven date, using the
       4  scoring path of the same shared build_features(as_of, ...) entr
          y point used by
       5 -build_training_features.py.                                    
       5 +build_training_features.py. The only difference from training i
         +s `customer_ids`.                                              
       6  
       7 -TODO:                                                          
       8 -- Parse --customer-id (one or more) and --as-of arguments.     
       9 -- Call the SAME features/build.py entry point as build_training
         -_features.py — the                                             
      10 -  training/scoring parity guarantee depends on there being exac
         -tly one code path.                                             
      11 -- Print the resulting feature values for each requested custome
         -r_id.                                                          
      12 -- Handle a nonexistent customer_id explicitly (clear error, not
         - a silent empty result).                                       
       7 +Exits non-zero, printing no features, if a requested id doesn't
         + exist or signed up on or                                      
       8 +after as_of (ValueError), or if any quality check fails (Qualit
         +yCheckError).                                                  
       9  """
      10  
      11 +from __future__ import annotations                             
      12 +                                                               
      13  import argparse
      14 +import json                                                    
      15 +from pathlib import Path                                       
      16 +import sys                                                     
      17  
      18 +sys.path.insert(0, str(Path(__file__).resolve().parents[1]))   
      19  
      18 -def main() -> None:                                            
      20 +from features import schema                                    
      21 +from features.build import build_features                      
      22 +from features.quality_checks import QualityCheckError          
      23 +                                                               
      24 +SPEC_AS_OF = "2026-05-02"                                      
      25 +                                                               
      26 +                                                               
      27 +def main(argv: list[str] | None = None) -> int:                
      28      parser = argparse.ArgumentParser(description=__doc__)
      29      parser.add_argument(
      30          "--customer-id",
     ...
      35      )
      36      parser.add_argument(
      37          "--as-of",
      29 -        default="2026-08-01",                                  
      30 -        help="As-of date for feature computation (default: 2026
         --08-01).",                                                     
      38 +        default=SPEC_AS_OF,                                    
      39 +        help=f"Feature cutoff date YYYY-MM-DD (default: {SPEC_A
         +S_OF}, feature_spec.md).",                                     
      40      )
      32 -    args = parser.parse_args()                                 
      41 +    parser.add_argument("--format", choices=["table", "json"], 
         +default="table")                                               
      42 +    args = parser.parse_args(argv)                             
      43  
      34 -    # TODO: implement — see module docstring above.            
      35 -    raise NotImplementedError                                  
      44 +    try:                                                       
      45 +        features = build_features(args.as_of, customer_ids=args
         +.customer_id)                                                  
      46 +    except (ValueError, QualityCheckError) as exc:             
      47 +        print(f"ERROR: {exc}", file=sys.stderr)                
      48 +        return 1                                               
      49  
      50 +    if args.format == "json":                                  
      51 +        records = features[["customer_id", *schema.FEATURE_NAME
         +S]].to_dict(orient="records")                                  
      52 +        print(json.dumps({"as_of": features["as_of_date"].iloc[
         +0].date().isoformat(), "customers": records}, indent=2))       
      53 +    else:                                                      
      54 +        print(f"as_of {features['as_of_date'].iloc[0].date()}")
      55 +        print(features.set_index("customer_id")[schema.FEATURE_
         +NAMES].T.to_string())                                          
      56 +    return 0                                                   
      57  
      58 +                                                               
      59  if __name__ == "__main__":
      39 -    main()                                                     
      60 +    sys.exit(main())                                           

● Write(scripts/generate_lineage.py)
  ⎿  Wrote 176 lines to scripts/generate_lineage.py
       1 #!/usr/bin/env python3
       3 Generates FEATURE_LINEAGE.md from features/schema.py and the fea
         ture store registry: one
         recompute every value by
         must let someone
       6 reconstruct").
       8 The text stored in the registry (description, source_columns, tr
         ansform_summary,
         stered. The schema supplies
         he same helpers the
     … +224 lines (ctrl+o to expand)

● I'm adding a public store_root() so the lineage script doesn't reach into a
  private helper, and fixing the path display in the training script.

● Bash(conda run --no-capture-output -n churn-pipeline python - <<'EOF'
      p = "feature_store/store.py"…)
  ⎿  Updated feature_store/store.py (+5 -0)
      88      return DEFAULT_STORE_DIR if store_dir is None else Path(sto
          re_dir).resolve()
      89  
      90  
      91 +def store_root(*, store_dir=None) -> Path:                     
      92 +    """The resolved store root: `store_dir`, or feature_store/ 
         +by default."""                                                 
      93 +    return _store_dir(store_dir)                               
      94 +                                                               
      95 +                                                               
      96  def registry_path(*, store_dir=None) -> Path:
      97      return _store_dir(store_dir) / REGISTRY_FILENAME
      98  
  ⎿  Updated scripts/build_training_features.py (+3 -3)
      33  
      34  from feature_store import store
      35  from features import schema
      36 -from features.build import REPO_ROOT, build_features           
      36 +from features.build import build_features                      
      37  from features.quality_checks import (
      38      CheckResult,
      39      QualityCheckError,
     ...
      42      raise_on_failure,
      43  )
      44  from features.transforms import parse_as_of
      45 -from scripts.generate_lineage import DEFAULT_LINEAGE_PATH, writ
         -e_lineage                                                      
      45 +from scripts.generate_lineage import DEFAULT_LINEAGE_PATH, disp
         +lay_path, write_lineage                                        
      46  
      47  SPEC_AS_OF = "2026-05-02"
      48  
     ...
      158          return 1
      159  
      160      out = write_lineage(Path(args.lineage_out), store_dir=args
           .store_dir)
      161 -    print(f"lineage written to {out.relative_to(REPO_ROOT) if 
          -out.is_relative_to(REPO_ROOT) else out}")                     
      161 +    print(f"lineage written to {display_path(out)}")          
      162      return 0
      163  
      164  
  ⎿  Updated scripts/generate_lineage.py (+4 -4)
      34  DEFAULT_LINEAGE_PATH = REPO_ROOT / "outputs" / "artifacts" / "F
          EATURE_LINEAGE.md"
      35  
      36  
      37 -def _display(path: Path) -> str:                               
      37 +def display_path(path: Path) -> str:                           
      38      path = path.resolve()
      39      return path.relative_to(REPO_ROOT).as_posix() if path.is_re
          lative_to(REPO_ROOT) else str(path)
      40  
     ...
      101  
      102  
      103  def render_lineage(*, store_dir=None) -> str:
      104 -    root = store._store_dir(store_dir)                        
      104 +    root = store.store_root(store_dir=store_dir)              
      105      as_of = store.store_as_of(store_dir=root)
      106      out = [
      107          "# FEATURE_LINEAGE.md",
      108          "",
      109          "> Generated by `scripts/generate_lineage.py` from `fe
           atures/schema.py` and "
      110 -        f"`{_display(store.registry_path(store_dir=root))}`. D
          -o not edit by hand: rerun "                                   
      110 +        f"`{display_path(store.registry_path(store_dir=root))}
          +`. Do not edit by hand: rerun "                               
      111          "`scripts/build_training_features.py` (or `scripts/gen
           erate_lineage.py`) instead.",
      112          "",
      113          f"- **Store as_of**: {as_of.date() if as_of is not Non
           e else 'store is empty'} — every snapshot "
     ...
      168      parser.add_argument("--store-dir", default=None, help="Fea
           ture store root (default: feature_store/).")
      169      parser.add_argument("--out", default=str(DEFAULT_LINEAGE_P
           ATH))
      170      args = parser.parse_args(argv)
      171 -    print(f"lineage written to {_display(write_lineage(Path(ar
          -gs.out), store_dir=args.store_dir))}")                        
      171 +    print(f"lineage written to {display_path(write_lineage(Pat
          +h(args.out), store_dir=args.store_dir))}")                    
      172      return 0
      173  
      174  

● Now the version-comparison script. It loads every version from the store,
  never recomputes, and applies the spec's per-feature threshold metric.

● Write(scripts/compare_feature_versions.py)
  ⎿  Added 309 lines, removed 18 lines
        1  #!/usr/bin/env python3
        2  """
        3 -Version comparison for whichever feature(s) your confirmed fea
          -ture_spec.md designates as                                    
        4 -versioned. The mechanism is required (the feature store must s
          -upport multiple versions of a                                 
        5 -feature); which feature(s), how many versions, and what parame
          -ter values are entirely your                                  
        6 -spec's own design choice — there is no single fixed feature or
          - fixed parameter value here.                                  
        3 +Version comparison for every feature features/schema.py versio
          +ns (feature_spec.md:                                          
        4 +avg_spend_per_active_month_6m, spend_trend_3m_vs_prior_3m, act
          +ive_month_share).                                             
        5  
        8 -TODO:                                                         
        9 -- For each feature feature_spec.md designates as versioned, lo
          -ad all of its registered                                      
       10 -  versions via feature_store/store.py's get_version(feature_na
          -me, version) for each version                                 
       11 -  number that feature has — do not recompute from scratch.    
       12 -- For each such feature, report how many customers' values cha
          -nged and the distribution                                     
       13 -  shift between versions (e.g. summary stats or a quantile com
          -parison).                                                     
       14 -- If a churn-risk ranking exists, report how many customers mo
          -ve across a reasonable                                        
       15 -  threshold; otherwise compare the feature's own rank-order ch
          -anges.                                                        
       16 -- Write the results and an interpretation to VERSION_COMPARISO
          -N.md, one section per                                         
       17 -  versioned feature (per skill.md).                           
        6 +For each versioned feature, every registered version is loaded
          + through the feature store                                    
        7 +(store.get_version for each version list_versions reports; not
          +hing is recomputed), and each                                 
        8 +consecutive pair (plus first vs latest when there are more tha
          +n two) is compared:                                           
        9 +- customers whose value changed (and in which direction);     
       10 +- summary statistics and quantiles per version;               
       11 +- Spearman rank correlation between versions, as a.rank().corr
          +(b.rank()) (no SciPy);                                        
       12 +- the threshold metric feature_spec.md assigns the feature:   
       13 +  - top_decile: customers entering/leaving the top decile, whe
          +re the cap acts;                                              
       14 +  - cap_ties: customers tied at each version's cap, and whethe
          +r all values below 1 are                                      
       15 +    unchanged;                                                
       16 +  - bottom_decile: customers entering/leaving the bottom decil
          +e within a subgroup                                           
       17 +    (has_ever_purchased = 1).                                 
       18 +  A decile holds every customer at or beyond that version's ow
          +n 90th (10th) percentile,                                     
       19 +  linear interpolation, ties included.                        
       20 +                                                              
       21 +No churn model or ranking exists yet, so the comparison is on 
          +the feature's own                                             
       22 +distribution and rank order. The results and their interpretat
          +ion go to                                                     
       23 +VERSION_COMPARISON.md.                                        
       24  """
       25  
       26 +from __future__ import annotations                            
       27  
       21 -def main() -> None:                                           
       22 -    # TODO: implement — see module docstring above.           
       23 -    raise NotImplementedError                                 
       28 +import argparse                                               
       29 +import json                                                   
       30 +from pathlib import Path                                      
       31 +import sys                                                    
       32  
       33 +sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  
       34  
       35 +import pandas as pd                                           
       36 +                                                              
       37 +from feature_store import store                               
       38 +from features import schema                                   
       39 +from features.build import REPO_ROOT                          
       40 +from scripts.generate_lineage import display_path             
       41 +                                                              
       42 +DEFAULT_OUT = REPO_ROOT / "outputs" / "artifacts" / "VERSION_C
          +OMPARISON.md"                                                 
       43 +QUANTILES = [0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99]        
       44 +                                                              
       45 +                                                              
       46 +def load_versions(name: str, store_dir) -> list[dict]:        
       47 +    """Every registered version of `name`, oldest first, read 
          +through the store."""                                         
       48 +    rows = store.list_versions(name, store_dir=store_dir).sort
          +_values("version")                                            
       49 +    out = []                                                  
       50 +    for row in rows.itertuples():                             
       51 +        snap = store.get_version(name, int(row.version), store
          +_dir=store_dir)                                               
       52 +        out.append({                                          
       53 +            "version": int(row.version),                      
       54 +            "parameters": json.loads(row.parameters),         
       55 +            "parquet_path": row.parquet_path,                 
       56 +            "is_latest": int(row.is_latest),                  
       57 +            "values": snap.set_index("customer_id")[name].sort
          +_index(),                                                     
       58 +            "as_of": pd.Timestamp(snap["as_of_date"].iloc[0]),
       59 +        })                                                    
       60 +    return out                                                
       61 +                                                              
       62 +                                                              
       63 +def rank_correlation(a: pd.Series, b: pd.Series) -> float:    
       64 +    return float(a.rank().corr(b.rank()))                     
       65 +                                                              
       66 +                                                              
       67 +def summary(values: pd.Series) -> dict:                       
       68 +    stats = {"count": int(values.size), "mean": values.mean(),
          + "std": values.std(), "min": values.min()}                    
       69 +    stats.update({f"p{int(q * 100)}": values.quantile(q) for q
          + in QUANTILES})                                               
       70 +    stats["max"] = values.max()                               
       71 +    return stats                                              
       72 +                                                              
       73 +                                                              
       74 +def decile(values: pd.Series, side: str) -> dict:             
       75 +    q = values.quantile(0.90 if side == "top" else 0.10)      
       76 +    members = values.index[values >= q] if side == "top" else 
          +values.index[values <= q]                                     
       77 +    boundary = values.loc[members].min() if side == "top" else
          + values.loc[members].max()                                    
       78 +    return {                                                  
       79 +        "threshold": float(q),                                
       80 +        "members": set(members),                              
       81 +        "boundary_value": float(boundary),                    
       82 +        "tied_at_boundary": int((values == boundary).sum()),  
       83 +    }                                                         
       84 +                                                              
       85 +                                                              
       86 +def threshold_metric(spec: schema.FeatureSpec, old: dict, new:
          + dict, store_dir) -> dict:                                    
       87 +    kind = spec.comparison["kind"]                            
       88 +    a, b = old["values"], new["values"]                       
       89 +    if kind in ("top_decile", "bottom_decile"):               
       90 +        side = "top" if kind == "top_decile" else "bottom"    
       91 +        subgroup = spec.comparison.get("subgroup")            
       92 +        if subgroup:                                          
       93 +            flag = store.get_latest(subgroup["feature"], store
          +_dir=store_dir).set_index("customer_id")[subgroup["feature"]] 
       94 +            keep = flag.index[flag == subgroup["value"]]      
       95 +            a, b = a.loc[a.index.intersection(keep)], b.loc[b.
          +index.intersection(keep)]                                     
       96 +        da, db = decile(a, side), decile(b, side)             
       97 +        return {                                              
       98 +            "kind": kind, "side": side, "subgroup": subgroup, 
          +"subgroup_size": int(a.size),                                 
       99 +            "old": da, "new": db,                             
      100 +            "entering": len(db["members"] - da["members"]), "l
          +eaving": len(da["members"] - db["members"]),                  
      101 +        }                                                     
      102 +    if kind == "cap_ties":                                    
      103 +        param, below = spec.comparison["parameter"], spec.comp
          +arison["unchanged_below"]                                     
      104 +        caps = {"old": old["parameters"][param], "new": new["p
          +arameters"][param]}                                           
      105 +        low = a.index[a < below]                              
      106 +        return {                                              
      107 +            "kind": kind, "parameter": param, "caps": caps,   
      108 +            "tied_old": int((a == caps["old"]).sum()), "tied_n
          +ew": int((b == caps["new"]).sum()),                           
      109 +            "below": below, "n_below_old": int(low.size), "n_b
          +elow_new": int((b < below).sum()),                            
      110 +            "below_unchanged": bool((a.loc[low] == b.loc[low])
          +.all() and set(low) == set(b.index[b < below])),              
      111 +        }                                                     
      112 +    raise ValueError(f"unknown comparison kind {kind!r} for {s
          +pec.name}")                                                   
      113 +                                                              
      114 +                                                              
      115 +def compare_pair(spec: schema.FeatureSpec, old: dict, new: dic
          +t, store_dir) -> dict:                                        
      116 +    a, b = old["values"], new["values"]                       
      117 +    if not a.index.equals(b.index):                           
      118 +        raise ValueError(f"{spec.name} v{old['version']} and v
          +{new['version']} cover different customers")                  
      119 +    if old["as_of"] != new["as_of"]:                          
      120 +        raise ValueError(f"{spec.name} v{old['version']} and v
          +{new['version']} have different as_of dates")                 
      121 +    diff = b - a                                              
      122 +    changed = diff != 0                                       
      123 +    return {                                                  
      124 +        "old": old, "new": new, "n": int(a.size),             
      125 +        "changed": int(changed.sum()), "increased": int((diff 
          +> 0).sum()), "decreased": int((diff < 0).sum()),              
      126 +        "mean_abs_change_changed": float(diff[changed].abs().m
          +ean()) if changed.any() else 0.0,                             
      127 +        "max_abs_change": float(diff.abs().max()),            
      128 +        "rank_correlation": rank_correlation(a, b),           
      129 +        "threshold": threshold_metric(spec, old, new, store_di
          +r),                                                           
      130 +    }                                                         
      131 +                                                              
      132 +                                                              
      133 +def compare_feature(spec: schema.FeatureSpec, store_dir) -> di
          +ct:                                                           
      134 +    versions = load_versions(spec.name, store_dir)            
      135 +    if len(versions) < 2:                                     
      136 +        raise ValueError(f"{spec.name} is versioned in schema.
          +py but the store holds {len(versions)} version(s)")           
      137 +    pairs = [(versions[i], versions[i + 1]) for i in range(len
          +(versions) - 1)]                                              
      138 +    if len(versions) > 2:                                     
      139 +        pairs.append((versions[0], versions[-1]))             
      140 +    return {"spec": spec, "versions": versions, "pairs": [comp
          +are_pair(spec, o, n, store_dir) for o, n in pairs]}           
      141 +                                                              
      142 +                                                              
      143 +# Rendering --------------------------------------------------
          +----------------------------                                  
      144 +def _fmt(x) -> str:                                           
      145 +    if isinstance(x, (int,)) or (isinstance(x, float) and x.is
          +_integer() and abs(x) >= 1000):                               
      146 +        return f"{int(x):,}"                                  
      147 +    return f"{x:.4g}" if isinstance(x, float) else str(x)     
      148 +                                                              
      149 +                                                              
      150 +def _param_label(spec: schema.FeatureSpec, params: dict) -> st
          +r:                                                            
      151 +    tunable = {k: v for k, v in params.items() if k != "defini
          +tion"}                                                        
      152 +    return ", ".join(f"{k} = {'none' if v is None else v}" for
          + k, v in tunable.items())                                     
      153 +                                                              
      154 +                                                              
      155 +def _verdict(rho: float) -> str:                              
      156 +    if rho >= 0.99:                                           
      157 +        return ("the ordering is essentially unchanged, so a r
          +ank-based model (gradient-boosted trees, "                    
      158 +                "or any model fed this feature as a rank or qu
          +antile) would barely notice")                                 
      159 +    if rho >= 0.95:                                           
      160 +        return "the ordering shifts a little; a rank-based mod
          +el would notice only at the margins"                          
      161 +    return ("the ordering changes materially, so any model con
          +suming this feature would notice and "                        
      162 +            "would need retraining on the new version")       
      163 +                                                              
      164 +                                                              
      165 +def interpretation(spec: schema.FeatureSpec, p: dict) -> list[
          +str]:                                                         
      166 +    old, new, t = p["old"], p["new"], p["threshold"]          
      167 +    sa, sb = summary(old["values"]), summary(new["values"])   
      168 +    rho = p["rank_correlation"]                               
      169 +    share = p["changed"] / p["n"]                             
      170 +    lines = [                                                 
      171 +        f"{p['changed']:,} of {p['n']:,} customers ({share:.1%
          +}) change: {p['increased']:,} up, "                           
      172 +        f"{p['decreased']:,} down. Rank correlation is {rho:.3
          +f}: {_verdict(rho)}.",                                        
      173 +    ]                                                         
      174 +    if t["kind"] == "top_decile":                             
      175 +        lines.append(                                         
      176 +            f"The change is confined to the top tail. The maxi
          +mum falls from {sa['max']:.2f} to "                           
      177 +            f"{sb['max']:.2f} and the standard deviation from 
          +{sa['std']:.2f} to {sb['std']:.2f}, while "                   
      178 +            f"the median stays at {sb['p50']:.2f}. Only {t['en
          +tering']} customers enter and "                               
      179 +            f"{t['leaving']} leave the top decile, so a 'top s
          +penders' segment barely moves. A linear "                     
      180 +            "or distance-based model would notice more than a 
          +tree: the largest values lose most of "                       
      181 +            "their leverage on the fitted slope."             
      182 +        )                                                     
      183 +    elif t["kind"] == "cap_ties":                             
      184 +        lines.append(                                         
      185 +            f"The cap only acts on growth: every one of the {t
          +['n_below_old']:,} values below "                             
      186 +            f"{t['below']:g} (spend falling or stopped) is {'u
          +nchanged' if t['below_unchanged'] else 'NOT unchanged'}, "    
      187 +            f"and {t['tied_new']:,} customers now share the ca
          +p value {t['caps']['new']:g} (against "                       
      188 +            f"{t['tied_old']:,} at {t['caps']['old']:g} before
          +). The mean falls from {sa['mean']:.2f} to "                  
      189 +            f"{sb['mean']:.2f} because the long right tail is 
          +cut, not because anyone's decline signal "                    
      190 +            "moved. For retention targeting, which acts on val
          +ues below 1, the two versions rank the "                      
      191 +            "at-risk customers identically; a linear model wou
          +ld notice the smaller scale, a tree "                         
      192 +            f"would only lose the ability to split among the {
          +t['tied_new']:,} tied growth customers."                      
      193 +        )                                                     
      194 +    elif t["kind"] == "bottom_decile":                        
      195 +        group = (f" among the {t['subgroup_size']:,} customers
          + with "                                                       
      196 +                 f"{t['subgroup']['feature']} = {t['subgroup']
          +['value']}") if t["subgroup"] else ""                         
      197 +        lines.append(                                         
      198 +            f"This is a real redefinition, not a tail adjustme
          +nt. The mean moves from {sa['mean']:.3f} "                    
      199 +            f"to {sb['mean']:.3f} and the median from {sa['p50
          +']:.3f} to {sb['p50']:.3f}. In the bottom "                   
      200 +            f"decile{group} (the least consistent buyers, the 
          +retention-relevant end), "                                    
      201 +            f"{t['entering']} customers enter and {t['leaving'
          +]} leave. A shorter window drops habits that "                
      202 +            "ended more than 6 months ago and gives each recen
          +t month more weight, so customers who "                       
      203 +            "were consistent last year but lapsed recently fal
          +l into the bottom decile, and those with "                    
      204 +            "a recent run of activity climb out."             
      205 +        )                                                     
      206 +    return lines                                              
      207 +                                                              
      208 +                                                              
      209 +def render(results: list[dict], store_dir) -> str:            
      210 +    root = store.store_root(store_dir=store_dir)              
      211 +    as_of = results[0]["versions"][0]["as_of"].date() if resul
          +ts else "n/a"                                                 
      212 +    out = [                                                   
      213 +        "# VERSION_COMPARISON.md",                            
      214 +        "",                                                   
      215 +        "> Generated by `scripts/compare_feature_versions.py` 
          +from the versions stored in "                                 
      216 +        f"`{display_path(store.registry_path(store_dir=root))}
          +` (loaded with `get_version`, not "                           
      217 +        "recomputed). Rank correlation is Spearman's ρ compute
          +d as `a.rank().corr(b.rank())`. "                             
      218 +        "Deciles hold every customer at or beyond that version
          +'s own 90th (10th) percentile, "                              
      219 +        "linear interpolation, ties included.",               
      220 +        "",                                                   
      221 +        f"Snapshot as_of: **{as_of}**. No churn model or ranki
          +ng exists yet, so each section compares "                     
      222 +        "the feature's own distribution, rank order and the th
          +reshold metric feature_spec.md assigns it.",                  
      223 +        "",                                                   
      224 +        "| Feature | Versions | Customers changed | Rank corre
          +lation | Threshold metric |",                                 
      225 +        "|---|---|---|---|---|",                              
      226 +    ]                                                         
      227 +    for r in results:                                         
      228 +        for p in r["pairs"]:                                  
      229 +            t = p["threshold"]                                
      230 +            if t["kind"] == "cap_ties":                       
      231 +                metric = (f"{t['tied_new']} tied at {t['caps']
          +['new']:g}; all {t['n_below_old']} values "                   
      232 +                          f"below {t['below']:g} {'unchanged' 
          +if t['below_unchanged'] else 'CHANGED'}")                     
      233 +            else:                                             
      234 +                metric = f"{t['side']} decile: {t['entering']}
          + in, {t['leaving']} out"                                      
      235 +            out.append(f"| `{r['spec'].name}` | v{p['old']['ve
          +rsion']} → v{p['new']['version']} | "                         
      236 +                       f"{p['changed']:,} of {p['n']:,} | {p['
          +rank_correlation']:.3f} | {metric} |")                        
      237 +    out.append("")                                            
      238 +                                                              
      239 +    for r in results:                                         
      240 +        spec = r["spec"]                                      
      241 +        out += [f"## {spec.spec_id} — `{spec.name}`", ""]     
      242 +        for v in r["versions"]:                               
      243 +            schema_v = next((sv for sv in spec.versions if sv.
          +parameters == v["parameters"]), None)                         
      244 +            reason = schema_v.change_reason if schema_v else "
          +parameters not in features/schema.py"                         
      245 +            out.append(f"- **v{v['version']}** ({_param_label(
          +spec, v['parameters'])}"                                      
      246 +                       f"{', latest' if v['is_latest'] else ''
          +}): `{v['parquet_path']}`. {reason}")                         
      247 +        out.append("")                                        
      248 +        for p in r["pairs"]:                                  
      249 +            old, new, t = p["old"], p["new"], p["threshold"]  
      250 +            out += [f"### v{old['version']} → v{new['version']
          +}", ""]                                                       
      251 +            stats = pd.DataFrame({f"v{old['version']}": summar
          +y(old["values"]), f"v{new['version']}": summary(new["values"])
          +})                                                            
      252 +            out += ["| Statistic | " + " | ".join(stats.column
          +s) + " |", "|---|" + "---|" * len(stats.columns)]             
      253 +            for stat, row in stats.iterrows():                
      254 +                out.append(f"| {stat} | " + " | ".join(_fmt(x)
          + for x in row) + " |")                                        
      255 +            out += [                                          
      256 +                "",                                           
      257 +                f"- Customers changed: **{p['changed']:,}** of
          + {p['n']:,} ({p['increased']:,} up, "                         
      258 +                f"{p['decreased']:,} down); mean absolute chan
          +ge among them {p['mean_abs_change_changed']:.4g}, "           
      259 +                f"largest {p['max_abs_change']:.4g}.",        
      260 +                f"- Rank correlation between versions: **{p['r
          +ank_correlation']:.3f}**.",                                   
      261 +            ]                                                 
      262 +            if t["kind"] == "cap_ties":                       
      263 +                out += [                                      
      264 +                    f"- Tied at the cap: {t['tied_old']:,} at 
          +{t['caps']['old']:g} in v{old['version']}, "                  
      265 +                    f"**{t['tied_new']:,} at {t['caps']['new']
          +:g}** in v{new['version']}.",                                 
      266 +                    f"- Values below {t['below']:g}: {t['n_bel
          +ow_old']:,} in v{old['version']}, "                           
      267 +                    f"{t['n_below_new']:,} in v{new['version']
          +}; all unchanged: **{t['below_unchanged']}**.",               
      268 +                ]                                             
      269 +            else:                                             
      270 +                group = (f" within {t['subgroup']['feature']} 
          += {t['subgroup']['value']} "                                  
      271 +                         f"({t['subgroup_size']:,} customers)"
          +) if t["subgroup"] else ""                                    
      272 +                for label, d in (("old", t["old"]), ("new", t[
          +"new"])):                                                     
      273 +                    vnum = (old if label == "old" else new)["v
          +ersion"]                                                      
      274 +                    pct = "90th" if t["side"] == "top" else "1
          +0th"                                                          
      275 +                    out.append(                               
      276 +                        f"- v{vnum} {t['side']} decile{group}:
          + {pct} percentile {d['threshold']:.4g}; "                     
      277 +                        f"**{len(d['members']):,} customers**;
          + {d['tied_at_boundary']:,} tied at the boundary "             
      278 +                        f"value {d['boundary_value']:.4g}."   
      279 +                    )                                         
      280 +                out.append(f"- Moving across the {t['side']}-d
          +ecile threshold: **{t['entering']} enter, "                   
      281 +                           f"{t['leaving']} leave**.")        
      282 +            out += ["", "**Interpretation.** " + " ".join(inte
          +rpretation(spec, p)), ""]                                     
      283 +    return "\n".join(out).rstrip() + "\n"                     
      284 +                                                              
      285 +                                                              
      286 +def main(argv: list[str] | None = None) -> int:               
      287 +    parser = argparse.ArgumentParser(description=__doc__, form
          +atter_class=argparse.RawDescriptionHelpFormatter)             
      288 +    parser.add_argument("--store-dir", default=None, help="Fea
          +ture store root (default: feature_store/).")                  
      289 +    parser.add_argument("--out", default=str(DEFAULT_OUT), hel
          +p="Markdown report path.")                                    
      290 +    args = parser.parse_args(argv)                            
      291 +                                                              
      292 +    versioned = [f for f in schema.FEATURE_SCHEMA if f.is_vers
          +ioned]                                                        
      293 +    try:                                                      
      294 +        results = [compare_feature(spec, args.store_dir) for s
          +pec in versioned]                                             
      295 +    except (KeyError, ValueError) as exc:                     
      296 +        print(f"ERROR: {exc}", file=sys.stderr)               
      297 +        return 1                                              
      298 +    for r in results:                                         
      299 +        for p in r["pairs"]:                                  
      300 +            print(f"{r['spec'].name} v{p['old']['version']} ->
          + v{p['new']['version']}: {p['changed']} changed, "            
      301 +                  f"rank correlation {p['rank_correlation']:.3
          +f}, threshold {json.dumps(_brief(p['threshold']))}")          
      302 +    out = Path(args.out)                                      
      303 +    out.parent.mkdir(parents=True, exist_ok=True)             
      304 +    out.write_text(render(results, args.store_dir), encoding="
          +utf-8")                                                       
      305 +    print(f"report written to {display_path(out)}")           
      306 +    return 0                                                  
      307 +                                                              
      308 +                                                              
      309 +def _brief(t: dict) -> dict:                                  
      310 +    if t["kind"] == "cap_ties":                               
      311 +        return {k: t[k] for k in ("tied_old", "tied_new", "n_b
          +elow_old", "below_unchanged")}                                
      312 +    return {"entering": t["entering"], "leaving": t["leaving"]
          +,                                                             
      313 +            "old_members": len(t["old"]["members"]), "new_memb
          +ers": len(t["new"]["members"]),                               
      314 +            "old_ties": t["old"]["tied_at_boundary"], "new_tie
          +s": t["new"]["tied_at_boundary"]}                             
      315 +                                                              
      316 +                                                              
      317  if __name__ == "__main__":
       27 -    main()                                                    
      318 +    sys.exit(main())                                          

● Before touching the real store, I'm doing a dry run of the full pipeline into
  a scratch store, then checking the numbers against the spec's expected values.

  Ran 1 shell command (ctrl+o to expand)

● All version-comparison numbers for F5, F6, and F7 match the spec exactly. Next
  I'll independently recompute the spec's other quoted figures using raw
  pandas, bypassing the features package, to rule out any shared bug between the
  pipeline and my checks.

  Ran 1 shell command (ctrl+o to expand)

● All the figures I recomputed match the spec exactly, including F6's case
  split, F5's 82/75 split, F7's 161 customers with tenure correlations, and the
  churn rates. Here's how the generated lineage and comparison reports read.

  Ran 1 shell command (ctrl+o to expand)

● The lineage document covers every reconstruction item the spec lists. Now the
  comparison report:

  Ran 1 shell command (ctrl+o to expand)

● I found three hard-coded phrases in the interpretation that should instead
  reflect the actual data: "median stays at", "ranks at-risk customers
  identically", and the explanation for F7's bottom decile churn. I'll make each
  conditional on computed values and add a measured count to verify the F7
  claim rather than assert it.

● Bash(conda run --no-capture-output -n churn-pipeline python - <<'EOF'
      p = "scripts/compare_feature_versions.py"…)
  ⎿  Updated scripts/compare_feature_versions.py (+41 -23)
       94              keep = flag.index[flag == subgroup["value"]]
       95              a, b = a.loc[a.index.intersection(keep)], b.loc[b.
           index.intersection(keep)]
       96          da, db = decile(a, side), decile(b, side)
       97 +        entering = sorted(db["members"] - da["members"])      
       98 +        leaving = sorted(da["members"] - db["members"])       
       99          return {
      100              "kind": kind, "side": side, "subgroup": subgroup, 
           "subgroup_size": int(a.size),
       99 -            "old": da, "new": db,                             
      100 -            "entering": len(db["members"] - da["members"]), "l
          -eaving": len(da["members"] - db["members"]),                  
      101 +            "old": da, "new": db, "entering": len(entering), "
          +leaving": len(leaving),                                       
      102 +            # direction of each mover's own value change, to b
          +ack the interpretation with data                              
      103 +            "entering_fell": int((b.loc[entering] < a.loc[ente
          +ring]).sum()),                                                
      104 +            "leaving_rose": int((b.loc[leaving] > a.loc[leavin
          +g]).sum()),                                                   
      105          }
      106      if kind == "cap_ties":
      107          param, below = spec.comparison["parameter"], spec.comp
           arison["unchanged_below"]
     ...
      166              "would need retraining on the new version")
      167  
      168  
      169 +def _median_phrase(sa: dict, sb: dict, digits: int) -> str:   
      170 +    if round(sa["p50"], digits) == round(sb["p50"], digits):  
      171 +        return f"the median stays at {sb['p50']:.{digits}f}"  
      172 +    return f"the median moves from {sa['p50']:.{digits}f} to {
          +sb['p50']:.{digits}f}"                                        
      173 +                                                              
      174 +                                                              
      175  def interpretation(spec: schema.FeatureSpec, p: dict) -> list[
           str]:
      176      old, new, t = p["old"], p["new"], p["threshold"]
      177      sa, sb = summary(old["values"]), summary(new["values"])
     ...
      183      ]
      184      if t["kind"] == "top_decile":
      185          lines.append(
      176 -            f"The change is confined to the top tail. The maxi
          -mum falls from {sa['max']:.2f} to "                           
      177 -            f"{sb['max']:.2f} and the standard deviation from 
          -{sa['std']:.2f} to {sb['std']:.2f}, while "                   
      178 -            f"the median stays at {sb['p50']:.2f}. Only {t['en
          -tering']} customers enter and "                               
      179 -            f"{t['leaving']} leave the top decile, so a 'top s
          -penders' segment barely moves. A linear "                     
      180 -            "or distance-based model would notice more than a 
          -tree: the largest values lose most of "                       
      181 -            "their leverage on the fitted slope."             
      186 +            f"The maximum falls from {sa['max']:.2f} to {sb['m
          +ax']:.2f} and the standard deviation from "                   
      187 +            f"{sa['std']:.2f} to {sb['std']:.2f}, while {_medi
          +an_phrase(sa, sb, 2)}. "                                      
      188 +            f"{t['entering']} customers enter and {t['leaving'
          +]} leave the top decile "                                     
     … 52 more lines (ctrl+o to expand)

● Confirmed with data: all 54 entering customers fell and all 46 leaving
  customers rose, backing the F7 claim. Now checking idempotence on rerun, the
  scoring script and its error path, and the wrong-as_of guard.

  Ran 1 shell command (ctrl+o to expand)

● Confirmed idempotency works correctly — reruns, invalid as_of, unknown ids,
  and bad label dates all fail safely with no registry changes. Now fixing the
  table view, which incorrectly prints integers as floats (e.g. 567.000000).

● Update(scripts/score_customer.py)
  ⎿  Added 2 lines, removed 1 line
      52          print(json.dumps({"as_of": features["as_of_date"].iloc[
          0].date().isoformat(), "customers": records}, indent=2))
      53      else:
      54          print(f"as_of {features['as_of_date'].iloc[0].date()}")
      55 -        print(features.set_index("customer_id")[schema.FEATURE_
         -NAMES].T.to_string())                                          
      55 +        # object dtype keeps the int features as ints through t
         +he transpose                                                   
      56 +        print(features.set_index("customer_id")[schema.FEATURE_
         +NAMES].astype(object).T.to_string())                           
      57      return 0
      58  
      59  

● Now the test suite. First I need concrete customer ids for the parity cases
  the spec names: a zero-activity customer, a 2026-05-01 purchaser, and checks
  on customers 3 and 5.

  Ran 1 shell command (ctrl+o to expand)

● I'll set up test fixtures using customers 27, 350, and 899 to cover edge cases
  like never-buyers, boundary purchase dates, and same-day signups, plus a
  session guard to prevent tests from touching the real store.

● Write(tests/conftest.py)
  ⎿  Wrote 62 lines to tests/conftest.py
      1 """Shared fixtures. Every test that writes to a feature store use
        s a tmp_path store; the
      2 session guard below fails the run if anything touched the real fe
        ature_store/."""
      3
      5 import sqlite3
      6 from pathlib import Path
      7
      8 import pandas as pd
      9 import pytest
     … +62 lines (ctrl+o to expand)

● Write(tests/test_transforms.py)
  ⎿  Added 281 lines, removed 23 lines
        1  """
        2 -Test skeletons for each feature function in features/transform
          -s.py, in isolation.                                           
        3 -                                                              
        4 -Per skill.md's Workflow: each feature needs, at minimum, a nor
          -mal case, a zero-activity                                     
        5 -case, a single-record case, and (where relevant) a case right 
          -at a time-window boundary.                                    
        6 -                                                              
        7 -Fill these in against your confirmed feature_spec.md and your 
          -actual transform functions —                                  
        8 -do not leave them skipped in your submission.                 
        2 +Each feature function in features/transforms.py in isolation, 
          +on small synthetic frames:                                    
        3 +a normal case, the zero-activity case, a single-record case, a
          +nd the window boundary                                        
        4 +cases feature_spec.md defines ([as_of - N days, as_of) on full
          + timestamps; N complete                                       
        5 +calendar months before as_of's month).                        
        6  """
        7  
        8 +from datetime import date, datetime, timezone                 
        9 +import inspect                                                
       10 +                                                              
       11 +import numpy as np                                            
       12 +import pandas as pd                                           
       13  import pytest
       14  
       15 +from features import schema, transforms as T                  
       16  
       14 -def test_normal_case():                                       
       15 -    """TODO: build a feature for a customer with a normal tran
          -saction history and assert                                    
       16 -    a sane, non-null value."""                                
       17 -    pytest.skip("TODO: implement against your confirmed featur
          -e_spec.md")                                                   
       17 +AS_OF = T.parse_as_of("2026-05-02")                           
       18  
       19  
       20 -def test_zero_activity_case():                                
       21 -    """TODO: build a feature for a customer with zero transact
          -ions and assert the                                           
       22 -    explicit default from feature_spec.md is returned, not NaN
          - or a crash."""                                               
       23 -    pytest.skip("TODO: implement against your confirmed featur
          -e_spec.md")                                                   
       20 +def pop(*rows) -> pd.DataFrame:                               
       21 +    """rows: (customer_id, signup_date)."""                   
       22 +    df = pd.DataFrame(rows, columns=["customer_id", "signup_da
          +te"])                                                         
       23 +    df["signup_date"] = pd.to_datetime(df["signup_date"]).asty
          +pe("datetime64[ns]")                                          
       24 +    return df.set_index("customer_id")                        
       25  
       26  
       26 -def test_single_record_case():                                
       27 -    """TODO: build a feature for a customer with exactly one t
          -ransaction."""                                                
       28 -    pytest.skip("TODO: implement against your confirmed featur
          -e_spec.md")                                                   
       27 +def txns(*rows) -> pd.DataFrame:                              
       28 +    """rows: (customer_id, txn_timestamp)."""                 
       29 +    df = pd.DataFrame(rows, columns=["customer_id", "txn_times
          +tamp"])                                                       
       30 +    df["txn_timestamp"] = pd.to_datetime(df["txn_timestamp"]).
          +astype("datetime64[ns]")                                      
       31 +    df["amount"] = 10.0                                       
       32 +    df["is_returned"] = 0                                     
       33 +    return df                                                 
       34  
       35  
       31 -def test_time_window_boundary_case():                         
       32 -    """TODO: build a feature for a customer with a transaction
          - exactly at a time-window                                     
       33 -    boundary (e.g. exactly 90 days before the as-of date) and 
          -assert it's handled per                                       
       34 -    the spec's stated inclusive/exclusive convention."""      
       35 -    pytest.skip("TODO: implement against your confirmed featur
          -e_spec.md")                                                   
       36 +def months(*rows) -> pd.DataFrame:                            
       37 +    """rows: (customer_id, 'YYYY-MM', total_amount)."""       
       38 +    df = pd.DataFrame(rows, columns=["customer_id", "txn_month
          +", "total_amount"])                                           
       39 +    df["txn_month"] = pd.to_datetime(df["txn_month"]).astype("
          +datetime64[ns]")                                              
       40 +    df["txn_count"] = 1                                       
       41 +    df["return_count"] = 0                                    
       42 +    return df                                                 
       43 +                                                              
       44 +                                                              
       45 +POP = pop((1, "2024-01-10"), (2, "2024-01-10"), (3, "2024-01-1
          +0"))                                                          
       46 +NO_TXNS = txns()                                              
       47 +NO_MONTHS = months()                                          
       48 +                                                              
       49 +                                                              
       50 +def v(series: pd.Series, cid: int):                           
       51 +    return series.loc[cid]                                    
       52 +                                                              
       53 +                                                              
       54 +# as_of parsing and windows ----------------------------------
          +-----------------------------                                 
       55 +@pytest.mark.parametrize("value", ["2026-05-02", date(2026, 5,
          + 2), datetime(2026, 5, 2), pd.Timestamp("2026-05-02")])       
       56 +def test_parse_as_of_accepts_calendar_dates(value):           
       57 +    assert T.parse_as_of(value) == pd.Timestamp("2026-05-02") 
       58 +                                                              
       59 +                                                              
       60 +@pytest.mark.parametrize(                                     
       61 +    "value",                                                  
       62 +    ["2026-05-02 10:00", "2026-05-02T00:00:00", "2026-5-2", "2
          +026-02-30",                                                   
       63 +     pd.Timestamp("2026-05-02 00:00:01"), datetime(2026, 5, 2,
          + tzinfo=timezone.utc)],                                       
       64 +)                                                             
       65 +def test_parse_as_of_rejects_time_components_and_bad_dates(val
          +ue):                                                          
       66 +    with pytest.raises(ValueError):                           
       67 +        T.parse_as_of(value)                                  
       68 +                                                              
       69 +                                                              
       70 +def test_parse_as_of_rejects_non_dates():                     
       71 +    with pytest.raises(TypeError):                            
       72 +        T.parse_as_of(20260502)                               
       73 +                                                              
       74 +                                                              
       75 +def test_windows_match_the_spec_dates():                      
       76 +    assert T.day_window(AS_OF, 90) == (pd.Timestamp("2026-02-0
          +1"), pd.Timestamp("2026-05-02"))                              
       77 +    assert T.month_window(AS_OF, 6) == (pd.Timestamp("2025-11-
          +01"), pd.Timestamp("2026-05-01"))                             
       78 +    assert T.month_window(AS_OF, 3, offset=3) == (pd.Timestamp
          +("2025-11-01"), pd.Timestamp("2026-02-01"))                   
       79 +    assert T.month_window(AS_OF, 12) == (pd.Timestamp("2025-05
          +-01"), pd.Timestamp("2026-05-01"))                            
       80 +    # as_of on the 1st: its own month is still excluded       
       81 +    assert T.month_window(T.parse_as_of("2026-05-01"), 1) == (
          +pd.Timestamp("2026-04-01"), pd.Timestamp("2026-05-01"))       
       82 +                                                              
       83 +                                                              
       84 +def test_population_is_signup_strictly_before_as_of():        
       85 +    customers = pd.DataFrame({                                
       86 +        "customer_id": [1, 2, 3],                             
       87 +        "signup_date": pd.to_datetime(["2026-05-01", "2026-05-
          +02", "2026-06-01"]).astype("datetime64[ns]"),                 
       88 +    })                                                        
       89 +    assert T.population(customers, AS_OF).index.tolist() == [1
          +]                                                             
       90 +                                                              
       91 +                                                              
       92 +def test_no_transform_defaults_as_of_or_its_parameters():     
       93 +    for name in schema.FEATURE_NAMES:                         
       94 +        params = inspect.signature(getattr(T, name)).parameter
          +s                                                             
       95 +        assert params["as_of"].default is inspect.Parameter.em
          +pty, name                                                     
       96 +        assert all(p.default is inspect.Parameter.empty for p 
          +in params.values()), name                                     
       97 +                                                              
       98 +                                                              
       99 +def test_each_transform_reads_only_its_spec_source():         
      100 +    db_features = {"days_since_last_txn", "has_ever_purchased"
          +, "txn_count_90d"}                                            
      101 +    parquet_features = {"avg_spend_per_active_month_6m", "spen
          +d_trend_3m_vs_prior_3m", "active_month_share"}                
      102 +    for name in schema.FEATURE_NAMES:                         
      103 +        args = set(inspect.signature(getattr(T, name)).paramet
          +ers)                                                          
      104 +        assert ("transactions" in args) == (name in db_feature
          +s), name                                                      
      105 +        assert ("monthly" in args) == (name in parquet_feature
          +s), name                                                      
      106 +                                                              
      107 +                                                              
      108 +# F1 tenure_days ---------------------------------------------
          +-----------------------------                                 
      109 +def test_tenure_days_normal_and_minimum():                    
      110 +    out = T.tenure_days(pop((1, "2025-05-02"), (2, "2026-05-01
          +")), AS_OF)                                                   
      111 +    assert out.tolist() == [365, 1]                           
      112 +    assert out.dtype == "int64"                               
      113 +                                                              
      114 +                                                              
      115 +# F2 days_since_last_txn -------------------------------------
          +-----------------------------                                 
      116 +def test_days_since_last_txn_normal_floors_whole_days():      
      117 +    t = txns((1, "2026-04-01 09:00"), (1, "2026-04-30 23:59"),
          + (2, "2026-04-01 00:00"))                                     
      118 +    out = T.days_since_last_txn(POP, t, AS_OF, cap_days=365)  
      119 +    assert v(out, 1) == 1  # 1 day 1 minute -> 1              
      120 +    assert v(out, 2) == 31                                    
      121 +                                                              
      122 +                                                              
      123 +def test_days_since_last_txn_zero_activity_gets_365():        
      124 +    out = T.days_since_last_txn(POP, NO_TXNS, AS_OF, cap_days=
          +365)                                                          
      125 +    assert out.tolist() == [365, 365, 365] and out.dtype == "i
          +nt64"                                                         
      126 +                                                              
      127 +                                                              
      128 +def test_days_since_last_txn_single_record():                 
      129 +    out = T.days_since_last_txn(POP, txns((3, "2026-05-01 12:0
          +0")), AS_OF, cap_days=365)                                    
      130 +    assert v(out, 3) == 0 and v(out, 1) == 365                
      131 +                                                              
      132 +                                                              
      133 +def test_days_since_last_txn_excludes_rows_at_or_after_as_of()
          +:                                                             
      134 +    # customer 697's case: a purchase on as_of's own day must 
          +not count                                                     
      135 +    t = txns((1, "2026-03-15 08:00"), (1, "2026-05-02 17:12"),
          + (2, "2026-05-02 00:00:00"),                                  
      136 +             (3, "2026-05-01 23:59:59.999999999"))            
      137 +    out = T.days_since_last_txn(POP, t, AS_OF, cap_days=365)  
      138 +    assert v(out, 1) == 47                                    
      139 +    assert v(out, 2) == 365                                   
      140 +    assert v(out, 3) == 0                                     
      141 +                                                              
      142 +                                                              
      143 +def test_days_since_last_txn_caps_long_gaps():                
      144 +    out = T.days_since_last_txn(POP, txns((1, "2024-12-01")), 
          +AS_OF, cap_days=365)                                          
      145 +    assert v(out, 1) == 365                                   
      146 +                                                              
      147 +                                                              
      148 +# F3 has_ever_purchased --------------------------------------
          +-----------------------------                                 
      149 +def test_has_ever_purchased_normal_zero_and_single():         
      150 +    t = txns((1, "2025-01-01"), (1, "2026-01-01"), (2, "2026-0
          +4-30"))                                                       
      151 +    out = T.has_ever_purchased(POP, t, AS_OF)                 
      152 +    assert out.tolist() == [1, 1, 0] and out.dtype == "int64" 
      153 +                                                              
      154 +                                                              
      155 +def test_has_ever_purchased_boundary_purchase_at_as_of_does_no
          +t_count():                                                    
      156 +    out = T.has_ever_purchased(POP, txns((1, "2026-05-02 00:00
          +:00"), (2, "2026-05-01 23:59")), AS_OF)                       
      157 +    assert v(out, 1) == 0 and v(out, 2) == 1                  
      158 +                                                              
      159 +                                                              
      160 +# F4 txn_count_90d -------------------------------------------
          +-----------------------------                                 
      161 +def test_txn_count_90d_normal_counts_returned_purchases():    
      162 +    t = txns((1, "2026-03-01"), (1, "2026-04-01"), (1, "2026-0
          +4-20"))                                                       
      163 +    t.loc[1, "is_returned"] = 1                               
      164 +    out = T.txn_count_90d(POP, t, AS_OF, window_days=90)      
      165 +    assert v(out, 1) == 3                                     
      166 +                                                              
      167 +                                                              
      168 +def test_txn_count_90d_zero_activity_and_single():            
      169 +    out = T.txn_count_90d(POP, txns((2, "2026-04-15")), AS_OF,
          + window_days=90)                                              
      170 +    assert out.tolist() == [0, 1, 0] and out.dtype == "int64" 
      171 +                                                              
      172 +                                                              
      173 +def test_txn_count_90d_window_boundaries():                   
      174 +    t = txns(                                                 
      175 +        (1, "2026-02-01 00:00:00"),            # exactly as_of
          + - 90 days: included                                          
      176 +        (1, "2026-01-31 23:59:59.999999999"),  # just before t
          +he window: excluded                                           
      177 +        (2, "2026-05-01 23:59:59"),            # last moment b
          +efore as_of: included                                         
      178 +        (2, "2026-05-02 00:00:00"),            # at as_of: exc
          +luded                                                         
      179 +    )                                                         
      180 +    out = T.txn_count_90d(POP, t, AS_OF, window_days=90)      
      181 +    assert v(out, 1) == 1 and v(out, 2) == 1                  
      182 +                                                              
      183 +                                                              
      184 +# F5 avg_spend_per_active_month_6m ---------------------------
          +-----------------------------                                 
      185 +def test_avg_spend_normal_averages_over_active_months_only(): 
      186 +    m = months((1, "2025-12", 100.0), (1, "2026-03", 50.0))   
      187 +    out = T.avg_spend_per_active_month_6m(POP, m, AS_OF, windo
          +w_months=6, monthly_cap=None)                                 
      188 +    assert v(out, 1) == pytest.approx(75.0)                   
      189 +                                                              
      190 +                                                              
      191 +def test_avg_spend_cap_applies_per_month():                   
      192 +    m = months((1, "2025-12", 300.0), (1, "2026-03", 100.0))  
      193 +    v1 = T.avg_spend_per_active_month_6m(POP, m, AS_OF, window
          +_months=6, monthly_cap=None)                                  
      194 +    v2 = T.avg_spend_per_active_month_6m(POP, m, AS_OF, window
          +_months=6, monthly_cap=197.10)                                
      195 +    assert v(v1, 1) == pytest.approx(200.0)                   
      196 +    assert v(v2, 1) == pytest.approx((197.10 + 100.0) / 2)    
      197 +                                                              
      198 +                                                              
      199 +def test_avg_spend_zero_activity_and_bought_only_before_window
          +():                                                           
      200 +    m = months((2, "2025-06", 80.0))                          
      201 +    out = T.avg_spend_per_active_month_6m(POP, m, AS_OF, windo
          +w_months=6, monthly_cap=197.10)                               
      202 +    assert out.tolist() == [0.0, 0.0, 0.0] and out.dtype == "f
          +loat64"                                                       
      203 +                                                              
      204 +                                                              
      205 +def test_avg_spend_single_record():                           
      206 +    out = T.avg_spend_per_active_month_6m(POP, months((3, "202
          +6-04", 42.5)), AS_OF, window_months=6, monthly_cap=None)      
      207 +    assert v(out, 3) == pytest.approx(42.5)                   
      208 +                                                              
      209 +                                                              
      210 +def test_avg_spend_month_boundaries():                        
      211 +    m = months((1, "2025-10", 999.0), (1, "2025-11", 10.0), (1
          +, "2026-05", 999.0))                                          
      212 +    out = T.avg_spend_per_active_month_6m(POP, m, AS_OF, windo
          +w_months=6, monthly_cap=None)                                 
      213 +    assert v(out, 1) == pytest.approx(10.0)  # 2025-10 is befo
          +re the window, 2026-05 is as_of's month                       
      214 +                                                              
      215 +                                                              
      216 +# F6 spend_trend_3m_vs_prior_3m ------------------------------
          +-----------------------------                                 
      217 +def test_spend_trend_four_cases():                            
      218 +    p4 = pop((1, "2024-01-10"), (2, "2024-01-10"), (3, "2024-0
          +1-10"), (4, "2024-01-10"))                                    
      219 +    m = months(                                               
      220 +        (1, "2025-12", 100.0), (1, "2026-03", 50.0),  # both: 
          +0.5                                                           
      221 +        (2, "2025-11", 80.0),                         # prior 
          +only: stopped                                                 
      222 +        (3, "2026-04", 30.0),                         # recent
          + only                                                         
      223 +    )                                                         
      224 +    out = T.spend_trend_3m_vs_prior_3m(p4, m, AS_OF, window_mo
          +nths=3, ratio_cap=3)                                          
      225 +    assert out.tolist() == [0.5, 0.0, 1.0, 1.0]  # customer 4:
          + zero activity                                                
      226 +    assert out.dtype == "float64"                             
      227 +                                                              
      228 +                                                              
      229 +def test_spend_trend_ratio_cap():                             
      230 +    m = months((1, "2026-01", 10.0), (1, "2026-02", 200.0))   
      231 +    assert v(T.spend_trend_3m_vs_prior_3m(POP, m, AS_OF, windo
          +w_months=3, ratio_cap=10), 1) == 10.0                         
      232 +    assert v(T.spend_trend_3m_vs_prior_3m(POP, m, AS_OF, windo
          +w_months=3, ratio_cap=3), 1) == 3.0                           
      233 +                                                              
      234 +                                                              
      235 +def test_spend_trend_single_record_each_side():               
      236 +    m = months((1, "2026-02", 25.0), (2, "2026-01", 25.0))    
      237 +    out = T.spend_trend_3m_vs_prior_3m(POP, m, AS_OF, window_m
          +onths=3, ratio_cap=3)                                         
      238 +    assert v(out, 1) == 1.0 and v(out, 2) == 0.0              
      239 +                                                              
      240 +                                                              
      241 +def test_spend_trend_month_boundaries():                      
      242 +    # recent = 2026-02..04, prior = 2025-11..2026-01; 2025-10 
          +and 2026-05 are ignored                                       
      243 +    m = months((1, "2025-10", 500.0), (1, "2025-11", 40.0), (1
          +, "2026-02", 20.0), (1, "2026-05", 500.0))                    
      244 +    out = T.spend_trend_3m_vs_prior_3m(POP, m, AS_OF, window_m
          +onths=3, ratio_cap=3)                                         
      245 +    assert v(out, 1) == pytest.approx(0.5)                    
      246 +                                                              
      247 +                                                              
      248 +# F7 active_month_share --------------------------------------
          +-----------------------------                                 
      249 +def test_eligible_months_starts_at_signup_month():            
      250 +    signup = pd.to_datetime(pd.Series(["2020-01-01", "2026-02-
          +15", "2026-04-30", "2026-05-01"])).astype("datetime64[ns]")   
      251 +    assert T.eligible_months(signup, AS_OF, 6).tolist() == [6,
          + 3, 1, 0]                                                     
      252 +                                                              
      253 +                                                              
      254 +def test_active_month_share_normal_and_recent_signup():       
      255 +    p = pop((1, "2020-01-01"), (2, "2026-02-15"))             
      256 +    m = months((1, "2025-12", 5.0), (1, "2026-04", 5.0), (2, "
          +2026-02", 5.0), (2, "2026-03", 5.0), (2, "2026-04", 5.0))     
      257 +    out = T.active_month_share(p, m, AS_OF, lookback_months=6)
      258 +    assert v(out, 1) == pytest.approx(2 / 6)                  
      259 +    assert v(out, 2) == pytest.approx(1.0)  # 3 of its 3 eligi
          +ble months                                                    
      260 +                                                              
      261 +                                                              
      262 +def test_active_month_share_zero_activity_and_zero_eligible():
      263 +    p = pop((1, "2020-01-01"), (2, "2026-05-01"))  # 2 is cust
          +omer 1022's case                                              
      264 +    out = T.active_month_share(p, NO_MONTHS, AS_OF, lookback_m
          +onths=12)                                                     
      265 +    assert out.tolist() == [0.0, 0.0] and out.dtype == "float6
          +4"                                                            
      266 +    assert not out.isna().any()                               
      267 +                                                              
      268 +                                                              
      269 +def test_active_month_share_single_record_and_lookback_boundar
          +y():                                                          
      270 +    p = pop((1, "2020-01-01"))                                
      271 +    m = months((1, "2025-05", 5.0))  # first month of the 12-m
          +onth window, outside the 6-month one                          
      272 +    assert v(T.active_month_share(p, m, AS_OF, lookback_months
          +=12), 1) == pytest.approx(1 / 12)                             
      273 +    assert v(T.active_month_share(p, m, AS_OF, lookback_months
          +=6), 1) == 0.0                                                
      274 +                                                              
      275 +                                                              
      276 +# Shared contract --------------------------------------------
          +-----------------------------                                 
      277 +def test_every_transform_returns_the_population_index_with_sch
          +ema_dtype_and_no_nulls():                                     
      278 +    t = txns((1, "2026-04-01"))                               
      279 +    m = months((1, "2026-04", 10.0))                          
      280 +    for spec in schema.FEATURE_SCHEMA:                        
      281 +        fn = getattr(T, spec.name)                            
      282 +        args = inspect.signature(fn).parameters               
      283 +        inputs = {"population": POP, "as_of": AS_OF}          
      284 +        if "transactions" in args:                            
      285 +            inputs["transactions"] = t                        
      286 +        if "monthly" in args:                                 
      287 +            inputs["monthly"] = m                             
      288 +        out = fn(**inputs, **spec.transform_kwargs(spec.latest
          +.parameters))                                                 
      289 +        assert out.index.equals(POP.index), spec.name         
      290 +        assert str(out.dtype) == spec.dtype, spec.name        
      291 +        assert out.name == spec.name                          
      292 +        assert not out.isna().any(), spec.name                
      293 +        assert np.isfinite(out.to_numpy(dtype="float64")).all(
          +), spec.name                                                  

● Write(tests/test_ingest.py)  ⎿  Wrote 70 lines to tests/test_ingest.py
       1 """features/ingest.py: one loader per source, pure I/O plus type
          coercion."""
       2
       3 import sqlite3
       4
       6 import pytest
       7
       8 from features import ingest
       9 from features.build import DEFAULT_SOURCE_PATHS
      10 from features.transforms import parse_as_of
     … +75 lines (ctrl+o to expand)

● Write(tests/test_parity.py)
  ⎿  Added 115 lines, removed 5 lines
        6  
        7  If these two paths can't be driven through the same test with 
           the same expected output,
        8  the parity guarantee in feature_spec.md isn't real — fix featu
           res/build.py, not this test.
        9 +                                                              
       10 +Runs with the latest parameters and with every versioned featu
          +re's v1 parameters, over the                                  
       11 +customers feature_spec.md requires.                           
       12  """
       13  
       14 +import inspect                                                
       15 +import json                                                   
       16 +import random                                                 
       17 +                                                              
       18 +import pandas as pd                                           
       19  import pytest
       20  
       21 +from feature_store import store                               
       22 +from features import schema, transforms                       
       23 +from features.build import build_features                     
       24 +from features.quality_checks import QualityCheckError         
       25 +from scripts import score_customer                            
       26 +from tests.conftest import AS_OF, version_params              
       27  
       14 -def test_training_and_scoring_paths_agree_for_same_customer():
       15 -    """TODO: call the shared build_features(as_of, ...) entry 
          -point once in "training"                                      
       16 -    mode (batch) and once in "scoring" mode (single customer_i
          -d), same as_of date, and                                      
       17 -    assert every feature value matches exactly for that custom
          -er."""                                                        
       18 -    pytest.skip("TODO: implement against features/build.py")  
       28 +PARITY_CUSTOMERS = {                                          
       29 +    27: "zero-activity: no transaction before as_of, 289 days 
          +of tenure",                                                   
       30 +    350: "first purchase on 2026-05-01: counts in txn_count_90
          +d, outside every month window",                               
       31 +    697: "purchase at 2026-05-02 17:12, at/after as_of, must b
          +e excluded",                                                  
       32 +    1022: "signed up 2026-05-01: 0 eligible months for active_
          +month_share",                                                 
       33 +    5: "spend_trend 'recent spend only' case",                
       34 +    3: "a month above monthly_cap, so avg_spend v1 and v2 diff
          +er",                                                          
       35 +}                                                             
       36 +PARAM_SETS = {"latest": None, "v1": version_params(1)}        
       37 +                                                              
       38 +                                                              
       39 +@pytest.fixture(scope="module")                               
       40 +def training(full_latest, full_v1):                           
       41 +    return {"latest": full_latest, "v1": full_v1}             
       42 +                                                              
       43 +                                                              
       44 +def _row(frame: pd.DataFrame, cid: int) -> pd.DataFrame:      
       45 +    return frame.loc[frame["customer_id"] == cid].reset_index(
          +drop=True)                                                    
       46 +                                                              
       47 +                                                              
       48 +def test_parity_customers_really_cover_their_cases(training): 
       49 +    latest = training["latest"].set_index("customer_id")      
       50 +    v1 = training["v1"].set_index("customer_id")              
       51 +    assert latest.loc[27, "has_ever_purchased"] == 0 and lates
          +t.loc[27, "days_since_last_txn"] == 365                       
       52 +    assert latest.loc[350, "txn_count_90d"] >= 1 and latest.lo
          +c[350, "active_month_share"] == 0.0                           
       53 +    assert latest.loc[697, "days_since_last_txn"] == 54  # not
          + 0: the 2026-05-02 17:12 purchase is excluded                 
       54 +    assert latest.loc[1022, "tenure_days"] == 1 and latest.loc
          +[1022, "active_month_share"] == 0.0                           
       55 +    assert latest.loc[5, "spend_trend_3m_vs_prior_3m"] == 1.0 
          +and latest.loc[5, "txn_count_90d"] > 0                        
       56 +    assert v1.loc[3, "avg_spend_per_active_month_6m"] != lates
          +t.loc[3, "avg_spend_per_active_month_6m"]                     
       57 +                                                              
       58 +                                                              
       59 +@pytest.mark.parametrize("param_set", PARAM_SETS)             
       60 +@pytest.mark.parametrize("cid", PARITY_CUSTOMERS)             
       61 +def test_training_and_scoring_paths_agree_for_same_customer(tr
          +aining, param_set, cid):                                      
       62 +    scored = build_features(AS_OF, customer_ids=[cid], feature
          +_params=PARAM_SETS[param_set])                                
       63 +    pd.testing.assert_frame_equal(scored, _row(training[param_
          +set], cid), check_exact=True)                                 
       64 +                                                              
       65 +                                                              
       66 +@pytest.mark.parametrize("param_set", PARAM_SETS)             
       67 +def test_scoring_every_customer_in_shuffled_order_matches_trai
          +ning(training, param_set):                                    
       68 +    """Catches a scoring-only divergence that would affect any
          + customer, not just the six above."""                         
       69 +    full = training[param_set]                                
       70 +    ids = full["customer_id"].tolist()                        
       71 +    random.Random(0).shuffle(ids)                             
       72 +    scored = build_features(AS_OF, customer_ids=ids, feature_p
          +arams=PARAM_SETS[param_set])                                  
       73 +    expected = full.set_index("customer_id").loc[ids].reset_in
          +dex()                                                         
       74 +    pd.testing.assert_frame_equal(scored, expected, check_exac
          +t=True)                                                       
       75 +                                                              
       76 +                                                              
       77 +def test_scoring_script_matches_the_training_scripts_stored_la
          +test_values(built_store, capsys):                             
       78 +    ids = list(PARITY_CUSTOMERS)                              
       79 +    argv = [a for cid in ids for a in ("--customer-id", str(ci
          +d))] + ["--format", "json"]                                   
       80 +    assert score_customer.main(argv) == 0                     
       81 +    scored = pd.DataFrame(json.loads(capsys.readouterr().out)[
          +"customers"]).set_index("customer_id")                        
       82 +    for name in schema.FEATURE_NAMES:                         
       83 +        stored = store.get_latest(name, store_dir=built_store)
          +.set_index("customer_id")[name]                               
       84 +        assert scored[name].tolist() == stored.loc[ids].tolist
          +(), name                                                      
       85 +                                                              
       86 +                                                              
       87 +@pytest.mark.parametrize(                                     
       88 +    "ids, message",                                           
       89 +    [([99999], "99999 does not exist"), ([899], "899 signed up
          + on 2026-05-02"), ([5, 5], "more than once"),                 
       90 +     ([], "empty"), (["5"], "not an integer")],               
       91 +)                                                             
       92 +def test_invalid_scoring_requests_raise_naming_the_id(ids, mes
          +sage):                                                        
       93 +    with pytest.raises(ValueError, match=message):            
       94 +        build_features(AS_OF, customer_ids=ids)               
       95 +                                                              
       96 +                                                              
       97 +def test_scoring_script_exits_non_zero_on_unknown_id(capsys): 
       98 +    assert score_customer.main(["--customer-id", "99999"]) == 
          +1                                                             
       99 +    captured = capsys.readouterr()                            
      100 +    assert captured.out == "" and "99999" in captured.err     
      101 +                                                              
      102 +                                                              
      103 +def test_checks_run_on_the_full_population_even_when_scoring(m
          +onkeypatch):                                                  
      104 +    """A defect for customer 27 must stop a scoring call for c
          +ustomer 5: QC1-QC8 sit on the                                 
      105 +    shared path before the selection, not behind an 'if traini
          +ng' branch."""                                                
      106 +    real = transforms.days_since_last_txn                     
      107 +                                                              
      108 +    def broken(population, transactions, as_of, *, cap_days): 
      109 +        out = real(population, transactions, as_of, cap_days=c
          +ap_days)                                                      
      110 +        out.loc[27] = 0                                       
      111 +        return out                                            
      112 +                                                              
      113 +    monkeypatch.setattr(transforms, "days_since_last_txn", bro
          +ken)                                                          
      114 +    with pytest.raises(QualityCheckError, match="QC3"):       
      115 +        build_features(AS_OF, customer_ids=[5])               
      116 +                                                              
      117 +                                                              
      118 +def test_as_of_is_a_required_argument_with_no_default():      
      119 +    params = inspect.signature(build_features).parameters     
      120 +    assert params["as_of"].default is inspect.Parameter.empty 
      121 +    with pytest.raises(ValueError):                           
      122 +        build_features("2026-05-02 00:00")                    
      123 +                                                              
      124 +                                                              
      125 +def test_build_never_returns_the_label(full_latest):          
      126 +    assert list(full_latest.columns) == ["customer_id", *schem
          +a.FEATURE_NAMES, "as_of_date"]                                
      127 +    assert "churned" not in full_latest.columns               
      128 +    assert (full_latest["as_of_date"] == pd.Timestamp(AS_OF)).
          +all()                                                         

● Shared constants and helpers are moving out of conftest.py into
  tests/helpers.py. Importing conftest as a module would load it twice.

● Bash(cat > tests/helpers.py <<'EOF'
      """Constants and helpers shared by the test modules (fixtures live in
      conftest.py)."""…)
  ⎿  Updated tests/conftest.py (+1 -16)
       2  session guard below fails the run if anything touched the real 
          feature_store/."""
       3  
       4  import hashlib
       5 -import sqlite3                                                 
       5  from pathlib import Path
       6  
       7  import pandas as pd
       8  import pytest
       9  
      10  from feature_store import store
      12 -from features import schema                                    
      11  from features.build import build_features
      14 -                                                               
      15 -AS_OF = "2026-05-02"                                           
      12 +from tests.helpers import AS_OF, version_params                
      13  
      14  
      15  def _real_store_fingerprint() -> dict[str, str]:
     ...
      25      assert _real_store_fingerprint() == before, "a test modifie
          d the real feature_store/"
      26  
      27  
      31 -def version_params(k: int) -> dict[str, dict]:                 
      32 -    """Each feature's k-th schema version (or its last, if it h
         -as fewer)."""                                                  
      33 -    return {f.name: f.versions[min(k, len(f.versions)) - 1].par
         -ameters for f in schema.FEATURE_SCHEMA}                        
      34 -                                                               
      35 -                                                               
      28  @pytest.fixture(scope="session")
      29  def full_latest() -> pd.DataFrame:
      30      return build_features(AS_OF)
     ...
      45      assert build_training_features.main(["--store-dir", str(roo
          t), "--lineage-out", str(lineage)]) == 0
      46      return root
      47  
      56 -                                                               
      57 -def registry_rows(root: Path) -> list[tuple]:                  
      58 -    path = root / store.REGISTRY_FILENAME                      
      59 -    if not path.exists():                                      
      60 -        return []                                              
      61 -    with sqlite3.connect(path) as con:                         
      62 -        return con.execute("SELECT * FROM feature_registry ORDE
         -R BY feature_name, version").fetchall()                        
  ⎿  Created tests/helpers.py (+27 -0)
       1 +"""Constants and helpers shared by the test modules (fixtures l
         +ive in conftest.py)."""                                        
       2 +                                                               
       3 +from pathlib import Path                                       
       4 +import sqlite3                                                 
       5 +                                                               
       6 +from feature_store import store                                
       7 +from features import schema                                    
       8 +                                                               
       9 +AS_OF = "2026-05-02"                                           
      10 +                                                               
      11 +                                                               
      12 +def version_params(k: int) -> dict[str, dict]:                 
      13 +    """Each feature's k-th schema version (or its last, if it h
         +as fewer)."""                                                  
      14 +    return {f.name: f.versions[min(k, len(f.versions)) - 1].par
         +ameters for f in schema.FEATURE_SCHEMA}                        
      15 +                                                               
      16 +                                                               
      17 +def registry_rows(root: Path) -> list[tuple]:                  
      18 +    path = root / store.REGISTRY_FILENAME                      
      19 +    if not path.exists():                                      
      20 +        return []                                              
      21 +    with sqlite3.connect(path) as con:                         
      22 +        return con.execute("SELECT * FROM feature_registry ORDE
         +R BY feature_name, version").fetchall()                        
      23 +                                                               
      24 +                                                               
      25 +def store_files(root: Path) -> list[str]:                      
      26 +    values = root / store.VALUES_DIRNAME                       
      27 +    return sorted(p.name for p in values.glob("*")) if values.e
         +xists() else []                                                
  ⎿  Updated tests/test_parity.py (+1 -1)
      23  from features.build import build_features
      24  from features.quality_checks import QualityCheckError
      25  from scripts import score_customer
      26 -from tests.conftest import AS_OF, version_params               
      26 +from tests.helpers import AS_OF, version_params                
      27  
      28  PARITY_CUSTOMERS = {
      29      27: "zero-activity: no transaction before as_of, 289 days o
          f tenure",

● Now the quality-check tests. Each check fires on a deliberately bad input
  built from the real data, passes on clean input, and is wired into
  build_features.

● Write(tests/test_quality_checks.py)
  ⎿  Added 261 lines, removed 9 lines
        1  """
        2  Feed each check in features/quality_checks.py a deliberately b
           ad input (an injected
        3  null, an out-of-range amount, a shifted distribution) and conf
           irm it fires; feed it
        4 -clean input and confirm it doesn't false-positive.            
        4 +clean input and confirm it doesn't false-positive. Bad inputs 
          +are realistic mutations of                                    
        5 +the real 2026-05-02 build, so each check is shown catching the
          + failure mode feature_spec.md                                 
        6 +names for it.                                                 
        7  """
        8  
        9 +import pandas as pd                                           
       10  import pytest
       11  
       12 +from features import ingest, schema, transforms               
       13 +from features import quality_checks as qc                     
       14 +from features.build import DEFAULT_SOURCE_PATHS, build_feature
          +s                                                             
       15 +from tests.helpers import AS_OF, version_params               
       16  
       10 -def test_null_check_fires_on_injected_null():                 
       11 -    pytest.skip("TODO: implement against features/quality_chec
          -ks.py")                                                       
       17 +AS_OF_TS = transforms.parse_as_of(AS_OF)                      
       18 +LATEST = schema.latest_parameters()                           
       19 +V1 = schema.resolve_feature_params(version_params(1))         
       20  
       21  
       14 -def test_outlier_check_fires_on_out_of_range_value():         
       15 -    pytest.skip("TODO: implement against features/quality_chec
          -ks.py")                                                       
       22 +@pytest.fixture(scope="module")                               
       23 +def sources():                                                
       24 +    transactions = ingest.load_transactions(DEFAULT_SOURCE_PAT
          +HS["transactions"])                                           
       25 +    monthly = ingest.load_monthly_agg(DEFAULT_SOURCE_PATHS["mo
          +nthly_agg"])                                                  
       26 +    return {                                                  
       27 +        "customers": ingest.load_customers(DEFAULT_SOURCE_PATH
          +S["customers"]),                                              
       28 +        "transactions": transactions,                         
       29 +        "monthly": monthly,                                   
       30 +        "buyer_ids": ingest.load_customer_ids_with_txn_before(
          +DEFAULT_SOURCE_PATHS["transactions"], AS_OF_TS),              
       31 +        "transactions_used": transactions.loc[transactions["tx
          +n_timestamp"] < AS_OF_TS],                                    
       32 +        "monthly_used": monthly.loc[monthly["txn_month"] < tra
          +nsforms.month_start(AS_OF_TS)],                               
       33 +    }                                                         
       34  
       35  
       18 -def test_drift_check_fires_on_shifted_distribution():         
       19 -    pytest.skip("TODO: implement against features/quality_chec
          -ks.py")                                                       
       36 +@pytest.fixture                                               
       37 +def clean(full_latest):                                       
       38 +    return full_latest.drop(columns="as_of_date").copy()      
       39  
       40  
       22 -def test_checks_do_not_false_positive_on_clean_input():       
       23 -    pytest.skip("TODO: implement against features/quality_chec
          -ks.py")                                                       
       41 +def output_checks(features, sources, params=LATEST):          
       42 +    return {r.check_id: r for r in qc.run_output_checks(      
       43 +        features, customers=sources["customers"], transactions
          +=sources["transactions"],                                     
       44 +        monthly=sources["monthly"], buyer_ids=sources["buyer_i
          +ds"], as_of=AS_OF_TS, params=params)}                         
       45 +                                                              
       46 +                                                              
       47 +def set_value(df, cid, column, value):                        
       48 +    df.loc[df["customer_id"] == cid, column] = value          
       49 +    return df                                                 
       50 +                                                              
       51 +                                                              
       52 +# Clean input: no false positives ----------------------------
          +-----------------------------                                 
       53 +def test_checks_do_not_false_positive_on_clean_input(clean, so
          +urces, full_v1):                                              
       54 +    for params, frame in ((LATEST, clean), (V1, full_v1.drop(c
          +olumns="as_of_date"))):                                       
       55 +        results = list(output_checks(frame, sources, params).v
          +alues()) + qc.run_input_checks(                               
       56 +            sources["transactions_used"], sources["monthly_use
          +d"], sources["transactions"],                                 
       57 +            sources["monthly"], AS_OF_TS, params)             
       58 +        assert all(r.passed for r in results), qc.format_resul
          +ts(results)                                                   
       59 +        qc.raise_on_failure(results)  # does not raise        
       60 +                                                              
       61 +                                                              
       62 +def test_spec_expected_counts_at_2026_05_02(full_latest, sourc
          +es):                                                          
       63 +    """feature_spec.md: 1,399 population customers, 232 of the
          +m zero-activity."""                                           
       64 +    assert len(full_latest) == 1399                           
       65 +    population = sources["customers"].loc[sources["customers"]
          +["signup_date"] < AS_OF_TS, "customer_id"]                    
       66 +    assert len(set(population) - sources["buyer_ids"]) == 232 
       67 +    assert int((full_latest["has_ever_purchased"] == 0).sum())
          + == 232                                                       
       68 +                                                              
       69 +                                                              
       70 +# QC1 --------------------------------------------------------
          +-----------------------------                                 
       71 +def test_qc1_fires_when_a_zero_activity_customer_is_dropped(cl
          +ean, sources):                                                
       72 +    result = output_checks(clean[clean["customer_id"] != 27], 
          +sources)["QC1"]                                               
       73 +    assert not result.passed and 27 in result.failing_rows    
       74 +                                                              
       75 +                                                              
       76 +def test_qc1_fires_on_a_duplicated_customer(clean, sources):  
       77 +    result = output_checks(pd.concat([clean, clean[clean["cust
          +omer_id"] == 3]]), sources)["QC1"]                            
       78 +    assert not result.passed and 3 in result.failing_rows     
       79 +                                                              
       80 +                                                              
       81 +def test_qc1_selection_fires_when_rows_do_not_match_the_reques
          +t(clean):                                                     
       82 +    assert qc.check_selection_rows(clean[clean["customer_id"].
          +isin([3, 5])], [3, 5]).passed                                 
       83 +    assert not qc.check_selection_rows(clean[clean["customer_i
          +d"].isin([3])], [3, 5]).passed                                
       84 +                                                              
       85 +                                                              
       86 +# QC2 --------------------------------------------------------
          +-----------------------------                                 
       87 +def test_qc2_null_check_fires_on_injected_null(clean, sources)
          +:                                                             
       88 +    result = output_checks(set_value(clean, 27, "avg_spend_per
          +_active_month_6m", float("nan")), sources)["QC2"]             
       89 +    assert not result.passed                                  
       90 +    assert result.failing_features == ("avg_spend_per_active_m
          +onth_6m",) and result.failing_rows == (27,)                   
       91 +                                                              
       92 +                                                              
       93 +# QC3 --------------------------------------------------------
          +-----------------------------                                 
       94 +@pytest.mark.parametrize("column, wrong", [                   
       95 +    ("days_since_last_txn", 0),            # 0 instead of 365 
       96 +    ("spend_trend_3m_vs_prior_3m", 0.0),   # "stopped" instead
          + of the 1.0 default                                           
       97 +    ("has_ever_purchased", 1),             # F3 disagrees with
          + the DB query                                                 
       98 +    ("active_month_share", 0.5),                              
       99 +])                                                            
      100 +def test_qc3_fires_on_a_wrong_zero_activity_default(clean, sou
          +rces, column, wrong):                                         
      101 +    result = output_checks(set_value(clean, 27, column, wrong)
          +, sources)["QC3"]                                             
      102 +    assert not result.passed and column in result.failing_feat
          +ures and result.failing_rows == (27,)                         
      103 +                                                              
      104 +                                                              
      105 +def test_qc3_fires_when_a_buyer_is_flagged_as_never_purchased(
          +clean, sources):                                              
      106 +    result = output_checks(set_value(clean, 5, "has_ever_purch
          +ased", 0), sources)["QC3"]                                    
      107 +    assert not result.passed and 5 in result.failing_rows     
      108 +                                                              
      109 +                                                              
      110 +# QC4 --------------------------------------------------------
          +-----------------------------                                 
      111 +@pytest.mark.parametrize("column, bad", [                     
      112 +    ("tenure_days", 0), ("days_since_last_txn", 366), ("has_ev
          +er_purchased", 2),                                            
      113 +    ("txn_count_90d", 101), ("spend_trend_3m_vs_prior_3m", 3.5
          +), ("active_month_share", 1.2),                               
      114 +    ("avg_spend_per_active_month_6m", -1.0),                  
      115 +])                                                            
      116 +def test_qc4_outlier_check_fires_on_out_of_range_value(clean, 
          +sources, column, bad):                                        
      117 +    result = output_checks(set_value(clean, 3, column, bad), s
          +ources)["QC4"]                                                
      118 +    assert not result.passed and result.failing_features == (c
          +olumn,) and result.failing_rows == (3,)                       
      119 +                                                              
      120 +                                                              
      121 +def test_qc4_monetary_bound_follows_the_version_cap(clean, sou
          +rces):                                                        
      122 +    above_cap = set_value(clean, 3, "avg_spend_per_active_mont
          +h_6m", 501.94)                                                
      123 +    assert not output_checks(above_cap, sources, LATEST)["QC4"
          +].passed  # v2: cap 197.10 not applied                        
      124 +    assert qc.check_ranges(above_cap, V1).passed              
          +           # v1: uncapped, bound 2,000                        
      125 +    assert not qc.check_ranges(set_value(clean, 3, "avg_spend_
          +per_active_month_6m", 2500.0), V1).passed                     
      126 +                                                              
      127 +                                                              
      128 +# QC5 --------------------------------------------------------
          +-----------------------------                                 
      129 +def test_qc5_fires_when_a_row_at_or_after_as_of_reaches_the_tr
          +ansforms(sources):                                            
      130 +    leaky = sources["transactions"].loc[sources["transactions"
          +]["txn_timestamp"] < AS_OF_TS + pd.Timedelta(days=1)]         
      131 +    result = qc.check_no_future_data(leaky, sources["monthly_u
          +sed"], AS_OF_TS)                                              
      132 +    assert not result.passed and 697 in result.failing_rows   
      133 +    assert "days_since_last_txn" in result.failing_features   
      134 +                                                              
      135 +                                                              
      136 +def test_qc5_fires_when_as_of_month_reaches_the_monthly_transf
          +orms(sources):                                                
      137 +    leaky = sources["monthly"].loc[sources["monthly"]["txn_mon
          +th"] <= AS_OF_TS]  # includes 2026-05                         
      138 +    result = qc.check_no_future_data(sources["transactions_use
          +d"], leaky, AS_OF_TS)                                         
      139 +    assert not result.passed and "avg_spend_per_active_month_6
          +m" in result.failing_features                                 
      140 +                                                              
      141 +                                                              
      142 +# QC6 --------------------------------------------------------
          +-----------------------------                                 
      143 +def test_qc6_fires_on_the_real_late_july_etl_gap(sources):    
      144 +    as_of = transforms.parse_as_of("2026-08-01")              
      145 +    result = qc.check_parquet_coverage(sources["transactions"]
          +, sources["monthly"], as_of, LATEST)                          
      146 +    assert not result.passed                                  
      147 +    assert "110 differ, 102 in one source only" in result.deta
          +il and "2026-07" in result.detail                             
      148 +    assert set(result.failing_features) == {                  
      149 +        "avg_spend_per_active_month_6m", "spend_trend_3m_vs_pr
          +ior_3m", "active_month_share"}                                
      150 +                                                              
      151 +                                                              
      152 +def test_build_stops_on_the_etl_gap_for_training_and_scoring_a
          +like():                                                       
      153 +    with pytest.raises(qc.QualityCheckError, match="QC6"):    
      154 +        build_features("2026-08-01")                          
      155 +    with pytest.raises(qc.QualityCheckError, match="QC6"):    
      156 +        build_features("2026-08-01", customer_ids=[3])        
      157 +                                                              
      158 +                                                              
      159 +def _shifted_monthly(sources, change):                        
      160 +    monthly = sources["monthly"].copy()                       
      161 +    row = monthly.index[(monthly["customer_id"] == 3) & (month
          +ly["txn_month"] == "2026-02-01")][0]                          
      162 +    return change(monthly, row)                               
      163 +                                                              
      164 +                                                              
      165 +@pytest.mark.parametrize("delta, fires", [(0.02, True), (0.005
          +, False)])                                                    
      166 +def test_qc6_amount_tolerance_is_one_cent(sources, delta, fire
          +s):                                                           
      167 +    monthly = _shifted_monthly(sources, lambda m, r: m.assign(
          +total_amount=m["total_amount"].where(m.index != r, m.at[r, "to
          +tal_amount"] + delta)))                                       
      168 +    result = qc.check_parquet_coverage(sources["transactions"]
          +, monthly, AS_OF_TS, LATEST)                                  
      169 +    assert result.passed is not fires                         
      170 +                                                              
      171 +                                                              
      172 +def test_qc6_fires_on_a_missing_or_duplicated_parquet_row(sour
          +ces):                                                         
      173 +    missing = _shifted_monthly(sources, lambda m, r: m.drop(in
          +dex=r))                                                       
      174 +    duplicated = _shifted_monthly(sources, lambda m, r: pd.con
          +cat([m, m.loc[[r]]]))                                         
      175 +    for monthly in (missing, duplicated):                     
      176 +        result = qc.check_parquet_coverage(sources["transactio
          +ns"], monthly, AS_OF_TS, LATEST)                              
      177 +        assert not result.passed and 3 in result.failing_rows 
      178 +                                                              
      179 +                                                              
      180 +def test_qc6_scope_follows_the_parameters_in_effect(sources): 
      181 +    """v1's 12-month lookback widens the months QC6 must cover
          +."""                                                          
      182 +    assert len(qc.coverage_months(AS_OF_TS, LATEST)) == 6     
      183 +    assert len(qc.coverage_months(AS_OF_TS, V1)) == 12        
      184 +                                                              
      185 +                                                              
      186 +# QC7 --------------------------------------------------------
          +-----------------------------                                 
      187 +def _snapshot(full, name="avg_spend_per_active_month_6m"):    
      188 +    return full[["customer_id", name, "as_of_date"]].reset_ind
          +ex(drop=True)                                                 
      189 +                                                              
      190 +                                                              
      191 +def test_qc7_reports_no_reference_and_passes_on_identical_valu
          +es(full_latest):                                              
      192 +    snap = _snapshot(full_latest)                             
      193 +    assert qc.check_drift_against_store("avg_spend_per_active_
          +month_6m", snap, None).detail == "no reference"               
      194 +    assert qc.check_drift_against_store("avg_spend_per_active_
          +month_6m", snap, snap.copy()).passed                          
      195 +                                                              
      196 +                                                              
      197 +def test_qc7_drift_check_fires_on_shifted_distribution(full_la
          +test):                                                        
      198 +    ref = _snapshot(full_latest)                              
      199 +    shifted = ref.assign(avg_spend_per_active_month_6m=ref["av
          +g_spend_per_active_month_6m"] * 1.05)                         
      200 +    result = qc.check_drift_against_store("avg_spend_per_activ
          +e_month_6m", shifted, ref)                                    
      201 +    assert not result.passed and result.failing_features == ("
          +avg_spend_per_active_month_6m",)                              
      202 +                                                              
      203 +                                                              
      204 +@pytest.mark.parametrize("delta, fires", [(1e-6, True), (1e-12
          +, False)])                                                    
      205 +def test_qc7_tolerance_is_1e_9(full_latest, delta, fires):    
      206 +    ref = _snapshot(full_latest)                              
      207 +    new = set_value(ref.copy(), 3, "avg_spend_per_active_month
          +_6m",                                                         
      208 +                    ref.loc[ref["customer_id"] == 3, "avg_spen
          +d_per_active_month_6m"].iloc[0] + delta)                      
      209 +    result = qc.check_drift_against_store("avg_spend_per_activ
          +e_month_6m", new, ref)                                        
      210 +    assert result.passed is not fires                         
      211 +    if fires:                                                 
      212 +        assert result.failing_rows == (3,)                    
      213 +                                                              
      214 +                                                              
      215 +def test_qc7_fires_on_missing_customers_or_a_different_as_of(f
          +ull_latest):                                                  
      216 +    ref = _snapshot(full_latest)                              
      217 +    assert not qc.check_drift_against_store("avg_spend_per_act
          +ive_month_6m", ref[ref["customer_id"] != 3], ref).passed      
      218 +    moved = ref.assign(as_of_date=pd.Timestamp("2026-04-01")) 
      219 +    assert "as_of_date differs" in qc.check_drift_against_stor
          +e("avg_spend_per_active_month_6m", moved, ref).detail         
      220 +                                                              
      221 +                                                              
      222 +# QC8 --------------------------------------------------------
          +-----------------------------                                 
      223 +def test_qc8_fires_on_a_double_counted_transaction(clean, sour
          +ces):                                                         
      224 +    doubled = set_value(clean, 3, "txn_count_90d", int(clean.l
          +oc[clean["customer_id"] == 3, "txn_count_90d"].iloc[0]) * 2)  
      225 +    result = output_checks(doubled, sources)["QC8"]           
      226 +    assert output_checks(doubled, sources)["QC4"].passed  # QC
          +4's bound of 100 cannot see it                                
      227 +    assert not result.passed and result.failing_features == ("
          +txn_count_90d",)                                              
      228 +                                                              
      229 +                                                              
      230 +def test_qc8_fires_on_a_double_counted_month(clean, sources): 
      231 +    # customer 3 has 3 active of 6 eligible months; counting o
          +ne twice gives 4/6                                            
      232 +    doubled = set_value(clean, 3, "active_month_share", 4 / 6)
      233 +    result = output_checks(doubled, sources)["QC8"]           
      234 +    assert output_checks(doubled, sources)["QC4"].passed      
      235 +    assert not result.passed and result.failing_features == ("
          +active_month_share",)                                         
      236 +                                                              
      237 +                                                              
      238 +# Wiring: a failing check raises and stops the build ---------
          +-----------------------------                                 
      239 +def test_raise_on_failure_lists_every_failure(clean, sources):
      240 +    bad = set_value(set_value(clean, 27, "days_since_last_txn"
          +, 0), 3, "active_month_share", 1.5)                           
      241 +    with pytest.raises(qc.QualityCheckError) as err:          
      242 +        qc.raise_on_failure(list(output_checks(bad, sources).v
          +alues()))                                                     
      243 +    assert {f.check_id for f in err.value.failures} == {"QC3",
          + "QC4", "QC8"}                                                
      244 +                                                              
      245 +                                                              
      246 +def test_build_raises_when_a_transform_leaves_zero_activity_cu
          +stomers_null(monkeypatch):                                    
      247 +    """The easy off-by-one: computing on buyers only and forge
          +tting the default fill."""                                    
      248 +                                                              
      249 +    def no_fill(population, transactions, as_of, *, cap_days):
      250 +        last = transactions.groupby("customer_id")["txn_timest
          +amp"].max()                                                   
      251 +        return ((as_of - last) // pd.Timedelta(days=1)).reinde
          +x(population.index).rename("days_since_last_txn")             
      252 +                                                              
      253 +    monkeypatch.setattr(transforms, "days_since_last_txn", no_
          +fill)                                                         
      254 +    with pytest.raises(qc.QualityCheckError) as err:          
      255 +        build_features(AS_OF)                                 
      256 +    assert {"QC2", "QC3"} <= {f.check_id for f in err.value.fa
          +ilures}                                                       
      257 +                                                              
      258 +                                                              
      259 +def test_build_attaches_every_check_result(full_latest):      
      260 +    ids = [r["check_id"] for r in full_latest.attrs["quality_c
          +hecks"]]                                                      
      261 +    assert ids == ["QC5", "QC6", "QC1", "QC2", "QC3", "QC4", "
          +QC8", "QC1"]                                                  
      262 +    assert all(r["passed"] for r in full_latest.attrs["quality
          +_checks"])                                                    
      263 +                                                              
      264 +                                                              
      265 +# Standalone runner ------------------------------------------
          +-----------------------------                                 
      266 +def test_standalone_runner_passes_on_the_built_store(built_sto
          +re, capsys):                                                  
      267 +    assert qc.main(["--store-dir", str(built_store)]) == 0    
      268 +    assert "FAIL" not in capsys.readouterr().out              
      269 +                                                              
      270 +                                                              
      271 +def test_standalone_runner_fails_on_a_bad_features_file(full_l
          +atest, tmp_path, capsys):                                     
      272 +    path = tmp_path / "features.parquet"                      
      273 +    set_value(full_latest.copy(), 27, "spend_trend_3m_vs_prior
          +_3m", 0.0).to_parquet(path)                                   
      274 +    assert qc.main(["--features", str(path)]) == 1            
      275 +    assert "FAIL QC3" in capsys.readouterr().out              

● Found that QC7 and QC8 crash on duplicate customer_ids due to reindexing
  instead of failing gracefully. Fixing both so they always return a proper
  result.

  Read 1 file (ctrl+o to expand)

● Bash(conda run --no-capture-output -n churn-pipeline python - <<'EOF'
      p = "features/quality_checks.py"…)
  ⎿  Updated features/quality_checks.py (+15 -8)
      287      the same as_of (to within 1e-9). No stored copy: passes, r
           eporting 'no reference'."""
      288      if reference is None:
      289          return CheckResult("QC7", f"drift vs store [{feature_n
           ame}]", True, "no reference")
      290 +    name = f"drift vs store [{feature_name}]"                 
      291 +    duplicated = _ids(values.loc[values["customer_id"].duplica
          +ted(), "customer_id"])                                        
      292 +    if duplicated:                                            
      293 +        return CheckResult("QC7", name, False, f"{len(duplicat
          +ed)} duplicated customer_ids in the rebuilt values",          
      294 +                           (feature_name,), duplicated)       
      295      new = values.set_index("customer_id")
      296      ref = reference.set_index("customer_id")
      297      new_dates = set(pd.to_datetime(new["as_of_date"]).unique()
           )
      298      ref_dates = set(pd.to_datetime(ref["as_of_date"]).unique()
           )
      299      if new_dates != ref_dates:
      300          return CheckResult(
      296 -            "QC7", f"drift vs store [{feature_name}]", False, 
      301 +            "QC7", name, False,                               
      302              f"as_of_date differs: rebuilt {sorted(new_dates)} 
           vs stored {sorted(ref_dates)}", (feature_name,),
      303          )
      304      missing, extra = _ids(ref.index.difference(new.index)), _i
           ds(new.index.difference(ref.index))
     ...
      312          f"(stored mean {b.mean():.6g}, rebuilt mean {a.mean():
           .6g}); missing={len(missing)}, extra={len(extra)}"
      313      )
      314      return CheckResult(
      310 -        "QC7", f"drift vs store [{feature_name}]", passed, det
          -ail,                                                          
      315 +        "QC7", name, passed, detail,                          
      316          () if passed else (feature_name,), _ids(list(changed))
            + missing + extra,
      317      )
      318  
     ...
      338      from features.transforms import day_window, month_window
      339  
      340      pop = customers.loc[customers["signup_date"] < as_of].set_
           index("customer_id")
      336 -    by_id = features.set_index("customer_id")                 
      341      problems, bad_features = [], []
      342  
      343      start, end = day_window(as_of, schema.get_spec("txn_count_
           90d").fixed["window_days"])
      344      ts = transactions["txn_timestamp"]
      345      db_rows = int((transactions["customer_id"].isin(pop.index)
            & (ts >= start) & (ts < end)).sum())
      342 -    feature_total = int(by_id["txn_count_90d"].sum())         
      346 +    feature_total = int(features["txn_count_90d"].sum())      
      347      if feature_total != db_rows:
      348          bad_features.append("txn_count_90d")
     … 20 more lines (ctrl+o to expand)

● Now the feature-store tests. They cover the API (including more than two
  versions), the append-only invariants enforced in the database, and the
  training script end to end in a temp store.

● Write(tests/test_feature_store.py)
  ⎿  Added 298 lines, removed 11 lines
        1  """
        2 -For each feature your confirmed feature_spec.md designates as 
          -versioned, register its                                       
        3 -versions (using that feature's own spec'd values is fine), con
          -firm each is independently                                    
        4 -retrievable by version number, confirm is_latest correctly ide
          -ntifies the highest version                                   
        5 -and not an earlier one, and confirm list_versions returns ever
          -y version with distinct                                       
        6 -parquet_path values that actually exist on disk.              
        2 +For each feature feature_spec.md designates as versioned, regi
          +ster its versions (with that                                  
        3 +feature's own spec'd values), confirm each is independently re
          +trievable by version number,                                  
        4 +confirm is_latest identifies the highest version and not an ea
          +rlier one, and confirm                                        
        5 +list_versions returns every version with distinct parquet_path
          + values that exist on disk.                                   
        6 +                                                              
        7 +Also: the store's append-only guarantees (no overwrite, no edi
          +t, no delete, one as_of), and                                 
        8 +the training script end to end — registration, idempotent reru
          +n, QC7, the cosmetic-bump                                     
        9 +rule, and the spec's expected version-comparison figures. Ever
          +y store here is a tmp_path.                                   
       10  """
       11  
       12 +import dataclasses                                            
       13 +import json                                                   
       14 +import sqlite3                                                
       15 +                                                              
       16 +import pandas as pd                                           
       17  import pytest
       18  
       19 +from feature_store import store                               
       20 +from features import schema, transforms                       
       21 +from features.build import build_features                     
       22 +from scripts import build_training_features, compare_feature_v
          +ersions, generate_lineage                                     
       23 +from tests.helpers import AS_OF, registry_rows, store_files, v
          +ersion_params                                                 
       24  
       12 -def test_register_and_retrieve_all_versions_of_a_versioned_fea
          -ture():                                                       
       13 -    pytest.skip("TODO: implement against feature_store/store.p
          -y")                                                           
       25 +VERSIONED = [f for f in schema.FEATURE_SCHEMA if f.is_versione
          +d]                                                            
       26  
       27  
       16 -def test_is_latest_points_to_highest_version_not_an_earlier_on
          -e():                                                          
       17 -    pytest.skip("TODO: implement against feature_store/store.p
          -y")                                                           
       28 +def snapshot(full: pd.DataFrame, name: str) -> pd.DataFrame:  
       29 +    return full[["customer_id", name, "as_of_date"]].reset_ind
          +ex(drop=True)                                                 
       30  
       31  
       20 -def test_list_versions_returns_distinct_parquet_paths_that_exi
          -st():                                                         
       21 -    pytest.skip("TODO: implement against feature_store/store.p
          -y")                                                           
       32 +def register(root, name, values, version_label="v", params=Non
          +e):                                                           
       33 +    return store.register_version(                            
       34 +        name, values, description=f"{name} {version_label}", s
          +ource_columns="test:col",                                     
       35 +        transform_summary="test", business_rationale="test", p
          +arameters=params or {}, store_dir=root,                       
       36 +    )                                                         
       37 +                                                              
       38 +                                                              
       39 +def toy(values, as_of=AS_OF, name="toy_feature"):             
       40 +    return pd.DataFrame({"customer_id": range(1, len(values) +
          + 1), name: values, "as_of_date": pd.Timestamp(as_of)})        
       41 +                                                              
       42 +                                                              
       43 +@pytest.fixture(scope="module")                               
       44 +def builds():                                                 
       45 +    n = max(len(f.versions) for f in schema.FEATURE_SCHEMA)   
       46 +    return {k: build_features(AS_OF, feature_params=version_pa
          +rams(k)) for k in range(1, n + 1)}                            
       47 +                                                              
       48 +                                                              
       49 +# The versioning floor ---------------------------------------
          +-----------------------------                                 
       50 +def test_spec_versions_at_least_two_features_each_with_at_leas
          +t_two_versions():                                             
       51 +    assert len(VERSIONED) >= 2                                
       52 +    assert {f.name for f in VERSIONED} == {                   
       53 +        "avg_spend_per_active_month_6m", "spend_trend_3m_vs_pr
          +ior_3m", "active_month_share"}                                
       54 +    for f in VERSIONED:                                       
       55 +        numbers = [v.version for v in f.versions]             
       56 +        assert numbers == list(range(1, len(numbers) + 1))    
       57 +        tunable = [{k: x for k, x in v.parameters.items() if k
          + != "definition"} for v in f.versions]                        
       58 +        assert len({json.dumps(t, sort_keys=True) for t in tun
          +able}) == len(tunable), f.name                                
       59 +                                                              
       60 +                                                              
       61 +# Spec'd versions, registered and retrieved ------------------
          +-----------------------------                                 
       62 +@pytest.mark.parametrize("spec", VERSIONED, ids=lambda f: f.na
          +me)                                                           
       63 +def test_register_and_retrieve_all_versions_of_a_versioned_fea
          +ture(tmp_path, builds, spec):                                 
       64 +    expected = {}                                             
       65 +    for k, version in enumerate(spec.versions, start=1):      
       66 +        values = snapshot(builds[k], spec.name)               
       67 +        assert register(tmp_path, spec.name, values, params=ve
          +rsion.parameters) == version.version                          
       68 +        expected[version.version] = values                    
       69 +    for number, values in expected.items():                   
       70 +        got = store.get_version(spec.name, number, store_dir=t
          +mp_path)                                                      
       71 +        pd.testing.assert_frame_equal(got, values.sort_values(
          +"customer_id", ignore_index=True), check_exact=True)          
       72 +    # the versions really differ from each other (not a cosmet
          +ic bump)                                                      
       73 +    firsts = [expected[n][spec.name] for n in sorted(expected)
          +]                                                             
       74 +    assert all(not a.equals(b) for a, b in zip(firsts, firsts[
          +1:]))                                                         
       75 +                                                              
       76 +                                                              
       77 +@pytest.mark.parametrize("spec", VERSIONED, ids=lambda f: f.na
          +me)                                                           
       78 +def test_is_latest_points_to_highest_version_not_an_earlier_on
          +e(tmp_path, builds, spec):                                    
       79 +    for k, version in enumerate(spec.versions, start=1):      
       80 +        register(tmp_path, spec.name, snapshot(builds[k], spec
          +.name), params=version.parameters)                            
       81 +        rows = store.list_versions(spec.name, store_dir=tmp_pa
          +th)                                                           
       82 +        assert rows.loc[rows["is_latest"] == 1, "version"].tol
          +ist() == [version.version]                                    
       83 +        assert store.latest_version(spec.name, store_dir=tmp_p
          +ath) == version.version                                       
       84 +    latest = store.get_latest(spec.name, store_dir=tmp_path)  
       85 +    pd.testing.assert_frame_equal(latest, store.get_version(sp
          +ec.name, spec.latest.version, store_dir=tmp_path))            
       86 +    assert not latest.equals(store.get_version(spec.name, 1, s
          +tore_dir=tmp_path))                                           
       87 +                                                              
       88 +                                                              
       89 +@pytest.mark.parametrize("spec", VERSIONED, ids=lambda f: f.na
          +me)                                                           
       90 +def test_list_versions_returns_distinct_parquet_paths_that_exi
          +st(tmp_path, builds, spec):                                   
       91 +    for k, version in enumerate(spec.versions, start=1):      
       92 +        register(tmp_path, spec.name, snapshot(builds[k], spec
          +.name), params=version.parameters)                            
       93 +    rows = store.list_versions(spec.name, store_dir=tmp_path) 
       94 +    assert rows["version"].tolist() == sorted((v.version for v
          + in spec.versions), reverse=True)                             
       95 +    assert rows["parquet_path"].is_unique                     
       96 +    for row in rows.itertuples():                             
       97 +        assert row.parquet_path == f"{tmp_path.name}/values/{s
          +pec.name}__v{row.version}.parquet"                            
       98 +        assert (tmp_path.parent / row.parquet_path).is_file() 
       99 +        assert json.loads(row.parameters) == spec.version(row.
          +version).parameters                                           
      100 +                                                              
      101 +                                                              
      102 +def test_more_than_two_versions_and_latest_is_the_highest(tmp_
          +path):                                                        
      103 +    for n in range(1, 5):                                     
      104 +        assert register(tmp_path, "toy_feature", toy([float(n)
          +] * 3)) == n                                                  
      105 +    assert store.latest_version("toy_feature", store_dir=tmp_p
          +ath) == 4                                                     
      106 +    assert store.get_latest("toy_feature", store_dir=tmp_path)
          +["toy_feature"].tolist() == [4.0] * 3                         
      107 +    for n in range(1, 5):                                     
      108 +        assert store.get_version("toy_feature", n, store_dir=t
          +mp_path)["toy_feature"].tolist() == [float(n)] * 3            
      109 +    assert store.list_versions("toy_feature", store_dir=tmp_pa
          +th)["version"].tolist() == [4, 3, 2, 1]                       
      110 +                                                              
      111 +                                                              
      112 +def test_versions_are_numbered_per_feature(tmp_path):         
      113 +    register(tmp_path, "toy_a", toy([1.0], name="toy_a"))     
      114 +    register(tmp_path, "toy_a", toy([2.0], name="toy_a"))     
      115 +    assert register(tmp_path, "toy_b", toy([1.0], name="toy_b"
          +)) == 1                                                       
      116 +    assert store.list_features(store_dir=tmp_path) == ["toy_a"
          +, "toy_b"]                                                    
      117 +                                                              
      118 +                                                              
      119 +# Append-only guarantees -------------------------------------
          +-----------------------------                                 
      120 +def test_an_existing_parquet_file_is_never_overwritten(tmp_pat
          +h):                                                           
      121 +    register(tmp_path, "toy_feature", toy([1.0, 2.0]))        
      122 +    squatter = tmp_path / "values" / "toy_feature__v2.parquet"
      123 +    squatter.write_bytes(b"not mine")                         
      124 +    with pytest.raises(store.StoreError, match="refusing to ov
          +erwrite"):                                                    
      125 +        register(tmp_path, "toy_feature", toy([3.0, 4.0]))    
      126 +    assert squatter.read_bytes() == b"not mine"               
      127 +    assert store.list_versions("toy_feature", store_dir=tmp_pa
          +th)["version"].tolist() == [1]                                
      128 +    assert store.latest_version("toy_feature", store_dir=tmp_p
          +ath) == 1                                                     
      129 +                                                              
      130 +                                                              
      131 +def test_registry_rows_cannot_be_edited_deleted_or_rolled_back
          +(tmp_path):                                                   
      132 +    register(tmp_path, "toy_feature", toy([1.0]))             
      133 +    register(tmp_path, "toy_feature", toy([2.0]))             
      134 +    with sqlite3.connect(tmp_path / "registry.db") as con:    
      135 +        for sql in (                                          
      136 +            "UPDATE feature_registry SET parameters = '{\"x\":
          + 1}' WHERE version = 1",                                      
      137 +            "UPDATE feature_registry SET parquet_path = 'elsew
          +here' WHERE version = 2",                                     
      138 +            "DELETE FROM feature_registry WHERE version = 1", 
      139 +            "UPDATE feature_registry SET is_latest = 1 WHERE v
          +ersion = 1",                                                  
      140 +            "INSERT INTO feature_registry VALUES ('toy_feature
          +', 1, 'x', 'x', 'x', 'x', 'x', '{}', 'p', 0)",                
      141 +        ):                                                    
      142 +            with pytest.raises(sqlite3.IntegrityError):       
      143 +                con.execute(sql)                              
      144 +    assert store.latest_version("toy_feature", store_dir=tmp_p
          +ath) == 2                                                     
      145 +                                                              
      146 +                                                              
      147 +def test_one_as_of_per_store(tmp_path):                       
      148 +    register(tmp_path, "toy_feature", toy([1.0]))             
      149 +    with pytest.raises(store.StoreError, match="one as_of per 
          +store"):                                                      
      150 +        register(tmp_path, "toy_feature", toy([2.0], as_of="20
          +26-04-01"))                                                   
      151 +    assert store.list_versions("toy_feature", store_dir=tmp_pa
          +th)["version"].tolist() == [1]                                
      152 +    assert store_files(tmp_path) == ["toy_feature__v1.parquet"
          +]                                                             
      153 +    assert store.store_as_of(store_dir=tmp_path) == pd.Timesta
          +mp(AS_OF)                                                     
      154 +                                                              
      155 +                                                              
      156 +@pytest.mark.parametrize("bad, message", [                    
      157 +    (toy([1.0, None]), "null values"),                        
      158 +    (pd.concat([toy([1.0]), toy([2.0])]), "duplicated customer
          +_id"),                                                        
      159 +    (toy([1.0]).drop(columns="as_of_date"), "exactly the colum
          +ns"),                                                         
      160 +    (toy([1.0, 2.0]).assign(as_of_date=[pd.Timestamp("2026-05-
          +02"), pd.Timestamp("2026-05-03")]), "one as_of_date"),        
      161 +])                                                            
      162 +def test_invalid_snapshots_are_rejected_before_anything_is_wri
          +tten(tmp_path, bad, message):                                 
      163 +    with pytest.raises(store.StoreError, match=message):      
      164 +        register(tmp_path, "toy_feature", bad)                
      165 +    assert registry_rows(tmp_path) == [] and store_files(tmp_p
          +ath) == []                                                    
      166 +                                                              
      167 +                                                              
      168 +def test_feature_names_cannot_escape_the_values_directory(tmp_
          +path):                                                        
      169 +    with pytest.raises(store.StoreError, match="must match"): 
      170 +        register(tmp_path, "../evil", toy([1.0], name="../evil
          +"))                                                           
      171 +                                                              
      172 +                                                              
      173 +def test_reads_on_an_empty_store_create_nothing(tmp_path):    
      174 +    root = tmp_path / "empty"                                 
      175 +    assert store.list_versions("toy_feature", store_dir=root).
          +empty                                                         
      176 +    with pytest.raises(KeyError):                             
      177 +        store.get_latest("toy_feature", store_dir=root)       
      178 +    with pytest.raises(KeyError):                             
      179 +        store.get_version("toy_feature", 1, store_dir=root)   
      180 +    assert store.store_as_of(store_dir=root) is None          
      181 +    assert not root.exists()                                  
      182 +                                                              
      183 +                                                              
      184 +def test_default_store_root_is_resolved_from_the_module_not_th
          +e_cwd(tmp_path, monkeypatch):                                 
      185 +    monkeypatch.chdir(tmp_path)                               
      186 +    assert store.store_root() == store.DEFAULT_STORE_DIR      
      187 +    assert store.DEFAULT_STORE_DIR.name == "feature_store" and
          + (store.DEFAULT_STORE_DIR / "store.py").is_file()             
      188 +                                                              
      189 +                                                              
      190 +# The training script, end to end ----------------------------
          +-----------------------------                                 
      191 +def test_training_script_registers_every_feature_and_every_ver
          +sion(built_store):                                            
      192 +    for spec in schema.FEATURE_SCHEMA:                        
      193 +        rows = store.list_versions(spec.name, store_dir=built_
          +store)                                                        
      194 +        assert rows["version"].tolist() == [v.version for v in
          + reversed(spec.versions)], spec.name                          
      195 +        assert [json.loads(p) for p in rows["parameters"]] == 
          +[v.parameters for v in reversed(spec.versions)]               
      196 +        latest = store.get_latest(spec.name, store_dir=built_s
          +tore)                                                         
      197 +        assert len(latest) == 1399 and (latest["as_of_date"] =
          += pd.Timestamp(AS_OF)).all()                                  
      198 +        assert not latest[spec.name].isna().any()             
      199 +        assert rows.loc[rows["is_latest"] == 1, "version"].tol
          +ist() == [spec.latest.version]                                
      200 +        first = rows.loc[rows["version"] == 1].iloc[0]        
      201 +        assert first["transform_summary"] == spec.transform_su
          +mmary(spec.versions[0].parameters)                            
      202 +        assert spec.versions[0].change_reason in first["busine
          +ss_rationale"]                                                
      203 +                                                              
      204 +                                                              
      205 +def test_training_store_matches_the_shared_build(built_store, 
          +full_latest):                                                 
      206 +    for name in schema.FEATURE_NAMES:                         
      207 +        pd.testing.assert_frame_equal(store.get_latest(name, s
          +tore_dir=built_store), snapshot(full_latest, name), check_exac
          +t=True)                                                       
      208 +                                                              
      209 +                                                              
      210 +def test_rerun_is_unchanged_skipped_and_writes_nothing(tmp_pat
          +h, capsys):                                                   
      211 +    root = tmp_path / "feature_store"                         
      212 +    args = ["--store-dir", str(root), "--lineage-out", str(tmp
          +_path / "lineage.md")]                                        
      213 +    assert build_training_features.main(args) == 0            
      214 +    before, files = registry_rows(root), store_files(root)    
      215 +    capsys.readouterr()                                       
      216 +    assert build_training_features.main(args) == 0            
      217 +    out = capsys.readouterr().out                             
      218 +    assert out.count("unchanged — skipped") == sum(len(f.versi
          +ons) for f in schema.FEATURE_SCHEMA)                          
      219 +    assert registry_rows(root) == before and store_files(root)
          + == files                                                     
      220 +                                                              
      221 +                                                              
      222 +def test_qc7_stops_a_silent_definition_change_and_nothing_is_w
          +ritten(tmp_path, monkeypatch, capsys):                        
      223 +    root = tmp_path / "feature_store"                         
      224 +    args = ["--store-dir", str(root), "--lineage-out", str(tmp
          +_path / "lineage.md")]                                        
      225 +    assert build_training_features.main(args) == 0            
      226 +    before, files = registry_rows(root), store_files(root)    
      227 +    real = transforms.txn_count_90d                           
      228 +                                                              
      229 +    def changed(population, transactions, as_of, *, window_day
          +s):  # e.g. someone "fixes" the window                        
      230 +        return real(population, transactions, as_of, window_da
          +ys=window_days + 1)                                           
      231 +                                                              
      232 +    monkeypatch.setattr(transforms, "txn_count_90d", changed) 
      233 +    assert build_training_features.main(args) == 1            
      234 +    err = capsys.readouterr().err                             
      235 +    assert "QC7" in err and "txn_count_90d" in err and "Nothin
          +g was registered" in err                                      
      236 +    assert registry_rows(root) == before and store_files(root)
          + == files                                                     
      237 +                                                              
      238 +                                                              
      239 +def test_a_version_whose_values_do_not_change_is_refused(tmp_p
          +ath, monkeypatch, capsys):                                    
      240 +    """A cosmetic bump: a new monthly_cap of 2,000 is above ev
          +ery customer-month in the                                     
      241 +    window, so v2 would equal v1 for every customer."""       
      242 +    spec = schema.get_spec("avg_spend_per_active_month_6m")   
      243 +    cosmetic = dataclasses.replace(spec, versions=(           
      244 +        spec.versions[0], dataclasses.replace(spec.versions[1]
          +, parameters={"monthly_cap": 2000.0, "definition": 1})))      
      245 +    patched = [cosmetic if f.name == spec.name else f for f in
          + schema.FEATURE_SCHEMA]                                       
      246 +    monkeypatch.setattr(schema, "FEATURE_SCHEMA", patched)    
      247 +    monkeypatch.setitem(schema._BY_NAME, spec.name, cosmetic) 
      248 +    root = tmp_path / "feature_store"                         
      249 +    assert build_training_features.main(["--store-dir", str(ro
          +ot), "--lineage-out", str(tmp_path / "l.md")]) == 1           
      250 +    assert "not a real version bump" in capsys.readouterr().er
          +r                                                             
      251 +    assert registry_rows(root) == [] and store_files(root) == 
          +[]                                                            
      252 +                                                              
      253 +                                                              
      254 +def test_training_script_refuses_a_different_as_of(built_store
          +, tmp_path, capsys):                                          
      255 +    before = registry_rows(built_store)                       
      256 +    args = ["--store-dir", str(built_store), "--as-of", "2026-
          +04-01", "--lineage-out", str(tmp_path / "l.md")]              
      257 +    assert build_training_features.main(args) == 1            
      258 +    assert "one as_of per store" in capsys.readouterr().err   
      259 +    assert registry_rows(built_store) == before               
      260 +                                                              
      261 +                                                              
      262 +def test_training_script_writes_nothing_when_a_build_check_fai
          +ls(tmp_path, capsys):                                         
      263 +    root = tmp_path / "feature_store"                         
      264 +    assert build_training_features.main(["--store-dir", str(ro
          +ot), "--as-of", "2026-08-01",                                 
      265 +                                         "--lineage-out", str(
          +tmp_path / "l.md")]) == 1                                     
      266 +    assert "QC6" in capsys.readouterr().err                   
      267 +    assert registry_rows(root) == [] and store_files(root) == 
          +[]                                                            
      268 +                                                              
      269 +                                                              
      270 +# Version comparison and lineage, from the store -------------
          +-----------------------------                                 
      271 +@pytest.fixture(scope="module")                               
      272 +def comparisons(built_store):                                 
      273 +    return {f.name: compare_feature_versions.compare_feature(f
          +, built_store)["pairs"][0] for f in VERSIONED}                
      274 +                                                              
      275 +                                                              
      276 +def test_version_comparison_matches_the_spec_expectations(comp
          +arisons):                                                     
      277 +    f5, f6, f7 = (comparisons[n] for n in (                   
      278 +        "avg_spend_per_active_month_6m", "spend_trend_3m_vs_pr
          +ior_3m", "active_month_share"))                               
      279 +    assert (f5["changed"], round(f5["rank_correlation"], 3)) =
          += (157, 0.997)                                                
      280 +    assert (f5["threshold"]["entering"], f5["threshold"]["leav
          +ing"]) == (25, 25)                                            
      281 +    assert len(f5["threshold"]["old"]["members"]) == len(f5["t
          +hreshold"]["new"]["members"]) == 140                          
      282 +    assert (f6["changed"], round(f6["rank_correlation"], 3)) =
          += (162, 0.999)                                                
      283 +    assert f6["threshold"]["tied_new"] == 162 and f6["threshol
          +d"]["n_below_old"] == 458                                     
      284 +    assert f6["threshold"]["below_unchanged"] is True         
      285 +    assert (f7["changed"], round(f7["rank_correlation"], 3)) =
          += (890, 0.849)                                                
      286 +    assert (f7["threshold"]["entering"], f7["threshold"]["leav
          +ing"]) == (54, 46)                                            
      287 +    assert (len(f7["threshold"]["old"]["members"]), len(f7["th
          +reshold"]["new"]["members"])) == (168, 176)                   
      288 +                                                              
      289 +                                                              
      290 +def test_version_comparison_reads_through_get_version(built_st
          +ore, monkeypatch):                                            
      291 +    calls = []                                                
      292 +    real = store.get_version                                  
      293 +    monkeypatch.setattr(store, "get_version", lambda *a, **k: 
          +calls.append(a[:2]) or real(*a, **k))                         
      294 +    spec = schema.get_spec("active_month_share")              
      295 +    compare_feature_versions.compare_feature(spec, built_store
          +)                                                             
      296 +    assert calls == [(spec.name, v.version) for v in spec.vers
          +ions]                                                         
      297 +                                                              
      298 +                                                              
      299 +def test_lineage_has_a_separate_entry_for_every_stored_version
          +(built_store):                                                
      300 +    text = generate_lineage.render_lineage(store_dir=built_sto
          +re)                                                           
      301 +    for spec in schema.FEATURE_SCHEMA:                        
      302 +        for row in store.list_versions(spec.name, store_dir=bu
          +ilt_store).itertuples():                                      
      303 +            assert f"### `{spec.name}` v{row.version} " in tex
          +t                                                             
      304 +            assert f"`{row.parquet_path}` — 1,399 rows" in tex
          +t                                                             
      305 +    assert "2025-11 to 2026-04 (2025-11-01 ≤ `txn_month` < 202
          +6-05-01)" in text                                             
      306 +    assert "2026-02-01 00:00:00 ≤ `txn_timestamp` < 2026-05-02
          + 00:00:00" in text                                            
      307 +    assert "197.098" in text                                  
      308 +    assert "Not in the current schema" not in text            

  Ran 1 shell command (ctrl+o to expand)

● QC8 flagged the injected window change before QC7 could run, which isn't what
  this test is meant to exercise. I'll switch to an off-by-one in buyers'
  recency instead, since that can't be caught by any build-time check.

● Update(tests/test_feature_store.py)
  ⎿  Added 9 lines, removed 5 lines
      224      args = ["--store-dir", str(root), "--lineage-out", str(tmp
           _path / "lineage.md")]
      225      assert build_training_features.main(args) == 0
      226      before, files = registry_rows(root), store_files(root)
      227 -    real = transforms.txn_count_90d                           
      227 +    real = transforms.days_since_last_txn                     
      228  
      229 -    def changed(population, transactions, as_of, *, window_day
          -s):  # e.g. someone "fixes" the window                        
      230 -        return real(population, transactions, as_of, window_da
          -ys=window_days + 1)                                           
      229 +    def changed(population, transactions, as_of, *, cap_days):
      230 +        """Rounding up instead of down: within QC4's range, ze
          +ro-activity defaults intact,                                  
      231 +        invisible to QC1-QC6 and QC8. Only the stored copy can
          + show it."""                                                  
      232 +        out = real(population, transactions, as_of, cap_days=c
          +ap_days)                                                      
      233 +        return out.where(out >= cap_days, out + 1)            
      234  
      232 -    monkeypatch.setattr(transforms, "txn_count_90d", changed) 
      235 +    monkeypatch.setattr(transforms, "days_since_last_txn", cha
          +nged)                                                         
      236      assert build_training_features.main(args) == 1
      237      err = capsys.readouterr().err
      235 -    assert "QC7" in err and "txn_count_90d" in err and "Nothin
          -g was registered" in err                                      
      238 +    assert "FAIL QC7 drift vs store [days_since_last_txn]" in 
          +err and "Nothing was registered" in err                       
      239 +    assert "FAIL QC7 drift vs store [tenure_days]" not in err 
      240      assert registry_rows(root) == before and store_files(root)
            == files
      241  
      242  

  Ran 1 shell command (ctrl+o to expand)

● Three of the failures come from my test helper parsing mixed timestamp
  formats, which needs format="ISO8601". The fourth one I need to see:

● Bash(sed -i 's/df\["txn_timestamp"\] =
      pd.to_datetime(df\["txn_timestamp"\]).astype/df["txn_timestamp"] =
      pd.to_datetime(df["txn_timestamp"], format="ISO8601").astyp…)
  ⎿  Updated tests/test_transforms.py (+1 -1)
      27  def txns(*rows) -> pd.DataFrame:
      28      """rows: (customer_id, txn_timestamp)."""
      29      df = pd.DataFrame(rows, columns=["customer_id", "txn_timest
          amp"])
      30 -    df["txn_timestamp"] = pd.to_datetime(df["txn_timestamp"]).a
         -stype("datetime64[ns]")                                        
      30 +    df["txn_timestamp"] = pd.to_datetime(df["txn_timestamp"], f
         +ormat="ISO8601").astype("datetime64[ns]")                      
      31      df["amount"] = 10.0
      32      df["is_returned"] = 0
      33      return df

● Here the code is right and the test was too strict. F7's subgroup flag is also
  read through the store, via get_latest and then get_version. I'm tightening
  the assertion to say exactly that.

● Update(tests/test_feature_store.py)
  ⎿  Added 2 lines, removed 1 line
      297      monkeypatch.setattr(store, "get_version", lambda *a, **k: 
           calls.append(a[:2]) or real(*a, **k))
      298      spec = schema.get_spec("active_month_share")
      299      compare_feature_versions.compare_feature(spec, built_store
           )
      300 -    assert calls == [(spec.name, v.version) for v in spec.vers
          -ions]                                                         
      300 +    # every version of the feature, plus the has_ever_purchase
          +d subgroup flag, all from the store                           
      301 +    assert calls == [(spec.name, v.version) for v in spec.vers
          +ions] + [("has_ever_purchased", 1)]                           
      302  
      303  
      304  def test_lineage_has_a_separate_entry_for_every_stored_version
           (built_store):

  Ran 1 shell command (ctrl+o to expand)

● All 143 tests pass. Now I'll run the parity mutation check in a scratch repo
  copy, injecting three subtle scoring-only splits (rounded vs. floored recency,
  a 90-day count leaking the current as_of day, and F5's cap miscomputed as a
  batch percentile) to confirm only the parity test catches them.

  Ran 1 shell command (ctrl+o to expand)

● All three splits were caught and the real build.py came out byte-identical,
  confirming leak M2 is only caught by the customer-697 case and the shuffled
  all-customers test. Now I'll do a coverage-of-intent check, comparing spec
  features/rules against the suite, and first need to pull a few real-data facts
  before writing the gap tests.

  Ran 1 shell command (ctrl+o to expand)

● The data matches the spec for F6, F5, F2, and F4, so now I'll add the tests
  the coverage check flagged as missing.

● Bash(cat >> tests/test_transforms.py <<'EOF'…)
  ⎿  Updated tests/test_feature_store.py (+18 -0)
      128      assert store.latest_version("toy_feature", store_dir=tmp_p
           ath) == 1
      129  
      130  
      131 +def test_a_failed_write_rolls_back_and_removes_its_file(tmp_pa
          +th, monkeypatch):                                             
      132 +    """is_latest is flipped in the same transaction as the ins
          +ert: a failure part-way leaves                                
      133 +    the previous latest in place and no orphan file."""       
      134 +    register(tmp_path, "toy_feature", toy([1.0]))             
      135 +                                                              
      136 +    def failing_write(self, handle, **kwargs):                
      137 +        handle.write(b"partial")                              
      138 +        raise OSError("disk full")                            
      139 +                                                              
      140 +    monkeypatch.setattr(pd.DataFrame, "to_parquet", failing_wr
          +ite)                                                          
      141 +    with pytest.raises(OSError, match="disk full"):           
      142 +        register(tmp_path, "toy_feature", toy([2.0]))         
      143 +    monkeypatch.undo()                                        
      144 +    assert store_files(tmp_path) == ["toy_feature__v1.parquet"
          +]                                                             
      145 +    assert store.latest_version("toy_feature", store_dir=tmp_p
          +ath) == 1                                                     
      146 +    assert register(tmp_path, "toy_feature", toy([2.0])) == 2 
      147 +                                                              
      148 +                                                              
      149  def test_registry_rows_cannot_be_edited_deleted_or_rolled_back
           (tmp_path):
      150      register(tmp_path, "toy_feature", toy([1.0]))
      151      register(tmp_path, "toy_feature", toy([2.0]))
  ⎿  Updated tests/test_parity.py (+16 -0)
      126      assert list(full_latest.columns) == ["customer_id", *schem
           a.FEATURE_NAMES, "as_of_date"]
      127      assert "churned" not in full_latest.columns
      128      assert (full_latest["as_of_date"] == pd.Timestamp(AS_OF)).
           all()
      129 +                                                              
      130 +                                                              
      131 +def test_scoring_script_exits_non_zero_and_prints_no_features_
          +on_a_failed_check(capsys):                                    
      132 +    assert score_customer.main(["--customer-id", "3", "--as-of
          +", "2026-08-01"]) == 1                                        
      133 +    captured = capsys.readouterr()                            
      134 +    assert captured.out == "" and "QC6" in captured.err       
      135 +                                                              
      136 +                                                              
      137 +@pytest.mark.parametrize("feature_params, message", [         
      138 +    ({"active_month_share": {"lookback_months": 6, "definition
          +": 2}}, "implements definition 1"),                           
      139 +    ({"active_month_share": {"lookback": 6, "definition": 1}},
          + "exactly the keys"),                                         
      140 +    ({"no_such_feature": {"definition": 1}}, "unknown feature"
          +),                                                            
      141 +])                                                            
      142 +def test_feature_params_are_validated_not_silently_defaulted(f
          +eature_params, message):                                      
      143 +    with pytest.raises((ValueError, KeyError), match=message):
      144 +        build_features(AS_OF, feature_params=feature_params)  
  ⎿  Updated tests/test_quality_checks.py (+13 -0)
      273      set_value(full_latest.copy(), 27, "spend_trend_3m_vs_prior
           _3m", 0.0).to_parquet(path)
      274      assert qc.main(["--features", str(path)]) == 1
      275      assert "FAIL QC3" in capsys.readouterr().out
      276 +                                                              
      277 +                                                              
      278 +def test_spec_expected_feature_figures_at_2026_05_02(full_late
          +st):                                                          
      279 +    """Figures feature_spec.md quotes for its features at 2026
          +-05-02 (coverage-of-intent check)."""                         
      280 +    f = full_latest                                           
      281 +    buyers = f["has_ever_purchased"] == 1                     
      282 +    assert f.loc[buyers, "days_since_last_txn"].max() == 289  
          +          # F2: only never-buyers hold 365                    
      283 +    assert int((f["txn_count_90d"] > 0).sum()) == 1049        
          +          # F4                                                
      284 +    assert int((f["tenure_days"] < 182).sum()) == 302         
          +          # F1: under six months                              
      285 +    assert int((f["avg_spend_per_active_month_6m"] == 0.0).sum
          +()) == 250  # F5: 232 never + 18 stopped earlier              
      286 +    assert int((f["spend_trend_3m_vs_prior_3m"] == 0.0).sum())
          + == 108   # F6: spending stopped                              
      287 +    assert int((f["spend_trend_3m_vs_prior_3m"] == 1.0).sum())
          + == 312 + 250  # F6: recent-only + neither                    
      288 +    assert f["avg_spend_per_active_month_6m"].max() == pytest.
          +approx(197.10)                                                
  ⎿  Updated tests/test_transforms.py (+8 -0)
      291          assert out.name == spec.name
      292          assert not out.isna().any(), spec.name
      293          assert np.isfinite(out.to_numpy(dtype="float64")).all(
           ), spec.name
      294 +                                                              
      295 +                                                              
      296 +def test_avg_spend_is_gross_of_returns():                     
      297 +    """feature_spec.md F5: the parquet has no returned amounts
          +, so return_count is ignored."""                              
      298 +    m = months((1, "2026-03", 60.0))                          
      299 +    m["return_count"] = 1                                     
      300 +    out = T.avg_spend_per_active_month_6m(POP, m, AS_OF, windo
          +w_months=6, monthly_cap=None)                                 
      301 +    assert v(out, 1) == pytest.approx(60.0)                   
  ⎿  (timeout 10m)

● 150 tests pass. Next is Step 3, the self-review: rereading the whole diff as a
  first-time reviewer, starting with the core modules.

  Read 1 file (ctrl+o to expand)

● I found a bug: if feature registration fails partway through, the script
  wrongly reports "Nothing was registered by this run." I'll also check whether
  F5's mean over capped months could round just above the cap, which would
  falsely trigger QC4.

  Ran 1 shell command (ctrl+o to expand)

● No problem there: in a 6-month window, the mean of up to six capped months is
  exactly 197.1. I'll record that as checked and leave the code alone. Fixing
  the partial-registration message:
  Read 1 file (ctrl+o to expand)

● Bash(conda run --no-capture-output -n churn-pipeline python - <<'EOF'
      p = "scripts/build_training_features.py"…)
  ⎿  Updated scripts/build_training_features.py (+18 -5)
      112      return f"{spec.rationale} Version {version.version}: {vers
           ion.change_reason}"
      113  
      114  
      115 -def register(plan: list[PlannedVersion], store_dir) -> None:  
      115 +def register(plan: list[PlannedVersion], store_dir, registered
          +: list[str]) -> None:                                         
      116 +    """Register the plan's new versions, appending each one to
          + `registered` as it commits."""                               
      117      for item in plan:
      118          spec, version = item.spec, item.schema_version
      119          label = f"{spec.name} schema v{version.version}"
      120          if item.action == "skip":
      121              print(f"{label}: unchanged — skipped (stored as v{
           item.stored_version})")
      122              continue
      122 -        registered = store.register_version(                  
      123 +        number = store.register_version(                      
      124              spec.name,
      125              item.values,
      126              description=spec.description,
     ...
      130              parameters=version.parameters,
      131              store_dir=store_dir,
      132          )
      132 -        note = "" if registered == version.version else " (sto
          -re numbering differs from schema.py)"                         
      133 -        print(f"{label}: registered as v{registered}{note}, pa
          -rameters {json.dumps(version.parameters)}")                   
      133 +        registered.append(f"{spec.name} v{number}")           
      134 +        note = "" if number == version.version else " (store n
          +umbering differs from schema.py)"                             
      135 +        print(f"{label}: registered as v{number}{note}, parame
          +ters {json.dumps(version.parameters)}")                       
      136  
      137  
      138  def main(argv: list[str] | None = None) -> int:
     ...
      154              )
      155          builds = build_all_versions(as_of)
      156          plan = plan_registrations(builds, args.store_dir)
      155 -        register(plan, args.store_dir)                        
      157      except (QualityCheckError, VersioningError, store.StoreErr
           or, ValueError) as exc:
      158          print(f"ERROR: {exc}\nNothing was registered by this r
           un.", file=sys.stderr)
      159          return 1
      160  
      161 +    # Each registration is its own transaction. If one fails p
          +art-way through the plan, say                                 
      162 +    # exactly what was committed; a rerun skips those (identic
          +al parameters, QC7) and                                       
      163 +    # registers the rest.                                     
      164 +    registered: list[str] = []                                
     … 11 more lines (ctrl+o to expand)
  ⎿  Updated tests/test_feature_store.py (+15 -0)
      329      assert "2026-02-01 00:00:00 ≤ `txn_timestamp` < 2026-05-02
            00:00:00" in text
      330      assert "197.098" in text
      331      assert "Not in the current schema" not in text
      332 +                                                              
      333 +                                                              
      334 +def test_a_registration_failure_part_way_reports_what_was_comm
          +itted(tmp_path, capsys):                                      
      335 +    root = tmp_path / "feature_store"                         
      336 +    (root / "values").mkdir(parents=True)                     
      337 +    (root / "values" / "txn_count_90d__v1.parquet").write_byte
          +s(b"stray file")  # 4th feature in the plan                   
      338 +    args = ["--store-dir", str(root), "--lineage-out", str(tmp
          +_path / "l.md")]                                              
      339 +    assert build_training_features.main(args) == 1            
      340 +    err = capsys.readouterr().err                             
      341 +    assert "Registered before the failure: tenure_days v1, day
          +s_since_last_txn v1, has_ever_purchased v1" in err            
      342 +    assert "Nothing was registered" not in err                
      343 +    (root / "values" / "txn_count_90d__v1.parquet").unlink()  
      344 +    assert build_training_features.main(args) == 0  # the reru
          +n skips the three and registers the rest                      
      345 +    assert len(registry_rows(root)) == sum(len(f.versions) for
          + f in schema.FEATURE_SCHEMA)                                  
      346 +    assert "tenure_days schema v1: unchanged — skipped" in cap
          +sys.readouterr().out                                          
  ⎿  (timeout 10m)

● The fix works, and a rerun recovers by skipping the three committed versions.
  Continuing the review with the store and build code:

  Read 2 files (ctrl+o to expand)

● I'll move the unnecessary lazy transforms import in QC to the top, and fix the
  lineage file so it regenerates after a partial registration failure. I'm also
  verifying no transform/check hard-codes a date and that the suite runs
  warning-free.

● Bash(echo "--- dates in features/ and feature_store/ code:"; grep -n
      "20[0-9][0-9]-[0-9][0-9]" features/*.py feature_store/*.py | grep -v
      "^\s*#" ; echo "--- now()/t…)
  ⎿  Updated features/quality_checks.py (+1 -4)
      32  import pandas as pd
      33  
      34  from features import schema
      35 +from features.transforms import day_window, month_window       
      36  
      37  DRIFT_TOLERANCE = 1e-9
      38  AMOUNT_TOLERANCE = 0.01
     ...
      226  # QC6 --------------------------------------------------------
           ----------------------------
      227  def coverage_months(as_of: pd.Timestamp, params: dict[str, dic
           t], only: str | None = None) -> list[pd.Timestamp]:
      228      """Month starts used by any month window at these paramete
           rs (or by feature `only`)."""
      228 -    from features.transforms import month_window              
      229 -                                                              
      229      months = set()
      230      for w in schema.month_windows_in_effect(params, only=only)
           :
      231          start, end = month_window(as_of, w.months, w.offset)
     ...
      334      params: dict[str, dict],
      335  ) -> CheckResult:
      336      """Totals behind txn_count_90d and active_month_share equa
           l raw source row counts."""
      338 -    from features.transforms import day_window, month_window  
      339 -                                                              
      337      pop = customers.loc[customers["signup_date"] < as_of].set_
           index("customer_id")
      338      problems, bad_features = [], []
      339  
  ⎿  Updated scripts/build_training_features.py (+2 -0)
      168          done = ", ".join(registered) if registered else "nothi
           ng"
      169          print(f"ERROR during registration: {exc}\nRegistered b
           efore the failure: {done}. "
      170                "Fix the cause and rerun: already-registered ver
           sions are skipped.", file=sys.stderr)
      171 +        if registered:  # keep the lineage in step with what t
          +he registry now holds                                         
      172 +            write_lineage(Path(args.lineage_out), store_dir=ar
          +gs.store_dir)                                                 
      173          return 1
      174  
      175      out = write_lineage(Path(args.lineage_out), store_dir=args
           .store_dir)
  ⎿  (timeout 10m)

● Dates only appear in lineage prose, with created_at as the sole clock
  reference, and all 151 tests pass cleanly. Next I'll build the actual feature
  store at the spec's default as_of, then run the version comparison and
  standalone QC check.

● Bash(conda run --no-capture-output -n churn-pipeline python
      scripts/build_training_features.py 2>&1 | tail -14; echo
      "exit=${PIPESTATUS[0]}"; conda run --no-capture-…)
  ⎿  Created outputs/artifacts/FEATURE_LINEAGE.md (+218 -0)
        1 +# FEATURE_LINEAGE.md                                          
        2 +                                                              
        3 +> Generated by `scripts/generate_lineage.py` from `features/sc
          +hema.py` and `feature_store/registry.db`. Do not edit by hand:
          + rerun `scripts/build_training_features.py` (or `scripts/gener
          +ate_lineage.py`) instead.                                     
        4 +                                                              
        5 +- **Store as_of**: 2026-05-02 — every snapshot in the store ha
          +s this `as_of_date`.                                          
        6 +- **Feature-set version**: 1.0. Every feature is `nullable = F
          +alse`.                                                        
        7 +- **Conventions**: as_of is a calendar date read as 00:00:00 n
          +aive local time. Day windows are `[as_of − N days, as_of)` on 
          +full timestamps. Month windows are N complete calendar months 
          +before as_of's month. Recency is whole days, rounded down. `ch
          +urned` is never an input.                                     
        8 +- **Shared code path**: every value below was produced by `fea
          +tures.build.build_features(as_of, feature_params=...)`, the sa
          +me function the scoring script calls.                         
        9 +                                                              
       10 +## Registered versions                                        
       11 +                                                              
       12 +| Spec | Feature | Version | Latest | Parameters | parquet_pat
          +h |                                                           
       13 +|---|---|---|---|---|---|                                     
       14 +| F1 | `tenure_days` | v1 | yes | `{"definition": 1}` | `featu
          +re_store/values/tenure_days__v1.parquet` |                    
       15 +| F2 | `days_since_last_txn` | v1 | yes | `{"cap_days": 365, "
          +definition": 1}` | `feature_store/values/days_since_last_txn__
          +v1.parquet` |                                                 
       16 +| F3 | `has_ever_purchased` | v1 | yes | `{"definition": 1}` |
          + `feature_store/values/has_ever_purchased__v1.parquet` |      
       17 +| F4 | `txn_count_90d` | v1 | yes | `{"definition": 1}` | `fea
          +ture_store/values/txn_count_90d__v1.parquet` |                
       18 +| F5 | `avg_spend_per_active_month_6m` | v1 | no | `{"definiti
          +on": 1, "monthly_cap": null}` | `feature_store/values/avg_spen
          +d_per_active_month_6m__v1.parquet` |                          
       19 +| F5 | `avg_spend_per_active_month_6m` | v2 | yes | `{"definit
          +ion": 1, "monthly_cap": 197.1}` | `feature_store/values/avg_sp
          +end_per_active_month_6m__v2.parquet` |                        
       20 +| F6 | `spend_trend_3m_vs_prior_3m` | v1 | no | `{"definition"
          +: 1, "ratio_cap": 10}` | `feature_store/values/spend_trend_3m_
          +vs_prior_3m__v1.parquet` |                                    
       21 +| F6 | `spend_trend_3m_vs_prior_3m` | v2 | yes | `{"definition
          +": 1, "ratio_cap": 3}` | `feature_store/values/spend_trend_3m_
          +vs_prior_3m__v2.parquet` |                                    
       22 +| F7 | `active_month_share` | v1 | no | `{"definition": 1, "lo
          +okback_months": 12}` | `feature_store/values/active_month_shar
          +e__v1.parquet` |                                              
       23 +| F7 | `active_month_share` | v2 | yes | `{"definition": 1, "l
          +ookback_months": 6}` | `feature_store/values/active_month_shar
          +e__v2.parquet` |                                              
       24 +                                                              
       25 +## F1 — `tenure_days`                                         
       26 +                                                              
       27 +Profile · dtype `int64` · nullable `False` · source customers.
          +csv · 1 stored version(s)                                     
       28 +                                                              
       29 +**Rationale**: Customers who joined recently have short, patch
          +y histories while they decide whether the store is worth retur
          +ning to; tenure lets a model tell 'habit not formed yet' apart
          + from an established customer whose habit is breaking.        
       30 +                                                              
       31 +**Accepted limitation**: Measures account age, not buying age:
          + first purchases come a median of 162 days after signup (data_
          +dictionary.md DQ8).                                           
       32 +                                                              
       33 +### `tenure_days` v1 — no tunable parameters (latest)         
       34 +                                                              
       35 +- **Registry row**: table `feature_registry`, key (`tenure_day
          +s`, 1); `is_latest` = 1; `created_at` 2026-10-22T09:03:12+00:0
          +0                                                             
       36 +- **Values**: `feature_store/values/tenure_days__v1.parquet` —
          + 1,399 rows, columns `customer_id`, `tenure_days`, `as_of_date
          +`; `as_of_date` = 2026-05-02; min 1, mean 407.162, max 808    
       37 +- **Source**: `data/customers.csv:customer_id,signup_date`    
       38 +- **Population**: Customers in data/customers.csv with signup_
          +date < as_of (as_of read as 00:00:00 naive local time). One ro
          +w per customer.                                               
       39 +- **Window bounds** (relative to as_of → this snapshot):      
       40 +  - none: computed from `signup_date` and as_of only          
     … 178 more lines (ctrl+o to expand)
  ⎿  Created outputs/artifacts/VERSION_COMPARISON.md (+100 -0)
        1 +# VERSION_COMPARISON.md                                       
        2 +                                                              
        3 +> Generated by `scripts/compare_feature_versions.py` from the 
          +versions stored in `feature_store/registry.db` (loaded with `g
          +et_version`, not recomputed). Rank correlation is Spearman's ρ
          + computed as `a.rank().corr(b.rank())`. Deciles hold every cus
          +tomer at or beyond that version's own 90th (10th) percentile, 
          +linear interpolation, ties included.                          
        4 +                                                              
        5 +Snapshot as_of: **2026-05-02**. No churn model or ranking exis
          +ts yet, so each section compares the feature's own distributio
          +n, rank order and the threshold metric feature_spec.md assigns
          + it.                                                          
        6 +                                                              
        7 +| Feature | Versions | Customers changed | Rank correlation | 
          +Threshold metric |                                            
        8 +|---|---|---|---|---|                                         
        9 +| `avg_spend_per_active_month_6m` | v1 → v2 | 157 of 1,399 | 0
          +.997 | top decile: 25 in, 25 out |                            
       10 +| `spend_trend_3m_vs_prior_3m` | v1 → v2 | 162 of 1,399 | 0.99
          +9 | 162 tied at 3; all 458 values below 1 unchanged |         
       11 +| `active_month_share` | v1 → v2 | 890 of 1,399 | 0.849 | bott
          +om decile: 54 in, 46 out |                                    
       12 +                                                              
       13 +## F5 — `avg_spend_per_active_month_6m`                       
       14 +                                                              
       15 +- **v1** (monthly_cap = none): `feature_store/values/avg_spend
          +_per_active_month_6m__v1.parquet`. First version: uncapped.   
       16 +- **v2** (monthly_cap = 197.1, latest): `feature_store/values/
          +avg_spend_per_active_month_6m__v2.parquet`. Monthly spend is r
          +ight-skewed (one customer-month reaches 1,753.23). Without a c
          +ap, a single unusually large month makes an occasional buyer l
          +ook like a big regular. The cap is per month because the parqu
          +et holds no order amounts and a feature never mixes sources; i
          +t keeps the feature about typical monthly basket size.        
       17 +                                                              
       18 +### v1 → v2                                                   
       19 +                                                              
       20 +| Statistic | v1 | v2 |                                       
       21 +|---|---|---|                                                 
       22 +| count | 1,399 | 1,399 |                                     
       23 +| mean | 58.29 | 54.34 |                                      
       24 +| std | 52.34 | 42.28 |                                       
       25 +| min | 0 | 0 |                                               
       26 +| p10 | 0 | 0 |                                               
       27 +| p25 | 23.98 | 23.98 |                                       
       28 +| p50 | 51.2 | 51.2 |                                         
       29 +| p75 | 79.21 | 77 |                                          
       30 +| p90 | 120 | 108.2 |                                         
       31 +| p95 | 143.2 | 128.7 |                                       
       32 +| p99 | 250.1 | 197.1 |                                       
       33 +| max | 501.9 | 197.1 |                                       
       34 +                                                              
       35 +- Customers changed: **157** of 1,399 (0 up, 157 down); mean a
          +bsolute change among them 35.17, largest 304.8.               
       36 +- Rank correlation between versions: **0.997**.               
       37 +- v1 top decile: 90th percentile 120; **140 customers**; 1 tie
          +d at the boundary value 120.2.                                
       38 +- v2 top decile: 90th percentile 108.2; **140 customers**; 1 t
          +ied at the boundary value 108.2.                              
       39 +- Moving across the top-decile threshold: **25 enter, 25 leave
          +**.                                                           
       40 +                                                              
     … 60 more lines (ctrl+o to expand)
  ⎿  … 11 more files changed
  ⎿  (timeout 10m)

● The real store is populated: 10 versions across 7 features, and the standalone
  QC runner passes against it. A final read-back through the public store API:

  Ran 1 shell command (ctrl+o to expand)

● I'll verify the purchase-gap statistics before writing the notes, checking
  whether the spec's median gap figures (26 days for F2, 25 days for F4/F7)
  reflect different definitions or an actual discrepancy.

  Ran 1 shell command (ctrl+o to expand)

● Both the 26-day and 25-day gap figures are correct but measure different
  things—one is the median of per-buyer medians, the other is the pooled median
  across all gaps. I'll document this distinction in ARTIFACT_NOTES rather than
  change the spec, then pull exact test counts for the reports.

  Ran 1 shell command (ctrl+o to expand)

● The mutation runs used the 25-test version of test_parity.py; it now has 29
  tests. I'm rerunning all three mutations against the final suite so
  PARITY_CHECK.md reports current numbers.

  Ran 1 shell command (ctrl+o to expand)

● Confirmed against the final 29-test suite: M1 fails 9, M2 fails 5, M3 fails 7,
  while the unmutated copy passes all 29 and build.py's checksum remains
  unchanged. Now writing the reports to outputs/artifacts/, starting with the
  parity check.

● Write(outputs/artifacts/PARITY_CHECK.md)
  ⎿  Wrote 135 lines to outputs/artifacts/PARITY_CHECK.md
       1 # PARITY_CHECK.md — train/score parity verification
       3 Claude ran both checks below during the build session on 2026-10
         -22, in the
       4 `churn-pipeline` conda env (Python 3.11, pandas 3.0.6). The resu
       5
       6 **Result: parity holds.** `tests/test_parity.py` (29 tests) pass
       7 caught each of three deliberately injected training/scoring spli
         ts, so no strengthening was
       8 needed.
      10 ## What is under test
     … +223 lines (ctrl+o to expand)

● Before moving on, I'm verifying two claims I just wrote about customer 697:
  its last gap has a fraction below .5, and it has a single month in the window.

  Ran 1 shell command (ctrl+o to expand)

● Both claims hold: 697's gap is 54.022 days, with one month in the window
  (2026-03). Now ARTIFACT_NOTES.md, the self-critique log:

● Write(outputs/artifacts/ARTIFACT_NOTES.md)
  ⎿  Wrote 83 lines to outputs/artifacts/ARTIFACT_NOTES.md
       1 # ARTIFACT_NOTES.md — self-critique log for the build
       3 Written by Claude, which generated the build stage on 2026-10-22
         : `features/`,
         n `outputs/artifacts/`.
       5 It implements the learner's confirmed `feature_spec.md`. No inpu
       6 `feature_spec.md`, `data_dictionary.md`, `requirements.txt`, `da
         ta/`, nor the learner's notes.
       8 ## 1. What I got wrong or reconsidered, and how each was resolve
         d
      10 | # | Where | What was wrong or reconsidered | Resolution |
     … +151 lines (ctrl+o to expand)

● Write(outputs/artifacts/review_log.md)
  ⎿  Wrote 35 lines to outputs/artifacts/review_log.md
      1 # review_log.md — Step 3 self-review
      2
         reviewer would: ingestion,
      4 transforms, build, schema, feature store, quality checks, scripts
        , lineage and version
        stions:
      6
      7 - (a) Could training and scoring silently diverge?
         spec?
      9 - (c) Could the store silently overwrite a version?
     … +91 lines (ctrl+o to expand)

● Update(outputs/artifacts/review_log.md)
  ⎿  Added 1 line, removed 2 lines
      31  | 17 | Test isolation | The tests could have touched the real `
          feature_store/`. | **Guarded.** A session fixture hashes `regis
          try.db` and `values/` before and after the run and fails if any
          thing changed. Every store test uses `tmp_path`. |
      32  | 18 | Warnings | Ran the suite with `-W error`: no pandas or n
          umpy deprecation warnings. | No issue |
      33  
      34 -All findings are resolved: 4 fixed (including one guard added) 
         -and 6 left with the reasons                                    
      35 -above. Final suite: 151 passed (`outputs/artifacts/pytest_outpu
         -t.txt`).                                                       
      34 +All findings are resolved: 3 fixed, 1 guard added and 6 left wi
         +th the reasons above. Final suite: 151 passed (`outputs/artifac
         +ts/pytest_output.txt`).                                        

● Bash(find . -name __pycache__ -not -path "./.git/*" -prune -exec rm -rf {} + ;
      timeout 900 conda run --no-capture-output -n churn-pipeline python -m
      pytest -v > outp…)
  ⎿  Created outputs/artifacts/pytest_output.txt (+160 -0)
        1 +============================= test session starts ============
          +==================                                            
        2 +platform linux -- Python 3.11.17, pytest-9.1.1, pluggy-1.7.0 -
          +- /home/subhadeep/miniconda3/envs/churn-pipeline/bin/python   
        3 +cachedir: .pytest_cache                                       
        4 +rootdir: /mnt/c/Users/Subhadeep.paul/Downloads/assignment     
        5 +configfile: pytest.ini                                        
        6 +collecting ... collected 151 items                            
        7 +                                                              
        8 +tests/test_feature_store.py::test_spec_versions_at_least_two_f
          +eatures_each_with_at_least_two_versions PASSED [  0%]         
        9 +tests/test_feature_store.py::test_register_and_retrieve_all_ve
          +rsions_of_a_versioned_feature[avg_spend_per_active_month_6m] P
          +ASSED [  1%]                                                  
       10 +tests/test_feature_store.py::test_register_and_retrieve_all_ve
          +rsions_of_a_versioned_feature[spend_trend_3m_vs_prior_3m] PASS
          +ED [  1%]                                                     
       11 +tests/test_feature_store.py::test_register_and_retrieve_all_ve
          +rsions_of_a_versioned_feature[active_month_share] PASSED [  2%
          +]                                                             
       12 +tests/test_feature_store.py::test_is_latest_points_to_highest_
          +version_not_an_earlier_one[avg_spend_per_active_month_6m] PASS
          +ED [  3%]                                                     
       13 +tests/test_feature_store.py::test_is_latest_points_to_highest_
          +version_not_an_earlier_one[spend_trend_3m_vs_prior_3m] PASSED 
          +[  3%]                                                        
       14 +tests/test_feature_store.py::test_is_latest_points_to_highest_
          +version_not_an_earlier_one[active_month_share] PASSED [  4%]  
       15 +tests/test_feature_store.py::test_list_versions_returns_distin
          +ct_parquet_paths_that_exist[avg_spend_per_active_month_6m] PAS
          +SED [  5%]                                                    
       16 +tests/test_feature_store.py::test_list_versions_returns_distin
          +ct_parquet_paths_that_exist[spend_trend_3m_vs_prior_3m] PASSED
          + [  5%]                                                       
       17 +tests/test_feature_store.py::test_list_versions_returns_distin
          +ct_parquet_paths_that_exist[active_month_share] PASSED [  6%] 
       18 +tests/test_feature_store.py::test_more_than_two_versions_and_l
          +atest_is_the_highest PASSED [  7%]                            
       19 +tests/test_feature_store.py::test_versions_are_numbered_per_fe
          +ature PASSED [  7%]                                           
       20 +tests/test_feature_store.py::test_an_existing_parquet_file_is_
          +never_overwritten PASSED [  8%]                               
       21 +tests/test_feature_store.py::test_a_failed_write_rolls_back_an
          +d_removes_its_file PASSED [  9%]                              
       22 +tests/test_feature_store.py::test_registry_rows_cannot_be_edit
          +ed_deleted_or_rolled_back PASSED [  9%]                       
       23 +tests/test_feature_store.py::test_one_as_of_per_store PASSED  
          +           [ 10%]                                             
       24 +tests/test_feature_store.py::test_invalid_snapshots_are_reject
          +ed_before_anything_is_written[bad0-null values] PASSED [ 11%] 
       25 +tests/test_feature_store.py::test_invalid_snapshots_are_reject
          +ed_before_anything_is_written[bad1-duplicated customer_id] PAS
          +SED [ 11%]                                                    
       26 +tests/test_feature_store.py::test_invalid_snapshots_are_reject
          +ed_before_anything_is_written[bad2-exactly the columns] PASSED
          + [ 12%]                                                       
       27 +tests/test_feature_store.py::test_invalid_snapshots_are_reject
          +ed_before_anything_is_written[bad3-one as_of_date] PASSED [ 13
          +%]                                                            
       28 +tests/test_feature_store.py::test_feature_names_cannot_escape_
          +the_values_directory PASSED [ 13%]                            
       29 +tests/test_feature_store.py::test_reads_on_an_empty_store_crea
          +te_nothing PASSED [ 14%]                                      
       30 +tests/test_feature_store.py::test_default_store_root_is_resolv
          +ed_from_the_module_not_the_cwd PASSED [ 15%]                  
       31 +tests/test_feature_store.py::test_training_script_registers_ev
          +ery_feature_and_every_version PASSED [ 15%]                   
       32 +tests/test_feature_store.py::test_training_store_matches_the_s
          +hared_build PASSED [ 16%]                                     
       33 +tests/test_feature_store.py::test_rerun_is_unchanged_skipped_a
          +nd_writes_nothing PASSED [ 17%]                               
       34 +tests/test_feature_store.py::test_qc7_stops_a_silent_definitio
          +n_change_and_nothing_is_written PASSED [ 17%]                 
       35 +tests/test_feature_store.py::test_a_version_whose_values_do_no
          +t_change_is_refused PASSED [ 18%]                             
       36 +tests/test_feature_store.py::test_training_script_refuses_a_di
          +fferent_as_of PASSED [ 19%]                                   
       37 +tests/test_feature_store.py::test_training_script_writes_nothi
          +ng_when_a_build_check_fails PASSED [ 19%]                     
       38 +tests/test_feature_store.py::test_version_comparison_matches_t
          +he_spec_expectations PASSED [ 20%]                            
       39 +tests/test_feature_store.py::test_version_comparison_reads_thr
          +ough_get_version PASSED [ 21%]                                
       40 +tests/test_feature_store.py::test_lineage_has_a_separate_entry
          +_for_every_stored_version PASSED [ 21%]                       
     … 120 more lines (ctrl+o to expand)
  ⎿  (timeout 10m)

● All 151 tests passed (exit 0), with the real store intact as confirmed by the
  session guard — output saved to outputs/artifacts/pytest_output.txt. I'll note
  for future sessions that the skill's file locations differ from the
  submission rule.

  Ran 1 shell command, wrote 1 memory (ctrl+o to expand)

● Written for: your assignment evaluator. The reports in outputs/artifacts/ are
  addressed to them; this summary is for you.

  The build is finished and all 151 tests pass. Nothing is committed, and none
  of the inputs or your notes files were touched. I checked that with git diff.

  What's built
  - Feature store: one code path, build_features, which both scripts call. The
    store in feature_store/ holds all 7 features at 2026-05-02. F5, F6 and F7
    each have v1 and v2, and get_latest returns v2 for all three.
  - Spec figures: every figure your spec quotes reproduced exactly, from both
    the pipeline and a separate pandas recomputation. That includes:
    - F5: 157 customers changed, 25 in and 25 out of the top decile, 140 in each
      decile.
    - F6: 162 tied at the cap, all 458 values below 1 unchanged.
    - F7: 54 in and 46 out of the bottom decile (168 and 176 customers), rank
      correlation 0.849.
    - Population: 1,399 customers, 232 of them zero-activity.
  - Parity mutation check: I injected three scoring-only bugs in a scratch copy,
    placed after the quality checks so only the parity tests could catch them.
    They were rounded recency, the customer-697 leak, and a cap recomputed from
    the scored batch. All three were caught. The 697 leak is caught only by the
    697 case and the all-customers test, which bears out your choice of that
    customer. Your real build.py was never edited.
  - Bugs caught along the way: QC7 and QC8 crashed on duplicated customers
    instead of failing. The training script could print "nothing was registered"
    after a partial registration, when some versions had in fact been
    committed. Both are fixed and tested. The full list is in ARTIFACT_NOTES.md
    and review_log.md.

  Decisions for you
  1. Scoring reads schema.py, not the store. If you add a version to the schema
     without rerunning the training script, scoring serves the new version while
     the store's latest is still the old one. Nothing detects this. Your spec
     defines scoring this way, so I documented it rather than changing it.
  2. Only definition: 1 is implemented. A future computation change would need
     the old code kept so earlier versions can still be rebuilt and checked. For
     now, any other definition is rejected with a clear error.
  3. The spec's two median gaps. It quotes 26 days (F2) and 25 days (F4, F7).
     Both are right but measure different things: 26 is the median of each
     repeat buyer's own median gap, and 25 is the median of all gaps pooled. You
     may want to label them in the spec.

  Two additions beyond the scaffold
  - QC3 also checks that every buyer has has_ever_purchased = 1, which is
    slightly stricter than the spec's wording.
  - I added scripts/generate_lineage.py, so FEATURE_LINEAGE.md is regenerated
    from the schema and registry after every successful training run.

  To rerun, from the repo root:
  - conda run --no-capture-output -n churn-pipeline python
    scripts/build_training_features.py. Rerunning skips every version as
    "unchanged" and writes nothing.
  - conda run --no-capture-output -n churn-pipeline python
    scripts/score_customer.py --customer-id 3 --customer-id 697.
  - conda run --no-capture-output -n churn-pipeline python -m pytest.

  All reports are in outputs/artifacts/:
  - PARITY_CHECK.md (outputs/artifacts/PARITY_CHECK.md)
  - VERSION_COMPARISON.md (outputs/artifacts/VERSION_COMPARISON.md)
  - FEATURE_LINEAGE.md (outputs/artifacts/FEATURE_LINEAGE.md)
  - ARTIFACT_NOTES.md (outputs/artifacts/ARTIFACT_NOTES.md)
  - review_log.md (outputs/artifacts/review_log.md)
  - pytest_output.txt (outputs/artifacts/pytest_output.txt)

  ARTIFACT_NOTES.md opens by stating that Claude generated the build, which
  should help with the rubric's AI-use disclosure.