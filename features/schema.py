"""
Versioned output feature schema: the authoritative list of every feature this pipeline
produces, with dtype, nullability, source(s), and feature-set version.

This is maintained from `feature_spec.md` and is consumed by `features/build.py` (which
parameters each transform gets), by `scripts/build_training_features.py` (which versions to
register, with what metadata) and by `scripts/generate_lineage.py` (FEATURE_LINEAGE.md).

Every tunable value lives here, never in a transform and never recomputed from a batch:
- `fixed` holds a feature's constants that are part of its definition (e.g. the 90-day
  window baked into `txn_count_90d`'s name). They are not registry parameters.
- each `FeatureVersion.parameters` is exactly what is written to the registry's
  `parameters` column: the tunable values plus the `definition` revision.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import Callable

FEATURE_SET_VERSION = "1.0"

# Every version defined below is definition revision 1. A change of computation is a new
# definition revision, and so a new version (feature_spec.md, "What forces a version bump").
CURRENT_DEFINITION = 1

POPULATION_RULE = (
    "Customers in data/customers.csv with signup_date < as_of (as_of read as 00:00:00 naive "
    "local time). One row per customer."
)

# QC4 bounds that are fixed constants rather than version parameters (feature_spec.md QC4).
TXN_COUNT_UPPER_BOUND = 100
UNCAPPED_MONTHLY_SPEND_UPPER_BOUND = 2000.0


@dataclass(frozen=True)
class DayWindow:
    """Half-open window [as_of - days, as_of) on full timestamps. days=None: everything
    before as_of."""

    label: str
    days: int | None


@dataclass(frozen=True)
class MonthWindow:
    """`months` complete calendar months ending `offset` months before as_of's month."""

    label: str
    months: int
    offset: int = 0


@dataclass(frozen=True)
class FeatureVersion:
    version: int
    parameters: dict
    change_reason: str


@dataclass(frozen=True)
class FeatureSpec:
    name: str
    dtype: str
    nullable: bool
    source: str
    feature_set_version: str
    spec_id: str
    family: str
    source_columns: str
    description: str
    rationale: str
    zero_activity_default: int | float | None
    default_note: str
    computation: Callable[[dict, dict], str]
    windows: Callable[[dict, dict], tuple]
    versions: tuple[FeatureVersion, ...]
    fixed: dict = field(default_factory=dict)
    parameter_notes: str = ""
    accepted_limitation: str = ""
    comparison: dict | None = None

    @property
    def latest(self) -> FeatureVersion:
        return self.versions[-1]

    @property
    def is_versioned(self) -> bool:
        return len(self.versions) >= 2

    def version(self, number: int) -> FeatureVersion:
        for v in self.versions:
            if v.version == number:
                return v
        raise KeyError(f"{self.name} has no schema version {number}")

    def transform_kwargs(self, parameters: dict) -> dict:
        """Keyword arguments for this feature's transform: fixed constants plus the
        version's tunable parameters (the `definition` revision is not a transform input)."""
        tunable = {k: v for k, v in parameters.items() if k != "definition"}
        return {**self.fixed, **tunable}

    def transform_summary(self, parameters: dict) -> str:
        return self.computation(self.fixed, parameters)


def _cap_text(cap: float | None) -> str:
    return "no cap" if cap is None else f"{cap:.2f}"


FEATURE_SCHEMA: list[FeatureSpec] = [
    FeatureSpec(
        name="tenure_days",
        dtype="int64",
        nullable=False,
        source="customers.csv",
        feature_set_version=FEATURE_SET_VERSION,
        spec_id="F1",
        family="Profile",
        source_columns="data/customers.csv:customer_id,signup_date",
        description="Whole days from the customer's signup_date to as_of. At least 1 for "
        "everyone in the population.",
        rationale=(
            "Customers who joined recently have short, patchy histories while they decide "
            "whether the store is worth returning to; tenure lets a model tell 'habit not "
            "formed yet' apart from an established customer whose habit is breaking."
        ),
        zero_activity_default=None,
        default_note="Not needed: every customer has a signup_date, with or without purchases.",
        computation=lambda fixed, p: "(as_of - signup_date) in whole days.",
        windows=lambda fixed, p: (),
        versions=(
            FeatureVersion(1, {"definition": 1}, "First version."),
        ),
        accepted_limitation=(
            "Measures account age, not buying age: first purchases come a median of 162 days "
            "after signup (data_dictionary.md DQ8)."
        ),
    ),
    FeatureSpec(
        name="days_since_last_txn",
        dtype="int64",
        nullable=False,
        source="transactions.db",
        feature_set_version=FEATURE_SET_VERSION,
        spec_id="F2",
        family="Recency",
        source_columns="data/transactions.db:transactions.customer_id,txn_timestamp",
        description="Whole days (rounded down) from the customer's latest transaction before "
        "as_of to as_of, capped at 365. Never-buyers get 365.",
        rationale=(
            "Customers who have gone longer since their last purchase have often already moved "
            "that spending elsewhere. Recency measures where the customer stands at the cutoff, "
            "not their average pace; 120 days out is past a repeat buyer's own usual rhythm "
            "(median personal gap 26 days, p90 57)."
        ),
        zero_activity_default=365,
        default_note="365 (= cap_days) for customers with no transaction before as_of. At "
        "2026-05-02 no buyer exceeds 289 days, so only never-buyers hold 365; "
        "has_ever_purchased marks them explicitly.",
        computation=lambda fixed, p: (
            f"min(floor((as_of - max(txn_timestamp before as_of)) / 1 day), {p['cap_days']})."
        ),
        windows=lambda fixed, p: (DayWindow("all transactions before as_of", None),),
        versions=(
            FeatureVersion(1, {"cap_days": 365, "definition": 1}, "First version."),
        ),
        parameter_notes="cap_days = 365 is a fixed constant (one year), also used as the "
        "never-buyer default.",
    ),
    FeatureSpec(
        name="has_ever_purchased",
        dtype="int64",
        nullable=False,
        source="transactions.db",
        feature_set_version=FEATURE_SET_VERSION,
        spec_id="F3",
        family="Activation",
        source_columns="data/transactions.db:transactions.customer_id,txn_timestamp",
        description="1 if the customer has at least one transaction before as_of, else 0.",
        rationale=(
            "Customers who have never bought are an activation case, not a lapse: they had no "
            "habit to lose. The flag also stops the other features' defaults (365, 0, 1.0) "
            "from being read as real behaviour."
        ),
        zero_activity_default=0,
        default_note="0. The feature is itself the zero-activity indicator.",
        computation=lambda fixed, p: "1 if any transaction has txn_timestamp < as_of, else 0.",
        windows=lambda fixed, p: (DayWindow("all transactions before as_of", None),),
        versions=(
            FeatureVersion(1, {"definition": 1}, "First version."),
        ),
    ),
    FeatureSpec(
        name="txn_count_90d",
        dtype="int64",
        nullable=False,
        source="transactions.db",
        feature_set_version=FEATURE_SET_VERSION,
        spec_id="F4",
        family="Frequency",
        source_columns="data/transactions.db:transactions.customer_id,txn_timestamp",
        description="Number of transactions with as_of - 90 days <= txn_timestamp < as_of. "
        "Returned purchases still count.",
        rationale=(
            "Customers buying less often in the months right before the cutoff tend to be "
            "partway out the door. Read alongside spend_trend_3m_vs_prior_3m and "
            "active_month_share, it separates a slowdown from a habitually infrequent buyer. "
            "90 days covers about three typical purchase cycles (median gap 25 days)."
        ),
        zero_activity_default=0,
        default_note="0 for customers with no transaction in the window, including never-buyers.",
        computation=lambda fixed, p: (
            f"COUNT(*) of transactions with as_of - {fixed['window_days']} days <= "
            "txn_timestamp < as_of (is_returned ignored)."
        ),
        windows=lambda fixed, p: (DayWindow("transaction window", fixed["window_days"]),),
        versions=(
            FeatureVersion(1, {"definition": 1}, "First version."),
        ),
        fixed={"window_days": 90},
    ),
    FeatureSpec(
        name="avg_spend_per_active_month_6m",
        dtype="float64",
        nullable=False,
        source="transactions_monthly_agg.parquet",
        feature_set_version=FEATURE_SET_VERSION,
        spec_id="F5",
        family="Monetary",
        source_columns="data/transactions_monthly_agg.parquet:customer_id,txn_month,total_amount",
        description="Average monthly spend over the months with a purchase among the 6 "
        "complete calendar months before as_of's month, each month capped at monthly_cap.",
        rationale=(
            "Customers whose usual monthly basket here is small probably spend most of their "
            "money elsewhere, so leaving costs them little. Averaging only over active months "
            "keeps it apart from purchase frequency (the all-months average contained the "
            "frequency count by construction)."
        ),
        zero_activity_default=0.0,
        default_note="0.0 for customers with no parquet row in the window: never-buyers and "
        "customers who bought only before the window.",
        computation=lambda fixed, p: (
            f"Over the {fixed['window_months']} complete calendar months before as_of's month: "
            f"cap each month's total_amount at {_cap_text(p['monthly_cap'])}, sum the capped "
            "totals, divide by the number of those months with a parquet row. Gross of returns."
        ),
        windows=lambda fixed, p: (MonthWindow("spend window", fixed["window_months"]),),
        versions=(
            FeatureVersion(
                1,
                {"monthly_cap": None, "definition": 1},
                "First version: uncapped.",
            ),
            FeatureVersion(
                2,
                {"monthly_cap": 197.10, "definition": 1},
                "Monthly spend is right-skewed (one customer-month reaches 1,753.23). Without a "
                "cap, a single unusually large month makes an occasional buyer look like a big "
                "regular. The cap is per month because the parquet holds no order amounts and a "
                "feature never mixes sources; it keeps the feature about typical monthly basket "
                "size.",
            ),
        ),
        fixed={"window_months": 6},
        parameter_notes="monthly_cap = 197.10 is the 95th percentile (linear interpolation) of "
        "the 5,900 parquet customer-months with txn_month before 2026-05 (197.098, rounded), "
        "fixed as a constant and never recomputed during a build.",
        comparison={"kind": "top_decile"},
    ),
    FeatureSpec(
        name="spend_trend_3m_vs_prior_3m",
        dtype="float64",
        nullable=False,
        source="transactions_monthly_agg.parquet",
        feature_set_version=FEATURE_SET_VERSION,
        spec_id="F6",
        family="Monetary trend",
        source_columns="data/transactions_monthly_agg.parquet:customer_id,txn_month,total_amount",
        description="Spend in the 3 complete months before as_of's month divided by spend in "
        "the 3 months before those, capped at ratio_cap; 0.0 when spending stopped, 1.0 when "
        "there is no earlier spend or no spend at all.",
        rationale=(
            "Customers whose spend is falling against their own previous quarter tend to be "
            "trading down or moving purchases to a competitor. Each customer is compared with "
            "their own baseline: a steady low spender sits at 1.0, a big spender who halves "
            "their spend at 0.5."
        ),
        zero_activity_default=1.0,
        default_note="1.0 when both quarters have no spend (never-buyers and customers who "
        "stopped before the prior quarter): there is no trend to measure. 0.0 means only one "
        "thing, spending stopped.",
        computation=lambda fixed, p: (
            "recent = SUM(total_amount) over the "
            f"{fixed['window_months']} complete months before as_of's month; prior = SUM over "
            f"the {fixed['window_months']} months before those. recent>0 and prior>0: "
            f"min(recent/prior, {p['ratio_cap']}); prior>0, recent=0: 0.0; recent>0, prior=0: "
            "1.0; both 0: 1.0."
        ),
        windows=lambda fixed, p: (
            MonthWindow("recent", fixed["window_months"], 0),
            MonthWindow("prior", fixed["window_months"], fixed["window_months"]),
        ),
        versions=(
            FeatureVersion(
                1,
                {"ratio_cap": 10, "definition": 1},
                "First version: ratio capped at 10.",
            ),
            FeatureVersion(
                2,
                {"ratio_cap": 3, "definition": 1},
                "Ratios above 3 come from a tiny previous quarter, often a single small order "
                "(largest observed 85.9). For retention targeting the useful signal is on the "
                "decline side (below 1); growth beyond 3x adds no information but would dominate "
                "the feature's scale.",
            ),
        ),
        fixed={"window_months": 3},
        parameter_notes="ratio_cap is a fixed constant per version (10, then 3), never "
        "recomputed from the batch.",
        comparison={"kind": "cap_ties", "parameter": "ratio_cap", "unchanged_below": 1.0},
    ),
    FeatureSpec(
        name="active_month_share",
        dtype="float64",
        nullable=False,
        source="transactions_monthly_agg.parquet + customers.csv",
        feature_set_version=FEATURE_SET_VERSION,
        spec_id="F7",
        family="Engagement consistency",
        source_columns=(
            "data/transactions_monthly_agg.parquet:customer_id,txn_month; "
            "data/customers.csv:customer_id,signup_date"
        ),
        description="Share of eligible months (window months on or after the signup month) "
        "with a purchase, over the lookback_months complete months before as_of's month.",
        rationale=(
            "Customers who buy in most of their available months have a habit one quiet month "
            "won't break; customers who buy in only a few can drift away unnoticed. Counting "
            "only months since signup keeps it from just measuring tenure."
        ),
        zero_activity_default=0.0,
        default_note="0.0 for customers with no parquet row in the window, and for customers "
        "with 0 eligible months (signed up in as_of's own month; 1 customer at 2026-05-02).",
        computation=lambda fixed, p: (
            f"Over the {p['lookback_months']} complete calendar months before as_of's month: "
            "active = number of months with a parquet row for the customer; eligible = number "
            "of those months on or after the customer's signup month; value = active / "
            "eligible (0.0 if eligible = 0)."
        ),
        windows=lambda fixed, p: (MonthWindow("lookback", p["lookback_months"]),),
        versions=(
            FeatureVersion(
                1,
                {"lookback_months": 12, "definition": 1},
                "First version: 12-month lookback.",
            ),
            FeatureVersion(
                2,
                {"lookback_months": 6, "definition": 1},
                "A 12-month window still credits a habit that ended months ago. With a median "
                "gap of 25 days between purchases, 6 months still covers about 7 purchase "
                "cycles, enough to judge consistency while reflecting the current habit.",
            ),
        ),
        parameter_notes="lookback_months is the versioned parameter (12, then 6). The feature "
        "was renamed from active_months_12m because the lookback differs between versions.",
        accepted_limitation=(
            "Eligible months start at signup, and first purchases come a median of 162 days "
            "later (DQ8), so newer customers' shares include idle months before their first "
            "purchase."
        ),
        comparison={
            "kind": "bottom_decile",
            "subgroup": {"feature": "has_ever_purchased", "value": 1},
            # what drives the change: eligible months start at signup, first purchases come later
            "eligibility_drivers": {"parameter": "lookback_months", "tenure_feature": "tenure_days"},
            # an independent read on who crosses the decile, from other stored features
            "mover_profile": {"recency_feature": "days_since_last_txn", "window_count_feature": "txn_count_90d"},
        },
    ),
]

FEATURE_NAMES: list[str] = [f.name for f in FEATURE_SCHEMA]
_BY_NAME = {f.name: f for f in FEATURE_SCHEMA}


def get_spec(name: str) -> FeatureSpec:
    try:
        return _BY_NAME[name]
    except KeyError:
        raise KeyError(f"unknown feature {name!r}; known: {FEATURE_NAMES}") from None


def latest_parameters() -> dict[str, dict]:
    return {f.name: copy.deepcopy(f.latest.parameters) for f in FEATURE_SCHEMA}


def resolve_feature_params(feature_params: dict[str, dict] | None) -> dict[str, dict]:
    """Full {feature_name: parameters} mapping: the latest schema version of every feature,
    overridden by `feature_params` for the features it names.

    An override must carry exactly the parameter keys the feature's versions declare, and a
    `definition` this code implements, so a typo can never silently fall back to a default.
    """
    resolved = latest_parameters()
    for name, params in (feature_params or {}).items():
        spec = get_spec(name)
        expected = set(spec.latest.parameters)
        if set(params) != expected:
            raise ValueError(
                f"parameters for {name} must have exactly the keys {sorted(expected)}, "
                f"got {sorted(params)}"
            )
        if params["definition"] != CURRENT_DEFINITION:
            raise ValueError(
                f"{name}: this code implements definition {CURRENT_DEFINITION}, "
                f"got definition {params['definition']}"
            )
        resolved[name] = copy.deepcopy(params)
    return resolved


def month_windows_in_effect(params: dict[str, dict], only: str | None = None) -> list[MonthWindow]:
    """Every month window any feature (or just feature `only`) uses at these parameters
    (QC6's coverage scope)."""
    windows = []
    for spec in FEATURE_SCHEMA:
        if only is not None and spec.name != only:
            continue
        windows.extend(
            w for w in spec.windows(spec.fixed, params[spec.name]) if isinstance(w, MonthWindow)
        )
    return windows
