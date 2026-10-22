#!/usr/bin/env python3
"""
Version comparison for every feature features/schema.py versions (feature_spec.md:
avg_spend_per_active_month_6m, spend_trend_3m_vs_prior_3m, active_month_share).

For each versioned feature, every registered version is loaded through the feature store
(store.get_version for each version list_versions reports; nothing is recomputed), and each
consecutive pair (plus first vs latest when there are more than two) is compared:
- customers whose value changed (and in which direction);
- summary statistics and quantiles per version;
- Spearman rank correlation between versions, as a.rank().corr(b.rank()) (no SciPy);
- the threshold metric feature_spec.md assigns the feature:
  - top_decile: customers entering/leaving the top decile, where the cap acts;
  - cap_ties: customers tied at each version's cap, and whether all values below 1 are
    unchanged;
  - bottom_decile: customers entering/leaving the bottom decile within a subgroup
    (has_ever_purchased = 1).
  A decile holds every customer at or beyond that version's own 90th (10th) percentile,
  linear interpolation, ties included. Each mover's rank in the older version is reported,
  so "the segment only moves at its edge" can be checked rather than assumed;
- the share of the feature's total squared deviation from its mean that the changed
  customers hold in each version (the weight they carry in a linear fit);
- for active_month_share, what drives the change. Eligible months start at signup, and
  first purchases come a median of 162 days later (feature_spec.md, accepted limitation),
  so buyers are split by whether they had full eligibility in the old window and whether
  their first purchase falls inside it. The shares are then recomputed with eligible
  months counted from the first-purchase month instead. Active months come from the stored
  shares (share x eligible months, an exact integer per QC8). First-purchase dates, which
  no stored feature holds, are read from data/transactions.db, cut at as_of.

Every sentence of the interpretation reads off one of these numbers; a statement that holds
by definition is labelled as such rather than offered as evidence. No churn model or ranking
exists yet, so the comparison is on the feature's own distribution and rank order. The
results and their interpretation go to VERSION_COMPARISON.md.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import numpy as np
import pandas as pd

from feature_store import store
from features import ingest, schema, transforms
from features.build import DEFAULT_SOURCE_PATHS, REPO_ROOT
from scripts.generate_lineage import display_path

DEFAULT_OUT = REPO_ROOT / "outputs" / "artifacts" / "VERSION_COMPARISON.md"
QUANTILES = [0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99]


def load_versions(name: str, store_dir) -> list[dict]:
    """Every registered version of `name`, oldest first, read through the store."""
    rows = store.list_versions(name, store_dir=store_dir).sort_values("version")
    out = []
    for row in rows.itertuples():
        snap = store.get_version(name, int(row.version), store_dir=store_dir)
        out.append({
            "version": int(row.version),
            "parameters": json.loads(row.parameters),
            "parquet_path": row.parquet_path,
            "is_latest": int(row.is_latest),
            "values": snap.set_index("customer_id")[name].sort_index(),
            "as_of": pd.Timestamp(snap["as_of_date"].iloc[0]),
        })
    return out


def rank_correlation(a: pd.Series, b: pd.Series) -> float:
    return float(a.rank().corr(b.rank()))


def summary(values: pd.Series) -> dict:
    stats = {"count": int(values.size), "mean": values.mean(), "std": values.std(), "min": values.min()}
    stats.update({f"p{int(q * 100)}": values.quantile(q) for q in QUANTILES})
    stats["max"] = values.max()
    return stats


def deviation_share(values: pd.Series, ids) -> dict:
    """Share of sum((x - mean)^2) held by `ids`, and by the single largest customer: the
    weight those values carry in a linear fit (leverage)."""
    sq = (values - values.mean()) ** 2
    total = float(sq.sum())
    return {"ids_share": float(sq.loc[ids].sum()) / total, "max_share": float(sq.max()) / total}


def _stored(name: str, store_dir) -> pd.Series:
    return store.get_latest(name, store_dir=store_dir).set_index("customer_id")[name].sort_index()


def first_purchase_dates(transactions_path, as_of: pd.Timestamp) -> pd.Series:
    """Each customer's first transaction before as_of (no stored feature holds it)."""
    t = ingest.load_transactions(transactions_path)
    return t.loc[t["txn_timestamp"] < as_of].groupby("customer_id")["txn_timestamp"].min()


def decile(values: pd.Series, side: str) -> dict:
    q = values.quantile(0.90 if side == "top" else 0.10)
    members = values.index[values >= q] if side == "top" else values.index[values <= q]
    boundary = values.loc[members].min() if side == "top" else values.loc[members].max()
    return {
        "threshold": float(q),
        "members": set(members),
        "boundary_value": float(boundary),
        "tied_at_boundary": int((values == boundary).sum()),
    }


def threshold_metric(spec: schema.FeatureSpec, old: dict, new: dict, store_dir) -> dict:
    kind = spec.comparison["kind"]
    a, b = old["values"], new["values"]
    if kind in ("top_decile", "bottom_decile"):
        side = "top" if kind == "top_decile" else "bottom"
        subgroup = spec.comparison.get("subgroup")
        if subgroup:
            flag = store.get_latest(subgroup["feature"], store_dir=store_dir).set_index("customer_id")[subgroup["feature"]]
            keep = flag.index[flag == subgroup["value"]]
            a, b = a.loc[a.index.intersection(keep)], b.loc[b.index.intersection(keep)]
        da, db = decile(a, side), decile(b, side)
        entering = sorted(db["members"] - da["members"])
        leaving = sorted(da["members"] - db["members"])
        # rank 1 = the most extreme value on the decile's side, ties sharing the best rank
        old_rank = a.rank(ascending=(side == "bottom"), method="min")
        return {
            "kind": kind, "side": side, "subgroup": subgroup, "subgroup_size": int(a.size),
            "subgroup_ids": list(a.index), "old": da, "new": db,
            "entering": len(entering), "leaving": len(leaving),
            "entering_ids": entering, "leaving_ids": leaving,
            "staying_ids": sorted(da["members"] & db["members"]),
            "entering_old_ranks": sorted(int(old_rank[i]) for i in entering),
            "leaving_old_ranks": sorted(int(old_rank[i]) for i in leaving),
        }
    if kind == "cap_ties":
        param, below = spec.comparison["parameter"], spec.comparison["unchanged_below"]
        caps = {"old": old["parameters"][param], "new": new["parameters"][param]}
        low = a.index[a < below]
        changed = a != b
        return {
            "kind": kind, "parameter": param, "caps": caps,
            "changed": int(changed.sum()), "changed_at_new_cap": int((changed & (b == caps["new"])).sum()),
            "tied_old": int((a == caps["old"]).sum()), "tied_new": int((b == caps["new"]).sum()),
            "below": below, "n_below_old": int(low.size), "n_below_new": int((b < below).sum()),
            "below_unchanged": bool((a.loc[low] == b.loc[low]).all() and set(low) == set(b.index[b < below])),
        }
    raise ValueError(f"unknown comparison kind {kind!r} for {spec.name}")


def eligibility_drivers(spec: schema.FeatureSpec, old: dict, new: dict, store_dir, first_purchase: pd.Series) -> dict:
    """Split the change in a share-of-eligible-months feature by eligibility and by where
    the first purchase falls, and recompute both versions with eligible months counted from
    the first-purchase month instead of the signup month."""
    cfg = spec.comparison["eligibility_drivers"]
    l_old, l_new = old["parameters"][cfg["parameter"]], new["parameters"][cfg["parameter"]]
    as_of, a, b = old["as_of"], old["values"], new["values"]
    n = int(a.size)
    signup = as_of - pd.to_timedelta(_stored(cfg["tenure_feature"], store_dir).reindex(a.index), unit="D")
    first = first_purchase.reindex(a.index)
    buyers = first.notna()

    e_old = transforms.eligible_months(signup, as_of, l_old)
    e_new = transforms.eligible_months(signup, as_of, l_new)
    active_old, active_new = a * e_old, b * e_new
    if not (np.allclose(active_old, active_old.round()) and np.allclose(active_new, active_new.round())):
        raise ValueError(f"{spec.name}: share x eligible months is not an integer; cannot recover active months")
    active_old, active_new = active_old.round(), active_new.round()

    fb = first[buyers]
    p_old = transforms.eligible_months(fb, as_of, l_old).reindex(a.index, fill_value=0)
    p_new = transforms.eligible_months(fb, as_of, l_new).reindex(a.index, fill_value=0)
    cf_old = (active_old / p_old).where(p_old > 0, 0.0)
    cf_new = (active_new / p_new).where(p_new > 0, 0.0)

    old_start, _ = transforms.month_window(as_of, l_old)
    new_start, _ = transforms.month_window(as_of, l_new)
    d = b - a
    rose, fell = d > 0, d < 0
    inside = buyers & (first >= old_start)
    full = e_old == l_old
    before = buyers & ~inside

    def counts(mask) -> dict:
        return {"n": int(mask.sum()), "rose": int((mask & rose).sum()), "fell": int((mask & fell).sum()),
                "unchanged": int((mask & ~rose & ~fell).sum()), "contribution": float(d[mask].sum()) / n}

    return {
        "old_lookback": l_old, "new_lookback": l_new, "old_start": old_start, "new_start": new_start,
        "buyers": int(buyers.sum()), "never_bought_changed": int((~buyers & (d != 0)).sum()),
        "cells": [{"full": f, "inside": i, **counts(buyers & (full == f) & (inside == i))}
                  for f in (True, False) for i in (True, False)],
        "rose": int(rose.sum()), "fell": int(fell.sum()), "mean_change": float(d.mean()),
        "rose_first_in_new_window": int((rose & buyers & (first >= new_start)).sum()),
        "rose_still_from_first_purchase": int((rose & (cf_new > cf_old)).sum()),
        # risers whose first purchase predates the new window: the only ones the recount can test
        "rose_earlier_first": int((rose & buyers & (first < new_start)).sum()),
        "rose_earlier_first_still": int((rose & buyers & (first < new_start) & (cf_new > cf_old)).sum()),
        "first_before_old_window": counts(before), "first_inside_old_window": counts(inside),
        "counterfactual_mean_change": float((cf_new - cf_old).mean()),
        "first_purchase": first,
    }


def mover_profile(spec: schema.FeatureSpec, t: dict, store_dir, first_purchase: pd.Series | None, old_start) -> dict:
    """Recency of the customers crossing the decile, from other stored features."""
    cfg = spec.comparison["mover_profile"]
    recency = _stored(cfg["recency_feature"], store_dir)
    count = _stored(cfg["window_count_feature"], store_dir)
    groups = {"entering": t["entering_ids"], "leaving": t["leaving_ids"], "staying": t["staying_ids"],
              "subgroup": t["subgroup_ids"]}
    out = {"recency_feature": cfg["recency_feature"], "window_count_feature": cfg["window_count_feature"]}
    for label, ids in groups.items():
        row = {"n": len(ids), "no_recent": int((count.loc[ids] == 0).sum()),
               "median_recency": float(recency.loc[ids].median()) if ids else float("nan")}
        if first_purchase is not None:
            row["first_inside_old_window"] = int((first_purchase.reindex(ids) >= old_start).sum())
        out[label] = row
    return out


def compare_pair(spec: schema.FeatureSpec, old: dict, new: dict, store_dir,
                 transactions_path=DEFAULT_SOURCE_PATHS["transactions"]) -> dict:
    a, b = old["values"], new["values"]
    if not a.index.equals(b.index):
        raise ValueError(f"{spec.name} v{old['version']} and v{new['version']} cover different customers")
    if old["as_of"] != new["as_of"]:
        raise ValueError(f"{spec.name} v{old['version']} and v{new['version']} have different as_of dates")
    diff = b - a
    changed = diff != 0
    changed_ids = a.index[changed]
    result = {
        "old": old, "new": new, "n": int(a.size),
        "changed": int(changed.sum()), "increased": int((diff > 0).sum()), "decreased": int((diff < 0).sum()),
        "mean_abs_change_changed": float(diff[changed].abs().mean()) if changed.any() else 0.0,
        "max_abs_change": float(diff.abs().max()),
        "rank_correlation": rank_correlation(a, b),
        "deviation_share": {"old": deviation_share(a, changed_ids), "new": deviation_share(b, changed_ids)},
        "threshold": threshold_metric(spec, old, new, store_dir),
        "drivers": None, "profile": None,
    }
    first = None
    if "eligibility_drivers" in spec.comparison:
        first = first_purchase_dates(transactions_path, old["as_of"])
        result["drivers"] = eligibility_drivers(spec, old, new, store_dir, first)
    if "mover_profile" in spec.comparison:
        old_start = result["drivers"]["old_start"] if result["drivers"] else None
        result["profile"] = mover_profile(spec, result["threshold"], store_dir,
                                          first.reindex(a.index) if first is not None else None, old_start)
    return result


def compare_feature(spec: schema.FeatureSpec, store_dir) -> dict:
    versions = load_versions(spec.name, store_dir)
    if len(versions) < 2:
        raise ValueError(f"{spec.name} is versioned in schema.py but the store holds {len(versions)} version(s)")
    pairs = [(versions[i], versions[i + 1]) for i in range(len(versions) - 1)]
    if len(versions) > 2:
        pairs.append((versions[0], versions[-1]))
    return {"spec": spec, "versions": versions, "pairs": [compare_pair(spec, o, n, store_dir) for o, n in pairs]}


# Rendering ------------------------------------------------------------------------------
def _fmt(x) -> str:
    if isinstance(x, (int,)) or (isinstance(x, float) and x.is_integer() and abs(x) >= 1000):
        return f"{int(x):,}"
    return f"{x:.4g}" if isinstance(x, float) else str(x)


def _param_label(spec: schema.FeatureSpec, params: dict) -> str:
    tunable = {k: v for k, v in params.items() if k != "definition"}
    return ", ".join(f"{k} = {'none' if v is None else v}" for k, v in tunable.items())


def _verdict(rho: float) -> str:
    if rho >= 0.99:
        return ("the ordering is essentially unchanged, so a rank-based model (gradient-boosted trees, "
                "or any model fed this feature as a rank or quantile) would barely notice")
    if rho >= 0.95:
        return "the ordering shifts a little; a rank-based model would notice only at the margins"
    return ("the ordering changes materially, so any model consuming this feature would notice and "
            "would need retraining on the new version")


def _median_phrase(sa: dict, sb: dict, digits: int) -> str:
    if round(sa["p50"], digits) == round(sb["p50"], digits):
        return f"the median stays at {sb['p50']:.{digits}f}"
    return f"the median moves from {sa['p50']:.{digits}f} to {sb['p50']:.{digits}f}"


def _weight_sentence(p: dict, vo: str, vn: str) -> str:
    ds = p["deviation_share"]
    less = ds["new"]["ids_share"] < ds["old"]["ids_share"]
    return (
        f"In a linear fit the {p['changed']:,} changed customers carry {'less' if less else 'more'} weight in "
        f"{vn}: they hold {ds['old']['ids_share']:.1%} of the feature's total squared deviation from its mean in "
        f"{vo} and {ds['new']['ids_share']:.1%} in {vn}, and the largest single customer's share goes from "
        f"{ds['old']['max_share']:.1%} to {ds['new']['max_share']:.1%}."
    )


def _ordinal(n: int) -> str:
    suffix = "th" if 10 <= n % 100 <= 20 else {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"


def _top_decile_lines(p: dict, sa: dict, sb: dict, vo: str, vn: str) -> list[str]:
    t = p["threshold"]
    size = len(t["old"]["members"])
    deep = [r for r in t["leaving_old_ranks"] if r <= size // 2]
    rest = [r for r in t["leaving_old_ranks"] if r > size // 2]
    lines = [
        f"The maximum falls from {sa['max']:.2f} to {sb['max']:.2f} and the standard deviation from "
        f"{sa['std']:.2f} to {sb['std']:.2f}, while {_median_phrase(sa, sb, 2)}.",
        _weight_sentence(p, vo, vn),
        f"{t['entering']} customers enter and {t['leaving']} leave the {size}-customer top decile.",
    ]
    if t["entering_old_ranks"]:
        lines.append(
            f"The entering customers ranked {_ordinal(t['entering_old_ranks'][0])} to "
            f"{_ordinal(t['entering_old_ranks'][-1])} in {vo}, within {t['entering_old_ranks'][-1] - size} places "
            "of the cutoff."
        )
    if deep:
        tail = (f"; the other {len(rest)} ranked {_ordinal(rest[0])} to {_ordinal(rest[-1])}" if rest else "")
        lines.append(
            f"The leavers are not all at the edge: {len(deep)} of the {t['leaving']} ranked in the upper half of "
            f"{vo}'s decile (as high as {_ordinal(deep[0])}){tail}. So {vn} changes the top-spender segment from "
            "inside, not only at its boundary."
        )
    elif rest:
        lines.append(
            f"All {len(rest)} leavers ranked in the lower half of {vo}'s decile ({_ordinal(rest[0])} to "
            f"{_ordinal(rest[-1])}), so the segment moves only at its edge."
        )
    return lines


def _cap_ties_lines(p: dict, sa: dict, sb: dict, vo: str, vn: str) -> list[str]:
    t = p["threshold"]
    if t["below_unchanged"]:
        lines = [
            f"A cap can only change values above it, and the spec's check confirms {vn} changed nothing else: all "
            f"{t['n_below_old']:,} values below {t['below']:g} are identical in both versions, so the decline side "
            "that retention targeting acts on is the same in both."
        ]
    else:
        lines = [
            f"Values below {t['below']:g} changed ({t['n_below_old']:,} in {vo}, {t['n_below_new']:,} in {vn}), "
            "which a cap alone cannot do: the implementation needs checking."
        ]
    if t["changed_at_new_cap"] == t["changed"]:
        lines.append(
            f"All {t['changed']:,} changed customers now sit exactly at the cap of {t['caps']['new']:g} "
            f"({t['tied_old']:,} sat at {t['caps']['old']:g} in {vo}), so a tree loses only the ability to tell "
            f"those {t['changed']:,} apart."
        )
    else:
        lines.append(
            f"Only {t['changed_at_new_cap']:,} of the {t['changed']:,} changed customers sit at the new cap of "
            f"{t['caps']['new']:g}, so {vn} also moved values the cap should not reach."
        )
    lines += [
        f"The mean falls from {sa['mean']:.2f} to {sb['mean']:.2f} and the standard deviation from "
        f"{sa['std']:.2f} to {sb['std']:.2f}.",
        _weight_sentence(p, vo, vn),
    ]
    return lines


def _drivers_lines(dr: dict, vo: str, vn: str) -> list[str]:
    lo, ln = dr["old_lookback"], dr["new_lookback"]
    gone = dr["rose"] - dr["rose_still_from_first_purchase"]
    earlier, earlier_still = dr["rose_earlier_first"], dr["rose_earlier_first_still"]
    est, ins = dr["first_before_old_window"], dr["first_inside_old_window"]
    lines = [
        f"What drives the change: never-buyers are 0.0 in both versions by definition "
        f"({dr['never_bought_changed']} change), so every change is among the {dr['buyers']:,} buyers.",
        f"{dr['rose_first_in_new_window']} of the {dr['rose']} risers made their first purchase inside the "
        f"{ln}-month window. Every extra month {vo} counts for them comes before that first purchase, so {vo} "
        f"cannot exceed {vn} for them (by construction).",
        f"Counting eligible months from the first-purchase month instead of from signup removes those idle "
        f"months. The {dr['rose_first_in_new_window']} then stop rising, by construction. The test is the other "
        f"{earlier}, whose first purchase came before the {ln}-month window: {earlier - earlier_still} of them stop "
        f"rising too and {earlier_still} still rise. So for {gone} of the {dr['rose']} risers ({gone / dr['rose']:.0%}) "
        f"the rise comes only from idle months between signup and first purchase, the limitation feature_spec.md "
        f"accepts for this feature; {dr['rose_still_from_first_purchase']} ({dr['rose_still_from_first_purchase'] / dr['rose']:.0%}) "
        "were active in more of the recent months.",
        f"Buyers whose first purchase predates the {lo}-month window number {est['n']} (by construction they have "
        f"{lo} eligible months and no idle month before a first purchase in either window). {est['fell']} of them "
        f"fell and {est['rose']} rose, moving the mean by {est['contribution']:+.3f}; buyers whose first purchase "
        f"falls inside the window move it by {ins['contribution']:+.3f}.",
    ]
    cf, actual = dr["counterfactual_mean_change"], dr["mean_change"]
    if actual > 0 >= cf:
        lines.append(
            f"With eligible months counted from first purchase the mean would change by {cf:+.3f} instead of "
            f"{actual:+.3f}, so the whole of {vn}'s rise in the mean comes from the eligibility rule, not from "
            "buyers being more consistent recently."
        )
    else:
        share = "most" if gone > dr["rose"] / 2 else "a minority"
        lines.append(
            f"With eligible months counted from first purchase the mean would change by {cf:+.3f} instead of "
            f"{actual:+.3f}; the eligibility rule accounts for {share} of the risers."
        )
    return lines


def _profile_lines(pr: dict, t: dict, dr: dict | None, vo: str) -> list[str]:
    e, l, sub = pr["entering"], pr["leaving"], pr["subgroup"]
    days = schema.get_spec(pr["window_count_feature"]).fixed.get("window_days")
    lapsed = e["n"] and e["no_recent"] / e["n"] > sub["no_recent"] / sub["n"]
    lines = [
        f"{t['entering']} customers enter and {t['leaving']} leave the bottom decile among the "
        f"{t['subgroup_size']:,} buyers.",
    ]
    if e["n"]:
        lines.append(
            f"On an independent measure the entering customers {'look lapsed' if lapsed else 'do not look more lapsed than other buyers'}: "
            f"{e['no_recent']} of {e['n']} ({e['no_recent'] / e['n']:.0%}) have no purchase in the {days} days before "
            f"as_of ({pr['window_count_feature']} = 0), against {sub['no_recent'] / sub['n']:.0%} of all buyers, and their "
            f"median {pr['recency_feature']} is {e['median_recency']:.0f}, against {sub['median_recency']:.0f}."
        )
    if dr is not None and l["n"]:
        share_l = l["first_inside_old_window"] / l["n"]
        share_all = sub["first_inside_old_window"] / sub["n"]
        lines.append(
            f"{l['first_inside_old_window']} of the {l['n']} leaving customers made their first purchase inside the "
            f"{dr['old_lookback']}-month window, against {share_all:.0%} of all buyers"
            + (", consistent with the eligibility effect above." if share_l > share_all else ".")
        )
    return lines


def interpretation(spec: schema.FeatureSpec, p: dict) -> list[str]:
    old, new, t = p["old"], p["new"], p["threshold"]
    vo, vn = f"v{old['version']}", f"v{new['version']}"
    sa, sb = summary(old["values"]), summary(new["values"])
    rho = p["rank_correlation"]
    lines = [
        f"{p['changed']:,} of {p['n']:,} customers ({p['changed'] / p['n']:.1%}) change: {p['increased']:,} up, "
        f"{p['decreased']:,} down. Rank correlation is {rho:.3f}: {_verdict(rho)}.",
    ]
    if t["kind"] == "top_decile":
        lines += _top_decile_lines(p, sa, sb, vo, vn)
    elif t["kind"] == "cap_ties":
        lines += _cap_ties_lines(p, sa, sb, vo, vn)
    elif t["kind"] == "bottom_decile":
        direction = "rises" if sb["mean"] > sa["mean"] else "falls"
        lines.append(f"The mean {direction} from {sa['mean']:.3f} to {sb['mean']:.3f} and {_median_phrase(sa, sb, 3)}.")
        if p["drivers"]:
            lines += _drivers_lines(p["drivers"], vo, vn)
        if p["profile"]:
            lines += _profile_lines(p["profile"], t, p["drivers"], vo)
        else:
            lines.append(f"{t['entering']} customers enter and {t['leaving']} leave the bottom decile.")
    return lines


def _drivers_table(dr: dict, vo: str) -> list[str]:
    lo = dr["old_lookback"]
    out = [
        f"What drives the change, among the {dr['buyers']:,} buyers (eligible months counted from signup, as the "
        "feature defines them):",
        "",
        f"| {lo} eligible months in {vo} | First purchase inside {vo}'s window | Buyers | Rose | Fell | Unchanged | "
        "Contribution to the mean's change |",
        "|---|---|---|---|---|---|---|",
    ]
    for c in dr["cells"]:
        note = " (none: signing up inside the window means buying inside it)" if not c["full"] and not c["inside"] else ""
        out.append(f"| {'yes' if c['full'] else 'no'} | {'yes' if c['inside'] else 'no'}{note} | {c['n']:,} | "
                   f"{c['rose']} | {c['fell']} | {c['unchanged']} | {c['contribution']:+.4f} |")
    out += [
        "",
        f"- Risers whose first purchase falls inside the {dr['new_lookback']}-month window: "
        f"**{dr['rose_first_in_new_window']} of {dr['rose']}**.",
        f"- Risers that still rise with eligible months counted from the first-purchase month: "
        f"**{dr['rose_still_from_first_purchase']} of {dr['rose']}** ({dr['rose_earlier_first_still']} of the "
        f"{dr['rose_earlier_first']} whose first purchase came before the {dr['new_lookback']}-month window; the "
        f"{dr['rose_first_in_new_window']} who first bought inside it cannot, by construction).",
        f"- Change in the mean: **{dr['mean_change']:+.4f}** as defined; **{dr['counterfactual_mean_change']:+.4f}** "
        "with eligible months counted from first purchase.",
    ]
    return out


def _profile_table(pr: dict, dr: dict | None, vo: str) -> list[str]:
    days = schema.get_spec(pr["window_count_feature"]).fixed.get("window_days")
    inside = dr is not None
    head = f"| Group | Customers | No purchase in {days} days | Median `{pr['recency_feature']}` |"
    sep = "|---|---|---|---|"
    if inside:
        head += f" First purchase inside {vo}'s window |"
        sep += "---|"
    out = ["Who crosses the bottom decile, from other stored features:", "", head, sep]
    for label, name in (("entering", "Entering"), ("leaving", "Leaving"), ("staying", "Staying"), ("subgroup", "All buyers")):
        g = pr[label]
        row = (f"| {name} | {g['n']:,} | {g['no_recent']:,} ({g['no_recent'] / g['n']:.0%}) | {g['median_recency']:.0f} |"
               if g["n"] else f"| {name} | 0 | – | – |")
        if inside:
            row += f" {g['first_inside_old_window']:,} ({g['first_inside_old_window'] / g['n']:.0%}) |" if g["n"] else " – |"
        out.append(row)
    return out


def render(results: list[dict], store_dir) -> str:
    root = store.store_root(store_dir=store_dir)
    as_of = results[0]["versions"][0]["as_of"].date() if results else "n/a"
    out = [
        "# VERSION_COMPARISON.md",
        "",
        "> Generated by `scripts/compare_feature_versions.py`. Version values are loaded from "
        f"`{display_path(store.registry_path(store_dir=root))}` with `get_version`, never recomputed. "
        "Rank correlation is Spearman's ρ computed as `a.rank().corr(b.rank())`. Deciles hold every customer "
        "at or beyond that version's own 90th (10th) percentile, linear interpolation, ties included. "
        "The `active_month_share` driver analysis also reads first-purchase dates from "
        "`data/transactions.db` (cut at as_of), because no stored feature holds them; its active-month counts "
        "come from the stored shares (share × eligible months), not from a recount. Every sentence of an "
        "interpretation reads off a number in its section; anything true by definition is labelled so.",
        "",
        f"Snapshot as_of: **{as_of}**. No churn model or ranking exists yet, so each section compares "
        "the feature's own distribution, rank order and the threshold metric feature_spec.md assigns it.",
        "",
        "| Feature | Versions | Customers changed | Rank correlation | Threshold metric |",
        "|---|---|---|---|---|",
    ]
    for r in results:
        for p in r["pairs"]:
            t = p["threshold"]
            if t["kind"] == "cap_ties":
                metric = (f"{t['tied_new']} tied at {t['caps']['new']:g}; all {t['n_below_old']} values "
                          f"below {t['below']:g} {'unchanged' if t['below_unchanged'] else 'CHANGED'}")
            else:
                metric = f"{t['side']} decile: {t['entering']} in, {t['leaving']} out"
            out.append(f"| `{r['spec'].name}` | v{p['old']['version']} → v{p['new']['version']} | "
                       f"{p['changed']:,} of {p['n']:,} | {p['rank_correlation']:.3f} | {metric} |")
    out.append("")

    for r in results:
        spec = r["spec"]
        out += [f"## {spec.spec_id} — `{spec.name}`", ""]
        for v in r["versions"]:
            schema_v = next((sv for sv in spec.versions if sv.parameters == v["parameters"]), None)
            reason = schema_v.change_reason if schema_v else "parameters not in features/schema.py"
            out.append(f"- **v{v['version']}** ({_param_label(spec, v['parameters'])}"
                       f"{', latest' if v['is_latest'] else ''}): `{v['parquet_path']}`. {reason}")
        out.append("")
        for p in r["pairs"]:
            old, new, t = p["old"], p["new"], p["threshold"]
            vo, vn = f"v{old['version']}", f"v{new['version']}"
            out += [f"### {vo} → {vn}", ""]
            stats = pd.DataFrame({vo: summary(old["values"]), vn: summary(new["values"])})
            out += ["| Statistic | " + " | ".join(stats.columns) + " |", "|---|" + "---|" * len(stats.columns)]
            for stat, row in stats.iterrows():
                out.append(f"| {stat} | " + " | ".join(_fmt(x) for x in row) + " |")
            ds = p["deviation_share"]
            out += [
                "",
                f"- Customers changed: **{p['changed']:,}** of {p['n']:,} ({p['increased']:,} up, "
                f"{p['decreased']:,} down); mean absolute change among them {p['mean_abs_change_changed']:.4g}, "
                f"largest {p['max_abs_change']:.4g}.",
                f"- Rank correlation between versions: **{p['rank_correlation']:.3f}**.",
                f"- Share of the total squared deviation from the mean held by the changed customers: "
                f"{ds['old']['ids_share']:.1%} in {vo}, {ds['new']['ids_share']:.1%} in {vn}; by the largest single "
                f"customer: {ds['old']['max_share']:.1%} in {vo}, {ds['new']['max_share']:.1%} in {vn}.",
            ]
            if t["kind"] == "cap_ties":
                out += [
                    f"- Tied at the cap: {t['tied_old']:,} at {t['caps']['old']:g} in {vo}, "
                    f"**{t['tied_new']:,} at {t['caps']['new']:g}** in {vn}; changed customers now exactly at the "
                    f"cap: {t['changed_at_new_cap']:,} of {t['changed']:,}.",
                    f"- Values below {t['below']:g}: {t['n_below_old']:,} in {vo}, "
                    f"{t['n_below_new']:,} in {vn}; all unchanged: **{t['below_unchanged']}**.",
                ]
            else:
                group = (f" within {t['subgroup']['feature']} = {t['subgroup']['value']} "
                         f"({t['subgroup_size']:,} customers)") if t["subgroup"] else ""
                for label, d in (("old", t["old"]), ("new", t["new"])):
                    vnum = (old if label == "old" else new)["version"]
                    pct = "90th" if t["side"] == "top" else "10th"
                    out.append(
                        f"- v{vnum} {t['side']} decile{group}: {pct} percentile {d['threshold']:.4g}; "
                        f"**{len(d['members']):,} customers**; {d['tied_at_boundary']:,} tied at the boundary "
                        f"value {d['boundary_value']:.4g}."
                    )
                out.append(f"- Moving across the {t['side']}-decile threshold: **{t['entering']} enter, "
                           f"{t['leaving']} leave**.")
                if t["kind"] == "top_decile":
                    out.append(
                        f"- {vo} ranks of the movers (1 = highest {vo} value): entering "
                        f"{', '.join(map(str, t['entering_old_ranks']))}; leaving "
                        f"{', '.join(map(str, t['leaving_old_ranks']))}."
                    )
            if p["drivers"]:
                out += [""] + _drivers_table(p["drivers"], vo)
            if p["profile"]:
                out += [""] + _profile_table(p["profile"], p["drivers"], vo)
            out += ["", "**Interpretation.** " + " ".join(interpretation(spec, p)), ""]
    return "\n".join(out).rstrip() + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--store-dir", default=None, help="Feature store root (default: feature_store/).")
    parser.add_argument("--out", default=str(DEFAULT_OUT), help="Markdown report path.")
    args = parser.parse_args(argv)

    versioned = [f for f in schema.FEATURE_SCHEMA if f.is_versioned]
    try:
        results = [compare_feature(spec, args.store_dir) for spec in versioned]
    except (KeyError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    for r in results:
        for p in r["pairs"]:
            print(f"{r['spec'].name} v{p['old']['version']} -> v{p['new']['version']}: {p['changed']} changed, "
                  f"rank correlation {p['rank_correlation']:.3f}, threshold {json.dumps(_brief(p['threshold']))}")
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(render(results, args.store_dir), encoding="utf-8")
    print(f"report written to {display_path(out)}")
    return 0


def _brief(t: dict) -> dict:
    if t["kind"] == "cap_ties":
        return {k: t[k] for k in ("tied_old", "tied_new", "n_below_old", "below_unchanged")}
    return {"entering": t["entering"], "leaving": t["leaving"],
            "old_members": len(t["old"]["members"]), "new_members": len(t["new"]["members"]),
            "old_ties": t["old"]["tied_at_boundary"], "new_ties": t["new"]["tied_at_boundary"]}


if __name__ == "__main__":
    sys.exit(main())
