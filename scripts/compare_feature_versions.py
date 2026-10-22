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
  linear interpolation, ties included.

No churn model or ranking exists yet, so the comparison is on the feature's own
distribution and rank order. The results and their interpretation go to
VERSION_COMPARISON.md.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd

from feature_store import store
from features import schema
from features.build import REPO_ROOT
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
        return {
            "kind": kind, "side": side, "subgroup": subgroup, "subgroup_size": int(a.size),
            "old": da, "new": db, "entering": len(entering), "leaving": len(leaving),
            # direction of each mover's own value change, to back the interpretation with data
            "entering_fell": int((b.loc[entering] < a.loc[entering]).sum()),
            "leaving_rose": int((b.loc[leaving] > a.loc[leaving]).sum()),
        }
    if kind == "cap_ties":
        param, below = spec.comparison["parameter"], spec.comparison["unchanged_below"]
        caps = {"old": old["parameters"][param], "new": new["parameters"][param]}
        low = a.index[a < below]
        return {
            "kind": kind, "parameter": param, "caps": caps,
            "tied_old": int((a == caps["old"]).sum()), "tied_new": int((b == caps["new"]).sum()),
            "below": below, "n_below_old": int(low.size), "n_below_new": int((b < below).sum()),
            "below_unchanged": bool((a.loc[low] == b.loc[low]).all() and set(low) == set(b.index[b < below])),
        }
    raise ValueError(f"unknown comparison kind {kind!r} for {spec.name}")


def compare_pair(spec: schema.FeatureSpec, old: dict, new: dict, store_dir) -> dict:
    a, b = old["values"], new["values"]
    if not a.index.equals(b.index):
        raise ValueError(f"{spec.name} v{old['version']} and v{new['version']} cover different customers")
    if old["as_of"] != new["as_of"]:
        raise ValueError(f"{spec.name} v{old['version']} and v{new['version']} have different as_of dates")
    diff = b - a
    changed = diff != 0
    return {
        "old": old, "new": new, "n": int(a.size),
        "changed": int(changed.sum()), "increased": int((diff > 0).sum()), "decreased": int((diff < 0).sum()),
        "mean_abs_change_changed": float(diff[changed].abs().mean()) if changed.any() else 0.0,
        "max_abs_change": float(diff.abs().max()),
        "rank_correlation": rank_correlation(a, b),
        "threshold": threshold_metric(spec, old, new, store_dir),
    }


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


def interpretation(spec: schema.FeatureSpec, p: dict) -> list[str]:
    old, new, t = p["old"], p["new"], p["threshold"]
    sa, sb = summary(old["values"]), summary(new["values"])
    rho = p["rank_correlation"]
    share = p["changed"] / p["n"]
    lines = [
        f"{p['changed']:,} of {p['n']:,} customers ({share:.1%}) change: {p['increased']:,} up, "
        f"{p['decreased']:,} down. Rank correlation is {rho:.3f}: {_verdict(rho)}.",
    ]
    if t["kind"] == "top_decile":
        lines.append(
            f"The maximum falls from {sa['max']:.2f} to {sb['max']:.2f} and the standard deviation from "
            f"{sa['std']:.2f} to {sb['std']:.2f}, while {_median_phrase(sa, sb, 2)}. "
            f"{t['entering']} customers enter and {t['leaving']} leave the top decile "
            f"(of {len(t['new']['members']):,}), so a 'top spenders' segment moves only at its edge. A "
            "linear or distance-based model would notice more than a tree: the largest values lose "
            "most of their leverage on a fitted slope."
        )
    elif t["kind"] == "cap_ties":
        if t["below_unchanged"]:
            lines.append(
                f"The cap only acts on growth: all {t['n_below_old']:,} values below {t['below']:g} "
                "(spend falling or stopped) are unchanged, so for retention targeting, which acts on "
                "that end, both versions rank the at-risk customers identically."
            )
        else:
            lines.append(
                f"Values below {t['below']:g} changed ({t['n_below_old']:,} before, {t['n_below_new']:,} "
                "after), so the cap reaches the decline side and changes which customers look at risk."
            )
        lines.append(
            f"{t['tied_new']:,} customers now share the cap value {t['caps']['new']:g} (against "
            f"{t['tied_old']:,} at {t['caps']['old']:g} before), and the mean falls from {sa['mean']:.2f} "
            f"to {sb['mean']:.2f} because the long right tail is cut. A linear model would notice the "
            f"smaller scale; a tree would only lose the ability to split among the {t['tied_new']:,} "
            "tied growth customers."
        )
    elif t["kind"] == "bottom_decile":
        group = (f" among the {t['subgroup_size']:,} customers with "
                 f"{t['subgroup']['feature']} = {t['subgroup']['value']}") if t["subgroup"] else ""
        lines.append(
            f"The mean moves from {sa['mean']:.3f} to {sb['mean']:.3f} and {_median_phrase(sa, sb, 3)}, "
            "but the reordering is what matters. In the bottom decile"
            f"{group} (the least consistent buyers, the retention-relevant end), {t['entering']} "
            f"customers enter and {t['leaving']} leave. {t['entering_fell']} of the {t['entering']} "
            "entering have a lower share over the shorter window, i.e. they bought less in its months "
            f"than in the earlier part of the old window; {t['leaving_rose']} of the {t['leaving']} "
            "leaving have a higher one. The shorter window stops crediting a habit that has since faded."
        )
    return lines


def render(results: list[dict], store_dir) -> str:
    root = store.store_root(store_dir=store_dir)
    as_of = results[0]["versions"][0]["as_of"].date() if results else "n/a"
    out = [
        "# VERSION_COMPARISON.md",
        "",
        "> Generated by `scripts/compare_feature_versions.py` from the versions stored in "
        f"`{display_path(store.registry_path(store_dir=root))}` (loaded with `get_version`, not "
        "recomputed). Rank correlation is Spearman's ρ computed as `a.rank().corr(b.rank())`. "
        "Deciles hold every customer at or beyond that version's own 90th (10th) percentile, "
        "linear interpolation, ties included.",
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
            out += [f"### v{old['version']} → v{new['version']}", ""]
            stats = pd.DataFrame({f"v{old['version']}": summary(old["values"]), f"v{new['version']}": summary(new["values"])})
            out += ["| Statistic | " + " | ".join(stats.columns) + " |", "|---|" + "---|" * len(stats.columns)]
            for stat, row in stats.iterrows():
                out.append(f"| {stat} | " + " | ".join(_fmt(x) for x in row) + " |")
            out += [
                "",
                f"- Customers changed: **{p['changed']:,}** of {p['n']:,} ({p['increased']:,} up, "
                f"{p['decreased']:,} down); mean absolute change among them {p['mean_abs_change_changed']:.4g}, "
                f"largest {p['max_abs_change']:.4g}.",
                f"- Rank correlation between versions: **{p['rank_correlation']:.3f}**.",
            ]
            if t["kind"] == "cap_ties":
                out += [
                    f"- Tied at the cap: {t['tied_old']:,} at {t['caps']['old']:g} in v{old['version']}, "
                    f"**{t['tied_new']:,} at {t['caps']['new']:g}** in v{new['version']}.",
                    f"- Values below {t['below']:g}: {t['n_below_old']:,} in v{old['version']}, "
                    f"{t['n_below_new']:,} in v{new['version']}; all unchanged: **{t['below_unchanged']}**.",
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
