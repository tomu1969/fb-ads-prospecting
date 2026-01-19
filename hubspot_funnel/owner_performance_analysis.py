#!/usr/bin/env python3
"""
Owner Performance Analysis

Comprehensive performance report comparing deal owners across:
- Deal metrics (volume, win rate, revenue)
- Velocity metrics (time to close, demo conversion)
- Quality metrics (no-show rate, hygiene rate)
- Activity metrics (calls, meetings, emails)

Usage:
    python hubspot_funnel/owner_performance_analysis.py
    python hubspot_funnel/owner_performance_analysis.py --days 90
"""

import os
import sys
import json
import argparse
import requests
from datetime import datetime, timedelta
from collections import Counter, defaultdict
from pathlib import Path

# =============================================================================
# CONFIGURATION
# =============================================================================

# Read API key from .env
def load_api_key():
    env_path = Path(__file__).parent.parent / ".env"
    with open(env_path) as f:
        for line in f:
            if line.startswith("HUBSPOT_API_KEY="):
                return line.strip().split("=", 1)[1]
    raise ValueError("HUBSPOT_API_KEY not found in .env")

API_KEY = load_api_key()
BASE_URL = "https://api.hubapi.com"

# Pipeline configuration
PIPELINE_ID = "719833388"
STAGES = {
    "calificado": "1102547555",
    "demo_agendado": "1049659495",
    "demo_presentado": "1049659496",
    "propuesta_aceptada": "1110769786",
    "suscripcion_pago": "1092762538",      # Closed Won (Paid)
    "suscripcion_trial": "1167117482",     # Closed Won (Trial)
    "closed_lost": "1102535235",
    "churn": "1148080018",
}

# Closed Won = Paid + Trial subscriptions
CLOSED_WON_STAGES = {STAGES["suscripcion_pago"], STAGES["suscripcion_trial"]}
CLOSED_LOST_STAGE = STAGES["closed_lost"]
DEMO_PRESENTADO_STAGE = STAGES["demo_presentado"]

# Loss reason labels
LOSS_REASON_LABELS = {
    "no_asistio_al_demo_y_no_se_pudo_reagendar": "No asistió al demo",
    "precio_alto_vs_expectativa": "Precio alto",
    "eligio_competidor": "Eligió competidor",
    "valorroi_no_claro": "Valor/ROI no claro",
    "falta_funcionalidad_clave": "Falta funcionalidad",
    "riesgocomplejidad_de_implementacion": "Riesgo/Complejidad",
    "va_a_construir_solucion_inhouse": "Solución in-house",
    "mal_timing__budget_freeze": "Mal timing/Budget",
    "higiene_de_lead_duplicadospamcontacto_invalido": "Higiene de lead",
    "no_interesado": "No interesado",
}


def load_owners():
    """Load target deal owners from JSON file."""
    owners_file = Path(__file__).parent / "config" / "deal_owners.json"
    with open(owners_file) as f:
        data = json.load(f)
    return {o["ownerId"]: o["name"] for o in data["owners"]}


# =============================================================================
# API HELPERS
# =============================================================================

def api_get(endpoint: str) -> dict:
    """Make GET request to HubSpot API."""
    headers = {"Authorization": f"Bearer {API_KEY}"}
    resp = requests.get(f"{BASE_URL}{endpoint}", headers=headers)
    resp.raise_for_status()
    return resp.json()


def api_post(endpoint: str, payload: dict) -> dict:
    """Make POST request to HubSpot API."""
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json"
    }
    resp = requests.post(f"{BASE_URL}{endpoint}", headers=headers, json=payload)
    resp.raise_for_status()
    return resp.json()


# =============================================================================
# DATA FETCHING
# =============================================================================

def fetch_deals(owner_ids: list, days_back: int = 180) -> list:
    """Fetch all deals for specified owners within date range."""
    cutoff_date = datetime.now() - timedelta(days=days_back)
    cutoff_ms = int(cutoff_date.timestamp() * 1000)

    all_deals = []

    for owner_id in owner_ids:
        after = None
        while True:
            payload = {
                "filterGroups": [{
                    "filters": [
                        {"propertyName": "hubspot_owner_id", "operator": "EQ", "value": owner_id},
                        {"propertyName": "createdate", "operator": "GTE", "value": str(cutoff_ms)}
                    ]
                }],
                "properties": [
                    "dealname", "amount", "dealstage", "closedate", "createdate",
                    "hubspot_owner_id", "motivo_de_cerrada_perdida",
                    "detalles_de_oportunidad_perdida", "hs_analytics_source",
                    "pipeline"
                ],
                "limit": 100
            }
            if after:
                payload["after"] = after

            data = api_post("/crm/v3/objects/deals/search", payload)
            all_deals.extend(data.get("results", []))

            paging = data.get("paging", {})
            if paging.get("next", {}).get("after"):
                after = paging["next"]["after"]
            else:
                break

    return all_deals


def fetch_engagements(owner_ids: list, days_back: int = 90) -> dict:
    """Fetch calls, meetings, and emails for specified owners."""
    cutoff_date = datetime.now() - timedelta(days=days_back)
    cutoff_ms = int(cutoff_date.timestamp() * 1000)

    engagements = {"calls": [], "meetings": [], "emails": []}

    for obj_type in ["calls", "meetings", "emails"]:
        timestamp_prop = "hs_timestamp" if obj_type != "emails" else "hs_timestamp"

        for owner_id in owner_ids:
            after = None
            while True:
                payload = {
                    "filterGroups": [{
                        "filters": [
                            {"propertyName": "hubspot_owner_id", "operator": "EQ", "value": owner_id},
                            {"propertyName": timestamp_prop, "operator": "GTE", "value": str(cutoff_ms)}
                        ]
                    }],
                    "properties": ["hubspot_owner_id", timestamp_prop, "hs_call_status", "hs_meeting_outcome"],
                    "limit": 100
                }
                if after:
                    payload["after"] = after

                try:
                    data = api_post(f"/crm/v3/objects/{obj_type}/search", payload)
                    engagements[obj_type].extend(data.get("results", []))

                    paging = data.get("paging", {})
                    if paging.get("next", {}).get("after"):
                        after = paging["next"]["after"]
                    else:
                        break
                except requests.HTTPError as e:
                    print(f"Warning: Could not fetch {obj_type} for owner {owner_id}: {e}", file=sys.stderr)
                    break

    return engagements


# =============================================================================
# DEAL PARSING
# =============================================================================

def parse_deals(deals: list, owners: dict) -> list:
    """Parse raw deals into structured format."""
    parsed = []
    for deal in deals:
        props = deal.get("properties", {})

        # Parse dates
        closedate = None
        createdate = None
        if props.get("closedate"):
            try:
                closedate = datetime.fromisoformat(props["closedate"].replace("Z", "+00:00"))
            except:
                pass
        if props.get("createdate"):
            try:
                createdate = datetime.fromisoformat(props["createdate"].replace("Z", "+00:00"))
            except:
                pass

        # Parse amount
        amount = 0
        if props.get("amount"):
            try:
                amount = float(props["amount"])
            except:
                pass

        owner_id = props.get("hubspot_owner_id", "")
        stage = props.get("dealstage", "")

        # Determine deal status
        if stage in CLOSED_WON_STAGES:
            status = "won"
        elif stage == CLOSED_LOST_STAGE:
            status = "lost"
        else:
            status = "open"

        # Check if deal reached Demo Presentado stage
        # (We'll use current stage or closed stage to determine this)
        reached_demo = stage == DEMO_PRESENTADO_STAGE or stage in CLOSED_WON_STAGES or stage == CLOSED_LOST_STAGE

        parsed.append({
            "id": deal.get("id"),
            "name": props.get("dealname", "Unknown"),
            "amount": amount,
            "stage": stage,
            "status": status,
            "closedate": closedate,
            "createdate": createdate,
            "owner_id": owner_id,
            "owner_name": owners.get(owner_id, owner_id),
            "loss_reason": props.get("motivo_de_cerrada_perdida", ""),
            "loss_details": props.get("detalles_de_oportunidad_perdida", ""),
            "lead_source": props.get("hs_analytics_source", ""),
            "reached_demo": reached_demo,
        })

    return parsed


# =============================================================================
# METRICS CALCULATION
# =============================================================================

def calculate_deal_metrics(deals: list, owners: dict, days: int = 90) -> dict:
    """Calculate deal metrics by owner."""
    now = datetime.now(tz=deals[0]["createdate"].tzinfo if deals and deals[0]["createdate"] else None)
    cutoff = now - timedelta(days=days)
    prev_cutoff = cutoff - timedelta(days=days)

    # Split into current and previous period
    current = [d for d in deals if d["createdate"] and d["createdate"] >= cutoff]
    previous = [d for d in deals if d["createdate"] and prev_cutoff <= d["createdate"] < cutoff]

    metrics = {}

    for owner_name in owners.values():
        # Current period
        owner_current = [d for d in current if d["owner_name"] == owner_name]
        owner_previous = [d for d in previous if d["owner_name"] == owner_name]

        # Volume metrics
        total = len(owner_current)
        won = len([d for d in owner_current if d["status"] == "won"])
        lost = len([d for d in owner_current if d["status"] == "lost"])
        open_deals = len([d for d in owner_current if d["status"] == "open"])

        win_rate = (won / (won + lost) * 100) if (won + lost) > 0 else 0

        # Revenue metrics
        revenue_won = sum(d["amount"] for d in owner_current if d["status"] == "won")
        revenue_lost = sum(d["amount"] for d in owner_current if d["status"] == "lost")
        avg_deal_won = revenue_won / won if won > 0 else 0

        # Velocity metrics
        won_deals = [d for d in owner_current if d["status"] == "won" and d["closedate"] and d["createdate"]]
        lost_deals = [d for d in owner_current if d["status"] == "lost" and d["closedate"] and d["createdate"]]

        avg_days_won = 0
        if won_deals:
            days_to_close = [(d["closedate"] - d["createdate"]).days for d in won_deals]
            avg_days_won = sum(days_to_close) / len(days_to_close)

        avg_days_lost = 0
        if lost_deals:
            days_to_close = [(d["closedate"] - d["createdate"]).days for d in lost_deals]
            avg_days_lost = sum(days_to_close) / len(days_to_close)

        # Demo conversion metrics
        demo_deals = [d for d in owner_current if d["reached_demo"]]
        demo_won = len([d for d in demo_deals if d["status"] == "won"])
        demo_lost = len([d for d in demo_deals if d["status"] == "lost"])
        demo_open = len([d for d in demo_deals if d["status"] == "open"])
        demo_total = len(demo_deals)
        demo_win_rate = (demo_won / demo_total * 100) if demo_total > 0 else 0
        demo_loss_rate = (demo_lost / demo_total * 100) if demo_total > 0 else 0

        # Quality metrics (from lost deals)
        noshow_count = len([d for d in owner_current if "no_asistio" in (d.get("loss_reason") or "").lower()])
        higiene_count = len([d for d in owner_current if "higiene" in (d.get("loss_reason") or "").lower()])
        noshow_rate = (noshow_count / lost * 100) if lost > 0 else 0
        higiene_rate = (higiene_count / lost * 100) if lost > 0 else 0
        quality_score = 100 - noshow_rate - higiene_rate

        # Previous period for comparison
        prev_total = len(owner_previous)
        prev_won = len([d for d in owner_previous if d["status"] == "won"])
        prev_lost = len([d for d in owner_previous if d["status"] == "lost"])
        prev_win_rate = (prev_won / (prev_won + prev_lost) * 100) if (prev_won + prev_lost) > 0 else 0

        metrics[owner_name] = {
            # Volume
            "total": total,
            "won": won,
            "lost": lost,
            "open": open_deals,
            "win_rate": win_rate,
            # Revenue
            "revenue_won": revenue_won,
            "revenue_lost": revenue_lost,
            "avg_deal_won": avg_deal_won,
            # Velocity
            "avg_days_won": avg_days_won,
            "avg_days_lost": avg_days_lost,
            # Demo conversion
            "demo_total": demo_total,
            "demo_won": demo_won,
            "demo_lost": demo_lost,
            "demo_open": demo_open,
            "demo_win_rate": demo_win_rate,
            "demo_loss_rate": demo_loss_rate,
            # Quality
            "noshow_count": noshow_count,
            "noshow_rate": noshow_rate,
            "higiene_count": higiene_count,
            "higiene_rate": higiene_rate,
            "quality_score": quality_score,
            # Comparison
            "prev_total": prev_total,
            "prev_won": prev_won,
            "prev_lost": prev_lost,
            "prev_win_rate": prev_win_rate,
            # Raw deals for detailed analysis
            "deals": owner_current,
        }

    return metrics


def calculate_activity_metrics(engagements: dict, owners: dict, deal_metrics: dict) -> dict:
    """Calculate activity metrics by owner."""
    activity = {}

    for owner_name, owner_id in [(v, k) for k, v in owners.items()]:
        calls = len([e for e in engagements["calls"]
                    if e.get("properties", {}).get("hubspot_owner_id") == owner_id])
        meetings = len([e for e in engagements["meetings"]
                       if e.get("properties", {}).get("hubspot_owner_id") == owner_id])
        emails = len([e for e in engagements["emails"]
                     if e.get("properties", {}).get("hubspot_owner_id") == owner_id])

        total_activity = calls + meetings + emails
        total_deals = deal_metrics.get(owner_name, {}).get("total", 0)
        activity_per_deal = total_activity / total_deals if total_deals > 0 else 0

        activity[owner_name] = {
            "calls": calls,
            "meetings": meetings,
            "emails": emails,
            "total": total_activity,
            "per_deal": activity_per_deal,
        }

    return activity


def get_top_deals(deals: list, status: str = "won", limit: int = 5) -> list:
    """Get top deals by amount."""
    filtered = [d for d in deals if d["status"] == status and d["amount"] > 0]
    sorted_deals = sorted(filtered, key=lambda x: x["amount"], reverse=True)

    top = []
    for d in sorted_deals[:limit]:
        days_to_close = 0
        if d["closedate"] and d["createdate"]:
            days_to_close = (d["closedate"] - d["createdate"]).days

        top.append({
            "name": d["name"],
            "owner": d["owner_name"],
            "amount": d["amount"],
            "days_to_close": days_to_close,
        })

    return top


# =============================================================================
# REPORT GENERATION
# =============================================================================

def generate_report(deal_metrics: dict, activity_metrics: dict, all_deals: list, days: int) -> str:
    """Generate markdown report."""
    now = datetime.now()
    period_end = now.strftime("%Y-%m-%d")
    period_start = (now - timedelta(days=days)).strftime("%Y-%m-%d")
    prev_start = (now - timedelta(days=days * 2)).strftime("%Y-%m-%d")

    lines = []
    lines.append("# Owner Performance Report")
    lines.append(f"**Generated:** {now.strftime('%Y-%m-%d %H:%M')}")
    lines.append(f"**Period:** {period_start} to {period_end} vs {prev_start} to {period_start}")
    lines.append("")

    # =========================================================================
    # EXECUTIVE SUMMARY
    # =========================================================================
    lines.append("## Executive Summary")
    lines.append("")
    lines.append("| Owner | Won | Lost | Win Rate | Revenue Won | Quality | Activity/Deal |")
    lines.append("|-------|-----|------|----------|-------------|---------|---------------|")

    for owner, m in sorted(deal_metrics.items(), key=lambda x: x[1]["revenue_won"], reverse=True):
        a = activity_metrics.get(owner, {})
        quality_emoji = "🟢" if m["quality_score"] >= 60 else "🟡" if m["quality_score"] >= 40 else "🔴"
        lines.append(f"| {owner} | {m['won']} | {m['lost']} | {m['win_rate']:.0f}% | ${m['revenue_won']:,.0f} | {quality_emoji} {m['quality_score']:.0f} | {a.get('per_deal', 0):.1f} |")
    lines.append("")

    # =========================================================================
    # 1. DEAL VOLUME & CONVERSION
    # =========================================================================
    lines.append("---")
    lines.append("## 1. Deal Volume & Conversion")
    lines.append("")

    # 1.1 Deals by Stage
    lines.append("### 1.1 Deals by Stage")
    lines.append("")
    lines.append("| Owner | Open | Won | Lost | Total | Win Rate |")
    lines.append("|-------|------|-----|------|-------|----------|")
    for owner, m in deal_metrics.items():
        wr_emoji = "🟢" if m["win_rate"] >= 30 else "🟡" if m["win_rate"] >= 15 else "🔴"
        lines.append(f"| {owner} | {m['open']} | {m['won']} | {m['lost']} | {m['total']} | {wr_emoji} {m['win_rate']:.0f}% |")
    lines.append("")

    # 1.2 Period Comparison
    lines.append("### 1.2 Period Comparison (Last 90d vs Prev 90d)")
    lines.append("")
    lines.append("| Owner | Prev Deals | Curr Deals | Change | Prev Win% | Curr Win% |")
    lines.append("|-------|------------|------------|--------|-----------|-----------|")
    for owner, m in deal_metrics.items():
        change = ((m["total"] - m["prev_total"]) / m["prev_total"] * 100) if m["prev_total"] > 0 else 0
        change_str = f"+{change:.0f}%" if change >= 0 else f"{change:.0f}%"
        lines.append(f"| {owner} | {m['prev_total']} | {m['total']} | {change_str} | {m['prev_win_rate']:.0f}% | {m['win_rate']:.0f}% |")
    lines.append("")

    # =========================================================================
    # 2. REVENUE PERFORMANCE
    # =========================================================================
    lines.append("---")
    lines.append("## 2. Revenue Performance")
    lines.append("")

    # 2.1 Revenue by Owner
    lines.append("### 2.1 Revenue by Owner")
    lines.append("")
    lines.append("| Owner | Revenue Won | Revenue Lost | Net | Avg Deal (Won) |")
    lines.append("|-------|-------------|--------------|-----|----------------|")
    for owner, m in sorted(deal_metrics.items(), key=lambda x: x[1]["revenue_won"], reverse=True):
        net = m["revenue_won"] - m["revenue_lost"]
        net_str = f"${net:,.0f}" if net >= 0 else f"-${abs(net):,.0f}"
        lines.append(f"| {owner} | ${m['revenue_won']:,.0f} | ${m['revenue_lost']:,.0f} | {net_str} | ${m['avg_deal_won']:,.0f} |")
    lines.append("")

    # 2.2 Top Won Deals
    top_won = get_top_deals(all_deals, "won", 5)
    if top_won:
        lines.append("### 2.2 Top Deals (Won)")
        lines.append("")
        lines.append("| Deal | Owner | Amount | Days to Close |")
        lines.append("|------|-------|--------|---------------|")
        for d in top_won:
            lines.append(f"| {d['name'][:35]} | {d['owner']} | ${d['amount']:,.0f} | {d['days_to_close']} |")
        lines.append("")

    # =========================================================================
    # 3. PIPELINE VELOCITY & DEMO CONVERSION
    # =========================================================================
    lines.append("---")
    lines.append("## 3. Pipeline Velocity & Demo Conversion")
    lines.append("")

    # 3.1 Time to Close
    lines.append("### 3.1 Time to Close")
    lines.append("")
    lines.append("| Owner | Avg Days to Won | Avg Days to Lost |")
    lines.append("|-------|-----------------|------------------|")
    for owner, m in deal_metrics.items():
        lines.append(f"| {owner} | {m['avg_days_won']:.0f} | {m['avg_days_lost']:.0f} |")
    lines.append("")

    # 3.2 Demo Presentado Conversion
    lines.append("### 3.2 Demo Presentado Conversion (Key Metric)")
    lines.append("")
    lines.append("| Owner | Demos | → Won | → Lost | → Open | Win Rate | Loss Rate |")
    lines.append("|-------|-------|-------|--------|--------|----------|-----------|")
    for owner, m in deal_metrics.items():
        wr_emoji = "🟢" if m["demo_win_rate"] >= 25 else "🟡" if m["demo_win_rate"] >= 10 else "🔴"
        lines.append(f"| {owner} | {m['demo_total']} | {m['demo_won']} | {m['demo_lost']} | {m['demo_open']} | {wr_emoji} {m['demo_win_rate']:.0f}% | {m['demo_loss_rate']:.0f}% |")
    lines.append("")
    lines.append("*Tracks deals that reached 'Demo Presentado' stage and their outcomes*")
    lines.append("")

    # =========================================================================
    # 4. LEAD QUALITY
    # =========================================================================
    lines.append("---")
    lines.append("## 4. Lead Quality")
    lines.append("")
    lines.append("| Owner | Total Lost | No-Show % | Higiene % | Quality Score |")
    lines.append("|-------|------------|-----------|-----------|---------------|")
    for owner, m in sorted(deal_metrics.items(), key=lambda x: x[1]["quality_score"], reverse=True):
        score_emoji = "🟢" if m["quality_score"] >= 60 else "🟡" if m["quality_score"] >= 40 else "🔴"
        lines.append(f"| {owner} | {m['lost']} | {m['noshow_rate']:.0f}% | {m['higiene_rate']:.0f}% | {score_emoji} {m['quality_score']:.0f} |")
    lines.append("")
    lines.append("*Quality Score = 100 - (No-Show% + Higiene%). Higher = better lead quality.*")
    lines.append("")

    # =========================================================================
    # 5. ACTIVITY METRICS
    # =========================================================================
    lines.append("---")
    lines.append("## 5. Activity Metrics")
    lines.append("")

    # 5.1 Total Activity
    lines.append("### 5.1 Total Activity")
    lines.append("")
    lines.append("| Owner | Calls | Meetings | Emails | Total | Per Deal |")
    lines.append("|-------|-------|----------|--------|-------|----------|")
    for owner, a in sorted(activity_metrics.items(), key=lambda x: x[1]["total"], reverse=True):
        lines.append(f"| {owner} | {a['calls']} | {a['meetings']} | {a['emails']} | {a['total']} | {a['per_deal']:.1f} |")
    lines.append("")

    # =========================================================================
    # 6. PERFORMANCE RANKINGS
    # =========================================================================
    lines.append("---")
    lines.append("## 6. Performance Rankings")
    lines.append("")

    # Calculate rankings
    rankings = []

    # Win Rate
    sorted_wr = sorted(deal_metrics.items(), key=lambda x: x[1]["win_rate"], reverse=True)
    if len(sorted_wr) >= 2:
        best_wr = sorted_wr[0]
        worst_wr = sorted_wr[-1]
        rankings.append(("Win Rate", f"{best_wr[0]} ({best_wr[1]['win_rate']:.0f}%)", f"{worst_wr[0]} ({worst_wr[1]['win_rate']:.0f}%)"))

    # Revenue
    sorted_rev = sorted(deal_metrics.items(), key=lambda x: x[1]["revenue_won"], reverse=True)
    if len(sorted_rev) >= 2:
        best_rev = sorted_rev[0]
        worst_rev = sorted_rev[-1]
        rankings.append(("Revenue", f"{best_rev[0]} (${best_rev[1]['revenue_won']:,.0f})", f"{worst_rev[0]} (${worst_rev[1]['revenue_won']:,.0f})"))

    # Quality Score
    sorted_qs = sorted(deal_metrics.items(), key=lambda x: x[1]["quality_score"], reverse=True)
    if len(sorted_qs) >= 2:
        best_qs = sorted_qs[0]
        worst_qs = sorted_qs[-1]
        rankings.append(("Quality Score", f"{best_qs[0]} ({best_qs[1]['quality_score']:.0f})", f"{worst_qs[0]} ({worst_qs[1]['quality_score']:.0f})"))

    # Activity per Deal
    sorted_apd = sorted(activity_metrics.items(), key=lambda x: x[1]["per_deal"], reverse=True)
    if len(sorted_apd) >= 2:
        best_apd = sorted_apd[0]
        worst_apd = sorted_apd[-1]
        rankings.append(("Activity/Deal", f"{best_apd[0]} ({best_apd[1]['per_deal']:.1f})", f"{worst_apd[0]} ({worst_apd[1]['per_deal']:.1f})"))

    lines.append("| Rank | Category | Best | Needs Work |")
    lines.append("|------|----------|------|------------|")
    for i, (cat, best, worst) in enumerate(rankings, 1):
        lines.append(f"| {i} | {cat} | {best} | {worst} |")
    lines.append("")

    # =========================================================================
    # 7. RECOMMENDATIONS
    # =========================================================================
    lines.append("---")
    lines.append("## 7. Recommendations")
    lines.append("")

    recommendations = []

    # Find owners needing coaching
    for owner, m in deal_metrics.items():
        if m["win_rate"] < 15 and m["total"] > 5:
            recommendations.append(f"- **Coaching needed:** {owner} - low win rate ({m['win_rate']:.0f}%), review qualification process")
        if m["noshow_rate"] > 30:
            recommendations.append(f"- **No-show issue:** {owner} - {m['noshow_rate']:.0f}% no-show rate, improve confirmation/reminder process")
        if m["higiene_rate"] > 25:
            recommendations.append(f"- **Lead quality:** {owner} - {m['higiene_rate']:.0f}% hygiene issues, review lead assignment")

    # Best practices
    best_performer = max(deal_metrics.items(), key=lambda x: x[1]["win_rate"])
    if best_performer[1]["win_rate"] > 25:
        recommendations.append(f"- **Best practices:** Study {best_performer[0]}'s approach ({best_performer[1]['win_rate']:.0f}% win rate)")

    if recommendations:
        for rec in recommendations:
            lines.append(rec)
    else:
        lines.append("- No critical issues identified")

    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("*Report generated by owner_performance_analysis.py*")

    return "\n".join(lines)


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Owner Performance Analysis")
    parser.add_argument("--days", type=int, default=90, help="Analysis period in days (default: 90)")
    args = parser.parse_args()

    print(f"Loading owners...", file=sys.stderr)
    owners = load_owners()
    owner_ids = list(owners.keys())
    print(f"Found {len(owners)} owners: {', '.join(owners.values())}", file=sys.stderr)

    print(f"Fetching deals (last {args.days * 2} days for comparison)...", file=sys.stderr)
    raw_deals = fetch_deals(owner_ids, days_back=args.days * 2)
    print(f"Found {len(raw_deals)} deals", file=sys.stderr)

    if not raw_deals:
        print("No deals found for the specified owners and date range.")
        return

    # Parse deals
    all_deals = parse_deals(raw_deals, owners)

    print(f"Fetching engagements (calls, meetings, emails)...", file=sys.stderr)
    engagements = fetch_engagements(owner_ids, days_back=args.days)
    print(f"Found {len(engagements['calls'])} calls, {len(engagements['meetings'])} meetings, {len(engagements['emails'])} emails", file=sys.stderr)

    print(f"Calculating metrics...", file=sys.stderr)
    deal_metrics = calculate_deal_metrics(all_deals, owners, days=args.days)
    activity_metrics = calculate_activity_metrics(engagements, owners, deal_metrics)

    print(f"Generating report...", file=sys.stderr)
    report = generate_report(deal_metrics, activity_metrics, all_deals, args.days)

    # Save report
    report_dir = Path(__file__).parent / "reports"
    report_dir.mkdir(exist_ok=True)
    report_path = report_dir / f"owner_performance_{datetime.now().strftime('%Y%m%d')}.md"
    report_path.write_text(report)
    print(f"Report saved to: {report_path}", file=sys.stderr)

    # Also print to stdout
    print(report)


if __name__ == "__main__":
    main()
