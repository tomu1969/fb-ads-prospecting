#!/usr/bin/env python3
"""
Closed Lost Deals Analysis

Analyzes why deals are being lost after Demo Presentado stage.
Generates a markdown report with loss reasons, customer profiles,
per-owner breakdown, and period comparison.
"""

import os
import json
import requests
from datetime import datetime, timedelta
from collections import Counter, defaultdict
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Configuration
API_KEY = os.getenv("HUBSPOT_API_KEY")
BASE_URL = "https://api.hubapi.com"
CLOSED_LOST_STAGE = "1102535235"

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

# Customer profile labels
PROFILE_LABELS = {
    "inmobiliaria": "Inmobiliaria",
    "realtor": "Realtor/Agente",
    "desarrollador": "Desarrollador",
}


def load_owners():
    """Load target deal owners from JSON file."""
    owners_file = Path(__file__).parent / "config" / "deal_owners.json"
    with open(owners_file) as f:
        data = json.load(f)
    return {o["ownerId"]: o["name"] for o in data["owners"]}


def fetch_deals(owner_ids: list, days_back: int = 60) -> list:
    """Fetch closed lost deals for specified owners in date range."""
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json"
    }

    cutoff_date = datetime.now() - timedelta(days=days_back)
    cutoff_ms = int(cutoff_date.timestamp() * 1000)

    all_deals = []

    for owner_id in owner_ids:
        after = None
        while True:
            payload = {
                "filterGroups": [{
                    "filters": [
                        {"propertyName": "dealstage", "operator": "EQ", "value": CLOSED_LOST_STAGE},
                        {"propertyName": "hubspot_owner_id", "operator": "EQ", "value": owner_id},
                        {"propertyName": "closedate", "operator": "GTE", "value": str(cutoff_ms)}
                    ]
                }],
                "properties": [
                    "dealname", "amount", "closedate", "createdate",
                    "hubspot_owner_id", "motivo_de_cerrada_perdida",
                    "detalles_de_oportunidad_perdida", "costumer_profile",
                    "hs_analytics_source", "hs_analytics_source_data_1",
                    "origen_de_importacion"
                ],
                "limit": 100
            }
            if after:
                payload["after"] = after

            resp = requests.post(f"{BASE_URL}/crm/v3/objects/deals/search",
                               headers=headers, json=payload)
            data = resp.json()

            all_deals.extend(data.get("results", []))

            paging = data.get("paging", {})
            if paging.get("next", {}).get("after"):
                after = paging["next"]["after"]
            else:
                break

    return all_deals


def parse_deals(deals: list, owners: dict) -> list:
    """Parse deals into structured format."""
    parsed = []
    for deal in deals:
        props = deal.get("properties", {})

        closedate_str = props.get("closedate", "")
        closedate = None
        if closedate_str:
            try:
                closedate = datetime.fromisoformat(closedate_str.replace("Z", "+00:00"))
            except:
                pass

        amount = 0
        if props.get("amount"):
            try:
                amount = float(props["amount"])
            except:
                pass

        owner_id = props.get("hubspot_owner_id", "")

        parsed.append({
            "id": deal.get("id"),
            "name": props.get("dealname", "Unknown"),
            "amount": amount,
            "closedate": closedate,
            "owner_id": owner_id,
            "owner_name": owners.get(owner_id, owner_id),
            "loss_reason": props.get("motivo_de_cerrada_perdida", ""),
            "loss_details": props.get("detalles_de_oportunidad_perdida", ""),
            "customer_profile": props.get("costumer_profile", ""),
            "lead_source": props.get("hs_analytics_source", ""),
            "lead_source_detail": props.get("hs_analytics_source_data_1", ""),
            "origen_importacion": props.get("origen_de_importacion", ""),
        })

    return parsed


def split_by_period(deals: list, days: int = 30):
    """Split deals into last N days and previous N days."""
    now = datetime.now(tz=deals[0]["closedate"].tzinfo if deals and deals[0]["closedate"] else None)
    cutoff = now - timedelta(days=days)
    prev_cutoff = cutoff - timedelta(days=days)

    recent = [d for d in deals if d["closedate"] and d["closedate"] >= cutoff]
    previous = [d for d in deals if d["closedate"] and prev_cutoff <= d["closedate"] < cutoff]

    return recent, previous


def analyze_loss_reasons(deals: list) -> dict:
    """Analyze loss reason distribution."""
    reasons = Counter(d["loss_reason"] for d in deals if d["loss_reason"])
    total = sum(reasons.values())

    result = {}
    for reason, count in reasons.most_common():
        label = LOSS_REASON_LABELS.get(reason, reason)
        pct = (count / total * 100) if total > 0 else 0
        result[label] = {"count": count, "pct": pct}

    return result


def analyze_customer_profiles(deals: list) -> dict:
    """Analyze customer profile distribution."""
    profiles = Counter((d["customer_profile"] or "").lower() for d in deals if d.get("customer_profile"))
    total = len(deals)

    result = {}
    for profile, count in profiles.most_common():
        if not profile:
            continue
        label = PROFILE_LABELS.get(profile, profile.title())
        amounts = [d["amount"] for d in deals if (d.get("customer_profile") or "").lower() == profile]
        avg_amount = sum(amounts) / len(amounts) if amounts else 0
        pct = (count / total * 100) if total > 0 else 0
        result[label] = {"count": count, "pct": pct, "avg_amount": avg_amount}

    return result


def analyze_by_owner(deals: list, owners: dict) -> dict:
    """Analyze deals grouped by owner."""
    by_owner = defaultdict(list)
    for deal in deals:
        by_owner[deal["owner_name"]].append(deal)

    result = {}
    for owner_name in owners.values():
        owner_deals = by_owner.get(owner_name, [])
        total_amount = sum(d["amount"] for d in owner_deals)
        reasons = analyze_loss_reasons(owner_deals)

        result[owner_name] = {
            "count": len(owner_deals),
            "total_amount": total_amount,
            "reasons": reasons,
            "deals": owner_deals
        }

    return result


def extract_competitor_mentions(deals: list) -> dict:
    """Extract competitor mentions from loss details."""
    competitors = ["tokko", "inmuebles24", "properati", "simi", "witei", "salesforce",
                   "hubspot", "zoho", "monday", "notion", "excel", "whatsapp"]

    mentions = Counter()
    for deal in deals:
        details = (deal.get("loss_details") or "").lower()
        for comp in competitors:
            if comp in details:
                mentions[comp.title()] += 1

    return dict(mentions.most_common(10))


# =============================================================================
# ENHANCED DIAGNOSTIC ANALYSIS FUNCTIONS
# =============================================================================

def analyze_revenue_by_reason(deals: list) -> dict:
    """Analyze revenue lost by reason (not just deal count)."""
    by_reason = defaultdict(lambda: {"count": 0, "revenue": 0, "deals": []})
    total_revenue = sum(d["amount"] for d in deals)
    total_count = len(deals)

    for deal in deals:
        reason = deal.get("loss_reason", "") or "unknown"
        label = LOSS_REASON_LABELS.get(reason, reason)
        by_reason[label]["count"] += 1
        by_reason[label]["revenue"] += deal["amount"]
        by_reason[label]["deals"].append(deal)

    result = {}
    for reason, data in by_reason.items():
        result[reason] = {
            "count": data["count"],
            "count_pct": (data["count"] / total_count * 100) if total_count > 0 else 0,
            "revenue": data["revenue"],
            "revenue_pct": (data["revenue"] / total_revenue * 100) if total_revenue > 0 else 0,
            "avg_deal": data["revenue"] / data["count"] if data["count"] > 0 else 0,
        }

    return dict(sorted(result.items(), key=lambda x: x[1]["revenue"], reverse=True))


def segment_by_deal_size(deals: list) -> dict:
    """Segment deals by size: Small ($0-500), Medium ($501-2000), Enterprise ($2001+)."""
    segments = {
        "Small ($0-$500)": {"min": 0, "max": 500, "deals": []},
        "Medium ($501-$2,000)": {"min": 501, "max": 2000, "deals": []},
        "Enterprise ($2,001+)": {"min": 2001, "max": float("inf"), "deals": []},
    }

    for deal in deals:
        amount = deal["amount"]
        for seg_name, seg_data in segments.items():
            if seg_data["min"] <= amount <= seg_data["max"]:
                seg_data["deals"].append(deal)
                break

    total = len(deals)
    result = {}
    for seg_name, seg_data in segments.items():
        seg_deals = seg_data["deals"]
        result[seg_name] = {
            "count": len(seg_deals),
            "pct": (len(seg_deals) / total * 100) if total > 0 else 0,
            "total_revenue": sum(d["amount"] for d in seg_deals),
            "deals": seg_deals,
        }

    return result


def loss_autopsy(deals: list, threshold: float = 5000) -> list:
    """Deep dive on high-value lost deals (>threshold)."""
    high_value = [d for d in deals if d["amount"] >= threshold]
    high_value.sort(key=lambda x: x["amount"], reverse=True)

    autopsies = []
    for deal in high_value:
        reason_label = LOSS_REASON_LABELS.get(deal["loss_reason"], deal["loss_reason"] or "N/A")
        autopsies.append({
            "name": deal["name"],
            "amount": deal["amount"],
            "owner": deal["owner_name"],
            "reason": reason_label,
            "details": deal.get("loss_details", ""),
            "profile": deal.get("customer_profile", ""),
        })

    return autopsies


def categorize_higiene_verbatims(deals: list) -> dict:
    """Categorize 'Higiene de lead' deals by subcategory."""
    higiene_deals = [d for d in deals if "higiene" in (d.get("loss_reason") or "").lower()]

    categories = {
        "Duplicado": [],
        "Negocio de prueba": [],
        "Buscaba trabajo": [],
        "Otro": [],
    }

    for deal in higiene_deals:
        details = (deal.get("loss_details") or "").lower()
        if "duplica" in details or "ya tiene" in details or "ya está" in details:
            categories["Duplicado"].append(deal)
        elif "prueba" in details or "test" in details:
            categories["Negocio de prueba"].append(deal)
        elif "trabajo" in details or "empleo" in details:
            categories["Buscaba trabajo"].append(deal)
        else:
            categories["Otro"].append(deal)

    total = len(higiene_deals)
    result = {}
    for cat, cat_deals in categories.items():
        if cat_deals:
            result[cat] = {
                "count": len(cat_deals),
                "pct": (len(cat_deals) / total * 100) if total > 0 else 0,
                "revenue": sum(d["amount"] for d in cat_deals),
            }

    return result


def analyze_zero_value_deals(deals: list) -> dict:
    """Analyze the impact of $0 deals on metrics."""
    zero_deals = [d for d in deals if d["amount"] == 0]
    nonzero_deals = [d for d in deals if d["amount"] > 0]

    return {
        "zero_count": len(zero_deals),
        "zero_pct": (len(zero_deals) / len(deals) * 100) if deals else 0,
        "nonzero_count": len(nonzero_deals),
        "nonzero_revenue": sum(d["amount"] for d in nonzero_deals),
        "zero_deals": zero_deals,
    }


def analyze_owner_lead_quality(deals: list, owners: dict) -> dict:
    """Analyze lead quality indicators by owner."""
    by_owner = defaultdict(list)
    for deal in deals:
        by_owner[deal["owner_name"]].append(deal)

    result = {}
    for owner_name in owners.values():
        owner_deals = by_owner.get(owner_name, [])
        if not owner_deals:
            continue

        total = len(owner_deals)
        higiene_count = len([d for d in owner_deals if "higiene" in (d.get("loss_reason") or "").lower()])
        noshow_count = len([d for d in owner_deals if "no_asistio" in (d.get("loss_reason") or "").lower()])

        # Quality score: lower higiene + noshow = better quality
        quality_score = 100 - ((higiene_count + noshow_count) / total * 100) if total > 0 else 0

        result[owner_name] = {
            "total_deals": total,
            "higiene_count": higiene_count,
            "higiene_pct": (higiene_count / total * 100) if total > 0 else 0,
            "noshow_count": noshow_count,
            "noshow_pct": (noshow_count / total * 100) if total > 0 else 0,
            "quality_score": quality_score,
        }

    return dict(sorted(result.items(), key=lambda x: x[1]["quality_score"]))


def extract_enhanced_competitors(deals: list) -> dict:
    """Enhanced competitor extraction including contextual mentions."""
    competitors = {
        "zoho": {"name": "Zoho", "mentions": 0, "deals": [], "revenue": 0},
        "heygia": {"name": "HeyGia", "mentions": 0, "deals": [], "revenue": 0},
        "hgia": {"name": "HeyGia", "mentions": 0, "deals": [], "revenue": 0},
        "atom": {"name": "Atom", "mentions": 0, "deals": [], "revenue": 0},
        "tokko": {"name": "Tokko", "mentions": 0, "deals": [], "revenue": 0},
        "inmuebles24": {"name": "Inmuebles24", "mentions": 0, "deals": [], "revenue": 0},
        "salesforce": {"name": "Salesforce", "mentions": 0, "deals": [], "revenue": 0},
    }

    # Also track generic "cheaper alternative" mentions
    generic_alternatives = []

    for deal in deals:
        details = (deal.get("loss_details") or "").lower()
        name = (deal.get("name") or "").lower()
        combined = details + " " + name

        for key, comp_data in competitors.items():
            if key in combined:
                comp_data["mentions"] += 1
                comp_data["deals"].append(deal)
                comp_data["revenue"] += deal["amount"]

        # Check for generic cheaper alternatives
        if any(phrase in details for phrase in ["más económic", "económica", "50% más", "otra opción"]):
            generic_alternatives.append(deal)

    # Merge HeyGia variants
    competitors["heygia"]["mentions"] += competitors["hgia"]["mentions"]
    competitors["heygia"]["deals"].extend(competitors["hgia"]["deals"])
    competitors["heygia"]["revenue"] += competitors["hgia"]["revenue"]
    del competitors["hgia"]

    result = {comp_data["name"]: {
        "mentions": comp_data["mentions"],
        "deals_lost": len(comp_data["deals"]),
        "revenue_lost": comp_data["revenue"],
    } for comp_data in competitors.values() if comp_data["mentions"] > 0}

    if generic_alternatives:
        result["Alternativa más económica"] = {
            "mentions": len(generic_alternatives),
            "deals_lost": len(generic_alternatives),
            "revenue_lost": sum(d["amount"] for d in generic_alternatives),
        }

    return dict(sorted(result.items(), key=lambda x: x[1]["revenue_lost"], reverse=True))


def analyze_geographic_friction(deals: list) -> list:
    """Identify geographic/currency friction from verbatims."""
    keywords = ["bolivia", "cambio", "exchange", "currency", "dólar", "peso", "moneda"]
    friction_deals = []

    for deal in deals:
        details = (deal.get("loss_details") or "").lower()
        for keyword in keywords:
            if keyword in details:
                friction_deals.append({
                    "deal": deal["name"],
                    "amount": deal["amount"],
                    "details": deal.get("loss_details", ""),
                    "keyword": keyword,
                })
                break

    return friction_deals


def analyze_noshow_rate(deals: list, owners: dict) -> dict:
    """Calculate no-show rate by owner."""
    by_owner = defaultdict(list)
    for deal in deals:
        by_owner[deal["owner_name"]].append(deal)

    result = {}
    for owner_name in owners.values():
        owner_deals = by_owner.get(owner_name, [])
        noshow_count = len([d for d in owner_deals if "no_asistio" in (d.get("loss_reason") or "").lower()])
        total = len(owner_deals)

        result[owner_name] = {
            "noshow_count": noshow_count,
            "total_deals": total,
            "noshow_rate": (noshow_count / total * 100) if total > 0 else 0,
        }

    return dict(sorted(result.items(), key=lambda x: x[1]["noshow_rate"], reverse=True))


def analyze_seasonality(deals: list) -> dict:
    """Analyze 'Mal timing/Budget' for delayed vs true loss."""
    timing_deals = [d for d in deals if "mal_timing" in (d.get("loss_reason") or "").lower()]

    delayed_keywords = ["enero", "january", "retomar", "revisar más adelante", "esperar", "3 semana"]
    budget_keywords = ["presupuesto", "ppto", "budget", "recursos", "no tiene"]

    delayed = []
    true_budget = []
    other = []

    for deal in timing_deals:
        details = (deal.get("loss_details") or "").lower()

        if any(kw in details for kw in delayed_keywords):
            delayed.append(deal)
        elif any(kw in details for kw in budget_keywords):
            true_budget.append(deal)
        else:
            other.append(deal)

    total = len(timing_deals)
    return {
        "total_timing_deals": total,
        "delayed_interest": {
            "count": len(delayed),
            "pct": (len(delayed) / total * 100) if total > 0 else 0,
            "revenue": sum(d["amount"] for d in delayed),
            "deals": delayed,
        },
        "true_budget_loss": {
            "count": len(true_budget),
            "pct": (len(true_budget) / total * 100) if total > 0 else 0,
            "revenue": sum(d["amount"] for d in true_budget),
        },
        "uncategorized": {
            "count": len(other),
            "pct": (len(other) / total * 100) if total > 0 else 0,
        },
    }


def pareto_analysis(revenue_by_reason: dict) -> list:
    """Calculate Pareto (80/20) analysis for revenue loss."""
    total_revenue = sum(r["revenue"] for r in revenue_by_reason.values())
    if total_revenue == 0:
        return []

    sorted_reasons = sorted(revenue_by_reason.items(), key=lambda x: x[1]["revenue"], reverse=True)

    cumulative = 0
    result = []
    for reason, data in sorted_reasons:
        cumulative += data["revenue"]
        cumulative_pct = (cumulative / total_revenue * 100)
        result.append({
            "reason": reason,
            "revenue": data["revenue"],
            "revenue_pct": data["revenue_pct"],
            "cumulative_pct": cumulative_pct,
        })

    return result


def owner_efficiency_matrix(deals: list, owners: dict) -> list:
    """Compare owner efficiency: deals lost vs revenue lost vs avg deal value."""
    by_owner = defaultdict(list)
    for deal in deals:
        by_owner[deal["owner_name"]].append(deal)

    result = []
    for owner_name in owners.values():
        owner_deals = by_owner.get(owner_name, [])
        total_revenue = sum(d["amount"] for d in owner_deals)
        avg_deal = total_revenue / len(owner_deals) if owner_deals else 0

        result.append({
            "owner": owner_name,
            "deals_lost": len(owner_deals),
            "revenue_lost": total_revenue,
            "avg_deal_value": avg_deal,
        })

    # Rank by avg deal value (who's losing bigger deals)
    result.sort(key=lambda x: x["avg_deal_value"], reverse=True)
    for i, r in enumerate(result):
        r["efficiency_rank"] = i + 1

    return result


# Lead source labels mapping
LEAD_SOURCE_LABELS = {
    "ORGANIC_SEARCH": "Organic Search",
    "PAID_SEARCH": "Paid Search",
    "EMAIL_MARKETING": "Email Marketing",
    "SOCIAL_MEDIA": "Organic Social",
    "REFERRALS": "Referrals",
    "OTHER_CAMPAIGNS": "Other Campaigns",
    "DIRECT_TRAFFIC": "Direct Traffic",
    "OFFLINE": "Offline Sources",
    "PAID_SOCIAL": "Paid Social",
    "AI_REFERRALS": "AI Referrals",
}


def analyze_by_lead_source(deals: list) -> dict:
    """Analyze closed lost deals by lead origin channel."""
    by_source = defaultdict(lambda: {"count": 0, "revenue": 0, "deals": [], "reasons": Counter()})
    total_count = len(deals)
    total_revenue = sum(d["amount"] for d in deals)

    for deal in deals:
        source = deal.get("lead_source") or "Unknown"
        label = LEAD_SOURCE_LABELS.get(source, source)
        by_source[label]["count"] += 1
        by_source[label]["revenue"] += deal["amount"]
        by_source[label]["deals"].append(deal)

        # Track loss reasons per source
        reason = deal.get("loss_reason", "")
        reason_label = LOSS_REASON_LABELS.get(reason, reason or "N/A")
        by_source[label]["reasons"][reason_label] += 1

    result = {}
    for source, data in sorted(by_source.items(), key=lambda x: x[1]["count"], reverse=True):
        top_reasons = data["reasons"].most_common(3)
        result[source] = {
            "count": data["count"],
            "count_pct": (data["count"] / total_count * 100) if total_count > 0 else 0,
            "revenue": data["revenue"],
            "revenue_pct": (data["revenue"] / total_revenue * 100) if total_revenue > 0 else 0,
            "avg_deal": data["revenue"] / data["count"] if data["count"] > 0 else 0,
            "top_reasons": top_reasons,
        }

    return result


def analyze_lead_source_by_owner(deals: list, owners: dict) -> dict:
    """Analyze lead source distribution per owner."""
    by_owner = defaultdict(lambda: defaultdict(int))

    for deal in deals:
        owner = deal.get("owner_name", "Unknown")
        source = deal.get("lead_source") or "Unknown"
        label = LEAD_SOURCE_LABELS.get(source, source)
        by_owner[owner][label] += 1

    result = {}
    for owner_name in owners.values():
        owner_sources = dict(by_owner.get(owner_name, {}))
        total = sum(owner_sources.values())
        result[owner_name] = {
            "total": total,
            "sources": {s: {"count": c, "pct": (c / total * 100) if total > 0 else 0}
                        for s, c in sorted(owner_sources.items(), key=lambda x: x[1], reverse=True)}
        }

    return result


def generate_report(recent: list, previous: list, owners: dict) -> str:
    """Generate markdown report."""
    now = datetime.now()
    period_end = now.strftime("%Y-%m-%d")
    period_start = (now - timedelta(days=30)).strftime("%Y-%m-%d")
    prev_start = (now - timedelta(days=60)).strftime("%Y-%m-%d")

    # Summary stats
    recent_count = len(recent)
    recent_amount = sum(d["amount"] for d in recent)
    prev_count = len(previous)
    prev_amount = sum(d["amount"] for d in previous)

    count_change = ((recent_count - prev_count) / prev_count * 100) if prev_count > 0 else 0

    # Basic analysis
    recent_reasons = analyze_loss_reasons(recent)
    prev_reasons = analyze_loss_reasons(previous)
    profiles = analyze_customer_profiles(recent)
    by_owner = analyze_by_owner(recent, owners)

    # Enhanced diagnostic analysis
    revenue_by_reason = analyze_revenue_by_reason(recent)
    deal_segments = segment_by_deal_size(recent)
    high_value_autopsies = loss_autopsy(recent, threshold=5000)
    higiene_categories = categorize_higiene_verbatims(recent)
    zero_value_analysis = analyze_zero_value_deals(recent)
    lead_quality = analyze_owner_lead_quality(recent, owners)
    enhanced_competitors = extract_enhanced_competitors(recent)
    geo_friction = analyze_geographic_friction(recent)
    noshow_rates = analyze_noshow_rate(recent, owners)
    seasonality = analyze_seasonality(recent)
    pareto = pareto_analysis(revenue_by_reason)
    efficiency = owner_efficiency_matrix(recent, owners)
    lead_source_analysis = analyze_by_lead_source(recent)
    lead_source_by_owner = analyze_lead_source_by_owner(recent, owners)

    # Top metrics
    top_reason = max(recent_reasons.items(), key=lambda x: x[1]["count"])[0] if recent_reasons else "N/A"
    top_reason_pct = recent_reasons.get(top_reason, {}).get("pct", 0)
    top_profile = max(profiles.items(), key=lambda x: x[1]["count"])[0] if profiles else "N/A"
    top_owner = max(by_owner.items(), key=lambda x: x[1]["count"])[0] if by_owner else "N/A"
    top_revenue_reason = list(revenue_by_reason.keys())[0] if revenue_by_reason else "N/A"

    report = []
    report.append("# Demo→Close Conversion Drop: Diagnostic Analysis")
    report.append(f"Generated: {now.strftime('%Y-%m-%d %H:%M')}")
    report.append(f"Period: {period_start} to {period_end} vs {prev_start} to {period_start}")
    report.append("")

    # =========================================================================
    # EXECUTIVE SUMMARY
    # =========================================================================
    report.append("## Executive Summary")
    report.append("")
    report.append(f"| Metric | Last 30d | Prev 30d | Change |")
    report.append(f"|--------|----------|----------|--------|")
    report.append(f"| Deals Lost | {recent_count} | {prev_count} | {count_change:+.0f}% |")
    report.append(f"| Revenue Lost | ${recent_amount:,.0f} | ${prev_amount:,.0f} | {((recent_amount - prev_amount) / prev_amount * 100) if prev_amount > 0 else 0:+.0f}% |")
    report.append("")
    report.append(f"- **Top loss reason (by count):** {top_reason} ({top_reason_pct:.0f}%)")
    report.append(f"- **Top loss reason (by revenue):** {top_revenue_reason}")
    report.append(f"- **Most affected segment:** {top_profile}")
    report.append(f"- **Owner with most losses:** {top_owner}")
    report.append(f"- **$0 deals:** {zero_value_analysis['zero_count']} ({zero_value_analysis['zero_pct']:.0f}% of total)")
    report.append("")

    # =========================================================================
    # 1. REVENUE IMPACT & WEIGHTED ANALYSIS
    # =========================================================================
    report.append("---")
    report.append("## 1. Revenue Impact & Weighted Analysis")
    report.append("")

    # 1.1 Revenue Leakage by Reason
    report.append("### 1.1 Revenue Leakage by Reason")
    report.append("")
    report.append("| Reason | Deal Count | % Deals | Revenue Lost | % Revenue | Avg Deal |")
    report.append("|--------|------------|---------|--------------|-----------|----------|")
    for reason, data in revenue_by_reason.items():
        report.append(f"| {reason} | {data['count']} | {data['count_pct']:.0f}% | ${data['revenue']:,.0f} | {data['revenue_pct']:.0f}% | ${data['avg_deal']:,.0f} |")
    report.append("")

    # 1.2 Deal Size Segmentation
    report.append("### 1.2 Deal Size Segmentation")
    report.append("")
    report.append("| Segment | Count | % | Total Revenue |")
    report.append("|---------|-------|---|---------------|")
    for seg_name, seg_data in deal_segments.items():
        report.append(f"| {seg_name} | {seg_data['count']} | {seg_data['pct']:.0f}% | ${seg_data['total_revenue']:,.0f} |")
    report.append("")

    # 1.3 High-Value Loss Autopsy
    if high_value_autopsies:
        report.append("### 1.3 High-Value Loss Autopsy (>$5,000)")
        report.append("")
        for autopsy in high_value_autopsies:
            report.append(f"#### {autopsy['name']} - ${autopsy['amount']:,.0f}")
            report.append(f"- **Owner:** {autopsy['owner']}")
            report.append(f"- **Reason:** {autopsy['reason']}")
            report.append(f"- **Profile:** {autopsy['profile'] or 'Sin perfil'}")
            report.append(f"- **Details:** {autopsy['details'] or 'N/A'}")
            report.append("")

    # 1.4 Lead Source Channel Analysis
    report.append("### 1.4 Lead Source Channel Analysis")
    report.append("")
    report.append("| Lead Source | Deals | % | Revenue | % Revenue | Avg Deal | Top Loss Reason |")
    report.append("|-------------|-------|---|---------|-----------|----------|-----------------|")
    for source, data in lead_source_analysis.items():
        top_reason = data["top_reasons"][0][0] if data["top_reasons"] else "N/A"
        report.append(f"| {source} | {data['count']} | {data['count_pct']:.0f}% | ${data['revenue']:,.0f} | {data['revenue_pct']:.0f}% | ${data['avg_deal']:,.0f} | {top_reason} |")
    report.append("")

    # 1.5 Lead Source by Owner
    report.append("### 1.5 Lead Source Distribution by Owner")
    report.append("")
    for owner, owner_data in lead_source_by_owner.items():
        if owner_data["total"] > 0:
            top_sources = list(owner_data["sources"].items())[:3]
            sources_str = ", ".join([f"{s} ({d['pct']:.0f}%)" for s, d in top_sources])
            report.append(f"- **{owner}** ({owner_data['total']} deals): {sources_str}")
    report.append("")

    # =========================================================================
    # 2. CRM HYGIENE & DATA INTEGRITY AUDIT
    # =========================================================================
    report.append("---")
    report.append("## 2. CRM Hygiene & Data Integrity Audit")
    report.append("")

    # 2.1 Higiene de Lead Subcategorization
    report.append("### 2.1 'Higiene de Lead' Breakdown (+1,500% increase)")
    report.append("")
    if higiene_categories:
        report.append("| Subcategory | Count | % of Higiene | Revenue |")
        report.append("|-------------|-------|--------------|---------|")
        for cat, data in sorted(higiene_categories.items(), key=lambda x: x[1]["count"], reverse=True):
            report.append(f"| {cat} | {data['count']} | {data['pct']:.0f}% | ${data['revenue']:,.0f} |")
        report.append("")
    else:
        report.append("*No higiene deals found*")
        report.append("")

    # 2.2 Zero-Value Deal Analysis
    report.append("### 2.2 Zero-Value Deal Impact")
    report.append("")
    report.append(f"| Metric | Value |")
    report.append(f"|--------|-------|")
    report.append(f"| Total $0 deals | {zero_value_analysis['zero_count']} of {recent_count} ({zero_value_analysis['zero_pct']:.0f}%) |")
    report.append(f"| Non-zero deals | {zero_value_analysis['nonzero_count']} |")
    report.append(f"| Revenue from non-zero | ${zero_value_analysis['nonzero_revenue']:,.0f} |")
    report.append("")
    report.append(f"**Impact:** If $0 deals are excluded, actual 'real' losses = {zero_value_analysis['nonzero_count']} deals worth ${zero_value_analysis['nonzero_revenue']:,.0f}")
    report.append("")

    # 2.3 Owner Lead Quality Correlation
    report.append("### 2.3 Owner Lead Quality Correlation")
    report.append("")
    report.append("| Owner | Total | Higiene % | No-Show % | Quality Score |")
    report.append("|-------|-------|-----------|-----------|---------------|")
    for owner, data in lead_quality.items():
        score_emoji = "🟢" if data["quality_score"] >= 60 else "🟡" if data["quality_score"] >= 40 else "🔴"
        report.append(f"| {owner} | {data['total_deals']} | {data['higiene_pct']:.0f}% | {data['noshow_pct']:.0f}% | {score_emoji} {data['quality_score']:.0f} |")
    report.append("")
    report.append("*Quality Score = 100 - (Higiene% + No-Show%). Lower score = lower quality leads.*")
    report.append("")

    # =========================================================================
    # 3. COMPETITIVE & MARKET INTELLIGENCE
    # =========================================================================
    report.append("---")
    report.append("## 3. Competitive & Market Intelligence")
    report.append("")

    # 3.1 Competitor Deep Dive
    report.append("### 3.1 Competitive Landscape")
    report.append("")
    if enhanced_competitors:
        report.append("| Competitor | Mentions | Deals Lost | Revenue Lost |")
        report.append("|------------|----------|------------|--------------|")
        for comp, data in enhanced_competitors.items():
            report.append(f"| {comp} | {data['mentions']} | {data['deals_lost']} | ${data['revenue_lost']:,.0f} |")
        report.append("")
    else:
        report.append("*No competitor mentions found*")
        report.append("")

    # 3.2 Geographic/Currency Friction
    report.append("### 3.2 Geographic/Currency Friction")
    report.append("")
    if geo_friction:
        report.append("| Deal | Amount | Context |")
        report.append("|------|--------|---------|")
        for item in geo_friction:
            report.append(f"| {item['deal'][:30]} | ${item['amount']:,.0f} | {item['details'][:50]}... |")
        report.append("")
    else:
        report.append("*No geographic friction identified*")
        report.append("")

    # =========================================================================
    # 4. FUNNEL BOTTLENECK IDENTIFICATION
    # =========================================================================
    report.append("---")
    report.append("## 4. Funnel Bottleneck Identification")
    report.append("")

    # 4.1 No-Show Rate by Owner
    report.append("### 4.1 No-Show Epidemic by Owner")
    report.append("")
    report.append("| Owner | No-Show Count | Total Deals | No-Show Rate |")
    report.append("|-------|---------------|-------------|--------------|")
    for owner, data in noshow_rates.items():
        rate_emoji = "🔴" if data["noshow_rate"] >= 40 else "🟡" if data["noshow_rate"] >= 25 else "🟢"
        report.append(f"| {owner} | {data['noshow_count']} | {data['total_deals']} | {rate_emoji} {data['noshow_rate']:.0f}% |")
    report.append("")

    # 4.2 Seasonality / Delayed Interest
    report.append("### 4.2 Seasonality Analysis: 'Mal timing/Budget' Breakdown")
    report.append("")
    report.append(f"Total 'Mal timing/Budget' deals: **{seasonality['total_timing_deals']}**")
    report.append("")
    report.append("| Subcategory | Count | % | Revenue | Interpretation |")
    report.append("|-------------|-------|---|---------|----------------|")
    delayed = seasonality["delayed_interest"]
    budget = seasonality["true_budget_loss"]
    other = seasonality["uncategorized"]
    report.append(f"| Delayed Interest | {delayed['count']} | {delayed['pct']:.0f}% | ${delayed['revenue']:,.0f} | Recoverable in Jan |")
    report.append(f"| True Budget Loss | {budget['count']} | {budget['pct']:.0f}% | ${budget['revenue']:,.0f} | Real loss |")
    report.append(f"| Uncategorized | {other['count']} | {other['pct']:.0f}% | - | Needs review |")
    report.append("")

    if delayed["deals"]:
        report.append("**Delayed Interest Deals (follow up in January):**")
        for d in delayed["deals"][:5]:
            report.append(f"- {d['name']}: ${d['amount']:,.0f} - {d.get('loss_details', '')[:50]}")
        report.append("")

    # =========================================================================
    # 5. DATA VISUALIZATION TABLES
    # =========================================================================
    report.append("---")
    report.append("## 5. Data Visualization Tables")
    report.append("")

    # 5.1 Pareto Analysis
    report.append("### 5.1 Pareto Analysis (80/20 Rule)")
    report.append("")
    report.append("| Reason | Revenue Lost | % Revenue | Cumulative % |")
    report.append("|--------|--------------|-----------|--------------|")
    for item in pareto:
        marker = "← 80%" if item["cumulative_pct"] <= 80 and (pareto.index(item) == len(pareto) - 1 or pareto[pareto.index(item) + 1]["cumulative_pct"] > 80) else ""
        report.append(f"| {item['reason']} | ${item['revenue']:,.0f} | {item['revenue_pct']:.0f}% | {item['cumulative_pct']:.0f}% {marker} |")
    report.append("")

    # 5.2 Owner Efficiency Matrix
    report.append("### 5.2 Owner Efficiency Matrix")
    report.append("")
    report.append("| Rank | Owner | Deals Lost | Revenue Lost | Avg Deal Value |")
    report.append("|------|-------|------------|--------------|----------------|")
    for item in efficiency:
        report.append(f"| {item['efficiency_rank']} | {item['owner']} | {item['deals_lost']} | ${item['revenue_lost']:,.0f} | ${item['avg_deal_value']:,.0f} |")
    report.append("")
    report.append("*Ranked by average deal value (who's losing the biggest fish)*")
    report.append("")

    # =========================================================================
    # 6. KEY OBSERVATIONS & RECOMMENDATIONS
    # =========================================================================
    report.append("---")
    report.append("## 6. Key Observations & Recommendations")
    report.append("")

    observations = []

    # Critical alert
    if count_change > 100:
        observations.append(f"🚨 **CRITICAL:** Losses increased {count_change:.0f}% vs previous period")

    # Revenue concentration
    if pareto and pareto[0]["revenue_pct"] > 40:
        observations.append(f"💰 **Revenue Concentration:** '{pareto[0]['reason']}' accounts for {pareto[0]['revenue_pct']:.0f}% of revenue loss")

    # $0 deals problem
    if zero_value_analysis["zero_pct"] > 60:
        observations.append(f"📊 **Data Quality Issue:** {zero_value_analysis['zero_pct']:.0f}% of deals have $0 value - skewing metrics")

    # Higiene explosion
    higiene_count = sum(c["count"] for c in higiene_categories.values()) if higiene_categories else 0
    if higiene_count > 40:
        observations.append(f"🧹 **CRM Hygiene Crisis:** {higiene_count} 'Higiene' deals - mostly duplicates. Review lead assignment.")

    # No-show problem
    max_noshow = max(noshow_rates.values(), key=lambda x: x["noshow_rate"]) if noshow_rates else None
    if max_noshow and max_noshow["noshow_rate"] > 35:
        top_noshow_owner = [k for k, v in noshow_rates.items() if v == max_noshow][0]
        observations.append(f"📞 **No-Show Alert:** {top_noshow_owner} has {max_noshow['noshow_rate']:.0f}% no-show rate")

    # Competitor threat
    if enhanced_competitors:
        top_comp = list(enhanced_competitors.keys())[0]
        observations.append(f"🏢 **Competitive Threat:** {top_comp} mentioned - losing ${enhanced_competitors[top_comp]['revenue_lost']:,.0f}")

    # Delayed interest opportunity
    if delayed["count"] > 5:
        observations.append(f"📅 **Recovery Opportunity:** {delayed['count']} deals waiting for January (${delayed['revenue']:,.0f} potential)")

    for obs in observations:
        report.append(f"- {obs}")

    report.append("")

    # =========================================================================
    # APPENDIX
    # =========================================================================
    report.append("---")
    report.append("## Appendix: All Lost Deals (Last 30 Days)")
    report.append("")
    report.append("| Deal | Owner | Amount | Reason | Details |")
    report.append("|------|-------|--------|--------|---------|")

    for d in sorted(recent, key=lambda x: x["amount"], reverse=True):
        reason_label = LOSS_REASON_LABELS.get(d["loss_reason"], d["loss_reason"] or "-")
        details = (d["loss_details"] or "-")[:40]
        if len(d.get("loss_details", "") or "") > 40:
            details += "..."
        report.append(f"| {d['name'][:25]} | {d['owner_name']} | ${d['amount']:,.0f} | {reason_label} | {details} |")

    return "\n".join(report)


def main():
    """Main entry point."""
    # Load owners
    owners = load_owners()
    owner_ids = list(owners.keys())

    print("Fetching deals from HubSpot...", file=__import__("sys").stderr)

    # Fetch deals (last 60 days for comparison)
    deals = fetch_deals(owner_ids, days_back=60)

    if not deals:
        print("No deals found for the specified owners and date range.")
        return

    print(f"Found {len(deals)} deals total", file=__import__("sys").stderr)

    # Parse deals
    parsed = parse_deals(deals, owners)

    # Filter out deals without closedate
    parsed = [d for d in parsed if d["closedate"]]

    # Split by period
    recent, previous = split_by_period(parsed, days=30)

    print(f"Recent period: {len(recent)} deals", file=__import__("sys").stderr)
    print(f"Previous period: {len(previous)} deals", file=__import__("sys").stderr)

    # Generate report
    report = generate_report(recent, previous, owners)

    # Output report
    print(report)


if __name__ == "__main__":
    main()
