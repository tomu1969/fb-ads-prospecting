"""AI-powered strategy suggester for pipeline enrichment optimization."""

import os
import json
import pandas as pd
from typing import Dict, List, Optional
from pathlib import Path
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

# Import enrichment config utilities
from utils.enrichment_config import ENRICHMENT_TYPES, get_cost_breakdown, estimate_cost_and_time


def analyze_input_data(input_file: Path, sample_size: int = 20) -> Dict:
    """
    Analyze input data to understand what's already available.

    Args:
        input_file: Path to input CSV/Excel file
        sample_size: Number of rows to sample for analysis (default 20)

    Returns:
        Dict with analysis results including fill rates and sample data
    """
    try:
        if input_file.suffix.lower() == '.csv':
            df = pd.read_csv(input_file, nrows=sample_size)
        else:
            df = pd.read_excel(input_file, nrows=sample_size)

        # Calculate fill rates for each column
        fill_rates = {}
        for col in df.columns:
            non_empty = df[col].notna().sum()
            # Also check for empty strings
            if df[col].dtype == 'object':
                non_empty = (df[col].notna() & (df[col].astype(str).str.strip() != '')).sum()
            fill_rates[col] = {
                'filled': int(non_empty),
                'total': len(df),
                'percent': round(100 * non_empty / len(df), 1) if len(df) > 0 else 0
            }

        analysis = {
            'total_rows': len(df),
            'columns': list(df.columns),
            'has_website': any('website' in col.lower() or 'url' in col.lower() for col in df.columns),
            'has_email': any('email' in col.lower() for col in df.columns),
            'has_phone': any('phone' in col.lower() or 'tel' in col.lower() for col in df.columns),
            'has_contact_name': any('name' in col.lower() or 'contact' in col.lower() for col in df.columns),
            'has_instagram': any('instagram' in col.lower() for col in df.columns),
            'fill_rates': fill_rates,
            'sample_data': df.head(sample_size).to_dict('records') if len(df) > 0 else [],
        }

        return analysis
    except Exception as e:
        return {'error': str(e)}


def suggest_enrichment_strategy(
    input_file: Path,
    current_config: Dict[str, bool],
    row_count: int,
    sample_size: int = 20
) -> Dict:
    """
    Use AI to suggest optimal enrichment strategy.

    Args:
        input_file: Path to input file
        current_config: Current enrichment configuration
        row_count: Total number of rows to process
        sample_size: Number of rows to sample for analysis (default 20)

    Returns:
        Dict with AI suggestions
    """
    # Analyze input data with larger sample for better recommendations
    analysis = analyze_input_data(input_file, sample_size)

    if 'error' in analysis:
        return {
            'error': analysis['error'],
            'recommendations': [],
            'rationale': 'Could not analyze input data'
        }

    # Format fill rates for display
    fill_rates_str = ""
    if 'fill_rates' in analysis:
        relevant_cols = []
        for col, stats in analysis['fill_rates'].items():
            col_lower = col.lower()
            if any(kw in col_lower for kw in ['website', 'url', 'email', 'phone', 'name', 'contact', 'instagram']):
                relevant_cols.append(f"  - {col}: {stats['filled']}/{stats['total']} ({stats['percent']}% filled)")
        if relevant_cols:
            fill_rates_str = "\n".join(relevant_cols)

    # Format sample data summary (first 5 rows for context, not all 20)
    sample_summary = ""
    if analysis.get('sample_data'):
        sample_summary = f"\nSAMPLE DATA (first 5 of {len(analysis['sample_data'])} rows analyzed):\n"
        sample_summary += json.dumps(analysis['sample_data'][:5], indent=2, default=str)

    # Prepare prompt for AI
    prompt = f"""You are an expert data enrichment strategist. Analyze the following input data and current enrichment configuration, then provide recommendations.

INPUT DATA ANALYSIS:
- Total rows in dataset: {row_count}
- Rows analyzed for quality: {analysis.get('total_rows', 0)}
- Columns available: {', '.join(analysis.get('columns', []))}
- Has website column: {analysis.get('has_website', False)}
- Has email column: {analysis.get('has_email', False)}
- Has phone column: {analysis.get('has_phone', False)}
- Has contact name column: {analysis.get('has_contact_name', False)}
- Has Instagram column: {analysis.get('has_instagram', False)}

DATA QUALITY (fill rates for key columns):
{fill_rates_str if fill_rates_str else "  No relevant columns found"}
{sample_summary}

CURRENT ENRICHMENT CONFIGURATION:
{json.dumps(current_config, indent=2)}

AVAILABLE ENRICHMENT TYPES:
1. websites - Find company websites (Free, ~2s per contact)
2. emails - Find and verify emails ($0.23/contact needing enrichment, ~45s per contact)
3. phones - Extract phone numbers (Free, ~3s per contact)
4. contact_names - Extract contact names (Free, ~3s per contact)
5. instagram_handles - Find Instagram handles ($0.05/contact, ~24s per contact)

Provide recommendations in JSON format with:
1. recommended_config: Dict of enrichment_type -> bool (what to enable/disable)
2. rationale: String explaining why, referencing specific data quality observations
3. optimization_tips: List of specific optimization tips based on data analysis
4. module_suggestions: Dict mapping module names to suggestions (skip, run, optimize)
5. data_quality_notes: String with observations about input data quality

Focus on:
- Analyzing the actual sample data to assess quality (not just column presence)
- Skipping enrichments for data that's already well-populated (>80% fill rate)
- Recommending enrichment for columns with low fill rates
- Prioritizing high-value, low-cost enrichments
- Balancing completeness vs cost/time

Respond ONLY with valid JSON, no markdown formatting."""

    try:
        client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
        
        response = client.chat.completions.create(
            model="gpt-4o-mini",  # Use cheaper model for suggestions
            messages=[
                {"role": "system", "content": "You are a data enrichment optimization expert. Always respond with valid JSON only."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            max_tokens=1000
        )
        
        response_text = response.choices[0].message.content.strip()
        
        # Remove markdown code blocks if present
        if response_text.startswith('```'):
            response_text = response_text.split('```')[1]
            if response_text.startswith('json'):
                response_text = response_text[4:]
            response_text = response_text.strip()
        
        suggestions = json.loads(response_text)
        
        # Calculate actual savings
        current_cost, current_time = estimate_cost_and_time(row_count, current_config, parallel_factor=1.5)
        recommended_config = suggestions.get('recommended_config', current_config)
        recommended_cost, recommended_time = estimate_cost_and_time(row_count, recommended_config, parallel_factor=1.5)
        
        suggestions['actual_cost_savings'] = current_cost - recommended_cost
        suggestions['actual_time_savings'] = current_time - recommended_time
        suggestions['current_cost'] = current_cost
        suggestions['current_time'] = current_time
        suggestions['recommended_cost'] = recommended_cost
        suggestions['recommended_time'] = recommended_time
        suggestions['current_config'] = current_config  # Store for display
        
        return suggestions
        
    except json.JSONDecodeError as e:
        return {
            'error': f'Failed to parse AI response: {e}',
            'raw_response': response_text if 'response_text' in locals() else None,
            'recommendations': [],
            'rationale': 'AI response was not valid JSON'
        }
    except Exception as e:
        return {
            'error': str(e),
            'recommendations': [],
            'rationale': 'Failed to get AI suggestions'
        }


def format_suggestions_for_display(suggestions: Dict) -> str:
    """
    Format AI suggestions for display to user.
    
    Args:
        suggestions: Dict from suggest_enrichment_strategy
        
    Returns:
        Formatted string for display
    """
    if 'error' in suggestions:
        return f"⚠️  Could not generate suggestions: {suggestions['error']}"
    
    lines = []
    lines.append("\n" + "=" * 60)
    lines.append("AI ENRICHMENT STRATEGY RECOMMENDATIONS")
    lines.append("=" * 60)
    
    # Rationale
    if 'rationale' in suggestions:
        lines.append(f"\n💡 RATIONALE:")
        lines.append(f"   {suggestions['rationale']}")
    
    # Cost/Time savings
    if 'actual_cost_savings' in suggestions:
        cost_savings = suggestions['actual_cost_savings']
        time_savings = suggestions['actual_time_savings']
        
        lines.append(f"\n💰 SAVINGS:")
        if cost_savings > 0:
            lines.append(f"   Cost: Save ${cost_savings:.2f} (${suggestions.get('current_cost', 0):.2f} → ${suggestions.get('recommended_cost', 0):.2f})")
        else:
            lines.append(f"   Cost: ${suggestions.get('recommended_cost', 0):.2f}")
        
        if time_savings > 0:
            lines.append(f"   Time: Save {time_savings:.1f} min ({suggestions.get('current_time', 0):.1f} min → {suggestions.get('recommended_time', 0):.1f} min)")
        else:
            lines.append(f"   Time: {suggestions.get('recommended_time', 0):.1f} min")
    
    # Recommended config changes
    if 'recommended_config' in suggestions:
        lines.append(f"\n📋 RECOMMENDED CHANGES:")
        recommended = suggestions['recommended_config']
        for enrichment_type, enabled in recommended.items():
            current_enabled = suggestions.get('current_config', {}).get(enrichment_type, True)
            if enabled != current_enabled:
                status = "ENABLE" if enabled else "DISABLE"
                name = ENRICHMENT_TYPES.get(enrichment_type, {}).get('name', enrichment_type)
                lines.append(f"   {status}: {name}")
    
    # Optimization tips
    if 'optimization_tips' in suggestions and suggestions['optimization_tips']:
        lines.append(f"\n⚡ OPTIMIZATION TIPS:")
        for tip in suggestions['optimization_tips']:
            lines.append(f"   • {tip}")
    
    # Module suggestions
    if 'module_suggestions' in suggestions and suggestions['module_suggestions']:
        lines.append(f"\n🔧 MODULE SUGGESTIONS:")
        for module, suggestion in suggestions['module_suggestions'].items():
            lines.append(f"   {module}: {suggestion}")

    # Data quality notes (from improved analysis)
    if 'data_quality_notes' in suggestions and suggestions['data_quality_notes']:
        lines.append(f"\n📊 DATA QUALITY:")
        lines.append(f"   {suggestions['data_quality_notes']}")

    lines.append("=" * 60)
    
    return "\n".join(lines)

