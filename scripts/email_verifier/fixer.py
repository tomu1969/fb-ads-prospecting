"""
Email Fixer - Automatically fixes issues found by the verification agent.

Handles:
- Invalid contact names (None, nan) → Generic greeting
- Template variables ({{...}}) → Remove problematic sentences
- Email/name mismatches → Extract name from email or use generic greeting
- Greeting mismatches → Fix greeting to match email recipient

Usage:
    python fixer.py --drafts output/email_campaign/drafts_batch2.csv
    python fixer.py --drafts drafts.csv --output fixed_drafts.csv
    python fixer.py --drafts drafts.csv --dry-run
"""

import argparse
import logging
import re
import sys
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional
from dataclasses import dataclass

import pandas as pd

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('email_fixer.log')
    ]
)
logger = logging.getLogger(__name__)

# Default paths
BASE_DIR = Path(__file__).parent.parent.parent
DEFAULT_OUTPUT_SUFFIX = "_fixed"

# Invalid contact name patterns
INVALID_NAMES = {
    'none', 'none none', 'nan', 'n/a', 'null', 'undefined', 'unknown',
    'meet our team', 'social media', 'featured video', 'your information',
    'contact', 'info', 'support', 'team', 'admin'
}

# Generic email prefixes (can't extract person name)
GENERIC_EMAIL_PREFIXES = {
    'info', 'contact', 'support', 'sales', 'hello', 'hi', 'team',
    'admin', 'office', 'general', 'mail', 'enquiries', 'inquiries',
    'help', 'service', 'services', 'marketing', 'press', 'media',
    'careers', 'jobs', 'hr', 'billing', 'accounts', 'orders', 'orderdesk',
    'editor', 'news', 'webmaster'
}


@dataclass
class Fix:
    """Record of a fix applied."""
    field: str
    original: str
    fixed: str
    reason: str


def is_invalid_name(name: str) -> bool:
    """Check if contact name is invalid."""
    if not name or pd.isna(name):
        return True
    name_lower = str(name).lower().strip()
    return name_lower in INVALID_NAMES or name_lower == 'nan'


def extract_name_from_email(email: str) -> Optional[str]:
    """
    Extract a person's first name from email address.

    Examples:
        john@company.com → John
        john.smith@company.com → John
        jsmith@company.com → None (can't determine)
        info@company.com → None (generic)

    Returns:
        Capitalized first name or None if can't extract
    """
    if not email or '@' not in email:
        return None

    prefix = email.split('@')[0].lower()

    # Skip generic prefixes
    if prefix in GENERIC_EMAIL_PREFIXES:
        return None

    # Common first names for validation
    common_names = {
        'john', 'mike', 'david', 'james', 'robert', 'michael', 'william',
        'mary', 'jennifer', 'linda', 'sarah', 'jessica', 'emily', 'ashley',
        'brian', 'chris', 'matt', 'jason', 'ryan', 'kevin', 'steve',
        'lisa', 'kim', 'anna', 'karen', 'nancy', 'betty', 'helen',
        'tim', 'tom', 'bob', 'joe', 'dan', 'mark', 'paul', 'peter',
        'amy', 'jen', 'kate', 'meg', 'sue', 'ann', 'pat', 'lee',
        'jeremy', 'madison', 'victoria', 'wendy', 'kelly', 'tia',
        'camille', 'charlee', 'erin', 'richard', 'roy', 'dean',
        'greg', 'alex', 'nick', 'tony', 'bill', 'ben', 'sam', 'max',
        'jake', 'luke', 'adam', 'jack', 'eric', 'sean', 'alan', 'carl',
        'chad', 'chad', 'cole', 'drew', 'evan', 'gary', 'glen', 'greg',
        'ivan', 'joel', 'kent', 'kirk', 'kurt', 'lane', 'lars', 'leon',
        'liam', 'luis', 'marc', 'neil', 'noah', 'omar', 'owen', 'phil',
        'reid', 'rick', 'ross', 'russ', 'seth', 'stan', 'todd', 'troy',
        'wade', 'walt', 'ward', 'zach', 'abby', 'ally', 'andy', 'beth',
        'cara', 'cary', 'dana', 'dawn', 'deb', 'gail', 'gina', 'hope',
        'jade', 'jane', 'jean', 'jill', 'joan', 'jodi', 'judy', 'june',
        'kara', 'katy', 'kris', 'lana', 'lara', 'lena', 'lily', 'lori',
        'lucy', 'lynn', 'macy', 'maya', 'megan', 'nina', 'nora', 'olga',
        'pam', 'rosa', 'rose', 'ruby', 'ruth', 'sara', 'tara', 'tina',
        'vera', 'vicky', 'zoe', 'aspen', 'chirag', 'hoshyar', 'keveanu',
        'terrance', 'carroll', 'stevie', 'lovetra', 'jesus', 'wendy'
    }

    # Pattern 1: firstname.lastname or firstname_lastname
    if '.' in prefix or '_' in prefix:
        parts = re.split(r'[._]', prefix)
        first_part = parts[0]
        # Must be at least 3 chars and be a known name or look like one
        if len(first_part) >= 3 and first_part.isalpha():
            if first_part in common_names:
                return first_part.capitalize()
            # Skip if it looks like initial+lastname (e.g., "dsmith" from d.smith)
            # These are usually just single letter, so first_part should be > 1 char

    # Pattern 2: firstname only (e.g., john@company.com)
    # Only accept if it's a known common name
    if prefix.isalpha() and prefix in common_names:
        return prefix.capitalize()

    # Pattern 3: firstnamelastname (e.g., johnsmith@)
    # Try to find common first name at the start
    for name in sorted(common_names, key=len, reverse=True):  # Try longer names first
        if prefix.startswith(name) and len(prefix) > len(name):
            # Make sure remaining part looks like a lastname (3+ chars)
            remainder = prefix[len(name):]
            if len(remainder) >= 3 and remainder.isalpha():
                return name.capitalize()
    
    return None


def fix_greeting(email_body: str, new_name: Optional[str]) -> Tuple[str, bool]:
    """
    Fix the greeting in email body.
    
    Args:
        email_body: Full email body
        new_name: New name to use, or None for generic greeting
    
    Returns:
        Tuple of (fixed_body, was_changed)
    """
    # Common greeting patterns
    greeting_patterns = [
        (r'^Hi\s+None\s*,', 'Hi there,'),
        (r'^Hi\s+nan\s*,', 'Hi there,'),
        (r'^Hello\s+None\s*,', 'Hello,'),
        (r'^Hello\s+nan\s*,', 'Hello,'),
        (r'^Dear\s+None\s*,', 'Hello,'),
        (r'^Dear\s+nan\s*,', 'Hello,'),
    ]
    
    fixed_body = email_body
    changed = False
    
    # Fix None/nan greetings first
    for pattern, replacement in greeting_patterns:
        if re.search(pattern, fixed_body, re.IGNORECASE | re.MULTILINE):
            if new_name:
                replacement = f"Hi {new_name},"
            fixed_body = re.sub(pattern, replacement, fixed_body, flags=re.IGNORECASE | re.MULTILINE)
            changed = True
    
    # If we have a new name and there's a different personal greeting, update it
    if new_name and not changed:
        # Match "Hi <AnyName>," pattern
        match = re.match(r'^(Hi|Hello|Dear)\s+([A-Za-z]+)\s*,', fixed_body.strip(), re.IGNORECASE)
        if match:
            old_greeting = match.group(0)
            old_name = match.group(2)
            # Extract first name from new_name for comparison
            new_name_first = new_name.split()[0] if new_name else ''
            # Only change if the first name is actually wrong
            if old_name.lower() != new_name_first.lower():
                new_greeting = f"{match.group(1)} {new_name_first},"
                fixed_body = fixed_body.replace(old_greeting, new_greeting, 1)
                changed = True

    return fixed_body, changed


def fix_template_variables(email_body: str) -> Tuple[str, bool, List[str]]:
    """
    Remove sentences containing template variables.
    
    Args:
        email_body: Full email body
    
    Returns:
        Tuple of (fixed_body, was_changed, removed_vars)
    """
    # Find all template variables
    template_vars = re.findall(r'\{\{[^}]+\}\}', email_body)
    
    if not template_vars:
        return email_body, False, []
    
    fixed_body = email_body
    
    # Remove sentences containing template variables
    # Split into sentences and filter out ones with {{...}}
    lines = fixed_body.split('\n')
    cleaned_lines = []
    
    for line in lines:
        if '{{' in line and '}}' in line:
            # Skip this line entirely
            continue
        cleaned_lines.append(line)
    
    fixed_body = '\n'.join(cleaned_lines)
    
    # Clean up any double newlines created
    fixed_body = re.sub(r'\n{3,}', '\n\n', fixed_body)
    
    return fixed_body, True, template_vars


def fix_draft(draft: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Fix]]:
    """
    Apply all fixes to a single draft.
    
    Args:
        draft: Draft dict
    
    Returns:
        Tuple of (fixed_draft, list_of_fixes)
    """
    fixed = dict(draft)
    fixes = []
    
    contact_name = str(draft.get('contact_name', ''))
    email = str(draft.get('primary_email', ''))
    email_body = str(draft.get('email_body', ''))
    
    # 1. Fix invalid contact names
    if is_invalid_name(contact_name):
        # Try to extract name from email
        extracted_name = extract_name_from_email(email)
        
        if extracted_name:
            fixed['contact_name'] = extracted_name
            fixes.append(Fix(
                field='contact_name',
                original=contact_name,
                fixed=extracted_name,
                reason=f"Extracted from email: {email}"
            ))
        else:
            fixed['contact_name'] = ''
            fixes.append(Fix(
                field='contact_name',
                original=contact_name,
                fixed='',
                reason="Invalid name, using generic greeting"
            ))
    
    # 2. Fix template variables
    fixed_body, vars_changed, removed_vars = fix_template_variables(email_body)
    if vars_changed:
        fixes.append(Fix(
            field='email_body',
            original=f"[contained {', '.join(removed_vars)}]",
            fixed="[removed problematic sentences]",
            reason=f"Removed template variables: {', '.join(removed_vars)}"
        ))
        email_body = fixed_body
    
    # 3. Fix greeting to match contact name or email
    # Determine the correct name to use in greeting
    final_name = fixed.get('contact_name', '')
    if not final_name or is_invalid_name(final_name):
        # Try extracting from email if we haven't already
        final_name = extract_name_from_email(email)
    
    fixed_body, greeting_changed = fix_greeting(email_body, final_name if final_name else None)
    if greeting_changed:
        fixes.append(Fix(
            field='email_body',
            original="[greeting]",
            fixed=f"Hi {final_name}," if final_name else "Hi there,",
            reason="Fixed greeting to match recipient"
        ))
        email_body = fixed_body
    
    # 4. Check for email/name mismatch and fix if needed
    if fixed.get('contact_name') and not is_invalid_name(fixed['contact_name']):
        email_prefix = email.split('@')[0].lower() if '@' in email else ''
        contact_first = fixed['contact_name'].split()[0].lower()

        # If names don't match and email isn't generic
        if email_prefix not in GENERIC_EMAIL_PREFIXES:
            if contact_first not in email_prefix and email_prefix not in contact_first:
                # Try to extract name from email
                email_name = extract_name_from_email(email)
                if email_name:
                    # Update contact name to match email
                    old_name = fixed['contact_name']
                    fixed['contact_name'] = email_name

                    # Also fix the greeting
                    fixed_body, _ = fix_greeting(email_body, email_name)
                    email_body = fixed_body

                    fixes.append(Fix(
                        field='contact_name',
                        original=old_name,
                        fixed=email_name,
                        reason=f"Changed to match email recipient: {email}"
                    ))
                else:
                    # Can't extract name - use generic greeting to avoid wrong name
                    old_name = fixed['contact_name']
                    fixed['contact_name'] = ''

                    # Change greeting to generic
                    fixed_body = re.sub(
                        r'^(Hi|Hello|Dear)\s+[A-Za-z]+\s*,',
                        'Hi there,',
                        email_body.strip(),
                        flags=re.IGNORECASE | re.MULTILINE
                    )
                    email_body = fixed_body

                    fixes.append(Fix(
                        field='contact_name',
                        original=old_name,
                        fixed='[generic]',
                        reason=f"Email '{email_prefix}@...' doesn't match '{old_name}', using generic greeting"
                    ))
    
    fixed['email_body'] = email_body
    
    return fixed, fixes


def fix_all_drafts(drafts_df: pd.DataFrame, dry_run: bool = False) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Fix all drafts and return fixed DataFrame with stats.
    
    Args:
        drafts_df: DataFrame of drafts
        dry_run: If True, don't modify, just report what would be fixed
    
    Returns:
        Tuple of (fixed_df, stats_dict)
    """
    stats = {
        'total': len(drafts_df),
        'fixed': 0,
        'fix_count': 0,
        'by_field': {},
        'fixes_list': []
    }
    
    fixed_rows = []
    
    for idx, draft in drafts_df.iterrows():
        fixed_draft, fixes = fix_draft(draft.to_dict())
        
        if fixes:
            stats['fixed'] += 1
            stats['fix_count'] += len(fixes)
            
            for fix in fixes:
                field = fix.field
                stats['by_field'][field] = stats['by_field'].get(field, 0) + 1
                stats['fixes_list'].append({
                    'page_name': draft.get('page_name', ''),
                    'field': fix.field,
                    'original': fix.original,
                    'fixed': fix.fixed,
                    'reason': fix.reason
                })
        
        fixed_rows.append(fixed_draft)
    
    fixed_df = pd.DataFrame(fixed_rows)
    
    return fixed_df, stats


def print_summary(stats: Dict[str, Any], dry_run: bool = False):
    """Print fix summary."""
    mode = "DRY RUN - " if dry_run else ""
    
    print("\n" + "=" * 60)
    print(f"{mode}EMAIL FIXER SUMMARY")
    print("=" * 60)
    
    print(f"\nTotal drafts:     {stats['total']}")
    print(f"Drafts fixed:     {stats['fixed']}")
    print(f"Total fixes:      {stats['fix_count']}")
    
    if stats['by_field']:
        print(f"\nFixes by field:")
        for field, count in sorted(stats['by_field'].items(), key=lambda x: -x[1]):
            print(f"  {field}: {count}")
    
    if stats['fixes_list']:
        print(f"\n{'='*60}")
        print("FIXES APPLIED:")
        print("=" * 60)
        
        for fix in stats['fixes_list'][:20]:  # Show first 20
            print(f"\n  Company: {fix['page_name']}")
            print(f"  Field:   {fix['field']}")
            print(f"  Before:  {fix['original'][:50]}...")
            print(f"  After:   {fix['fixed'][:50]}...")
            print(f"  Reason:  {fix['reason']}")
        
        if len(stats['fixes_list']) > 20:
            print(f"\n  ... and {len(stats['fixes_list']) - 20} more fixes")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Fix issues in email drafts'
    )
    
    parser.add_argument(
        '--drafts', '-d',
        type=str,
        required=True,
        help='Path to email drafts CSV'
    )
    
    parser.add_argument(
        '--output', '-o',
        type=str,
        default=None,
        help='Output path for fixed drafts (default: adds _fixed suffix)'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be fixed without saving'
    )
    
    args = parser.parse_args()
    
    print("""
    ╔═══════════════════════════════════════════════╗
    ║             EMAIL FIXER AGENT                 ║
    ║   Auto-fix issues in personalized emails      ║
    ╚═══════════════════════════════════════════════╝
    """)
    
    # Resolve paths
    drafts_path = Path(args.drafts)
    if not drafts_path.is_absolute():
        drafts_path = BASE_DIR / args.drafts
    
    if not drafts_path.exists():
        logger.error(f"Drafts file not found: {drafts_path}")
        sys.exit(1)
    
    # Determine output path
    if args.output:
        output_path = Path(args.output)
        if not output_path.is_absolute():
            output_path = BASE_DIR / args.output
    else:
        output_path = drafts_path.parent / f"{drafts_path.stem}{DEFAULT_OUTPUT_SUFFIX}{drafts_path.suffix}"
    
    # Load drafts
    logger.info(f"Loading drafts from: {drafts_path}")
    drafts_df = pd.read_csv(drafts_path)
    logger.info(f"Loaded {len(drafts_df)} drafts")
    
    # Fix drafts
    logger.info("Applying fixes...")
    fixed_df, stats = fix_all_drafts(drafts_df, dry_run=args.dry_run)
    
    # Print summary
    print_summary(stats, dry_run=args.dry_run)
    
    # Save if not dry run
    if not args.dry_run and stats['fixed'] > 0:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fixed_df.to_csv(output_path, index=False)
        logger.info(f"Fixed drafts saved to: {output_path}")
        print(f"\n[OK] Fixed drafts saved to: {output_path}")
    elif args.dry_run:
        print(f"\n[DRY RUN] No files modified")
    else:
        print(f"\n[OK] No fixes needed")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
