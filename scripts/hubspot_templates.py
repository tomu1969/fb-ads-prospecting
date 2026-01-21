#!/usr/bin/env python3
"""HubSpot Templates Sync - Push email templates to HubSpot Marketing Hub.

Creates or updates email templates in HubSpot Marketing Hub for use in workflows.
"""

import os
import json
import logging
import argparse
from pathlib import Path
from typing import Dict, Any, Optional, List

import requests
from dotenv import load_dotenv

load_dotenv()

# Module logger
logger = logging.getLogger(__name__)

# HubSpot Configuration
HUBSPOT_API_KEY = os.getenv("HUBSPOT_API_KEY")
HUBSPOT_BASE_URL = "https://api.hubapi.com"

# Paths
CONFIG_DIR = Path(__file__).parent.parent / "config" / "email_templates"
TEMPLATES_JSON = CONFIG_DIR / "templates.json"


def load_registry() -> Dict[str, Any]:
    """Load the templates registry JSON."""
    if not TEMPLATES_JSON.exists():
        logger.error(f"Templates registry not found: {TEMPLATES_JSON}")
        return {"templates": [], "variable_sources": {}}

    with open(TEMPLATES_JSON) as f:
        return json.load(f)


def save_registry(registry: Dict[str, Any]) -> None:
    """Save the templates registry JSON."""
    with open(TEMPLATES_JSON, 'w') as f:
        json.dump(registry, f, indent=2)
    logger.info(f"Saved registry to {TEMPLATES_JSON}")


def load_template_file(template_path: str) -> tuple[str, str, Dict[str, Any]]:
    """
    Load template file and parse subject/body.

    Args:
        template_path: Relative path from config/email_templates/.

    Returns:
        Tuple of (subject, body, frontmatter).
    """
    full_path = CONFIG_DIR / template_path

    with open(full_path) as f:
        content = f.read()

    # Parse frontmatter
    frontmatter = {}
    body_start = 0

    if content.startswith('---'):
        lines = content.split('\n')
        for i, line in enumerate(lines[1:], 1):
            if line.strip() == '---':
                body_start = i + 1
                break
            if ':' in line:
                key, value = line.split(':', 1)
                frontmatter[key.strip()] = value.strip()

    # Extract subject and body
    remaining = '\n'.join(content.split('\n')[body_start:]).strip()
    subject = ""
    body = remaining

    for line in remaining.split('\n'):
        if line.lower().startswith('subject:'):
            subject = line.split(':', 1)[1].strip()
            body = '\n'.join(remaining.split('\n')[1:]).strip()
            break

    return subject, body, frontmatter


def convert_to_hubspot_tokens(text: str, variable_sources: Dict[str, Any]) -> str:
    """
    Convert {{ var_name }} to HubSpot tokens.

    Args:
        text: Text with {{ var_name }} placeholders.
        variable_sources: Mapping from var names to HubSpot tokens.

    Returns:
        Text with HubSpot tokens.
    """
    import re

    def replace_var(match):
        var_name = match.group(1)
        if var_name in variable_sources:
            return variable_sources[var_name].get("hubspot_token", match.group(0))
        return match.group(0)

    return re.sub(r'\{\{\s*(\w+)\s*\}\}', replace_var, text)


def list_hubspot_templates() -> List[Dict[str, Any]]:
    """
    List existing email templates in HubSpot.

    Returns:
        List of template objects.
    """
    if not HUBSPOT_API_KEY:
        logger.error("HUBSPOT_API_KEY not configured")
        return []

    headers = {
        "Authorization": f"Bearer {HUBSPOT_API_KEY}",
        "Content-Type": "application/json"
    }

    # HubSpot Marketing Email templates endpoint
    url = f"{HUBSPOT_BASE_URL}/marketing/v3/emails"

    try:
        resp = requests.get(url, headers=headers, params={"limit": 100})
        if resp.status_code == 200:
            data = resp.json()
            return data.get("results", [])
        else:
            logger.error(f"Failed to list templates: HTTP {resp.status_code}")
            logger.debug(resp.text)
            return []
    except Exception as e:
        logger.error(f"Error listing templates: {e}")
        return []


def get_hubspot_template(template_id: str) -> Optional[Dict[str, Any]]:
    """
    Get a specific HubSpot email template.

    Args:
        template_id: HubSpot template ID.

    Returns:
        Template object or None.
    """
    if not HUBSPOT_API_KEY:
        return None

    headers = {
        "Authorization": f"Bearer {HUBSPOT_API_KEY}",
        "Content-Type": "application/json"
    }

    url = f"{HUBSPOT_BASE_URL}/marketing/v3/emails/{template_id}"

    try:
        resp = requests.get(url, headers=headers)
        if resp.status_code == 200:
            return resp.json()
        else:
            logger.error(f"Failed to get template: HTTP {resp.status_code}")
            return None
    except Exception as e:
        logger.error(f"Error getting template: {e}")
        return None


def create_hubspot_template(
    name: str,
    subject: str,
    body: str,
    folder_id: Optional[str] = None
) -> Optional[str]:
    """
    Create a new email template in HubSpot.

    Args:
        name: Template name.
        subject: Email subject line (with HubSpot tokens).
        body: Email body HTML (with HubSpot tokens).
        folder_id: Optional folder ID.

    Returns:
        Created template ID or None.
    """
    if not HUBSPOT_API_KEY:
        logger.error("HUBSPOT_API_KEY not configured")
        return None

    headers = {
        "Authorization": f"Bearer {HUBSPOT_API_KEY}",
        "Content-Type": "application/json"
    }

    # Convert plain text to simple HTML
    html_body = body.replace('\n', '<br>\n')
    html_body = f"<div style=\"font-family: Arial, sans-serif; font-size: 14px;\">\n{html_body}\n</div>"

    payload = {
        "name": name,
        "subject": subject,
        "content": {
            "html": html_body,
            "plainText": body
        },
        "type": "REGULAR_EMAIL"
    }

    if folder_id:
        payload["folderId"] = folder_id

    url = f"{HUBSPOT_BASE_URL}/marketing/v3/emails"

    try:
        resp = requests.post(url, headers=headers, json=payload)
        if resp.status_code in (200, 201):
            data = resp.json()
            template_id = data.get("id")
            logger.info(f"Created template: {name} (ID: {template_id})")
            return template_id
        else:
            logger.error(f"Failed to create template: HTTP {resp.status_code}")
            logger.debug(resp.text)
            return None
    except Exception as e:
        logger.error(f"Error creating template: {e}")
        return None


def update_hubspot_template(
    template_id: str,
    name: str,
    subject: str,
    body: str
) -> bool:
    """
    Update an existing HubSpot email template.

    Args:
        template_id: HubSpot template ID.
        name: Template name.
        subject: Email subject line.
        body: Email body.

    Returns:
        True if successful.
    """
    if not HUBSPOT_API_KEY:
        logger.error("HUBSPOT_API_KEY not configured")
        return False

    headers = {
        "Authorization": f"Bearer {HUBSPOT_API_KEY}",
        "Content-Type": "application/json"
    }

    html_body = body.replace('\n', '<br>\n')
    html_body = f"<div style=\"font-family: Arial, sans-serif; font-size: 14px;\">\n{html_body}\n</div>"

    payload = {
        "name": name,
        "subject": subject,
        "content": {
            "html": html_body,
            "plainText": body
        }
    }

    url = f"{HUBSPOT_BASE_URL}/marketing/v3/emails/{template_id}"

    try:
        resp = requests.patch(url, headers=headers, json=payload)
        if resp.status_code == 200:
            logger.info(f"Updated template: {name} (ID: {template_id})")
            return True
        else:
            logger.error(f"Failed to update template: HTTP {resp.status_code}")
            logger.debug(resp.text)
            return False
    except Exception as e:
        logger.error(f"Error updating template: {e}")
        return False


def push_template(template_id: str, dry_run: bool = False) -> bool:
    """
    Push a local template to HubSpot.

    Args:
        template_id: Local template ID.
        dry_run: If True, show what would be pushed without doing it.

    Returns:
        True if successful.
    """
    registry = load_registry()
    variable_sources = registry.get("variable_sources", {})

    # Find template in registry
    template_meta = None
    template_index = -1
    for i, t in enumerate(registry.get("templates", [])):
        if t["id"] == template_id:
            template_meta = t
            template_index = i
            break

    if not template_meta:
        logger.error(f"Template not found in registry: {template_id}")
        return False

    # Load template file
    subject, body, frontmatter = load_template_file(template_meta["path"])

    # Convert to HubSpot tokens
    hs_subject = convert_to_hubspot_tokens(subject, variable_sources)
    hs_body = convert_to_hubspot_tokens(body, variable_sources)

    name = f"[Pipeline] {template_meta['name']}"

    logger.info(f"Template: {template_id}")
    logger.info(f"Name: {name}")
    logger.info(f"Subject: {hs_subject}")

    if dry_run:
        print("\n" + "=" * 60)
        print("[DRY RUN] Would push to HubSpot:")
        print("=" * 60)
        print(f"Name: {name}")
        print(f"Subject: {hs_subject}")
        print("-" * 60)
        print(hs_body)
        print("=" * 60)
        return True

    # Check if template already exists in HubSpot
    existing_id = template_meta.get("hubspot_template_id")

    if existing_id:
        # Update existing
        success = update_hubspot_template(existing_id, name, hs_subject, hs_body)
    else:
        # Create new
        new_id = create_hubspot_template(name, hs_subject, hs_body)
        if new_id:
            # Update registry with HubSpot ID
            registry["templates"][template_index]["hubspot_template_id"] = new_id
            save_registry(registry)
            success = True
        else:
            success = False

    return success


def sync_all(dry_run: bool = False) -> Dict[str, bool]:
    """
    Sync all local templates to HubSpot.

    Args:
        dry_run: If True, show what would be synced.

    Returns:
        Dict of template_id -> success.
    """
    registry = load_registry()
    results = {}

    for t in registry.get("templates", []):
        logger.info(f"\nSyncing: {t['id']}")
        results[t["id"]] = push_template(t["id"], dry_run=dry_run)

    return results


def list_status() -> None:
    """Print sync status of all templates."""
    registry = load_registry()

    print("\nTemplate Sync Status")
    print("=" * 70)
    print(f"{'ID':<30} {'HubSpot ID':<20} {'Status':<10}")
    print("-" * 70)

    for t in registry.get("templates", []):
        hs_id = t.get("hubspot_template_id")
        if hs_id:
            status = "Synced"
        else:
            status = "Local only"

        print(f"{t['id']:<30} {hs_id or 'N/A':<20} {status:<10}")

    print("=" * 70)


def setup_logging(verbose: bool = False) -> logging.Logger:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO

    logging.basicConfig(
        level=level,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('hubspot_templates.log')
        ]
    )

    return logging.getLogger(__name__)


def parse_args(args: Optional[List[str]] = None) -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Sync email templates with HubSpot Marketing Hub',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List sync status
  python hubspot_templates.py list

  # Push a single template (dry run)
  python hubspot_templates.py push --template pre_demo_confirmation --dry-run

  # Push a single template
  python hubspot_templates.py push --template pre_demo_confirmation

  # Sync all templates
  python hubspot_templates.py sync-all

  # Sync all (dry run)
  python hubspot_templates.py sync-all --dry-run
        """
    )

    subparsers = parser.add_subparsers(dest='command', help='Command to run')

    # List command
    list_parser = subparsers.add_parser('list', help='List template sync status')

    # Push command
    push_parser = subparsers.add_parser('push', help='Push a template to HubSpot')
    push_parser.add_argument('--template', '-t', required=True, help='Template ID')
    push_parser.add_argument('--dry-run', action='store_true', help='Preview without pushing')

    # Sync-all command
    sync_parser = subparsers.add_parser('sync-all', help='Sync all templates to HubSpot')
    sync_parser.add_argument('--dry-run', action='store_true', help='Preview without pushing')

    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')

    return parser.parse_args(args)


def main():
    """Main entry point."""
    args = parse_args()
    setup_logging(verbose=args.verbose)

    if not HUBSPOT_API_KEY:
        logger.error("HUBSPOT_API_KEY not configured in .env")
        return 1

    if args.command == 'list':
        list_status()
        return 0

    elif args.command == 'push':
        success = push_template(args.template, dry_run=args.dry_run)
        return 0 if success else 1

    elif args.command == 'sync-all':
        results = sync_all(dry_run=args.dry_run)
        failed = sum(1 for v in results.values() if not v)
        logger.info(f"\nSynced: {len(results) - failed}/{len(results)}")
        return 0 if failed == 0 else 1

    else:
        print("Use 'list', 'push', or 'sync-all' command. See --help for usage.")
        return 1


if __name__ == '__main__':
    exit(main())
