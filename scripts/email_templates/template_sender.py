#!/usr/bin/env python3
"""Template Sender - Send sales pipeline emails using templates.

Fetches deal/contact data from HubSpot and sends templated emails via Gmail.
"""

import os
import json
import logging
import argparse
import smtplib
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple

import requests
from dotenv import load_dotenv

from .template_loader import load_template, render_template, list_templates, Template

load_dotenv()

# Module logger
logger = logging.getLogger(__name__)

# HubSpot Configuration
HUBSPOT_API_KEY = os.getenv("HUBSPOT_API_KEY")
HUBSPOT_BASE_URL = "https://api.hubapi.com"

# Gmail Configuration
GMAIL_ADDRESS = os.getenv('GMAIL_ADDRESS')
GMAIL_APP_PASSWORD = os.getenv('GMAIL_APP_PASSWORD')
GMAIL_SEND_AS = os.getenv('GMAIL_SEND_AS', GMAIL_ADDRESS)
SMTP_SERVER = 'smtp.gmail.com'
SMTP_PORT = 587

# Owner names (fallback if not in HubSpot)
OWNER_NAMES = {
    "1918855052": "Tomas",
    "1951969820": "Lina",
}

# Logs directory
LOGS_DIR = Path(__file__).parent.parent.parent / "output" / "email_campaign" / "template_logs"


def fetch_deal(deal_id: str) -> Optional[Dict[str, Any]]:
    """
    Fetch deal details from HubSpot.

    Args:
        deal_id: HubSpot deal ID.

    Returns:
        Deal properties dict or None if not found.
    """
    if not HUBSPOT_API_KEY:
        logger.error("HUBSPOT_API_KEY not configured")
        return None

    headers = {
        "Authorization": f"Bearer {HUBSPOT_API_KEY}",
        "Content-Type": "application/json"
    }

    properties = [
        "dealname", "amount", "dealstage", "closedate",
        "hubspot_owner_id", "demo_date", "demo_time",
        "meeting_link", "proposal_link"
    ]

    url = f"{HUBSPOT_BASE_URL}/crm/v3/objects/deals/{deal_id}"
    params = {
        "properties": ",".join(properties),
        "associations": "contacts,companies"
    }

    try:
        resp = requests.get(url, headers=headers, params=params)
        if resp.status_code == 200:
            data = resp.json()
            return data.get("properties", {})
        else:
            logger.error(f"Failed to fetch deal {deal_id}: HTTP {resp.status_code}")
            return None
    except Exception as e:
        logger.error(f"Error fetching deal: {e}")
        return None


def fetch_contact(contact_id: str) -> Optional[Dict[str, Any]]:
    """
    Fetch contact details from HubSpot.

    Args:
        contact_id: HubSpot contact ID.

    Returns:
        Contact properties dict or None if not found.
    """
    if not HUBSPOT_API_KEY:
        return None

    headers = {
        "Authorization": f"Bearer {HUBSPOT_API_KEY}",
        "Content-Type": "application/json"
    }

    properties = ["firstname", "lastname", "email", "company"]

    url = f"{HUBSPOT_BASE_URL}/crm/v3/objects/contacts/{contact_id}"
    params = {"properties": ",".join(properties)}

    try:
        resp = requests.get(url, headers=headers, params=params)
        if resp.status_code == 200:
            data = resp.json()
            return data.get("properties", {})
        else:
            logger.error(f"Failed to fetch contact {contact_id}: HTTP {resp.status_code}")
            return None
    except Exception as e:
        logger.error(f"Error fetching contact: {e}")
        return None


def fetch_associated_contact(deal_id: str) -> Optional[Dict[str, Any]]:
    """
    Fetch the primary contact associated with a deal.

    Args:
        deal_id: HubSpot deal ID.

    Returns:
        Contact properties dict or None.
    """
    if not HUBSPOT_API_KEY:
        return None

    headers = {
        "Authorization": f"Bearer {HUBSPOT_API_KEY}",
        "Content-Type": "application/json"
    }

    url = f"{HUBSPOT_BASE_URL}/crm/v3/objects/deals/{deal_id}/associations/contacts"

    try:
        resp = requests.get(url, headers=headers)
        if resp.status_code == 200:
            data = resp.json()
            results = data.get("results", [])
            if results:
                contact_id = results[0].get("id")
                return fetch_contact(contact_id)
        return None
    except Exception as e:
        logger.error(f"Error fetching associated contact: {e}")
        return None


def fetch_associated_company(deal_id: str) -> Optional[Dict[str, Any]]:
    """
    Fetch the primary company associated with a deal.

    Args:
        deal_id: HubSpot deal ID.

    Returns:
        Company properties dict or None.
    """
    if not HUBSPOT_API_KEY:
        return None

    headers = {
        "Authorization": f"Bearer {HUBSPOT_API_KEY}",
        "Content-Type": "application/json"
    }

    url = f"{HUBSPOT_BASE_URL}/crm/v3/objects/deals/{deal_id}/associations/companies"

    try:
        resp = requests.get(url, headers=headers)
        if resp.status_code == 200:
            data = resp.json()
            results = data.get("results", [])
            if results:
                company_id = results[0].get("id")
                # Fetch company details
                company_url = f"{HUBSPOT_BASE_URL}/crm/v3/objects/companies/{company_id}"
                params = {"properties": "name,domain"}
                company_resp = requests.get(company_url, headers=headers, params=params)
                if company_resp.status_code == 200:
                    return company_resp.json().get("properties", {})
        return None
    except Exception as e:
        logger.error(f"Error fetching associated company: {e}")
        return None


def get_owner_name(owner_id: str) -> str:
    """
    Get owner name from ID.

    Args:
        owner_id: HubSpot owner ID.

    Returns:
        Owner name string.
    """
    # Try local mapping first
    if owner_id in OWNER_NAMES:
        return OWNER_NAMES[owner_id]

    # Try to fetch from HubSpot
    if HUBSPOT_API_KEY:
        headers = {"Authorization": f"Bearer {HUBSPOT_API_KEY}"}
        url = f"{HUBSPOT_BASE_URL}/crm/v3/owners/{owner_id}"
        try:
            resp = requests.get(url, headers=headers)
            if resp.status_code == 200:
                data = resp.json()
                return data.get("firstName", "Team")
        except Exception:
            pass

    return "Team"


def build_variables_from_deal(deal_id: str) -> Dict[str, Any]:
    """
    Build template variables from HubSpot deal data.

    Args:
        deal_id: HubSpot deal ID.

    Returns:
        Dict of variable values.
    """
    variables = {}

    # Fetch deal
    deal = fetch_deal(deal_id)
    if deal:
        variables["demo_date"] = deal.get("demo_date", "[FECHA]")
        variables["demo_time"] = deal.get("demo_time", "[HORA]")
        variables["meeting_link"] = deal.get("meeting_link", "[LINK]")
        variables["proposal_link"] = deal.get("proposal_link", "[LINK]")
        variables["proposal_amount"] = deal.get("amount", "[MONTO]")

        owner_id = deal.get("hubspot_owner_id")
        if owner_id:
            variables["owner_name"] = get_owner_name(owner_id)
        else:
            variables["owner_name"] = "Team"

    # Fetch contact
    contact = fetch_associated_contact(deal_id)
    if contact:
        variables["first_name"] = contact.get("firstname", "[NOMBRE]")
        variables["contact_email"] = contact.get("email", "")

    # Fetch company
    company = fetch_associated_company(deal_id)
    if company:
        variables["company_name"] = company.get("name", "[EMPRESA]")
    elif contact and contact.get("company"):
        variables["company_name"] = contact.get("company")
    else:
        variables["company_name"] = "[EMPRESA]"

    return variables


def send_email(
    to: str,
    subject: str,
    body: str,
    login_address: str,
    password: str,
    send_as_address: Optional[str] = None
) -> Tuple[bool, Optional[str]]:
    """
    Send an email via Gmail SMTP.

    Args:
        to: Recipient email address.
        subject: Email subject line.
        body: Email body content.
        login_address: Email for SMTP login.
        password: Gmail app password.
        send_as_address: "From" address (alias).

    Returns:
        Tuple of (success, error_message).
    """
    from_address = send_as_address or login_address

    try:
        msg = MIMEMultipart()
        msg['From'] = from_address
        msg['To'] = to
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))

        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(login_address, password)
            server.send_message(msg)

        logger.info(f"Sent to {to}")
        return True, None

    except Exception as e:
        if isinstance(e.args, tuple) and len(e.args) >= 2:
            error_msg = f"{e.args[0]}: {e.args[1].decode() if isinstance(e.args[1], bytes) else e.args[1]}"
        else:
            error_msg = str(e)
        logger.error(f"Failed to send to {to}: {error_msg}")
        return False, error_msg


def log_send(
    template_id: str,
    deal_id: str,
    recipient: str,
    subject: str,
    success: bool,
    error: Optional[str] = None
) -> None:
    """Log email send to file."""
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    log_file = LOGS_DIR / f"{template_id}_log.jsonl"
    entry = {
        "timestamp": datetime.now().isoformat(),
        "template_id": template_id,
        "deal_id": deal_id,
        "recipient": recipient,
        "subject": subject,
        "success": success,
        "error": error
    }

    with open(log_file, 'a') as f:
        f.write(json.dumps(entry) + '\n')


def send_template_email(
    template_id: str,
    deal_id: str,
    recipient_email: Optional[str] = None,
    dry_run: bool = False,
    extra_variables: Optional[Dict[str, Any]] = None
) -> Tuple[bool, Optional[str]]:
    """
    Send a templated email for a HubSpot deal.

    Args:
        template_id: Template identifier.
        deal_id: HubSpot deal ID.
        recipient_email: Override recipient (uses contact email if None).
        dry_run: If True, preview without sending.
        extra_variables: Additional variables to override.

    Returns:
        Tuple of (success, error_message).
    """
    # Load template
    template = load_template(template_id)
    if not template:
        return False, f"Template not found: {template_id}"

    # Build variables from deal
    variables = build_variables_from_deal(deal_id)

    # Apply extra variables
    if extra_variables:
        variables.update(extra_variables)

    # Get recipient
    to_email = recipient_email or variables.get("contact_email")
    if not to_email:
        return False, "No recipient email found"

    # Render template
    try:
        subject, body = render_template(template, variables)
    except ValueError as e:
        return False, str(e)

    logger.info(f"Template: {template.name}")
    logger.info(f"To: {to_email}")
    logger.info(f"Subject: {subject}")

    if dry_run:
        print("\n" + "=" * 60)
        print("[DRY RUN] Email Preview")
        print("=" * 60)
        print(f"To: {to_email}")
        print(f"Subject: {subject}")
        print("-" * 60)
        print(body)
        print("=" * 60)
        log_send(template_id, deal_id, to_email, subject, True, "dry_run")
        return True, None

    # Validate Gmail credentials
    if not GMAIL_ADDRESS or not GMAIL_APP_PASSWORD:
        return False, "Gmail credentials not configured"

    # Send email
    success, error = send_email(
        to=to_email,
        subject=subject,
        body=body,
        login_address=GMAIL_ADDRESS,
        password=GMAIL_APP_PASSWORD,
        send_as_address=GMAIL_SEND_AS
    )

    log_send(template_id, deal_id, to_email, subject, success, error)
    return success, error


def setup_logging(verbose: bool = False) -> logging.Logger:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO

    logging.basicConfig(
        level=level,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('template_sender.log')
        ]
    )

    return logging.getLogger(__name__)


def parse_args(args: Optional[List[str]] = None) -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Send templated emails for HubSpot deals',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Preview email (dry run)
  python template_sender.py --template pre_demo_confirmation --deal-id 12345 --dry-run

  # Send email for a deal
  python template_sender.py --template post_demo_proposal --deal-id 12345

  # Override recipient
  python template_sender.py --template pre_demo_confirmation --deal-id 12345 --to test@example.com --dry-run

  # Provide variables manually (no HubSpot fetch)
  python template_sender.py --template pre_demo_confirmation --to test@example.com --dry-run \\
    --vars '{"first_name": "Juan", "company_name": "Test Inc", "demo_date": "2024-01-20", "demo_time": "10:00 AM", "meeting_link": "https://zoom.us/j/123", "owner_name": "Tomas"}'
        """
    )

    parser.add_argument(
        '--template', '-t',
        type=str,
        required=True,
        help='Template ID to send'
    )

    parser.add_argument(
        '--deal-id', '-d',
        type=str,
        help='HubSpot deal ID'
    )

    parser.add_argument(
        '--to',
        type=str,
        help='Override recipient email'
    )

    parser.add_argument(
        '--vars',
        type=str,
        help='JSON string of variables (overrides HubSpot data)'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview email without sending'
    )

    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )

    return parser.parse_args(args)


def main():
    """Main entry point."""
    args = parse_args()
    setup_logging(verbose=args.verbose)

    # Parse extra variables
    extra_vars = {}
    if args.vars:
        try:
            extra_vars = json.loads(args.vars)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON for --vars: {e}")
            return 1

    # Validate inputs
    if not args.deal_id and not args.to:
        logger.error("Must provide either --deal-id or --to")
        return 1

    if not args.deal_id and not extra_vars:
        logger.error("Without --deal-id, must provide --vars with all template variables")
        return 1

    logger.info("=" * 50)
    logger.info("TEMPLATE SENDER")
    logger.info(f"Template: {args.template}")
    logger.info(f"Deal ID: {args.deal_id or 'N/A'}")
    logger.info(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")
    logger.info("=" * 50)

    success, error = send_template_email(
        template_id=args.template,
        deal_id=args.deal_id or "",
        recipient_email=args.to,
        dry_run=args.dry_run,
        extra_variables=extra_vars
    )

    if success:
        logger.info("Email sent successfully" if not args.dry_run else "Dry run completed")
        return 0
    else:
        logger.error(f"Failed: {error}")
        return 1


if __name__ == '__main__':
    exit(main())
