#!/usr/bin/env python3
"""Template Loader - Load and render sales pipeline email templates.

Loads email templates from config/email_templates/, parses YAML frontmatter,
and substitutes variables from deal/contact data.
"""

import os
import re
import json
import logging
import argparse
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List

# Module logger
logger = logging.getLogger(__name__)

# Paths
CONFIG_DIR = Path(__file__).parent.parent.parent / "config" / "email_templates"
TEMPLATES_JSON = CONFIG_DIR / "templates.json"


@dataclass
class Template:
    """Represents an email template with metadata and content."""
    id: str
    name: str
    description: str
    path: str
    trigger: Dict[str, Any]
    variables: List[str]
    hubspot_template_id: Optional[str]
    subject: str = ""
    body: str = ""
    raw_content: str = ""
    frontmatter: Dict[str, Any] = field(default_factory=dict)


def parse_frontmatter(content: str) -> tuple[Dict[str, Any], str]:
    """
    Parse YAML frontmatter from template content.

    Args:
        content: Raw template content with optional YAML frontmatter.

    Returns:
        Tuple of (frontmatter dict, body content).
    """
    # Check for frontmatter delimiter
    if not content.startswith('---'):
        return {}, content

    # Find end of frontmatter
    lines = content.split('\n')
    end_idx = -1
    for i, line in enumerate(lines[1:], 1):
        if line.strip() == '---':
            end_idx = i
            break

    if end_idx == -1:
        return {}, content

    # Parse frontmatter as simple key-value
    frontmatter = {}
    for line in lines[1:end_idx]:
        line = line.strip()
        if not line or line.startswith('#'):
            continue

        # Handle simple key: value
        if ':' in line:
            key, value = line.split(':', 1)
            key = key.strip()
            value = value.strip()

            # Handle arrays (- item)
            if value == '':
                # Check for array items following
                continue
            elif value.startswith('"') and value.endswith('"'):
                frontmatter[key] = value[1:-1]
            elif value.lower() == 'null':
                frontmatter[key] = None
            elif value.lower() in ('true', 'false'):
                frontmatter[key] = value.lower() == 'true'
            else:
                try:
                    frontmatter[key] = int(value)
                except ValueError:
                    frontmatter[key] = value
        elif line.startswith('- '):
            # Array item - find the parent key
            item = line[2:].strip()
            # Find the most recent key without a value
            for prev_line in reversed(lines[1:end_idx]):
                if ':' in prev_line and prev_line.split(':', 1)[1].strip() == '':
                    parent_key = prev_line.split(':')[0].strip()
                    if parent_key not in frontmatter:
                        frontmatter[parent_key] = []
                    if isinstance(frontmatter[parent_key], list):
                        frontmatter[parent_key].append(item)
                    break

    # Extract body (everything after frontmatter)
    body = '\n'.join(lines[end_idx + 1:]).strip()

    return frontmatter, body


def extract_subject_body(content: str) -> tuple[str, str]:
    """
    Extract subject line and body from template content.

    Args:
        content: Template content after frontmatter.

    Returns:
        Tuple of (subject, body).
    """
    lines = content.split('\n')
    subject = ""
    body_start = 0

    for i, line in enumerate(lines):
        if line.lower().startswith('subject:'):
            subject = line.split(':', 1)[1].strip()
            body_start = i + 1
            break

    # Skip empty lines after subject
    while body_start < len(lines) and not lines[body_start].strip():
        body_start += 1

    body = '\n'.join(lines[body_start:]).strip()
    return subject, body


def load_registry() -> Dict[str, Any]:
    """
    Load the templates registry JSON.

    Returns:
        Registry dict with templates and variable sources.
    """
    if not TEMPLATES_JSON.exists():
        logger.error(f"Templates registry not found: {TEMPLATES_JSON}")
        return {"templates": [], "variable_sources": {}}

    with open(TEMPLATES_JSON) as f:
        return json.load(f)


def list_templates() -> List[Dict[str, Any]]:
    """
    List all available templates.

    Returns:
        List of template metadata dicts.
    """
    registry = load_registry()
    return registry.get("templates", [])


def load_template(template_id: str) -> Optional[Template]:
    """
    Load a template by ID.

    Args:
        template_id: Template identifier (e.g., 'pre_demo_confirmation').

    Returns:
        Template object or None if not found.
    """
    registry = load_registry()

    # Find template metadata
    template_meta = None
    for t in registry.get("templates", []):
        if t["id"] == template_id:
            template_meta = t
            break

    if not template_meta:
        logger.error(f"Template not found: {template_id}")
        return None

    # Load template file
    template_path = CONFIG_DIR / template_meta["path"]
    if not template_path.exists():
        logger.error(f"Template file not found: {template_path}")
        return None

    with open(template_path) as f:
        raw_content = f.read()

    # Parse frontmatter and body
    frontmatter, content = parse_frontmatter(raw_content)
    subject, body = extract_subject_body(content)

    return Template(
        id=template_meta["id"],
        name=template_meta["name"],
        description=template_meta["description"],
        path=template_meta["path"],
        trigger=template_meta["trigger"],
        variables=template_meta["variables"],
        hubspot_template_id=template_meta.get("hubspot_template_id"),
        subject=subject,
        body=body,
        raw_content=raw_content,
        frontmatter=frontmatter
    )


def render_template(
    template: Template,
    variables: Dict[str, Any],
    strict: bool = False
) -> tuple[str, str]:
    """
    Render a template with variable substitution.

    Args:
        template: Template object to render.
        variables: Dict of variable values to substitute.
        strict: If True, raise error on missing variables.

    Returns:
        Tuple of (rendered_subject, rendered_body).

    Raises:
        ValueError: If strict=True and variables are missing.
    """
    subject = template.subject
    body = template.body

    # Find all variables in template ({{ var_name }} format)
    var_pattern = r'\{\{\s*(\w+)\s*\}\}'

    def replace_var(match, text):
        var_name = match.group(1)
        if var_name in variables:
            value = variables[var_name]
            return str(value) if value is not None else ""
        elif strict:
            raise ValueError(f"Missing required variable: {var_name}")
        else:
            logger.warning(f"Variable not provided: {var_name}")
            return match.group(0)  # Keep placeholder

    # Substitute in subject and body
    subject = re.sub(var_pattern, lambda m: replace_var(m, subject), subject)
    body = re.sub(var_pattern, lambda m: replace_var(m, body), body)

    return subject, body


def preview_template(template: Template, variables: Optional[Dict[str, Any]] = None) -> str:
    """
    Generate a preview of a template.

    Args:
        template: Template object to preview.
        variables: Optional variables for rendering.

    Returns:
        Formatted preview string.
    """
    output = []
    output.append("=" * 60)
    output.append(f"Template: {template.name}")
    output.append(f"ID: {template.id}")
    output.append(f"Description: {template.description}")
    output.append("-" * 60)
    output.append(f"Variables: {', '.join(template.variables)}")
    output.append(f"Trigger: {template.trigger}")
    output.append("=" * 60)

    if variables:
        subject, body = render_template(template, variables)
        output.append(f"\nSubject: {subject}")
        output.append(f"\n{body}")
    else:
        output.append(f"\nSubject: {template.subject}")
        output.append(f"\n{template.body}")

    output.append("=" * 60)
    return '\n'.join(output)


def setup_logging(verbose: bool = False) -> logging.Logger:
    """Configure logging for the module."""
    level = logging.DEBUG if verbose else logging.INFO

    logging.basicConfig(
        level=level,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[logging.StreamHandler()]
    )

    return logging.getLogger(__name__)


def parse_args(args: Optional[List[str]] = None) -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Load and preview email templates',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List all templates
  python template_loader.py --list

  # Preview a template
  python template_loader.py --template pre_demo_confirmation --preview

  # Preview with sample variables
  python template_loader.py --template pre_demo_confirmation --preview --vars '{"first_name": "Juan", "company_name": "Inmobiliaria XYZ"}'
        """
    )

    parser.add_argument(
        '--list',
        action='store_true',
        help='List all available templates'
    )

    parser.add_argument(
        '--template', '-t',
        type=str,
        help='Template ID to load'
    )

    parser.add_argument(
        '--preview',
        action='store_true',
        help='Preview the template'
    )

    parser.add_argument(
        '--vars',
        type=str,
        help='JSON string of variables for preview'
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

    if args.list:
        templates = list_templates()
        print("\nAvailable Templates:")
        print("-" * 60)
        for t in templates:
            print(f"  {t['id']}")
            print(f"    Name: {t['name']}")
            print(f"    Trigger: {t['trigger'].get('type', 'N/A')}")
            if t.get('hubspot_template_id'):
                print(f"    HubSpot ID: {t['hubspot_template_id']}")
            print()
        return 0

    if args.template:
        template = load_template(args.template)
        if not template:
            return 1

        if args.preview:
            variables = {}
            if args.vars:
                try:
                    variables = json.loads(args.vars)
                except json.JSONDecodeError as e:
                    logger.error(f"Invalid JSON for --vars: {e}")
                    return 1

            print(preview_template(template, variables if variables else None))
        else:
            print(f"Loaded template: {template.name}")
            print(f"Variables: {template.variables}")

        return 0

    # No action specified
    print("Use --list to see templates or --template <id> to load one")
    return 1


if __name__ == '__main__':
    exit(main())
