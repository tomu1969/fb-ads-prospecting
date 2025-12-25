"""Module 3: Contact Scraper - Extract contact information from websites."""

import re
import time
import json
import sys
from urllib.parse import urljoin, urlparse
import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}
TIMEOUT = 10
REQUEST_DELAY = 1.5
CONTACT_PATHS = ["/contact", "/about", "/team", "/agents", "/about-us", "/our-team", "/contact-us"]


def scrape_website(url, timeout=TIMEOUT):
    """Fetch website HTML content."""
    try:
        response = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        response.raise_for_status()
        return response.text
    except requests.RequestException:
        return None


def find_contact_pages(base_url, html, max_pages=5):
    """Find links to contact/about/team pages."""
    if not html:
        return []

    soup = BeautifulSoup(html, "lxml")
    found_urls = set()
    parsed_base = urlparse(base_url)
    base_domain = parsed_base.netloc

    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"].lower()

        for path in CONTACT_PATHS:
            if path in href:
                full_url = urljoin(base_url, a_tag["href"])
                if urlparse(full_url).netloc == base_domain:
                    found_urls.add(full_url)
                    break

        if len(found_urls) >= max_pages:
            break

    return list(found_urls)[:max_pages]


def extract_emails(html):
    """Extract email addresses from HTML."""
    if not html:
        return []

    pattern = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"
    emails = re.findall(pattern, html)

    excluded = ["@2x.", "@3x.", "noreply@", "no-reply@", "example.com", ".png", ".jpg", ".gif", ".svg"]
    filtered = []
    for email in emails:
        email_lower = email.lower()
        if not any(ex in email_lower for ex in excluded):
            filtered.append(email.lower())

    return list(set(filtered))


def extract_phones(html):
    """Extract phone numbers from HTML."""
    if not html:
        return []

    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text()

    patterns = [
        r"\(\d{3}\)\s*\d{3}[-.\s]?\d{4}",
        r"\d{3}[-.\s]\d{3}[-.\s]\d{4}",
        r"\+1\s*\d{3}[-.\s]?\d{3}[-.\s]?\d{4}",
    ]

    phones = []
    for pattern in patterns:
        matches = re.findall(pattern, text)
        phones.extend(matches)

    cleaned = []
    for phone in phones:
        digits = re.sub(r"\D", "", phone)
        if len(digits) in [10, 11]:
            cleaned.append(phone.strip())

    return list(set(cleaned))


def is_valid_name(text):
    """Check if text looks like a valid person name."""
    if not text or len(text) < 3 or len(text) > 50:
        return False

    # Must have 2-4 words
    words = text.split()
    if len(words) < 2 or len(words) > 4:
        return False

    # Filter out navigation/UI text patterns
    bad_patterns = [
        r'(HOME|ABOUT|CONTACT|BLOG|MENU|LOGIN|SIGN|SEARCH|PROPERTY|LISTING)',
        r'(Bedroom|Bathroom|sqft|acre|price|\$)',
        r'(Click|Learn|Read|View|See|More|Submit)',
        r'[0-9]{3,}',  # Phone numbers, IDs
        r'@',  # Emails
        r'https?://',  # URLs
    ]
    for pattern in bad_patterns:
        if re.search(pattern, text, re.IGNORECASE):
            return False

    # Each word should start with capital and have lowercase
    for word in words:
        if not re.match(r'^[A-Z][a-z]+$', word):
            # Allow common suffixes like Jr., Sr., III
            if word not in ['Jr.', 'Sr.', 'Jr', 'Sr', 'II', 'III', 'IV']:
                return False

    return True


def extract_contact_details(html):
    """Extract contact name and position from HTML."""
    if not html:
        return "", ""

    soup = BeautifulSoup(html, "lxml")

    # Try to find JSON-LD structured data first
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            import json
            data = json.loads(script.string)
            if isinstance(data, dict):
                # Check for Person or RealEstateAgent
                if data.get("@type") in ["Person", "RealEstateAgent", "Employee"]:
                    name = data.get("name", "")
                    title = data.get("jobTitle", "")
                    if is_valid_name(name):
                        return name, title
        except Exception:
            continue

    # Look for team/agent cards with proper structure
    for container in soup.find_all(["div", "section", "article", "li"],
                                    class_=re.compile(r"team|agent|staff|member|card|profile", re.I)):
        # Find name element
        name_el = container.find(["h2", "h3", "h4", "h5", "strong", "span", "a"],
                                  class_=re.compile(r"name|title|heading", re.I))
        if not name_el:
            name_el = container.find(["h2", "h3", "h4", "h5"])

        if name_el:
            name = name_el.get_text(strip=True)
            if is_valid_name(name):
                # Find position element
                title_el = container.find(["p", "span", "div"],
                                          class_=re.compile(r"position|role|title|job|designation", re.I))
                position = title_el.get_text(strip=True) if title_el else ""
                # Clean position
                if len(position) > 50:
                    position = ""
                return name, position

    # Try regex patterns on clean text
    titles = r"(?:CEO|President|Owner|Broker|Agent|Manager|Director|Founder|Partner|Realtor|Principal)"
    name_title_patterns = [
        rf"([A-Z][a-z]+\s+[A-Z][a-z]+)\s*[,\-–—]\s*({titles})",
        rf"({titles})\s*[:\-–—]\s*([A-Z][a-z]+\s+[A-Z][a-z]+)",
    ]

    # Get clean text from main content areas only
    main_content = soup.find(["main", "article"]) or soup.find("body")
    if main_content:
        text = main_content.get_text(separator=" ")

        for pattern in name_title_patterns:
            match = re.search(pattern, text)
            if match:
                groups = match.groups()
                if groups[0].lower() in ["ceo", "president", "owner", "broker", "agent",
                                          "manager", "director", "founder", "partner", "realtor"]:
                    name, title = groups[1], groups[0]
                else:
                    name, title = groups[0], groups[1]
                if is_valid_name(name):
                    return name.strip(), title.strip()

    return "", ""


def extract_company_info(html):
    """Extract company description and services from HTML."""
    if not html:
        return "", []

    soup = BeautifulSoup(html, "lxml")

    description = ""
    meta_desc = soup.find("meta", attrs={"name": "description"})
    if meta_desc and meta_desc.get("content"):
        description = meta_desc["content"].strip()

    if not description:
        og_desc = soup.find("meta", attrs={"property": "og:description"})
        if og_desc and og_desc.get("content"):
            description = og_desc["content"].strip()

    if not description:
        about_section = soup.find(["section", "div"], class_=re.compile(r"about|intro|hero", re.I))
        if about_section:
            p_tag = about_section.find("p")
            if p_tag:
                description = p_tag.get_text(strip=True)[:500]

    service_keywords = [
        "residential", "commercial", "property management", "buying", "selling",
        "rental", "mortgage", "investment", "luxury", "first-time buyers",
        "relocation", "foreclosure", "short sale", "new construction"
    ]

    text_lower = soup.get_text().lower()
    services = [kw for kw in service_keywords if kw in text_lower]

    return description, services


def extract_social_links(html):
    """Extract social media profile URLs."""
    if not html:
        return {}

    soup = BeautifulSoup(html, "lxml")
    social_patterns = {
        "linkedin": r"linkedin\.com/(?:in|company)/[a-zA-Z0-9_-]+",
        "twitter": r"(?:twitter|x)\.com/[a-zA-Z0-9_]+",
        "facebook": r"facebook\.com/[a-zA-Z0-9.]+",
        "instagram": r"instagram\.com/[a-zA-Z0-9_.]+",
    }

    social_links = {}

    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        for platform, pattern in social_patterns.items():
            if platform not in social_links and re.search(pattern, href, re.I):
                social_links[platform] = href

    return social_links


def extract_instagram_handles(html):
    """Extract Instagram handles from HTML text and links."""
    if not html:
        return []

    soup = BeautifulSoup(html, "lxml")
    handles = set()

    # Extract from Instagram URLs
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        match = re.search(r'instagram\.com/([a-zA-Z0-9_.]+)/?', href, re.I)
        if match:
            handle = match.group(1).lower()
            if handle not in ['p', 'explore', 'accounts', 'direct', 'stories', 'reels']:
                handles.add(f"@{handle}")

    # Extract @ mentions from text (common on real estate sites)
    text = soup.get_text()
    # Pattern for Instagram handles: @username (allowing underscores, dots, and realtor/realestate suffixes)
    mention_pattern = r'@([a-zA-Z][a-zA-Z0-9_.]{2,29})'
    for match in re.finditer(mention_pattern, text):
        handle = match.group(1).lower()
        # Filter out common non-handle patterns
        if not any(x in handle for x in ['gmail', 'yahoo', 'hotmail', 'outlook', '.com', '.net', '.org']):
            handles.add(f"@{handle}")

    return list(handles)[:10]  # Limit to 10 handles


def extract_team_members(html):
    """Extract team member names and their social profiles."""
    if not html:
        return []

    soup = BeautifulSoup(html, "lxml")
    team_members = []

    # Look for team/agent cards
    for container in soup.find_all(["div", "section", "article", "li", "figure"],
                                    class_=re.compile(r"team|agent|staff|member|card|profile|speaker|trainer", re.I)):
        member = {"name": "", "position": "", "instagram": "", "email": ""}

        # Find name - look in headings first
        for tag in ["h2", "h3", "h4", "h5", "strong"]:
            name_el = container.find(tag)
            if name_el:
                name_text = name_el.get_text(strip=True)
                # Check if it looks like a name (2-4 capitalized words)
                words = name_text.split()
                if 2 <= len(words) <= 4 and all(w[0].isupper() for w in words if w):
                    member["name"] = name_text
                    break

        # Find Instagram link for this member
        for a_tag in container.find_all("a", href=True):
            href = a_tag["href"]
            if "instagram.com" in href.lower():
                match = re.search(r'instagram\.com/([a-zA-Z0-9_.]+)', href, re.I)
                if match:
                    member["instagram"] = f"@{match.group(1).lower()}"
                    break

        # Find position/title
        for p_tag in container.find_all(["p", "span", "div"]):
            text = p_tag.get_text(strip=True)
            if any(title in text.lower() for title in ["realtor", "agent", "broker", "ceo", "founder", "manager", "director"]):
                if len(text) < 50:
                    member["position"] = text
                    break

        if member["name"] or member["instagram"]:
            team_members.append(member)

    return team_members[:10]  # Limit to 10 members


def scrape_contact(url):
    """Scrape all contact information from a website."""
    result = {
        "contact_name": "",
        "contact_position": "",
        "emails": [],
        "phones": [],
        "company_description": "",
        "services": [],
        "social_links": {},
        "instagram_handles": [],
        "team_members": [],
    }

    if not url or pd.isna(url):
        return result

    if not url.startswith(("http://", "https://")):
        url = "https://" + url

    main_html = scrape_website(url)
    if not main_html:
        return result

    all_html = [main_html]

    contact_pages = find_contact_pages(url, main_html)
    for page_url in contact_pages:
        time.sleep(REQUEST_DELAY / 2)
        page_html = scrape_website(page_url)
        if page_html:
            all_html.append(page_html)

    all_instagram = []
    all_team_members = []

    for html in all_html:
        result["emails"].extend(extract_emails(html))
        result["phones"].extend(extract_phones(html))

        # Extract Instagram handles
        all_instagram.extend(extract_instagram_handles(html))

        # Extract team members
        all_team_members.extend(extract_team_members(html))

        social = extract_social_links(html)
        for platform, link in social.items():
            if platform not in result["social_links"]:
                result["social_links"][platform] = link

        if not result["contact_name"]:
            name, position = extract_contact_details(html)
            if name:
                result["contact_name"] = name
                result["contact_position"] = position

        if not result["company_description"]:
            desc, services = extract_company_info(html)
            if desc:
                result["company_description"] = desc
            result["services"].extend(services)

    # Deduplicate and limit
    result["emails"] = list(set(result["emails"]))[:5]
    result["phones"] = list(set(result["phones"]))[:3]
    result["services"] = list(set(result["services"]))
    result["instagram_handles"] = list(set(all_instagram))[:10]

    # Deduplicate team members by name
    seen_names = set()
    unique_team = []
    for member in all_team_members:
        name_key = member.get("name", "").lower() or member.get("instagram", "").lower()
        if name_key and name_key not in seen_names:
            seen_names.add(name_key)
            unique_team.append(member)
    result["team_members"] = unique_team[:10]

    # If no contact name found, try to get first team member name
    if not result["contact_name"] and result["team_members"]:
        first_member = result["team_members"][0]
        if first_member.get("name"):
            result["contact_name"] = first_member["name"]
            result["contact_position"] = first_member.get("position", "")

    return result


def scrape_all(df):
    """Scrape contact information for all rows in DataFrame."""
    new_cols = {
        "contact_name": [],
        "contact_position": [],
        "emails": [],
        "phones": [],
        "company_description": [],
        "services": [],
        "social_links": [],
        "instagram_handles": [],
        "team_members": [],
    }

    for _, row in tqdm(df.iterrows(), total=len(df), desc="Scraping contacts"):
        url = row.get("website_url", "")
        contact_data = scrape_contact(url)

        new_cols["contact_name"].append(contact_data["contact_name"])
        new_cols["contact_position"].append(contact_data["contact_position"])
        new_cols["emails"].append(json.dumps(contact_data["emails"]))
        new_cols["phones"].append(json.dumps(contact_data["phones"]))
        new_cols["company_description"].append(contact_data["company_description"])
        new_cols["services"].append(json.dumps(contact_data["services"]))
        new_cols["social_links"].append(json.dumps(contact_data["social_links"]))
        new_cols["instagram_handles"].append(json.dumps(contact_data["instagram_handles"]))
        new_cols["team_members"].append(json.dumps(contact_data["team_members"]))

        time.sleep(REQUEST_DELAY)

    for col, values in new_cols.items():
        df[col] = values

    return df


if __name__ == "__main__":
    import os

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    input_file = os.path.join(base_dir, "processed", "02_enriched.csv")
    output_file = os.path.join(base_dir, "processed", "03_contacts.csv")

    if not os.path.exists(input_file):
        print(f"Input file not found: {input_file}")
        print("Creating sample data for testing...")

        sample_data = pd.DataFrame({
            "page_name": ["Zillow", "Redfin", "Realtor.com"],
            "ad_count": [10, 8, 5],
            "total_page_likes": [1000000, 500000, 300000],
            "ad_texts": ["[]", "[]", "[]"],
            "platforms": ["[]", "[]", "[]"],
            "is_active": [True, True, True],
            "first_ad_date": ["2024-01-01", "2024-02-01", "2024-03-01"],
            "website_url": ["https://www.zillow.com", "https://www.redfin.com", "https://www.realtor.com"],
            "search_confidence": [1.0, 1.0, 1.0],
            "linkedin_url": ["", "", ""],
        })
        os.makedirs(os.path.dirname(input_file), exist_ok=True)
        sample_data.to_csv(input_file, index=False)
        print(f"Sample data created: {input_file}")

    df = pd.read_csv(input_file)
    print(f"Loaded {len(df)} rows from {input_file}")

    run_all = "--all" in sys.argv
    if not run_all:
        df = df.head(3)
        print("Testing with first 3 rows (use --all for full run)")

    df = scrape_all(df)

    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    df.to_csv(output_file, index=False)
    print(f"Saved {len(df)} rows to {output_file}")

    print("\nSample output:")
    for col in ["contact_name", "emails", "phones"]:
        if col in df.columns:
            print(f"  {col}: {df[col].iloc[0] if len(df) > 0 else 'N/A'}")
