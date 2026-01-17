"""
Constants for ICP Discovery Pipeline

CTA mappings, keyword patterns, scoring thresholds, and helper functions.
Includes hardened rules for conversational gate, domain/path splitting,
and EN/ES pattern matching with ASCII normalization.
"""

import re
import unicodedata

# =============================================================================
# TEXT NORMALIZATION (ASCII FOLD)
# =============================================================================

def normalize_text(text: str) -> str:
    """
    ASCII-fold accented characters for regex matching.
    Ensures "evaluacion" matches text containing "evaluación".
    """
    if not text:
        return ''
    # Decompose and remove diacritics
    nfkd = unicodedata.normalize('NFKD', str(text))
    ascii_text = ''.join(c for c in nfkd if not unicodedata.combining(c))
    return ascii_text.lower()


def compile_patterns(patterns: list) -> list:
    """Pre-compile regex patterns for efficiency."""
    return [re.compile(p, re.IGNORECASE) for p in patterns]


# =============================================================================
# DESTINATION TYPE CLASSIFICATION
# =============================================================================

# CTA types that indicate MESSAGE destination (direct conversation)
MESSAGE_CTA_TYPES = {
    'MESSAGE_PAGE',
    'SEND_MESSAGE',
    'WHATSAPP_MESSAGE',
    'CONTACT_US',
}

# URL patterns that indicate MESSAGE destination
MESSAGE_URL_PATTERNS = [
    r'm\.me/',              # Messenger deep link
    r'messenger\.com',
    r'wa\.me/',             # WhatsApp
    r'api\.whatsapp\.com',
    r'ig\.me/',             # Instagram DM
]

# CTA types that indicate CALL destination
CALL_CTA_TYPES = {
    'CALL_NOW',
    'CALL',
}

# URL patterns that indicate CALL destination
CALL_URL_PATTERNS = [
    r'^tel:',
    r'click-to-call',
]

# CTA types that indicate FORM destination (lead capture with follow-up)
FORM_CTA_TYPES = {
    'APPLY_NOW',
    'SIGN_UP',
    'GET_QUOTE',
    'REQUEST_TIME',
    'BOOK_TRAVEL',
    'BOOK_NOW',
    'CONTACT_US',
}

# URL patterns that indicate FORM destination
FORM_URL_PATTERNS = [
    r'/lead[-_]?form',
    r'/contact[-_]?us',
    r'/get[-_]?quote',
    r'/apply',
    r'/book',
    r'/schedule',
    r'typeform\.com',
    r'calendly\.com',
    r'forms\.gle',
    r'jotform\.com',
    r'hubspot.*form',
]

# Lead intent CTAs - require consult language to be conversational
LEAD_INTENT_CTAS = {
    'SIGN_UP', 'LEARN_MORE', 'APPLY_NOW', 'GET_QUOTE',
    'BOOK_NOW', 'CONTACT_US', 'BOOK_TRAVEL', 'REQUEST_TIME',
}

# CTA types that indicate WEB destination (generic traffic)
WEB_CTA_TYPES = {
    'LEARN_MORE',
    'SHOP_NOW',
    'BUY_NOW',
    'BUY_TICKETS',
    'DOWNLOAD',
    'INSTALL_APP',
    'INSTALL_MOBILE_APP',
    'USE_APP',
    'PLAY_GAME',
    'OPEN_LINK',
    'WATCH_MORE',
    'WATCH_VIDEO',
    'LISTEN_NOW',
    'SEE_MORE',
    'VIEW_INSTAGRAM_PROFILE',
    'LIKE_PAGE',
    'NO_BUTTON',
    'ORDER_NOW',
    'GET_DIRECTIONS',
    'GET_OFFER',
    'GET_SHOWTIMES',
    'SUBSCRIBE',
}

# =============================================================================
# TRANSACTIONAL DETECTION (FOR GATE EXCLUSION)
# =============================================================================

# Transactional CTAs - these indicate no conversation is needed
TRANSACTIONAL_CTA_TYPES = {
    'SHOP_NOW',
    'BUY_NOW',
    'BUY_TICKETS',
    'ORDER_NOW',
    'DOWNLOAD',
    'INSTALL_APP',
    'INSTALL_MOBILE_APP',
    'USE_APP',
    'PLAY_GAME',
    'SUBSCRIBE',
}

# Transactional DOMAINS (match against netloc only)
TRANSACTIONAL_DOMAINS = [
    'shopify.com',
    'myshopify.com',
    'stripe.com',
    'gumroad.com',
    'square.com',
    'squareup.com',
    'play.google.com',
    'apps.apple.com',
    'amazon.com',
    'amazon.',
    'etsy.com',
    'ebay.com',
    'paypal.com',
    'checkout.stripe.com',
]

# Transactional URL PATHS (match against path only)
TRANSACTIONAL_PATHS = [
    r'/checkout',
    r'/cart',
    r'/product/',
    r'/products/',
    r'/buy/',
    r'/shop/',
    r'/add-to-cart',
    r'/purchase',
    r'/order/',
]

# Transactional copy patterns
TRANSACTIONAL_COPY_PATTERNS = [
    r'\$\d+(?:\.\d{2})?',  # Price mentions
    r'\d+%\s*off\b',
    r'\bfree\s+shipping\b',
    r'\bbuy\s+now\b',
    r'\bshop\s+now\b',
    r'\binstant\s+download\b',
    r'\badd\s+to\s+cart\b',
    r'\bsale\s+ends\b',
    r'\bflash\s+sale\b',
    r'\blimited\s+stock\b',
    r'\border\s+now\b',
    r'\border\s+today\b',
]

# Price/discount patterns (heavy transactional signal)
PRICE_DISCOUNT_PATTERNS = [
    r'\$\d+',
    r'\d+%\s*off',
    r'free\s+shipping',
    r'sale\s+ends',
    r'buy\s+now',
    r'order\s+now',
    r'limited\s+stock',
    r'add\s+to\s+cart',
    r'promo\s+code',
    r'discount\s+code',
    r'coupon',
]

# =============================================================================
# CONSULT/QUALIFICATION LANGUAGE (EN + ES - ASCII NORMALIZED)
# =============================================================================

# Follow-up language that indicates human conversation will follow
FOLLOWUP_PHRASES = [
    # English
    r"\bwe'll\s+(call|contact|reach)",
    r"\bwe\s+will\s+(call|contact|reach)",
    r'\bexpect\s+a\s+call\b',
    r'\bsomeone\s+will\s+(call|contact)',
    r'\ba\s+representative\s+will',
    r'\bour\s+team\s+will\s+contact',
    # Spanish (ASCII-safe - will match after normalization)
    r'\bte\s+contactamos\b',
    r'\bte\s+llamamos\b',
    r'\bnos\s+comunicamos\b',
    r'\bun\s+asesor\s+te\s+(llamara|contactara)',
]

# Consult language (EN + ES) - signals conversation-driven business
CONSULT_LANGUAGE_EN = [
    r'\bapply\b',
    r'\beligib',
    r'\bqualify\b',
    r'\brequirements?\b',
    r"\bwe'll\s+contact\b",
    r'\bwe\s+will\s+contact\b',
    r'\bspeak\s+with\b',
    r'\btalk\s+to\b',
    r'\bassessment\b',
    r'\bpre-qualify\b',
    r'\bscreening\b',
    r'\bschedule\b',
    r'\bbook\b',
    r'\bconsult\b',
    r'\bquote\b',
    r'\bconsultation\b',
    r'\bappointment\b',
    r'\bget\s+in\s+touch\b',
    r'\bfree\s+consultation\b',
    r'\bfree\s+estimate\b',
    r'\bfree\s+quote\b',
]

# Spanish patterns (ASCII-safe versions - will match after normalize_text)
CONSULT_LANGUAGE_ES = [
    r'\bcalifica\b',
    r'\belegible\b',
    r'\brequisitos\b',
    r'\bagenda\b',
    r'\bcita\b',
    r'\bcotiza\b',
    r'\bte\s+contactamos\b',
    r'\bsolicita\b',
    r'\bevaluacion\b',  # ASCII-folded from evaluación
    r'\basesoria\b',    # ASCII-folded from asesoría
    r'\bllamada\b',
    r'\bconsulta\b',
    r'\bpresupuesto\b',
    r'\bgratis\b',
    r'\bsin\s+costo\b',
    r'\bagenda\s+tu\b',
    r'\bagenda\s+una\s+llamada\b',
]

# Combined consult language
CONSULT_LANGUAGE = CONSULT_LANGUAGE_EN + CONSULT_LANGUAGE_ES

# =============================================================================
# QUALIFICATION PATTERNS FOR FIT SCORE
# =============================================================================

QUALIFICATION_PHRASES_EN = [
    r'\bapply\b',
    r'\beligib',
    r'\bqualify\b',
    r'\brequirements?\b',
    r'\bapproval\b',
    r'\bwe\s+will\s+contact\b',
    r'\bspeak\s+with\b',
    r'\btalk\s+to\b',
    r'\bget\s+in\s+touch\b',
    r'\bassessment\b',
    r'\bpre-qualify\b',
    r'\bscreening\b',
    r'\bdo\s+you\s+qualify\b',
    r'\bare\s+you\s+eligible\b',
    r'\bmeet\s+requirements\b',
    r'\bcriteria\b',
    r'\bcheck\s+your\s+eligibility\b',
]

QUALIFICATION_PHRASES_ES = [
    r'\bcalifica\b',
    r'\belegible\b',
    r'\brequisitos\b',
    r'\bagenda\s+una\s+llamada\b',
    r'\bcotiza\b',
    r'\bte\s+contactamos\b',
    r'\bsolicita\b',
    r'\bevaluacion\b',  # ASCII-folded
    r'\bverifica\s+tu\b',
    r'\bcumples\s+con\b',
]

QUALIFICATION_PHRASES_ALL = QUALIFICATION_PHRASES_EN + QUALIFICATION_PHRASES_ES

# Consult/booking intent patterns for fit score
CONSULT_BOOKING_PATTERNS = [
    # English
    r'\bbook\b',
    r'\bschedule\b',
    r'\bconsult\b',
    r'\bquote\b',
    r'\bapply\b',
    r'\bappointment\b',
    r'\bestimate\b',
    # Spanish
    r'\bagenda\b',
    r'\bcita\b',
    r'\bllamada\b',
    r'\basesoria\b',  # ASCII-folded
    r'\bcotizacion\b',  # ASCII-folded
    r'\bpresupuesto\b',
]

# =============================================================================
# EXPANDED FIT SCORING PATTERNS (v2)
# =============================================================================

# Expanded qualification/depth patterns for fit scoring
FIT_QUALIFICATION_EXPANDED_EN = [
    # Core qualification
    r'\brequirements?\b',
    r'\beligib',
    r'\bqualify\b',
    r'\bapproval\b',
    r'\bpre[-\s]?qual',
    # Financial/licensing
    r'\bfinanc',
    r'\blicensed?\b',
    r'\binsurance\b',
    r'\bestimate\b',
    r'\bcredit\s+(check|score)\b',
    r'\bincome\s+verification\b',
    # Assessment/screening
    r'\bassessment\b',
    r'\bscreening\b',
    r'\bevaluat',
    r'\breview\s+your\b',
    r'\bcheck\s+your\b',
]

FIT_QUALIFICATION_EXPANDED_ES = [
    # Core qualification (ASCII-safe)
    r'\brequisitos\b',
    r'\bcalifica\b',
    r'\belegible\b',
    r'\baprobado\b',
    r'\baprobacion\b',
    # Financial/licensing (ASCII-safe)
    r'\bfinanciamiento\b',
    r'\blicencia\b',
    r'\bseguro\b',
    r'\bcotizacion\b',
    r'\bestimacion\b',
    r'\bcredito\b',
    # Assessment
    r'\bevaluacion\b',
    r'\bverificacion\b',
    r'\brevision\b',
]

FIT_QUALIFICATION_EXPANDED = FIT_QUALIFICATION_EXPANDED_EN + FIT_QUALIFICATION_EXPANDED_ES

# Followup/response language patterns
FIT_FOLLOWUP_PATTERNS = [
    # English - "we will contact"
    r"\bwe('ll|\s+will)\s+(call|contact|reach|get\s+back)",
    r'\bexpect\s+a\s+(call|response)\b',
    r'\bsomeone\s+will\s+(call|contact)',
    r'\bour\s+team\s+will\s+(call|contact|reach)',
    r'\brespond\s+within\s+\d+',
    r'\bget\s+back\s+to\s+you\b',
    r'\bhear\s+from\s+us\b',
    r'\ba\s+representative\s+will\b',
    r'\bcall\s+you\s+back\b',
    # Spanish (ASCII-safe)
    r'\bte\s+contactamos\b',
    r'\bte\s+llamamos\b',
    r'\bnos\s+comunicamos\b',
    r'\bun\s+asesor\s+te\b',
    r'\bespera\s+(una\s+)?llamada\b',
    r'\brespuesta\s+en\s+\d+',
]

# Multi-step/questionnaire language
FIT_MULTISTEP_PATTERNS = [
    # English
    r'\banswer\s+(a\s+few|some)\s+questions?\b',
    r'\bfill\s+out\s+(a|the|this)\s+form\b',
    r'\bcomplete\s+(a|the|this)\s+(short\s+)?form\b',
    r'\bstep\s+\d+\b',
    r'\bfirst\s+step\b',
    r'\bnext\s+step\b',
    r'\bget\s+started\b',
    r'\bstart\s+your\b',
    r'\btells?\s+us\s+about\b',
    r'\blearn\s+more\s+about\s+you\b',
    r'\bshort\s+(quiz|survey|form)\b',
    # Spanish (ASCII-safe)
    r'\bresponde\s+(unas|algunas)\s+preguntas?\b',
    r'\bllena\s+(el|este)\s+formulario\b',
    r'\bcompleta\s+(el|este)\s+formulario\b',
    r'\bpaso\s+\d+\b',
    r'\bprimer\s+paso\b',
    r'\bcomienza\b',
    r'\binicia\b',
]

# =============================================================================
# IMPLICIT FIT SCORING PATTERNS (Qualification Load During Conversation)
# =============================================================================

# Advisor/Human Mediation Language (EN/ES)
ADVISOR_LANGUAGE_EN = [
    r'\badvisor\b',
    r'\bspecialist\b',
    r'\bexpert\b',
    r'\bconsultant\b',
    r'\bour\s+team\b',
    r'\bspeak\s+with\s+(an?|our)\b',
    r'\btalk\s+to\s+(an?|our)\b',
    r'\bpersonal\s+(advisor|consultant|specialist)\b',
    r'\bdedicated\s+(rep|representative|agent)\b',
    r'\bprofessional\b',
    r'\banalyst\b',
    r'\bagent\b',
]

ADVISOR_LANGUAGE_ES = [
    r'\basesor\b',          # asesor (ASCII-folded)
    r'\bespecialista\b',
    r'\bexperto\b',
    r'\bconsultor\b',
    r'\bequipo\b',
    r'\bhabla\s+con\s+(un|nuestro)\b',
    r'\bprofesional\b',
    r'\bagente\b',
    r'\brepresentante\b',
]

ADVISOR_LANGUAGE = ADVISOR_LANGUAGE_EN + ADVISOR_LANGUAGE_ES

# Service Breadth / Ambiguity Language
SERVICE_BREADTH_PATTERNS = [
    # Generic service phrases (EN)
    r'\bwe\s+help\s+(with|you)\b',
    r'\bwe\s+offer\b',
    r'\bour\s+services\b',
    r'\bservices\s+include\b',
    r'\bwide\s+range\b',
    r'\bvariety\s+of\b',
    r'\btalk\s+to\s+us\s+about\b',
    r'\bcontact\s+us\s+for\b',
    r'\blearn\s+more\s+about\b',
    r'\bfind\s+out\s+(how|what)\b',
    r'\blet\s+us\s+help\b',
    r'\bcustomized?\s+solutions?\b',
    r'\btailored\s+to\b',
    r'\byour\s+needs\b',
    r'\bwhatever\s+you\s+need\b',
    # Generic service phrases (ES)
    r'\bte\s+ayudamos\b',
    r'\bofrecemos\b',
    r'\bnuestros\s+servicios\b',
    r'\bamplia\s+gama\b',
    r'\bvariedad\s+de\b',
    r'\bsoluciones\s+personalizadas\b',
    r'\btus\s+necesidades\b',
]

# Regulated / High-Consideration Domains
# These categories imply qualification is unavoidable but deferred to conversation
REGULATED_DOMAIN_PATTERNS = [
    # Legal
    r'\battorney\b',
    r'\blawyer\b',
    r'\blaw\s+firm\b',
    r'\blegal\b',
    r'\babogado\b',      # Spanish lawyer
    # Education
    r'\bschool\b',
    r'\buniversity\b',
    r'\bcollege\b',
    r'\beducation\b',
    r'\btraining\b',
    r'\bcertification\b',
    r'\bcdl\b',          # Commercial Driver's License
    r'\btruck\s+driving\b',
    r'\bescuela\b',      # Spanish school
    r'\bcapacitacion\b', # Spanish training
    # Healthcare
    r'\bmedical\b',
    r'\bhealthcare\b',
    r'\bdoctor\b',
    r'\bdentist\b',
    r'\bclinic\b',
    r'\btreatment\b',
    r'\btherapy\b',
    r'\bcounseling\b',
    r'\bhospital\b',
    r'\bsalud\b',        # Spanish health
    r'\bmedico\b',       # Spanish doctor
    # Finance
    r'\bmortgage\b',
    r'\bloan\b',
    r'\bfinancing\b',
    r'\bcredit\b',
    r'\bdebt\b',
    r'\btax\b',
    r'\baccounting\b',
    r'\bbookkeeping\b',
    r'\bhipoteca\b',     # Spanish mortgage
    r'\bprestamo\b',     # Spanish loan
    r'\bcredito\b',      # Spanish credit
    # Real Estate / Housing
    r'\breal\s+estate\b',
    r'\bhomes?\s+for\b',
    r'\bproperty\b',
    r'\bhousing\b',
    r'\bapartment\b',
    r'\brental\b',
    r'\binmobiliaria?\b',  # Spanish real estate
    r'\bbienes\s+raices\b',
    # Insurance
    r'\binsurance\b',
    r'\bcoverage\b',
    r'\bpolicy\b',
    r'\bseguro\b',       # Spanish insurance
]

# Page categories that indicate regulated/high-consideration (exact matches)
REGULATED_PAGE_CATEGORIES = {
    # Legal
    'lawyer & law firm', 'legal service', 'attorney',
    # Education
    'school', 'college & university', 'education', 'trade school',
    'driving school', 'vocational school', 'educational consultant',
    # Healthcare
    'doctor', 'dentist', 'medical & health', 'hospital', 'clinic',
    'mental health service', 'therapist', 'counselor',
    # Finance
    'financial service', 'bank', 'mortgage brokers', 'loan service',
    'accountant', 'tax preparation service', 'financial planner',
    # Real Estate
    'real estate', 'real estate agent', 'property management company',
    'real estate service', 'real estate investment firm',
    # Insurance
    'insurance company', 'insurance agent', 'insurance broker',
}

# =============================================================================
# URGENCY SCORING KEYWORDS
# =============================================================================

IMMEDIACY_KEYWORDS = [
    r'\btoday\b',
    r'\bnow\b',
    r'\basap\b',
    r'\bimmediately\b',
    r'\burgent\b',
    r'\blimited\s+time\b',
    r'\blast\s+chance\b',
    r'\bcall\s+now\b',
    r'\bbook\s+today\b',
    r'\bschedule\s+now\b',
    r'\binstant\s+quote\b',
    r'\bfree\s+consultation\b',
    r'\b24/7\b',
    r'\bsame\s+day\b',
    r'\bnext\s+day\b',
    r'\bavailable\s+now\b',
    r'\bready\s+to\s+move\b',
    r'\bdon\'t\s+wait\b',
    r"\bdon't\s+wait\b",
    r'\bact\s+now\b',
    r'\bwhile\s+supplies\s+last\b',
    # Spanish immediacy
    r'\bhoy\b',
    r'\bahora\b',
    r'\bya\b',
    r'\burgente\b',
    r'\binmediato\b',
    r'\bllamanos\b',  # ASCII-folded
    r'\bagenda\s+hoy\b',
]

QUALIFICATION_KEYWORDS = [
    r'\bpre[-\s]?qualify\b',
    r'\beligib',
    r'\bapply\b',
    r'\brequirements?\b',
    r'\bqualify\b',
    r'\bcriteria\b',
    r'\bapproval\b',
    r'\bassessment\b',
    r'\bevaluation\b',
    r'\bdo\s+you\s+qualify\b',
    r'\bare\s+you\s+eligible\b',
    r'\bcheck\s+your\s+eligibility\b',
    # Spanish
    r'\bcalifica\b',
    r'\brequisitos\b',
    r'\bevaluacion\b',  # ASCII-folded
    r'\baprobacion\b',  # ASCII-folded
]

# =============================================================================
# JUNK/CONTENT FARM DETECTION
# =============================================================================

CONTENT_KEYWORDS = [
    r'\bnovel\b',
    r'\bstory\b',
    r'\bstories\b',
    r'\bfiction\b',
    r'\bdrama\b',
    r'\bshorts\b',
    r'\bepisode\b',
    r'\bepisodes\b',
    r'\bchapter\b',
    r'\bchapters\b',
    r'\bwatch\b',
    r'\bstream\b',
    r'\bseries\b',
    r'\bmovie\b',
    r'\bfilm\b',
    r'\btrailer\b',
    r'\bwebnovel\b',
    r'\bwattpad\b',
    r'\bfanfic\b',
    r'\bclip\b',
    r'\bviral\b',
    r'\bmeme\b',
    r'\bfunny\b',
    r'\blol\b',
    r'\bromance\b',
    r'\bthriller\b',
]

# =============================================================================
# MONEY SCORE THRESHOLDS (HARDENED - ad_count CAPPED)
# =============================================================================

# Ad volume thresholds - CAPPED contribution via log scale
# Max contribution: 15/50 = 30%
AD_VOLUME_THRESHOLDS = [
    (100, 15),  # Cap at 15 points
    (50, 13),
    (20, 11),
    (10, 9),
    (5, 6),
    (3, 4),
    (1, 2),
    (0, 0),
]

# Velocity thresholds (new_ads_30d -> score) - Use log scale in code
VELOCITY_THRESHOLDS = [
    (10, 10),
    (5, 7),
    (2, 4),
    (1, 2),
    (0, 0),
]

# Scale thresholds (page_like_count -> score)
SCALE_THRESHOLDS = [
    (100000, 10),
    (50000, 8),
    (10000, 6),
    (5000, 4),
    (1000, 2),
    (0, 0),
]

# =============================================================================
# CONVERSATIONAL GATE THRESHOLDS
# =============================================================================

# Minimum conversation share to pass gate
MIN_CONVERSATION_SHARE = 0.05  # 5% MESSAGE + CALL + FORM

# Minimum form share (with follow-up implied)
MIN_FORM_SHARE = 0.2

# Share thresholds for cluster assignment
MESSAGE_SHARE_THRESHOLD = 0.2
CALL_SHARE_THRESHOLD = 0.2
FORM_SHARE_THRESHOLD = 0.2

# =============================================================================
# RESCUE PATH: REGULATED BUSINESS NAME PATTERNS
# =============================================================================
# These patterns detect business NAMES that indicate regulated/high-consideration
# verticals. Used by the gate rescue path to let through businesses that use
# WEB destinations but still need conversational qualification.
#
# IMPORTANT: These match against page_name only (not ad copy) to avoid
# false positives from content farms with stories about "doctors" etc.

REGULATED_BUSINESS_NAME_PATTERNS = [
    # Real Estate
    r'\brealt[yo]r?\b',           # realtor, realty
    r'\brealty\b',
    r'\breal\s*estate\b',
    r'\bproperty\s*(group|management)?\b',
    r'\bmortgage\b',
    r'\blending\b',
    r'\bhomes?\s+(by|for|of)\b',
    r'\bre/max\b',
    r'\bkeller\s*williams\b',
    r'\bcentury\s*21\b',
    r'\bcoldwell\s*banker\b',
    r'\bsotheby\b',
    r'\bbroker\b',
    # Legal
    r'\battorney\b',
    r'\blaw\s*(firm|office|group)?\b',
    r'\blegal\s*(services?)?\b',
    r'\babogado\b',
    r'\besq\.?\b',
    # Healthcare / Medical
    r'\bmedical\b',
    r'\bdental\b',
    r'\bdentist\b',
    r'\bclinic\b',
    r'\bdoctor\b',
    r'\bdr\.\s',
    r'\bmd\b',
    r'\bdds\b',
    r'\bchiropractic\b',
    r'\btherapy\b',
    r'\bcounseling\b',
    # Finance / Accounting
    r'\bcpa\b',
    r'\baccountant\b',
    r'\baccounting\b',
    r'\btax\s*(services?|prep)?\b',
    r'\bfinancial\s*(advisor|planning|services?)?\b',
    r'\badvisory\b',
    r'\bwealth\s*management\b',
    # Insurance
    r'\binsurance\b',
    r'\ballstate\b',
    r'\bstate\s*farm\b',
    r'\bgeico\b',
    r'\bprogressive\b',
    # Education / Training
    r'\bschool\b',
    r'\bacademy\b',
    r'\buniversity\b',
    r'\bcollege\b',
    r'\binstitute\b',
    r'\btraining\s*(center)?\b',
    r'\bcdl\b',
    # Home Services (high-ticket, need qualification)
    r'\broofing\b',
    r'\bplumbing\b',
    r'\bhvac\b',
    r'\belectrical\b',
    r'\bcontractor\b',
    r'\bconstruction\b',
    r'\bremodel\b',
    r'\brenovation\b',
    r'\bpainting\b',
    r'\bcoatings\b',
    r'\bglass\s*(doctor|repair)?\b',
    r'\brepair\s*(service)?\b',
    # Auto (sales, not parts)
    r'\bauto\s*(sales|deal|group)\b',
    r'\bcar\s*(sales|dealer)\b',
    r'\btruck\s*(sales)?\b',
    r'\btrailer\s*(sales)?\b',
    r'\bdealership\b',
    # Professional Services
    r'\bconsulting\b',
    r'\bconsultant\b',
    r'\bservices?\s*(llc|inc|co)?\b',
]

# =============================================================================
# PRE-COMPILED PATTERNS
# =============================================================================

COMPILED_MESSAGE_URL_PATTERNS = compile_patterns(MESSAGE_URL_PATTERNS)
COMPILED_CALL_URL_PATTERNS = compile_patterns(CALL_URL_PATTERNS)
COMPILED_FORM_URL_PATTERNS = compile_patterns(FORM_URL_PATTERNS)
COMPILED_TRANSACTIONAL_COPY = compile_patterns(TRANSACTIONAL_COPY_PATTERNS)
COMPILED_TRANSACTIONAL_PATHS = compile_patterns(TRANSACTIONAL_PATHS)
COMPILED_PRICE_DISCOUNT = compile_patterns(PRICE_DISCOUNT_PATTERNS)
COMPILED_FOLLOWUP = compile_patterns(FOLLOWUP_PHRASES)
COMPILED_CONSULT = compile_patterns(CONSULT_LANGUAGE)
COMPILED_QUALIFICATION_ALL = compile_patterns(QUALIFICATION_PHRASES_ALL)
COMPILED_CONSULT_BOOKING = compile_patterns(CONSULT_BOOKING_PATTERNS)
COMPILED_IMMEDIACY = compile_patterns(IMMEDIACY_KEYWORDS)
COMPILED_QUALIFICATION = compile_patterns(QUALIFICATION_KEYWORDS)
COMPILED_CONTENT = compile_patterns(CONTENT_KEYWORDS)

# Expanded fit scoring patterns (v2)
COMPILED_FIT_QUALIFICATION_EXPANDED = compile_patterns(FIT_QUALIFICATION_EXPANDED)
COMPILED_FIT_FOLLOWUP = compile_patterns(FIT_FOLLOWUP_PATTERNS)
COMPILED_FIT_MULTISTEP = compile_patterns(FIT_MULTISTEP_PATTERNS)

# Implicit fit scoring patterns
COMPILED_ADVISOR_LANGUAGE = compile_patterns(ADVISOR_LANGUAGE)
COMPILED_SERVICE_BREADTH = compile_patterns(SERVICE_BREADTH_PATTERNS)
COMPILED_REGULATED_DOMAIN = compile_patterns(REGULATED_DOMAIN_PATTERNS)

# Rescue path patterns (for gate)
COMPILED_REGULATED_BUSINESS_NAME = compile_patterns(REGULATED_BUSINESS_NAME_PATTERNS)
