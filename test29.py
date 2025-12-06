import json
import random
import re
from sklearn.model_selection import train_test_split

# ======================================================
# CONFIG
# ======================================================

TOTAL_SAMPLES = 60000          # <- 60k records total
TRAIN_OUT = "train.jsonl"
DEV_OUT = "dev.jsonl"

random.seed(42)

# ======================================================
# PAYMENT METHODS (from your CSV)
# ======================================================

base_methods = [
    "1 Day Payment",
    "Africa Domestic Payment",
    "Africa Electronic Funds Transfer",
    "Africa Urgent Payment",
    "BACS",
    "BKT",
    "CHAPS",
    "Currency/International Payment",
    "DRAFT",
    "Domestic Payment",
    "Drafts",
    "EFT",
    "ESARS",
    "Externally Created Payment",
    "FPS",
    "HV",
    "IFT",
    "IP",
    "Inter account transfer",
    "International Payment (Africa)",
    "LET",
    "LV",
    "MDSC",
    "Mobile Payment",
    "MP",
    "Multi debit single credit",
    "ODP",
    "RTPB",
    "SCT",
    "SDMC",
    "SEPA Payments",
    "Single debit multi credit",
    "Single debit single credit",
    "TRI",
    "TRO",
    "UK Faster payment (FPS)",
    "UK Same day payment (CHAPS)",
    "UK Three day payment (Bacs)",
    "UP",
    "UPS",
    "US",
    "US ACH (Domestic Non Urgent)",
    "US Domestic Payment",
    "US Payment",
    "US Wire transfer (Domestic urgent)",
    "ZA Inward",
    "ZA Outward",
    "ZAIFT",
    "ZAIP",
]

def extract_paren_abbr(m: str):
    # e.g. "UK Faster payment (FPS)" -> ["FPS"]
    m = m.strip()
    inside = re.findall(r"\(([^)]+)\)", m)
    return [x.strip() for x in inside if x.strip()]

def make_initial_abbr(m: str):
    # Africa Domestic Payment -> ADP
    # US Wire transfer (Domestic urgent) -> UWTDU -> then trim
    words = re.sub(r"\([^)]*\)", "", m)  # remove parentheses
    words = re.sub(r"[/]", " ", words)
    parts = [w for w in words.split() if w and w[0].isalpha()]
    if not parts:
        return None
    abbr = "".join(p[0].upper() for p in parts)
    return abbr if len(abbr) >= 2 else None

def misspell(m: str):
    # very small, realistic typos
    s = m
    if len(s) < 4:
        return s
    # drop a random vowel
    vowels = "aeiouAEIOU"
    vowel_idx = [i for i, ch in enumerate(s) if ch in vowels]
    if vowel_idx and random.random() < 0.5:
        i = random.choice(vowel_idx)
        s = s[:i] + s[i+1:]
    # swap two adjacent chars
    if len(s) > 4 and random.random() < 0.5:
        i = random.randint(0, len(s) - 2)
        s = s[:i] + s[i+1] + s[i] + s[i+2:]
    return s

method_set = set()

for m in base_methods:
    m_clean = m.strip()
    if not m_clean:
        continue
    method_set.add(m_clean)
    method_set.add(m_clean.lower())
    method_set.add(m_clean.upper())
    # abbr from parentheses
    for ab in extract_paren_abbr(m_clean):
        method_set.add(ab)
        method_set.add(ab.lower())
        method_set.add(ab.upper())
    # abbr from initials
    ab2 = make_initial_abbr(m_clean)
    if ab2:
        method_set.add(ab2)
        method_set.add(ab2.lower())
    # typos
    if random.random() < 0.7:
        typo = misspell(m_clean)
        method_set.add(typo)
        method_set.add(typo.lower())

payment_methods_variants = list(method_set)

# ======================================================
# BENEFICIARY GENERATOR
# ======================================================

first_names = [
    "john", "jacob", "ashish", "rahul", "vikram", "maria", "david",
    "ramesh", "anita", "steve", "sarah", "peter", "rohit", "arjun"
]

last_names = [
    "jain", "patel", "iyer", "khan", "sharma", "gomez",
    "lee", "wong", "jacob", "rajput", "reddy", "kumar"
]

def make_beneficiary():
    r = random.random()
    fn = random.choice(first_names)
    ln = random.choice(last_names)
    if r < 0.25:
        return fn
    elif r < 0.55:
        return f"{fn} {ln}"
    elif r < 0.80:
        mid = random.choice(last_names)
        return f"{fn} {mid} {ln}"
    else:
        return f"{fn[0].upper()}. {ln}"

# ======================================================
# ACCOUNT NAME GENERATOR (very strong)
# ======================================================

sys_words = [
    "CLIENT", "CALL", "NOTICE", "INTERNAL", "CLEARING", "TREASURY",
    "CORPORATE", "GENERAL", "LEDGER", "GLOBAL", "POOL", "OPERATIONS"
]

generic_single_accounts = [
    "account", "bank", "salary", "current", "savings", "saving",
    "client", "notice", "draft", "payment", "deposit", "income",
    "company", "office", "primary", "business"
]

generic_multi_accounts = [
    "bank account", "bank ac", "salary account", "salary ac",
    "current account", "current ac", "savings account", "saving account",
    "main account", "primary account", "my account", "my ac",
    "office account", "payroll account", "business account",
    "client account", "client notice account"
]

banks = ["HDFC", "ICICI", "SBI", "AXIS", "KOTAK", "YES"]

account_bases_tokens = ["account", "ac", "a/c", "acct", "ledger", "unit", "pool", "fund"]

def make_system_account():
    w1 = random.choice(sys_words)
    w2 = random.choice(sys_words)
    num = random.randint(10, 99999)
    # allow hyphen or slash sometimes
    sep = random.choice([" ", " ", "-", " / "])
    return f"{w1} {w2}{sep}ACC{num}"

def make_human_account():
    name = random.choice(first_names).capitalize()
    t = random.choice(["salary", "ops", "current", "savings", "draft"])
    base = random.choice(account_bases_tokens)
    return f"{name} {t} {base}"

def make_bank_account():
    bank = random.choice(banks)
    base = random.choice(["account", "ac", "a/c", "current account", "salary account"])
    return f"{bank} {base}"

def make_account_name():
    r = random.random()
    if r < 0.30:
        # generic only
        if random.random() < 0.4:
            return random.choice(generic_single_accounts)
        else:
            return random.choice(generic_multi_accounts)
    elif r < 0.55:
        return make_bank_account()
    elif r < 0.80:
        return make_human_account()
    else:
        return make_system_account()

# ======================================================
# CURRENCY
# ======================================================

currency_list = ["USD", "EUR", "INR", "GBP", "JPY", "CAD"]
currency_variants = currency_list + [c.lower() for c in currency_list]

# ======================================================
# SENTENCE TEMPLATES (with account phrase)
# ======================================================

templates = [
    "pay {amount} {currency} to {beneficiary} using {method} {account_phrase}",
    "pay {amount} {currency} to {beneficiary} via {method} {account_phrase}",
    "transfer {amount} to {beneficiary} with {method} {account_phrase}",
    "send {amount} {currency} for {beneficiary} using {method} {account_phrase}",
    "please transfer {amount} {currency} to {beneficiary} {account_phrase} with {method}",
    "{beneficiary} should receive {amount} using {method} {account_phrase}",
    "{account_phrase} pay {beneficiary} {amount} by {method}",
    "initiate {method} of {amount} {currency} for {beneficiary} {account_phrase}",
    "process {amount} payment to {beneficiary} via {method} {account_phrase}",
    "from {beneficiary} {account_phrase} send {amount} {currency} by {method}",
]

account_phrase_patterns = [
    "from {acc}",
    "from my {acc}",
    "from our {acc}",
    "using {acc}",
    "via {acc}",
    "with {acc}",
    "to be debited from {acc}",
    "deduct from {acc}",
    "charge to {acc}",
    "out of {acc}",
    "against {acc}",
    "into {acc}",
    "in {acc}",
    "through {acc}",
    "by {acc}",
]

def build_account_phrase(acc: str) -> str:
    pattern = random.choice(account_phrase_patterns)
    return pattern.format(acc=acc)

def add_noise(text: str) -> str:
    if random.random() < 0.2:
        text = re.sub(r"\s+", " ", text)
    if random.random() < 0.15:
        text = text.capitalize()
    if random.random() < 0.1:
        text = text.upper()
    if random.random() < 0.07:
        text = text.replace("pay", "py")  # tiny typo
    return text.strip()

# ======================================================
# SAFE OFFSET FINDER (NO OVERLAPS)
# ======================================================

def find_offset_safe(text: str, value: str, used_spans):
    if not value or not value.strip():
        return None, None

    pattern = re.escape(value)
    for m in re.finditer(pattern, text):
        s, e = m.start(), m.end()
        conflict = False
        for (ps, pe, _) in used_spans:
            if not (e <= ps or s >= pe):
                conflict = True
                break
        if not conflict:
            return s, e
    return None, None

# ======================================================
# DATASET CREATION LOOP
# ======================================================

records = []

for _ in range(TOTAL_SAMPLES):
    amount = str(round(random.uniform(10, 9999), 2))
    currency = random.choice(currency_variants) if random.random() < 0.85 else ""

    method = random.choice(payment_methods_variants) if random.random() < 0.90 else ""
    beneficiary = make_beneficiary()
    account = make_account_name()
    account_phrase = build_account_phrase(account)

    template = random.choice(templates)
    text = template.format(
        amount=amount,
        currency=currency,
        beneficiary=beneficiary,
        method=method,
        account_phrase=account_phrase,
    )
    text = re.sub(r"\s+", " ", text)
    text = add_noise(text)

    ents = []
    used_spans = []

    for label, value in [
        ("amountHint", amount),
        ("currencyHint", currency),
        ("beneficiaryHint", beneficiary),
        ("methodHint", method),
        ("accountHint", account),
    ]:
        if value and value.strip():
            start, end = find_offset_safe(text, value, used_spans)
            if start is not None:
                ents.append([start, end, label])
                used_spans.append((start, end, label))

    records.append({"text": text, "entities": ents})

# ======================================================
# TRAIN / DEV SPLIT
# ======================================================

train, dev = train_test_split(records, test_size=0.2, random_state=42)

def save_jsonl(path, data):
    with open(path, "w", encoding="utf-8") as f:
        for r in data:
            f.write(json.dumps(r) + "\n")

save_jsonl(TRAIN_OUT, train)
save_jsonl(DEV_OUT, dev)

print("✓ v10c dataset generation completed.")
print(f"Train size: {len(train)}")
print(f"Dev size:   {len(dev)}")