import json
import random
import re
from pathlib import Path
from sklearn.model_selection import train_test_split

# ---------------------------------------
# CONFIG
# ---------------------------------------
TOTAL_SAMPLES = 15000
TRAIN_OUT = "train.jsonl"
DEV_OUT = "dev.jsonl"

# Your provided payment method CSV list (cleaned)
payment_methods = [
    "1 Day Payment", "Africa Domestic Payment", "Africa Electronic Funds Transfer",
    "Africa Urgent Payment", "BACS", "BKT", "CHAPS", "Currency/International Payment",
    "DRAFT", "Domestic Payment", "Drafts", "EFT", "ESARS", "Externally Created Payment",
    "FPS", "HV", "IFT", "IP", "Inter account transfer", "International Payment (Africa)",
    "LET", "LV", "MDSC", "Mobile Payment", "MP", "Multi debit single credit",
    "ODP", "RTPB", "SCT", "SDMC", "SEPA Payments", "Single debit multi credit",
    "Single debit single credit", "TRI", "TRO", "UK Faster payment (FPS)",
    "UK Same day payment (CHAPS)", "UP", "UPS", "US ACH (Domestic Non Urgent)",
    "US Domestic Payment", "US Payment", "US Wire transfer (Domestic urgent)",
    "ZA Inward", "ZA Outward", "ZAIFT", "ZAIP"
]

# Normalize methods for variation
payment_methods_variants = payment_methods + \
    [m.lower() for m in payment_methods] + \
    [m.upper() for m in payment_methods]

# ---------------------------------------
# BENEFICIARY NAME POOL
# ---------------------------------------
first_names = [
    "john", "jacob", "ashish", "rahul", "vikram", "maria", "david",
    "ramesh", "anita", "steve", "sarah", "peter", "rohit", "arjun",
]

last_names = [
    "jain", "patel", "iyer", "khan", "sharma", "gomez", "lee", "wong",
    "jacob", "rajput", "reddy", "kumar"
]

beneficiary_formats = [
    lambda: random.choice(first_names),
    lambda: random.choice(first_names) + " " + random.choice(last_names),
    lambda: random.choice(first_names)[0] + ". " + random.choice(last_names),
    lambda: random.choice(first_names) + " " + random.choice(last_names) + " " + random.choice(last_names),
]

# ---------------------------------------
# HYBRID ACCOUNT NAME GENERATOR
# ---------------------------------------
sys_words = [
    "CLIENT", "CALL", "NOTICE", "INTERNAL", "CLEARING", "TREASURY",
    "CORPORATE", "GENERAL", "LEDGER", "GLOBAL", "POOL", "OPERATIONS"
]

account_bases = [
    "account", "acct", "a/c", "ledger", "unit", "pool", "fund"
]

def make_account_name():
    # 50% system-style, 50% human-like
    if random.random() < 0.5:
        # System-style
        parts = [
            random.choice(sys_words),
            random.choice(sys_words),
            f"ACC{random.randint(10,9999)}"
        ]
        return " ".join(parts)
    else:
        # Human-style account
        b = random.choice(first_names).capitalize()
        t = random.choice(["salary", "payroll", "operations", "draft", "savings"])
        base = random.choice(account_bases)
        return f"{b} {t} {base}"

# ---------------------------------------
# CURRENCY POOL
# ---------------------------------------
currency_list = ["USD", "EUR", "INR", "GBP", "JPY", "CAD"]
currency_variants = currency_list + [c.lower() for c in currency_list]

# ---------------------------------------
# SENTENCE TEMPLATES (High Variation)
# ---------------------------------------
templates = [
    "pay {amount} {currency} to {beneficiary} via {method} from {account}",
    "transfer {amount} to {beneficiary} with {method} from {account}",
    "send {amount} {currency} for {beneficiary} using {method} from {account}",
    "please transfer {amount} {currency} to {beneficiary} from {account} with {method}",
    "{beneficiary} should receive {amount} using {method} from {account}",
    "from {account} pay {beneficiary} {amount} by {method}",
    "initiate {method} of {amount} {currency} for {beneficiary} from {account}",
    "process {amount} payment to {beneficiary} via {method} from {account}",
]

# Minor noise variations
def add_noise(text):
    if random.random() < 0.2:
        text = text.replace("  ", " ")
    if random.random() < 0.15:
        text = text.capitalize()
    if random.random() < 0.1:
        text = text.upper()
    if random.random() < 0.1:
        text = text.replace("pay", "py")  # small typo
    return text


# ---------------------------------------
# OFFSET CALCULATION
# ---------------------------------------
def find_offset(text, value):
    matches = list(re.finditer(re.escape(value), text))
    if matches:
        m = matches[0]
        return m.start(), m.end()
    return None, None

# ---------------------------------------
# BUILD DATASET
# ---------------------------------------
records = []

for _ in range(TOTAL_SAMPLES):
    amount = str(round(random.uniform(10, 9999), 2))
    currency = random.choice(currency_variants) if random.random() < 0.85 else ""
    method = random.choice(payment_methods_variants) if random.random() < 0.90 else ""
    beneficiary = random.choice(beneficiary_formats)()
    account = make_account_name()

    # Fill template
    t = random.choice(templates)
    t = t.format(amount=amount, currency=currency, beneficiary=beneficiary,
                 method=method, account=account)
    t = t.replace("  ", " ")
    t = add_noise(t).strip()

    ents = []
    for label, value in [
        ("amountHint", amount),
        ("currencyHint", currency),
        ("beneficiaryHint", beneficiary),
        ("methodHint", method),
        ("accountHint", account),
    ]:
        if value.strip():
            start, end = find_offset(t, value)
            if start is not None:
                ents.append([start, end, label])

    records.append({"text": t, "entities": ents})

# ---------------------------------------
# TRAIN/DEV SPLIT
# ---------------------------------------
train, dev = train_test_split(records, test_size=0.2, random_state=42)

def save_jsonl(path, items):
    with open(path, "w", encoding="utf-8") as f:
        for r in items:
            f.write(json.dumps(r) + "\n")

save_jsonl(TRAIN_OUT, train)
save_jsonl(DEV_OUT, dev)

print("✓ Dataset generated:")
print(f" - Train: {len(train)}")
print(f" - Dev:   {len(dev)}")