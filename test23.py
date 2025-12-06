import json
import random
import re
from pathlib import Path
from sklearn.model_selection import train_test_split

# ======================================================
# CONFIG
# ======================================================

TOTAL_SAMPLES = 15000
TRAIN_OUT = "train.jsonl"
DEV_OUT = "dev.jsonl"

# ======================================================
# PAYMENT METHODS (from CSV)
# ======================================================

payment_methods = [
    "1 Day Payment", "Africa Domestic Payment", "Africa Electronic Funds Transfer",
    "Africa Urgent Payment", "BACS", "BKT", "CHAPS",
    "Currency/International Payment", "DRAFT", "Domestic Payment",
    "Drafts", "EFT", "ESARS", "Externally Created Payment",
    "FPS", "HV", "IFT", "IP", "Inter account transfer",
    "International Payment (Africa)", "LET", "LV", "MDSC", "Mobile Payment",
    "MP", "Multi debit single credit", "ODP", "RTPB", "SCT", "SDMC",
    "SEPA Payments", "Single debit multi credit", "Single debit single credit",
    "TRI", "TRO", "UK Faster payment (FPS)", "UK Same day payment (CHAPS)",
    "UP", "UPS", "US ACH (Domestic Non Urgent)", "US Domestic Payment",
    "US Payment", "US Wire transfer (Domestic urgent)", "ZA Inward",
    "ZA Outward", "ZAIFT", "ZAIP"
]

payment_methods_variants = (
    payment_methods
    + [m.lower() for m in payment_methods]
    + [m.upper() for m in payment_methods]
)

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

beneficiary_formats = [
    lambda: random.choice(first_names),
    lambda: random.choice(first_names) + " " + random.choice(last_names),
    lambda: random.choice(first_names)[0] + ". " + random.choice(last_names),
    lambda: random.choice(first_names) + " " + random.choice(last_names) + " " + random.choice(last_names),
]

# ======================================================
# ACCOUNT NAME GENERATOR (Hybrid)
# ======================================================

sys_words = [
    "CLIENT", "CALL", "NOTICE", "INTERNAL", "CLEARING", "TREASURY",
    "CORPORATE", "GENERAL", "LEDGER", "GLOBAL", "POOL", "OPERATIONS"
]

account_bases = ["account", "acct", "a/c", "ledger", "unit", "pool", "fund"]

def make_account_name():
    if random.random() < 0.5:
        return (
            random.choice(sys_words)
            + " "
            + random.choice(sys_words)
            + " ACC"
            + str(random.randint(10, 99999))
        )
    else:
        n = random.choice(first_names).capitalize()
        t = random.choice(["salary", "savings", "draft", "ops", "current"])
        b = random.choice(account_bases)
        return f"{n} {t} {b}"

# ======================================================
# CURRENCY
# ======================================================

currency_list = ["USD", "EUR", "INR", "GBP", "JPY", "CAD"]
currency_variants = currency_list + [c.lower() for c in currency_list]

# ======================================================
# HIGH VARIATION TEMPLATES
# ======================================================

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

def add_noise(text):
    if random.random() < 0.2:
        text = text.replace("  ", " ")
    if random.random() < 0.15:
        text = text.capitalize()
    if random.random() < 0.1:
        text = text.upper()
    if random.random() < 0.1:
        text = text.replace("pay", "py")  # tiny typo
    return text

# ======================================================
# SAFE OFFSET FINDER (prevents overlap)  <-- IMPORTANT PATCH
# ======================================================

def find_offset_safe(text, value, used_spans):
    if not value or value.strip() == "":
        return None, None

    pattern = re.escape(value)
    matches = list(re.finditer(pattern, text, flags=re.IGNORECASE))

    for m in matches:
        s, e = m.start(), m.end()

        # check for overlap with previous spans
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
    beneficiary = random.choice(beneficiary_formats)()
    account = make_account_name()

    t = random.choice(templates).format(
        amount=amount,
        currency=currency,
        beneficiary=beneficiary,
        method=method,
        account=account,
    )

    t = add_noise(t).strip()

    ents = []
    used_spans = []

    for label, value in [
        ("amountHint", amount),
        ("currencyHint", currency),
        ("beneficiaryHint", beneficiary),
        ("methodHint", method),
        ("accountHint", account),
    ]:
        if value.strip():
            start, end = find_offset_safe(t, value, used_spans)
            if start is not None:
                ents.append([start, end, label])
                used_spans.append((start, end, label))

    records.append({"text": t, "entities": ents})

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

print("✓ Dataset generation completed")
print(f"Train size: {len(train)}")
print(f"Dev size:   {len(dev)}")