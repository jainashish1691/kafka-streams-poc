import random
import json
import re
import csv
from pathlib import Path

# ===========================================================
# GLOBAL CONFIG
# ===========================================================
TOTAL_SAMPLES = 7000       # total examples (train + dev)
DEV_SIZE = 1000            # fixed dev size, rest = train
METHOD_CSV_PATH = Path("methods.csv")  # <-- set your CSV file name/path here

# methods: 80% from CSV, 20% synthetic
CSV_METHOD_RATIO = 0.8

random.seed(42)

# ===========================================================
# DATA POOLS (BENEFICIARY, ACCOUNT, NOISE)
# ===========================================================

# --- Beneficiaries: ONLY human-like names, no digits ---
BENEFICIARY_FIRST = [
    "rahul", "john", "amit", "anita", "rohan", "rohit", "alex", "sara",
    "arjun", "maria", "sunil", "kapil", "kiran", "deepa", "vijay", "naina",
    "sameer", "farhan", "priya", "raj", "neha", "yash", "sabby", "sanjay",
    "aditya", "manish", "pankaj", "suresh", "tina", "varun", "vivek",
    "rakesh", "alok", "meena", "harsh", "komal", "gopal", "jacob"
]

BENEFICIARY_LAST = [
    "sharma", "patel", "iyer", "khan", "verma", "gupta", "reddy",
    "singh", "jain", "kapoor", "joshi", "desai", "pawar", "agarwal",
    "mehta", "banerjee", "jacob"
]

# --- Synthetic methods (fallback, for generalization) ---
SYNTH_METHODS = [
    "upi", "neft", "imps", "rtgs", "swift", "sepa", "eft",
    "faster payment", "bank transfer", "wire transfer",
    "insta pay", "fps", "ach", "instant transfer",
    "fast pay", "ift", "quick pay", "express pay",
    "mobile pay", "instant send", "fast route",
    "instant mode", "local transfer"
]

# --- Simple / human-style account patterns ---
ACCOUNT_BASE_HUMAN = [
    "salary account", "savings account", "payroll account", "primary account",
    "main account", "current account", "business account", "joint account",
    "wallet account", "bonus account", "office account", "family account",
    "home account", "sun account", "user account", "expense account"
]

BANKS = [
    "sbi", "hdfc", "icici", "axis", "kotak", "hsbc", "dbs",
    "rbl", "pnb", "idfc", "yes", "boi"
]

ACCOUNT_ADJ = [
    "draft", "main", "primary", "secondary", "new", "old",
    "corporate", "temporary", "domestic", "central"
]

# --- Technical tokens for complex accounts (WCAS Regression LE1 ACC7wertwert etc.) ---
ACC_TECH_PREFIX = [
    "WCAS", "LEI", "ACC", "SYS", "MOD", "UNIT", "PAYREF",
    "GL", "LEDGER", "CORE", "NODE", "BRANCH"
]

ACC_TECH_MID = [
    "Regression", "Module", "Main", "Primary", "Engine",
    "Service", "Layer", "Cluster", "Channel"
]

ACC_TECH_SUFFIX = [
    "LE1", "LE2", "R1", "R2", "R3", "55001", "9988",
    "ACC7wertwert", "XTZ55MAIN", "88KAPA", "ZX1", "BR01", "BR02"
]

CURRENCIES = ["usd", "inr", "eur", "gbp", "aed", "cad", "aud"]

VERBS = [
    "pay", "send", "transfer", "dispatch", "give", "process",
    "credit", "initiate payment of", "please transfer",
    "kindly send", "make payment", "execute transfer of"
]

PREPOSITIONS = ["via", "using", "through", "by", "with"]

NOISE_TOKENS = [
    "pls", "plz", "yaar", "bro", "urgent", "asap", "now", "today", "tomm",
    "ok", "thanks", "kk", "ref", "txn", "id", "check", "manual", "internal",
    "fast", "jaldi", "boss", "note", "info", "immediate", "required"
]

EMOJIS = ["🙂", "😀", "🙏", "👍", "🔥", "🚀", "💸", "✅", "❗"]
PUNCT = [".", "!", "!!", "...", "?", "??", ""]


# ===========================================================
# CSV LOADING (METHODS)
# ===========================================================
def load_methods_from_csv(path: Path):
    """
    Load unique method names from CSV column 'method'.
    Normalize for uniqueness (lowercase), but keep one original form.
    """
    methods_lower_to_original = {}
    if not path.exists():
        print(f"WARNING: CSV file '{path}' not found. Using only synthetic methods.")
        return []

    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if "method" not in reader.fieldnames:
            raise ValueError(f"CSV '{path}' must have a column named 'method'.")

        for row in reader:
            val = (row.get("method") or "").strip()
            if not val:
                continue
            low = val.lower()
            if low not in methods_lower_to_original:
                methods_lower_to_original[low] = val

    methods = list(methods_lower_to_original.values())
    print(f"Loaded {len(methods)} unique methods from CSV.")
    return methods


CSV_METHODS = load_methods_from_csv(METHOD_CSV_PATH)


# ===========================================================
# HELPER FUNCTIONS
# ===========================================================
def rand_amount():
    if random.random() < 0.25:
        return f"{random.randint(10, 9999)}.{random.randint(0, 99)}"
    return str(random.randint(10, 99999))


def rand_currency():
    return random.choice(CURRENCIES)


def rand_beneficiary():
    """
    Generate human-like full names (no digits), with ~90% multi-token:
    - first last               -> john jacob
    - first middle last        -> john rahul jacob
    - first initial last       -> john a jacob
    - initial last             -> a jacob
    - a.k jacob / A.K. Jacob   -> a.k jacob / A.K. Jacob / A K Jacob
    - hyphenated               -> john-jacob
    - single first name        -> (rare) john
    """
    fn = random.choice(BENEFICIARY_FIRST)
    ln = random.choice(BENEFICIARY_LAST)
    mid = random.choice(BENEFICIARY_FIRST)
    initial1 = fn[0]
    initial2 = mid[0]

    r = random.random()

    # ~10% single-token, 90% multi-token
    if r < 0.10:
        # single first name
        name = fn
    elif r < 0.35:
        # first + last
        name = f"{fn} {ln}"
    elif r < 0.55:
        # first + middle + last
        name = f"{fn} {mid} {ln}"
    elif r < 0.70:
        # first + initial + last  (john a jacob)
        name = f"{fn} {initial2.lower()} {ln}"
    elif r < 0.82:
        # initial + last  (a jacob)
        name = f"{initial1.lower()} {ln}"
    elif r < 0.94:
        # "a.k jacob" / "A.K. Jacob" / "A K Jacob"
        style = random.randint(0, 2)
        if style == 0:
            name = f"{initial1.lower()}.{initial2.lower()} {ln}"       # a.k jacob
        elif style == 1:
            name = f"{initial1.upper()}.{initial2.upper()}. {ln}"      # A.K. Jacob
        else:
            name = f"{initial1.upper()} {initial2.upper()} {ln}"       # A K Jacob
    else:
        # hyphenated first-last
        name = f"{fn}-{ln}"

    return name


def random_case_variant(text: str) -> str:
    """Apply random casing to a base string."""
    mode = random.randint(0, 3)
    if mode == 0:
        return text
    if mode == 1:
        return text.lower()
    if mode == 2:
        return text.upper()
    return text.title()


def rand_method():
    """
    Choose method: 80% from CSV, 20% from synthetic.
    Apply random casing variation.
    """
    use_csv = CSV_METHODS and random.random() < CSV_METHOD_RATIO
    if use_csv:
        base = random.choice(CSV_METHODS)
    else:
        base = random.choice(SYNTH_METHODS)
    return random_case_variant(base)


def rand_simple_account():
    """
    Simple / human-like accounts.
    Balanced mix of:
    - <base>
    - <bank> account
    - <bank> <base>
    - <adj> <base>
    - <adj> <bank> account
    Some with hyphens.
    """
    base = random.choice(ACCOUNT_BASE_HUMAN)
    bank = random.choice(BANKS)
    adj = random.choice(ACCOUNT_ADJ)

    # normalize base
    if base.endswith("account"):
        base_root = base.replace(" account", "")
    else:
        base_root = base

    r = random.random()
    if r < 0.25:
        name = base                        # "salary account"
    elif r < 0.45:
        name = f"{bank} account"           # "sbi account"
    elif r < 0.65:
        name = f"{bank} {base}"            # "sbi salary account"
    elif r < 0.80:
        name = f"{adj} {base}"             # "draft salary account"
    else:
        # include hyphen: "draft sbi-main account" / "sbi-main account"
        if random.random() < 0.5:
            name = f"{adj} {bank}-{base_root} account"
        else:
            name = f"{bank}-{base_root} account"

    return name


def rand_tech_account():
    """
    Technical accounts like:
    WCAS Regression LE1 ACC7wertwert
    LEI Module 9988 BR01
    """
    prefix = random.choice(ACC_TECH_PREFIX)
    mid = random.choice(ACC_TECH_MID)
    suffix1 = random.choice(ACC_TECH_SUFFIX)
    if random.random() < 0.5:
        suffix2 = random.choice(ACC_TECH_SUFFIX)
        return f"{prefix} {mid} {suffix1} {suffix2}"
    else:
        return f"{prefix} {mid} {suffix1}"


def rand_account():
    """
    Balanced account strategy:
    ~50% simple / human accounts, ~50% technical accounts.
    Account can contain digits (Option A).
    """
    if random.random() < 0.5:
        return rand_simple_account()
    else:
        return rand_tech_account()


def maybe_noise_token():
    return random.choice(NOISE_TOKENS) if random.random() < 0.35 else ""


def maybe_emoji():
    return random.choice(EMOJIS) if random.random() < 0.2 else ""


def find_offsets(text, substring):
    idx = text.lower().find(substring.lower())
    if idx == -1:
        return None
    return idx, idx + len(substring)


# ===========================================================
# SENTENCE GENERATION
# ===========================================================
def generate_sentence():
    # Presence probabilities
    amount = rand_amount() if random.random() < 0.97 else None
    currency = rand_currency() if random.random() < 0.45 else None
    beneficiary = rand_beneficiary() if random.random() < 0.92 else None
    method = rand_method() if random.random() < 0.80 else None
    account = rand_account() if random.random() < 0.80 else None

    verb = random.choice(VERBS)
    chunks = []

    # amount + currency
    if amount and currency:
        if random.random() < 0.5:
            chunks.append(f"{amount} {currency}")
        else:
            chunks.append(f"{currency} {amount}")
    elif amount:
        chunks.append(amount)

    # beneficiary chunk
    if beneficiary:
        if random.random() < 0.5:
            chunks.append(f"to {beneficiary}")
        else:
            chunks.append(beneficiary)

    # method chunk (always with preposition for context)
    if method:
        prep = random.choice(PREPOSITIONS)
        chunks.append(f"{prep} {method}")

    # account chunk (often with 'from')
    if account:
        if random.random() < 0.7:
            chunks.append(f"from {account}")
        else:
            chunks.append(account)

    # verb chunk
    chunks.append(verb)

    # noise chunks
    for _ in range(random.randint(0, 3)):
        t = maybe_noise_token()
        if t:
            chunks.append(t)

    # random reference number
    if random.random() < 0.5:
        chunks.append(f"ref{random.randint(1000, 9999)}")

    random.shuffle(chunks)
    sentence = " ".join(chunks).strip()

    # emojis + punctuation
    if random.random() < 0.3:
        sentence = maybe_emoji() + " " + sentence
    punct = random.choice(PUNCT)
    if punct:
        sentence = sentence + punct
    if random.random() < 0.2:
        sentence = sentence + " " + maybe_emoji()

    sentence = re.sub(r"\s+", " ", sentence).strip()

    # CONSISTENT casing for sentence and all entity strings
    mode = random.randint(0, 3)

    def apply_case(s: str) -> str:
        if mode == 1:
            return s.lower()
        elif mode == 2:
            return s.upper()
        elif mode == 3:
            return s.title()
        return s

    sentence = apply_case(sentence)
    if amount:
        amount = apply_case(amount)
    if currency:
        currency = apply_case(currency)
    if beneficiary:
        beneficiary = apply_case(beneficiary)
    if method:
        method = apply_case(method)
    if account:
        account = apply_case(account)

    return sentence, amount, currency, beneficiary, method, account


# ===========================================================
# RECORD BUILDING
# ===========================================================
def build_record(sentence, amount, currency, beneficiary, method, account):
    entities = []

    if amount:
        off = find_offsets(sentence, amount)
        if off:
            entities.append([off[0], off[1], "amountHint"])

    if currency:
        off = find_offsets(sentence, currency)
        if off:
            entities.append([off[0], off[1], "currencyHint"])

    if beneficiary:
        off = find_offsets(sentence, beneficiary)
        if off:
            entities.append([off[0], off[1], "beneficiaryHint"])

    if method:
        off = find_offsets(sentence, method)
        if off:
            entities.append([off[0], off[1], "methodHint"])

    if account:
        off = find_offsets(sentence, account)
        if off:
            entities.append([off[0], off[1], "accountHint"])

    return {"text": sentence, "entities": entities}


# ===========================================================
# MAIN GENERATOR
# ===========================================================
def generate():
    all_data = []

    for _ in range(TOTAL_SAMPLES):
        sent, amt, cur, ben, meth, acc = generate_sentence()
        rec = build_record(sent, amt, cur, ben, meth, acc)
        all_data.append(rec)

    random.shuffle(all_data)

    dev_size = min(DEV_SIZE, len(all_data) // 3)  # safety guard
    train = all_data[:-dev_size]
    dev = all_data[-dev_size:]

    with open("train.jsonl", "w", encoding="utf-8") as f:
        for r in train:
            f.write(json.dumps(r) + "\n")

    with open("dev.jsonl", "w", encoding="utf-8") as f:
        for r in dev:
            f.write(json.dumps(r) + "\n")

    print(f"✔ train.jsonl generated: {len(train)} samples")
    print(f"✔ dev.jsonl generated:   {len(dev)} samples")
    print("🔥 v6.0 dataset ready (CSV-powered methods + robust entities)!")


if __name__ == "__main__":
    generate()