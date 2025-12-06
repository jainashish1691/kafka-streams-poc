import random
import json
import re

# ===========================================================
# GLOBAL CONFIG
# ===========================================================
TOTAL_SAMPLES = 12000      # total examples (train + dev)
DEV_RATIO = 0.20           # 20% dev
random.seed(42)

# ===========================================================
# DATA POOLS
# ===========================================================

# --- Beneficiaries: ONLY human-like names ---
BENEFICIARY_FIRST = [
    "rahul", "john", "amit", "anita", "rohan", "rohit", "alex", "sara",
    "arjun", "maria", "sunil", "kapil", "kiran", "deepa", "vijay", "naina",
    "sameer", "farhan", "priya", "raj", "neha", "yash", "sabby", "sanjay",
    "aditya", "manish", "pankaj", "suresh", "tina", "varun", "vivek", "rakesh",
    "alok", "meena", "harsh", "komal", "gopal"
]

BENEFICIARY_LAST = [
    "sharma", "patel", "iyer", "khan", "verma", "gupta", "reddy",
    "singh", "jain", "kapoor", "joshi", "desai", "pawar", "agarwal",
    "mehta", "banerjee"
]

# --- Methods: short, payment-ish phrases ONLY (no technical tokens) ---
REAL_METHODS = [
    "upi", "neft", "imps", "rtgs", "swift", "sepa", "eft",
    "faster payment", "bank transfer", "wire transfer",
    "insta pay", "fps", "ach", "instant transfer", "fast pay"
]

NOISE_METHODS = [
    "quick pay", "express pay", "mobile pay", "instant send",
    "fast route", "instant mode", "local transfer"
]

METHOD_POOL = REAL_METHODS + NOISE_METHODS

# --- Account bases (human / banking style) ---
ACCOUNT_BASE_HUMAN = [
    "salary account", "savings account", "payroll account", "primary account",
    "main account", "current account", "business account", "joint account",
    "wallet account", "bonus account", "office account", "family account",
    "home account", "sun account", "user account", "expense account"
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
# HELPERS
# ===========================================================
def rand_amount():
    # sometimes decimal
    if random.random() < 0.25:
        return f"{random.randint(10, 9999)}.{random.randint(0, 99)}"
    return str(random.randint(10, 99999))

def rand_currency():
    return random.choice(CURRENCIES)

def rand_beneficiary():
    """Return human-looking beneficiary name (never technical)."""
    fn = random.choice(BENEFICIARY_FIRST)
    ln = random.choice(BENEFICIARY_LAST)
    r = random.random()
    if r < 0.4:
        return f"{fn} {ln}"
    elif r < 0.7:
        return fn
    elif r < 0.85:
        return f"{fn[0]}. {ln}"
    else:
        return f"{fn}-{ln}"

def rand_method():
    return random.choice(METHOD_POOL)

def rand_human_account():
    base = random.choice(ACCOUNT_BASE_HUMAN)
    r = random.random()
    if r < 0.4:
        return base
    elif r < 0.8:
        return f"{base} {random.randint(10000, 99999)}"
    else:
        return f"{base} {random.choice(['One','Two','Alpha','Beta'])}"

def rand_tech_account():
    """
    Build technical account like:
    WCAS Regression LE1 ACC7wertwert
    LEI Module 9988 MAIN
    SYS PAYREF 9911 PRIMARY
    """
    prefix = random.choice(ACC_TECH_PREFIX)
    mid = random.choice(ACC_TECH_MID)
    suffix1 = random.choice(ACC_TECH_SUFFIX)
    # sometimes add extra tech token
    if random.random() < 0.5:
        suffix2 = random.choice(ACC_TECH_SUFFIX)
        return f"{prefix} {mid} {suffix1} {suffix2}"
    else:
        return f"{prefix} {mid} {suffix1}"

def rand_account():
    """Return either human-style or technical-style account."""
    if random.random() < 0.5:
        return rand_human_account()
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
    method = rand_method() if random.random() < 0.75 else None
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

    # method chunk (ALWAYS with payment preposition so model learns context)
    if method:
        prep = random.choice(PREPOSITIONS)
        chunks.append(f"{prep} {method}")

    # account chunk (often with 'from', sometimes bare)
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

    # random reference id / number
    if random.random() < 0.5:
        chunks.append(f"ref{random.randint(1000, 9999)}")

    # shuffle for extreme variation
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

    # CONSISTENT casing for sentence and entities
    mode = random.randint(0, 3)
    def apply_case(s):
        if mode == 1:
            return s.lower()
        elif mode == 2:
            return s.upper()
        elif mode == 3:
            return s.title()
        return s

    sentence = apply_case(sentence)
    if amount: amount = apply_case(amount)
    if currency: currency = apply_case(currency)
    if beneficiary: beneficiary = apply_case(beneficiary)
    if method: method = apply_case(method)
    if account: account = apply_case(account)

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
    split = int(len(all_data) * (1 - DEV_RATIO))

    train = all_data[:split]
    dev = all_data[split:]

    with open("train.jsonl", "w", encoding="utf-8") as f:
        for r in train:
            f.write(json.dumps(r) + "\n")

    with open("dev.jsonl", "w", encoding="utf-8") as f:
        for r in dev:
            f.write(json.dumps(r) + "\n")

    print(f"✔ train.jsonl generated: {len(train)} samples")
    print(f"✔ dev.jsonl generated: {len(dev)} samples")
    print("🔥 v4.0 dataset ready (complex accounts + separated methods)!")

if __name__ == "__main__":
    generate()