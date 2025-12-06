import random
import json
import re

# ------------------------------
# CONFIGURATION
# ------------------------------
NUM_SAMPLES = 7000

BENEFICIARY_FIRST_NAMES = [
    "rahul","john","amit","anita","rohan","rohit","alex","sara","arjun","maria",
    "sunil","kapil","kiran","deepa","vijay","naina","sameer","farhan","priya",
]

BENEFICIARY_LAST_NAMES = [
    "sharma","patel","iyer","khan","verma","gupta","reddy","singh"
]

REAL_METHODS = [
    "upi","neft","imps","rtgs","swift","sepa","eft","faster payment","bank transfer",
    "wire transfer","insta pay","express transfer"
]

NOISE_METHODS = [
    "iftr","intr","1ftr","ift","ipms","uft","ivtr","imt",
    "trx99","qpx1","fxp12","xpayr","rptx","uqmt"
]

# multi-word "safe" account patterns
ACCOUNT_PATTERNS = [
    "salary account", "sun account", "main account", "home account",
    "office account", "user account", "primary account", "current account",
    "family account", "business account", "wallet account", "bonus account",
    "joint account", "salary", "savings account"
]


CURRENCIES = ["usd","inr","eur","gbp","aed"]

VERBS = [
    "pay","send","transfer","dispatch","give","process","initiate payment of",
    "please transfer","kindly send"
]

PREPOSITIONS = ["via","using","through","by","with"]


# -------------------------------------------------------
# HELPER FUNCTIONS
# -------------------------------------------------------

def rand_amount():
    return str(random.choice([random.randint(1, 99999), random.randint(10, 9999)]))

def rand_currency():
    return random.choice(CURRENCIES)

def rand_beneficiary():
    # 40% full name, 60% first name only
    if random.random() < 0.4:
        return random.choice(BENEFICIARY_FIRST_NAMES) + " " + random.choice(BENEFICIARY_LAST_NAMES)
    return random.choice(BENEFICIARY_FIRST_NAMES)

def rand_method():
    # 70% real methods, 30% unknown noise
    if random.random() < 0.7:
        return random.choice(REAL_METHODS)
    return random.choice(NOISE_METHODS)

def rand_account():
    # 60% verbal-only accounts, 40% accounts with numbers
    base = random.choice(ACCOUNT_PATTERNS)
    if random.random() < 0.4:
        return f"{base} {random.randint(10000,99999)}"
    return base


# -------------------------------------------------------
# ENTITY OFFSET CALCULATOR
# -------------------------------------------------------

def find_offsets(text, substring):
    """Find exact start/end offsets for substring (first occurrence)."""
    idx = text.lower().find(substring.lower())
    if idx == -1:
        return None
    return idx, idx + len(substring)


# -------------------------------------------------------
# SENTENCE GENERATOR
# -------------------------------------------------------

def generate_sentence():
    amount = rand_amount() if random.random() < 0.9 else None
    currency = rand_currency() if random.random() < 0.3 else None
    beneficiary = rand_beneficiary() if random.random() < 0.8 else None
    method = rand_method() if random.random() < 0.7 else None
    account = rand_account() if random.random() < 0.6 else None

    verb = random.choice(VERBS)

    templates = []

    # full sentences
    templates.append(f"{verb} {amount} {currency or ''} to {beneficiary or ''} {random.choice(PREPOSITIONS)} {method or ''} from {account or ''}")
    templates.append(f"{verb} {amount} to {beneficiary or ''} {random.choice(PREPOSITIONS)} {method or ''}")
    templates.append(f"{verb} {amount} {currency or ''} from {account or ''}")
    templates.append(f"{beneficiary or ''} needs {amount} {currency or ''} using {method or ''}")
    templates.append(f"{verb} {amount} for {beneficiary or ''} through {method or ''} from {account or ''}")

    # minimalistic variations
    templates.append(f"send {amount} to {beneficiary or ''}")
    templates.append(f"{verb} {amount} now")
    templates.append(f"{amount} to {beneficiary or ''} via {method or ''}")
    templates.append(f"from {account or ''} send {amount}")

    # Choose final sentence
    sentence = random.choice(templates)
    sentence = re.sub(r"\s+", " ", sentence).strip()

    return sentence, amount, currency, beneficiary, method, account


# -------------------------------------------------------
# BUILD FINAL JSONL RECORD
# -------------------------------------------------------

def build_record(sentence, amount, currency, beneficiary, method, account):
    entities = []

    if amount:
        off = find_offsets(sentence, amount)
        if off: entities.append([off[0], off[1], "amountHint"])

    if currency:
        off = find_offsets(sentence, currency)
        if off: entities.append([off[0], off[1], "currencyHint"])

    if beneficiary:
        off = find_offsets(sentence, beneficiary)
        if off: entities.append([off[0], off[1], "beneficiaryHint"])

    if method:
        off = find_offsets(sentence, method)
        if off: entities.append([off[0], off[1], "methodHint"])

    if account:
        off = find_offsets(sentence, account)
        if off: entities.append([off[0], off[1], "accountHint"])

    return {
        "text": sentence,
        "entities": entities
    }


# -------------------------------------------------------
# MAIN GENERATOR
# -------------------------------------------------------

def generate_dataset(n=NUM_SAMPLES, output="train.jsonl"):
    with open(output, "w", encoding="utf-8") as f:
        for _ in range(n):
            sentence, amount, currency, beneficiary, method, account = generate_sentence()
            record = build_record(sentence, amount, currency, beneficiary, method, account)
            f.write(json.dumps(record) + "\n")

    print(f"✔ Dataset generated: {output}")
    print(f"✔ Total samples: {n}")


# Run generator
generate_dataset()