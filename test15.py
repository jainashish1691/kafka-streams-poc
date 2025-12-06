import random
import json
import re

# ------------------------------
# CONFIGURATION
# ------------------------------
TOTAL_SAMPLES = 7000
DEV_RATIO = 0.20   # 20% dev set

BENEFICIARY_FIRST = [
    "rahul","john","amit","anita","rohan","rohit","alex","sara","arjun","maria",
    "sunil","kapil","kiran","deepa","vijay","naina","sameer","farhan","priya",
]

BENEFICIARY_LAST = ["sharma","patel","iyer","khan","verma","gupta","reddy","singh"]

REAL_METHODS = [
    "upi","neft","imps","rtgs","swift","sepa","eft",
    "faster payment","bank transfer","wire transfer","insta pay"
]

NOISE_METHODS = [
    "iftr","intr","1ftr","ift","ipms","uft","ivtr","imt",
    "trx99","qpx1","fxp12","xpayr","rptx","uqmt"
]

ACCOUNT_PATTERNS = [
    "salary account", "sun account", "main account", "home account",
    "office account", "user account", "primary account", "current account",
    "family account", "business account", "wallet account", "bonus account",
    "joint account", "salary", "savings account"
]

CURRENCIES = ["usd","inr","eur","gbp","aed"]

VERBS = [
    "pay","send","transfer","dispatch","give","process",
    "initiate payment of","please transfer","kindly send"
]

PREPOSITIONS = ["via","using","through","by","with"]


# ------------------------------
# HELPERS
# ------------------------------

def rand_amount():
    return str(random.randint(10, 99999))

def rand_currency():
    return random.choice(CURRENCIES)

def rand_beneficiary():
    if random.random() < 0.4:
        return random.choice(BENEFICIARY_FIRST) + " " + random.choice(BENEFICIARY_LAST)
    return random.choice(BENEFICIARY_FIRST)

def rand_method():
    return random.choice(REAL_METHODS) if random.random() < 0.7 else random.choice(NOISE_METHODS)

def rand_account():
    base = random.choice(ACCOUNT_PATTERNS)
    if random.random() < 0.4:
        return f"{base} {random.randint(10000,99999)}"
    return base

def find_offsets(text, substring):
    idx = text.lower().find(substring.lower())
    if idx == -1:
        return None
    return idx, idx + len(substring)


# -----------------------------------
# SENTENCE GENERATOR
# -----------------------------------

def generate_sentence():
    amount = rand_amount() if random.random() < 0.9 else None
    currency = rand_currency() if random.random() < 0.3 else None
    beneficiary = rand_beneficiary() if random.random() < 0.8 else None
    method = rand_method() if random.random() < 0.7 else None
    account = rand_account() if random.random() < 0.6 else None

    verb = random.choice(VERBS)

    templates = [
        f"{verb} {amount} {currency or ''} to {beneficiary or ''} {random.choice(PREPOSITIONS)} {method or ''} from {account or ''}",
        f"{verb} {amount} to {beneficiary or ''} {random.choice(PREPOSITIONS)} {method or ''}",
        f"{verb} {amount} {currency or ''} from {account or ''}",
        f"{beneficiary or ''} needs {amount} {currency or ''} using {method or ''}",
        f"{verb} {amount} for {beneficiary or ''} through {method or ''} from {account or ''}",
        f"send {amount} to {beneficiary or ''}",
        f"{verb} {amount} now",
        f"{amount} to {beneficiary or ''} via {method or ''}",
        f"from {account or ''} send {amount}",
    ]

    sentence = random.choice(templates)
    sentence = re.sub(r"\s+", " ", sentence).strip()

    return sentence, amount, currency, beneficiary, method, account


# -----------------------------------
# RECORD BUILDER
# -----------------------------------

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

    return {"text": sentence, "entities": entities}


# -----------------------------------
# MAIN GENERATOR
# -----------------------------------

def generate():
    all_data = []

    for _ in range(TOTAL_SAMPLES):
        sent, amt, cur, ben, meth, acc = generate_sentence()
        rec = build_record(sent, amt, cur, ben, meth, acc)
        all_data.append(rec)

    random.shuffle(all_data)

    split = int(len(all_data) * (1 - DEV_RATIO))

    train_data = all_data[:split]
    dev_data = all_data[split:]

    with open("train.jsonl", "w", encoding="utf-8") as f:
        for r in train_data:
            f.write(json.dumps(r) + "\n")

    with open("dev.jsonl", "w", encoding="utf-8") as f:
        for r in dev_data:
            f.write(json.dumps(r) + "\n")

    print("✔ train.jsonl generated:", len(train_data))
    print("✔ dev.jsonl generated:", len(dev_data))


# Run script
generate()