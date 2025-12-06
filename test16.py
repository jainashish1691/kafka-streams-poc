import random
import json
import re

# ------------------------------
# CONFIG
# ------------------------------
TOTAL_SAMPLES = 10000     # change to 7000 / 15000 / etc if you want
DEV_RATIO = 0.2           # 20% dev set

random.seed(42)

BENEFICIARY_FIRST = [
    "rahul","john","amit","anita","rohan","rohit","alex","sara","arjun","maria",
    "sunil","kapil","kiran","deepa","vijay","naina","sameer","farhan","priya",
    "raj","neha","yash","sabby","sanjay","aditya","manish","pankaj"
]

BENEFICIARY_LAST = [
    "sharma","patel","iyer","khan","verma","gupta","reddy","singh","jain","kapoor"
]

REAL_METHODS = [
    "upi","neft","imps","rtgs","swift","sepa","eft",
    "faster payment","bank transfer","wire transfer","insta pay","fps","ach"
]

NOISE_METHODS = [
    "iftr","intr","1ftr","ift","ipms","uft","ivtr","imt",
    "trx99","qpx1","fxp12","xpayr","rptx","uqmt","mode9","pxline"
]

ACCOUNT_PATTERNS = [
    "salary account","sun account","main account","home account",
    "office account","user account","primary account","current account",
    "family account","business account","wallet account","bonus account",
    "joint account","salary","savings account","payroll account"
]

CURRENCIES = ["usd","inr","eur","gbp","aed","cad","aud"]

VERBS = [
    "pay","send","transfer","dispatch","give","process",
    "initiate payment of","please transfer","kindly send","credit"
]

PREPOSITIONS = ["via","using","through","by","with"]

NOISE_TOKENS = [
    "pls","plz","yaar","bro","urgent","asap","now","today","tomm",
    "ok","thanks","kk","ref","txn","id","check","manual","internal",
    "fast","jaldi","boss","note","info"
]

EMOJIS = ["🙂","😀","🙏","👍","🔥","🚀","💸","✅","❗"]

PUNCT = [".", "!", "!!", "...", "?", "??", ""]


# ------------------------------
# HELPERS
# ------------------------------

def rand_amount():
    # sometimes clean, sometimes with decimal
    if random.random() < 0.2:
        return f"{random.randint(10, 9999)}.{random.randint(0,99)}"
    return str(random.randint(10, 99999))

def rand_currency():
    return random.choice(CURRENCIES)

def rand_beneficiary():
    r = random.random()
    fn = random.choice(BENEFICIARY_FIRST)
    ln = random.choice(BENEFICIARY_LAST)
    if r < 0.3:
        return fn + " " + ln
    elif r < 0.6:
        return fn
    elif r < 0.8:
        return f"{fn[0]}. {ln}"
    else:
        return f"{fn}-{ln}"

def rand_method():
    return random.choice(REAL_METHODS) if random.random() < 0.65 else random.choice(NOISE_METHODS)

def rand_account():
    base = random.choice(ACCOUNT_PATTERNS)
    if random.random() < 0.5:
        # with number
        return f"{base} {random.randint(10000,99999)}"
    return base

def maybe_noise_token():
    return random.choice(NOISE_TOKENS) if random.random() < 0.35 else ""

def maybe_emoji():
    return random.choice(EMOJIS) if random.random() < 0.2 else ""

def find_offsets(text, substring):
    idx = text.lower().find(substring.lower())
    if idx == -1:
        return None
    return idx, idx + len(substring)


# ------------------------------
# SENTENCE GENERATION (EXTREME)
# ------------------------------

def generate_sentence():
    # presence probabilities (amount & beneficiary almost always there)
    amount = rand_amount() if random.random() < 0.95 else None
    currency = rand_currency() if random.random() < 0.35 else None
    beneficiary = rand_beneficiary() if random.random() < 0.9 else None
    method = rand_method() if random.random() < 0.7 else None
    account = rand_account() if random.random() < 0.65 else None

    verb = random.choice(VERBS)

    # Build semantic chunks (without order)
    chunks = []

    if amount and currency:
        if random.random() < 0.5:
            chunks.append(f"{amount} {currency}")
        else:
            chunks.append(f"{currency} {amount}")
    elif amount:
        chunks.append(amount)

    if beneficiary:
        # sometimes add word "to"
        if random.random() < 0.5:
            chunks.append(f"to {beneficiary}")
        else:
            chunks.append(beneficiary)

    if method:
        prep = random.choice(PREPOSITIONS)
        chunks.append(f"{prep} {method}")

    if account:
        if random.random() < 0.6:
            chunks.append(f"from {account}")
        else:
            chunks.append(account)

    # add verb as a separate chunk
    chunks.append(verb)

    # add random noise chunks
    for _ in range(random.randint(1, 4)):
        t = maybe_noise_token()
        if t:
            chunks.append(t)
    # maybe add a fake ref id / random number (noise)
    if random.random() < 0.5:
        chunks.append(f"ref{random.randint(1000,9999)}")

    # Shuffle chunks to create extreme variability
    random.shuffle(chunks)

    sentence = " ".join(chunks).strip()

    # add some punctuation & emojis at start/end
    if random.random() < 0.3:
        sentence = maybe_emoji() + " " + sentence
    punct = random.choice(PUNCT)
    if punct:
        sentence = sentence + punct
    if random.random() < 0.2:
        sentence = sentence + " " + maybe_emoji()

    # normalize spaces
    sentence = re.sub(r"\s+", " ", sentence).strip()

    # apply random casing to full sentence AND keep entity strings consistent
    case_mode = random.randint(0, 3)
    def apply_case(s):
        if case_mode == 1:
            return s.lower()
        elif case_mode == 2:
            return s.upper()
        elif case_mode == 3:
            return s.title()
        else:
            return s

    sentence = apply_case(sentence)

    # Also transform entity strings according to same rule, so offsets still match
    if amount: amount = apply_case(amount)
    if currency: currency = apply_case(currency)
    if beneficiary: beneficiary = apply_case(beneficiary)
    if method: method = apply_case(method)
    if account: account = apply_case(account)

    return sentence, amount, currency, beneficiary, method, account


# ------------------------------
# BUILD RECORD
# ------------------------------

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


# ------------------------------
# MAIN GENERATOR
# ------------------------------

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


if __name__ == "__main__":
    generate()