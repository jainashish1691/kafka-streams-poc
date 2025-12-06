import random
import json
import re

# ===========================================================
# CONFIG
# ===========================================================
TOTAL_SAMPLES = 12000        # produces ~9600 train, ~2400 dev
DEV_RATIO = 0.20
random.seed(42)

# ===========================================================
# DATA POOLS
# ===========================================================
BENEFICIARY_FIRST = [
    "rahul","john","amit","anita","rohan","rohit","alex","sara",
    "arjun","maria","sunil","kapil","kiran","deepa","vijay","naina",
    "sameer","farhan","priya","raj","neha","yash","sabby","sanjay",
    "aditya","manish","pankaj","suresh","tina","varun","vivek"
]

BENEFICIARY_LAST = [
    "sharma","patel","iyer","khan","verma","gupta","reddy",
    "singh","jain","kapoor","joshi","desai","pawar","agarwal"
]

REAL_METHODS = [
    "upi","neft","imps","rtgs","swift","sepa","eft",
    "faster payment","bank transfer","wire transfer","insta pay","fps","ach"
]

NOISE_METHODS = [
    "iftr","intr","1ftr","ift","ipms","uft","ivtr","imt",
    "trx99","qpx1","fxp12","xpayr","rptx","uqmt","mode9","pxline"
]

ACCOUNT_BASE = [
    "salary account","savings account","payroll account","primary account",
    "main account","current account","business account","joint account",
    "wallet account","bonus account","office account","family account",
    "home account","sun account","user account","expense account"
]

ACCOUNT_SUFFIX = [
    "One","Two","Three","Alpha","Beta","Main","Prime","Plus",
    "R1","R2","R3","55001","1100","ZX","Bravo","Gold","Pro"
]

CURRENCIES = ["usd","inr","eur","gbp","aed","cad","aud"]

VERBS = [
    "pay","send","transfer","dispatch","give","process","credit",
    "initiate payment of","please transfer","kindly send","make payment"
]

PREPOSITIONS = ["via","using","through","by","with"]

NOISE_TOKENS = [
    "pls","plz","yaar","bro","urgent","asap","now","today","tomm",
    "ok","thanks","kk","ref","txn","id","check","manual","internal",
    "fast","jaldi","boss","note","info","immediate","required"
]

EMOJIS = ["🙂","😀","🙏","👍","🔥","🚀","💸","✅","❗"]
PUNCT = [".", "!", "!!", "...", "?", "??", ""]

# ===========================================================
# HELPERS
# ===========================================================
def rand_amount():
    if random.random() < 0.25:
        return f"{random.randint(10,9999)}.{random.randint(0,99)}"
    return str(random.randint(10,99999))

def rand_currency():
    return random.choice(CURRENCIES)

def rand_beneficiary():
    fn = random.choice(BENEFICIARY_FIRST)
    ln = random.choice(BENEFICIARY_LAST)
    r = random.random()
    if r < 0.3:
        return fn + " " + ln
    elif r < 0.6:
        return fn
    elif r < 0.8:
        return f"{fn}-{ln}"
    else:
        return f"{fn[0]}. {ln}"

def rand_method():
    return random.choice(REAL_METHODS) if random.random() < 0.65 else random.choice(NOISE_METHODS)

def rand_account():
    base = random.choice(ACCOUNT_BASE)
    r = random.random()
    if r < 0.3:
        return base  # 2 words
    elif r < 0.7:
        return f"{base} {random.choice(ACCOUNT_SUFFIX)}"
    else:
        return f"{base} {random.choice(ACCOUNT_SUFFIX)} {random.choice(ACCOUNT_SUFFIX)}"  # 3–4 tokens

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
    amount = rand_amount() if random.random() < 0.97 else None
    currency = rand_currency() if random.random() < 0.45 else None
    beneficiary = rand_beneficiary() if random.random() < 0.92 else None
    method = rand_method() if random.random() < 0.75 else None
    account = rand_account() if random.random() < 0.75 else None

    verb = random.choice(VERBS)

    chunks = []

    # amount + currency combos
    if amount and currency:
        if random.random() < 0.5:
            chunks.append(f"{amount} {currency}")
        else:
            chunks.append(f"{currency} {amount}")
    elif amount:
        chunks.append(amount)

    if beneficiary:
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

    chunks.append(verb)

    # noise
    for _ in range(random.randint(0,3)):
        t = maybe_noise_token()
        if t:
            chunks.append(t)

    if random.random() < 0.5:
        chunks.append(f"ref{random.randint(1000,9999)}")

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

    # CONSISTENT casing
    mode = random.randint(0, 3)
    def apply_case(s):
        if mode == 1: return s.lower()
        if mode == 2: return s.upper()
        if mode == 3: return s.title()
        return s

    sentence = apply_case(sentence)
    if amount: amount = apply_case(amount)
    if currency: currency = apply_case(currency)
    if beneficiary: beneficiary = apply_case(beneficiary)
    if method: method = apply_case(method)
    if account: account = apply_case(account)

    return sentence, amount, currency, beneficiary, method, account

# ===========================================================
# BUILD RECORD
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
# MAIN GENERATION LOGIC
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
    print("🔥 Multi-word account + advanced noise dataset ready!")

if __name__ == "__main__":
    generate()