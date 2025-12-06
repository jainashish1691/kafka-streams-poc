import random
import json
import re
from faker import Faker

fake = Faker()
random.seed(42)

TOTAL = 7000
TRAIN_RATIO = 0.80

# ----------------------------------------------
# PAYMENT METHODS — REAL + SYNTHETIC + CHAOS
# ----------------------------------------------
REAL_METHODS = [
    "NEFT", "RTGS", "IMPS", "UPI", "SWIFT", "FPS", "ACH",
    "SEPA", "SEPA INSTANT", "PAYNOW", "PIX", "FEDNOW",
    "BACS", "CHAPS", "ZELLE", "WIRE", "EFT",
    "INSTANTPAY", "DIRECT DEBIT", "BANK TRANSFER"
]

SYN_METHODS = [
    "FASTPAY-X1", "PAYMODE-X31", "METHOD_ABC_FAST",
    "PAYTYPE-RANDOM", "SUPER TRANSFER", "XPAY",
    "TRF_METHOD_22", "PAYMODE QUICKFAST"
]

TYPO_METHODS = [
    "nefft", "impps", "upii", "swifft", "achh",
    "faster paymnt", "instnt trnsfer", "quikpay"
]

RANDOM_METHODS = [
    "fast pay", "quick send", "mobile pay", "express route",
    "instant send", "bank pay", "local transfer", "smart pay"
]

ALL_METHODS = REAL_METHODS + SYN_METHODS + TYPO_METHODS + RANDOM_METHODS

# ----------------------------------------------
# CURRENCIES
# ----------------------------------------------
CURRENCIES = ["INR", "USD", "EUR", "GBP", "JPY", "AED", "CAD", "AUD"]

# ----------------------------------------------
# ACCOUNT VARIATIONS
# ----------------------------------------------
ACCOUNT_PATTERNS = [
    "salary account {num}",
    "savings acc {num}",
    "primary acct {num}",
    "wallet account {num}",
    "corporate account {num}",
    "ACC{num}",
    "ACCT-{num}",
    "account {num}",
    "acct no {num}",
    "useracct{num}",
    "collection account {num}"
]

def random_account():
    num = str(random.randint(100000, 999999))
    pattern = random.choice(ACCOUNT_PATTERNS)
    return pattern.format(num=num)

# ----------------------------------------------
# BENEFICIARY NAME DIVERSITY
# ----------------------------------------------
def random_beneficiary():
    patterns = [
        fake.first_name() + " " + fake.last_name(),
        fake.first_name(),
        fake.first_name()[0] + ". " + fake.last_name(),
        fake.first_name() + "-" + fake.last_name(),
        fake.first_name().upper() + " " + fake.last_name().lower(),
        fake.first_name() + " " + fake.last_name()[0]
    ]
    return random.choice(patterns)

# ----------------------------------------------
# NOISE GENERATOR (typos, slang, extra tokens)
# ----------------------------------------------
NOISE = ["pls", "plz", "urgent", "now", "quick", "bro", "ok", "fast", "asap", "immediately"]

def maybe_noise():
    return (" " + random.choice(NOISE)) if random.random() < 0.25 else ""

def typo(text):
    if random.random() < 0.15:
        i = random.randint(0, len(text) - 2)
        return text[:i] + text[i+1] + text[i] + text[i+2:]
    return text

# ----------------------------------------------
# TEMPLATES (40+ patterns)
# ----------------------------------------------
TEMPLATES = [
    "Transfer {amount}{curr} to {benef}{meth}{acct}{noise}.",
    "Send {amount}{curr} to {benef}{meth}{acct}{noise}.",
    "Pay {benef} {amount}{curr}{meth}{acct}{noise}.",
    "Please transfer {amount}{curr} for {benef}{meth}{acct}{noise}.",
    "Move {amount}{curr} to {benef}{meth}{acct}{noise}.",
    "Using {method}, send {amount}{curr} to {benef}{acct}{noise}.",
    "Route {amount}{curr} for {benef}{meth}{acct}{noise}.",
    "{method_upper} transfer {amount}{curr} to {benef}{acct}{noise}.",
    "For {benef}, pay {amount}{curr}{meth}{acct}{noise}.",
    "{benef} should receive {amount}{curr}{meth}{acct}{noise}.",
    "Debit {acct} and send {amount}{curr} to {benef}{meth}{noise}.",
    "From {acct}, send {amount}{curr} to {benef}{meth}{noise}.",
    "Issue payment of {amount}{curr} to {benef}{meth}{acct}{noise}.",
    "Send money {amount}{curr} to {benef}{meth}{noise}.",
    "Kindly process {amount}{curr} to {benef}{meth}{acct}{noise}.",

    # Chaotic free-form users
    "{benef} {amount}{curr} via {method}{acct}{noise}",
    "{amount}{curr} {benef} {method}{acct}{noise}",
    "pay {amount}{curr} {benef}{noise}",
    "send {benef} {amount}{curr}{noise}",
    "{amount} to {benef}{noise}",
    "{method} send {amount}{curr} {benef}{acct}{noise}",
    "{benef} get {amount}{curr} using {method}{noise}",
]

# ----------------------------------------------
# OFFSET HELPER
# ----------------------------------------------
def find_offset(text, value, label):
    if not value:
        return None
    idx = text.lower().find(value.lower())
    if idx == -1:
        return None
    return [idx, idx + len(value), label]

# ----------------------------------------------
# GENERATE DATASET
# ----------------------------------------------
dataset = []

for _ in range(TOTAL):

    amount = str(random.randint(50, 999999))
    beneficiary = random_beneficiary()

    include_currency = random.random() < 0.70
    include_method = random.random() < 0.70
    include_account = random.random() < 0.80

    currency = random.choice(CURRENCIES) if include_currency else ""
    method = random.choice(ALL_METHODS) if include_method else ""
    account = random_account() if include_account else ""

    curr = f" {currency}" if currency else ""
    meth = f" via {method}" if method else ""
    acct = f" from {account}" if account else ""
    noise = maybe_noise()

    template = random.choice(TEMPLATES)

    text = template.format(
        amount=amount,
        curr=curr,
        benef=beneficiary,
        meth=meth,
        method=method,
        method_upper=method.upper() if method else "",
        acct=acct,
        account=account,
        noise=noise
    )

    text = re.sub(r"\s+", " ", text).strip()

    # Add random typo to text
    text = typo(text)

    entities = []

    for value, label in [
        (amount, "amountHint"),
        (currency if include_currency else None, "currencyHint"),
        (beneficiary, "beneficiaryHint"),
        (method if include_method else None, "methodHint"),
        (account if include_account else None, "accountHint")
    ]:
        off = find_offset(text, value, label)
        if off:
            entities.append(off)

    dataset.append({"text": text, "entities": entities})

# SHUFFLE BEFORE SPLIT (very important)
random.shuffle(dataset)

# ----------------------------------------------
# WRITE train.jsonl / dev.jsonl
# ----------------------------------------------
split = int(TOTAL * TRAIN_RATIO)

with open("train.jsonl", "w") as f:
    for d in dataset[:split]:
        f.write(json.dumps(d) + "\n")

with open("dev.jsonl", "w") as f:
    for d in dataset[split:]:
        f.write(json.dumps(d) + "\n")

print("Generated train.jsonl:", split)
print("Generated dev.jsonl:", TOTAL - split)