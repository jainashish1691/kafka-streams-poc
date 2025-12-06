import random
import json
import re
from faker import Faker

fake = Faker()
random.seed(42)

TOTAL = 7000
TRAIN_RATIO = 0.80

# -----------------------------
# PAYMENT METHODS (very broad)
# -----------------------------
REAL_METHODS = [
    "NEFT", "RTGS", "IMPS", "UPI", "SWIFT", "FPS", "ACH",
    "SEPA", "SEPA INSTANT", "PAYNOW", "PIX", "FEDNOW",
    "BACS", "CHAPS", "ZELLE", "WIRE", "EFT",
    "INSTANTPAY", "DIRECT DEBIT", "BANK TRANSFER"
]

SYN_METHODS = [
    "FASTPAY-X1", "PAYMODE-X31", "METHOD_ABC_FAST",
    "PAYTYPE-RANDOM", "SUPER TRANSFER", "XPAY",
    "TRF_METHOD_22", "PAYMODE QUICKFAST", "NEWPAY_9000",
]

TYPO_METHODS = [
    "nefft", "impps", "upii", "swifft", "achh",
    "faster paymnt", "instnt trnsfer", "quikpay"
]

RANDOM_METHODS = [
    "fast pay", "quick send", "mobile pay", "express route",
    "instant send", "bank pay", "local transfer", "smart pay",
    "faster payment", "instant route"
]

ALL_METHODS = REAL_METHODS + SYN_METHODS + TYPO_METHODS + RANDOM_METHODS

# -----------------------------
# CURRENCIES
# -----------------------------
CURRENCIES = ["INR", "USD", "EUR", "GBP", "JPY", "AED", "CAD", "AUD"]

# -----------------------------
# ACCOUNTS (many formats)
# -----------------------------
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
    "collection account {num}",
    "acc{num}",
    "my_savings_{num}",
]

def random_account():
    num = str(random.randint(100000, 999999))
    return random.choice(ACCOUNT_PATTERNS).format(num=num)

# -----------------------------
# BENEFICIARY VARIANTS
# -----------------------------
def random_beneficiary():
    options = [
        fake.first_name() + " " + fake.last_name(),
        fake.first_name(),
        fake.first_name()[0] + ". " + fake.last_name(),
        fake.first_name() + "-" + fake.last_name(),
        fake.first_name().upper() + " " + fake.last_name().lower(),
        fake.first_name() + " " + fake.last_name()[0],
    ]
    return random.choice(options)

# -----------------------------
# NOISE WORDS (slang, filler)
# -----------------------------
NOISE_WORDS = [
    "pls", "plz", "urgent", "asap", "now", "quick", "fast", "bro",
    "yaar", "immediately", "today", "tomm", "ok", "thanks", "kk",
    "note", "manual", "internal", "salary", "bonus"
]

def maybe_noise(prob=0.3):
    return (" " + random.choice(NOISE_WORDS)) if random.random() < prob else ""

# -----------------------------
# OFFSET HELPER
# -----------------------------
def find_offset(text, value, label):
    if not value:
        return None
    start = text.lower().find(value.lower())
    if start == -1:
        return None
    end = start + len(value)
    return [start, end, label]

# -----------------------------
# BUILD ONE CHAOTIC SENTENCE
# -----------------------------
def build_sample():
    # Always present
    amount = str(random.randint(50, 999999))
    beneficiary = random_beneficiary()

    # Optional entities
    include_currency = random.random() < 0.7
    include_method   = random.random() < 0.7
    include_account  = random.random() < 0.8

    currency = random.choice(CURRENCIES) if include_currency else ""
    method   = random.choice(ALL_METHODS) if include_method else ""
    account  = random_account() if include_account else ""

    # Parts as phrases
    amount_part = amount
    if include_currency:
        # sometimes "5000 INR", sometimes "INR 5000"
        if random.random() < 0.5:
            amount_curr = f"{amount} {currency}"
        else:
            amount_curr = f"{currency} {amount}"
    else:
        amount_curr = amount

    method_phrases = []
    if include_method:
        method_phrases = [
            f"via {method}",
            f"using {method}",
            f"by {method}",
            f"through {method}",
            method,  # just bare method
            f"with {method} transfer"
        ]

    account_phrases = []
    if include_account:
        account_phrases = [
            f"from {account}",
            f"to be debited from {account}",
            f"using {account}",
            f"from acct {account}",
            account
        ]

    # Pick actual phrases
    curr_phrase    = amount_curr
    method_phrase  = random.choice(method_phrases) if method_phrases else ""
    account_phrase = random.choice(account_phrases) if account_phrases else ""

    # Modes: formal, semi-structured, chaotic bags of words
    mode = random.randint(1, 4)

    if mode == 1:
        # Formal-ish
        text = f"Please transfer {curr_phrase} to {beneficiary}"
        if method_phrase:
            text += f" {method_phrase}"
        if account_phrase:
            text += f" {account_phrase}"
        text += maybe_noise()
        text += "."
    elif mode == 2:
        # Chatty
        text = f"{beneficiary} {amount_curr} {maybe_noise()} send"
        if method_phrase:
            text += f" {method_phrase}"
        if account_phrase:
            text += f" {account_phrase}"
        text += maybe_noise()
    elif mode == 3:
        # Chaotic: random ordering
        parts = [
            f"{beneficiary}",
            f"{amount_curr}",
            method_phrase,
            account_phrase,
            maybe_noise(),
            maybe_noise()
        ]
        # shuffle and join
        random.shuffle(parts)
        text = " ".join([p for p in parts if p]).strip()
    else:
        # Very loose, like chat / speech-to-text
        text = ""
        order = ["amount", "benef", "method", "account"]
        random.shuffle(order)
        for item in order:
            if item == "amount":
                text += f"{amount_curr}{maybe_noise()} "
            elif item == "benef":
                text += f"{beneficiary}{maybe_noise()} "
            elif item == "method" and method_phrase:
                # sometimes drop preposition: only method token
                if random.random() < 0.5:
                    text += f"{method}{maybe_noise()} "
                else:
                    text += f"{method_phrase}{maybe_noise()} "
            elif item == "account" and account_phrase:
                text += f"{account_phrase}{maybe_noise()} "
        text = text.strip()

    # Normalize spaces
    text = re.sub(r"\s+", " ", text).strip()

    # Random casing: lower, upper, title, or original
    case_mode = random.randint(0, 3)
    if case_mode == 1:
        text_for_offsets = text.lower()
    elif case_mode == 2:
        text_for_offsets = text.upper()
    elif case_mode == 3:
        text_for_offsets = text.title()
    else:
        text_for_offsets = text

    # BUT: offsets computed vs lowercase both, so we can just use the displayed text
    text = text_for_offsets

    # Collect entities
    entities = []
    for value, label in [
        (amount, "amountHint"),
        (currency if include_currency else None, "currencyHint"),
        (beneficiary, "beneficiaryHint"),
        (method if include_method else None, "methodHint"),
        (account if include_account else None, "accountHint"),
    ]:
        if value:
            off = find_offset(text, value, label)
            if off:
                entities.append(off)

    return {"text": text, "entities": entities}

# -----------------------------
# BUILD FULL DATASET
# -----------------------------
dataset = [build_sample() for _ in range(TOTAL)]

# Shuffle BEFORE split (very important)
random.shuffle(dataset)

split = int(TOTAL * TRAIN_RATIO)
train_data = dataset[:split]
dev_data   = dataset[split:]

with open("train.jsonl", "w", encoding="utf8") as f:
    for rec in train_data:
        f.write(json.dumps(rec) + "\n")

with open("dev.jsonl", "w", encoding="utf8") as f:
    for rec in dev_data:
        f.write(json.dumps(rec) + "\n")

print("Generated:")
print("  train.jsonl:", len(train_data))
print("  dev.jsonl  :", len(dev_data))