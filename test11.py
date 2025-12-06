import random
import json
import re
from faker import Faker

fake = Faker()
random.seed(42)

TOTAL = 7000
TRAIN_RATIO = 0.80

# ==========================================================
# PAYMENT METHODS
# ==========================================================
REAL_METHODS = [
    "NEFT", "RTGS", "IMPS", "UPI", "SWIFT", "FPS", "ACH",
    "SEPA", "SEPA INSTANT", "PAYNOW", "PIX", "FEDNOW",
    "BACS", "CHAPS", "ZELLE", "WIRE", "EFT"
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

NOISE_METHODS = [
    "iftr", "intr", "1ftr", "ift", "ipms", "uft",
    "ivtr", "imt", "tfrx", "xpayr", "uyt", "transx",
    "fastopt", "newmode", "qktrans", "spclpay"
]

RANDOM_METHODS = [
    "fast pay", "quick send", "mobile pay", "express route",
    "instant send", "bank pay", "local transfer",
    "smart pay", "faster payment", "instant route"
]

ALL_METHODS = (
    REAL_METHODS
    + SYN_METHODS
    + TYPO_METHODS
    + NOISE_METHODS
    + RANDOM_METHODS
)

# ==========================================================
# CURRENCIES
# ==========================================================
CURRENCIES = ["INR", "USD", "EUR", "GBP", "JPY", "AED", "CAD", "AUD"]

# ==========================================================
# ACCOUNT PATTERNS (including salary-only)
# ==========================================================
ACCOUNT_PATTERNS = [
    "salary",
    "salary acct",
    "salaryaccount",
    "salary-acct",
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

# ==========================================================
# BENEFICIARY VARIANTS (very diverse)
# ==========================================================
def random_beneficiary():
    b = fake.first_name(), fake.last_name()
    variants = [
        b[0],
        f"{b[0]} {b[1]}",
        f"{b[0][0]}. {b[1]}",
        f"{b[0]}-{b[1]}",
        f"{b[0].upper()} {b[1].lower()}",
        f"{b[0]} {b[1][0]}",
        f"{b[0].lower()} {b[1].upper()}",
        fake.first_name()  # random extra
    ]
    return random.choice(variants)

# ==========================================================
# NOISE WORDS
# ==========================================================
NOISE_WORDS = [
    "pls", "plz", "urgent", "asap", "now", "quick", "fast",
    "bro", "yaar", "immediately", "today", "tomm", "ok",
    "thanks", "kk", "note", "manual", "internal"
]

def maybe_noise(prob=0.3):
    return (" " + random.choice(NOISE_WORDS)) if random.random() < prob else ""

# ==========================================================
# OFFSET HELPERS
# ==========================================================
def find_offset(text, value, label):
    start = text.lower().find(value.lower())
    if start == -1:
        return None
    end = start + len(value)
    return [start, end, label]

# ==========================================================
# BUILD ONE SAMPLE
# ==========================================================
def build_sample():
    amount = str(random.randint(50, 999999))
    beneficiary = random_beneficiary()

    # Balanced: methods appear in only 30% of samples
    include_method = random.random() < 0.30
    include_currency = random.random() < 0.65
    include_account = random.random() < 0.75

    currency = random.choice(CURRENCIES) if include_currency else ""
    method = random.choice(ALL_METHODS) if include_method else ""
    account = random_account() if include_account else ""

    # ---------- Amount + Currency ---------------------------------
    if include_currency:
        amount_curr = (
            f"{amount} {currency}" if random.random() < 0.5 else f"{currency} {amount}"
        )
    else:
        amount_curr = amount

    # ---------- Method Phrases (NO bare methods now) ---------------
    method_phrases = []
    if include_method:
        prep = random.choice(["via", "using", "by", "through", "with"])
        method_phrases.append(f"{prep} {method}")

    # ---------- Account Phrases -----------------------------------
    account_phrases = []
    if include_account:
        account_phrases = [
            f"from {account}",
            f"to be debited from {account}",
            f"using {account}",
            account
        ]

    method_phrase = random.choice(method_phrases) if method_phrases else ""
    account_phrase = random.choice(account_phrases) if account_phrases else ""

    # ---------- Sentence Generation Modes --------------------------
    mode = random.randint(1, 4)

    if mode == 1:
        text = f"Please transfer {amount_curr} to {beneficiary}"
        if method_phrase: text += f" {method_phrase}"
        if account_phrase: text += f" {account_phrase}"
        text += maybe_noise() + "."

    elif mode == 2:
        text = f"{beneficiary} {amount_curr} send"
        if method_phrase: text += f" {method_phrase}"
        if account_phrase: text += f" {account_phrase}"
        text += maybe_noise()

    elif mode == 3:
        parts = [
            f"pay {amount_curr} to {beneficiary}",
            method_phrase,
            account_phrase,
            maybe_noise(),
        ]
        text = " ".join([p for p in parts if p]).strip()

    else:
        parts = [
            beneficiary,
            amount_curr,
            method_phrase,
            account_phrase,
            maybe_noise(),
            maybe_noise()
        ]
        random.shuffle(parts)
        text = " ".join([p for p in parts if p]).strip()

    # ---------- Normalize ------------------------------------------
    text = re.sub(r"\s+", " ", text).strip()

    # ---------- Random casing ---------------------------------------
    case_mode = random.randint(0, 3)
    if case_mode == 1: text = text.lower()
    elif case_mode == 2: text = text.upper()
    elif case_mode == 3: text = text.title()

    # ---------- Entity Offsets -------------------------------------
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
            if off: entities.append(off)

    return {"text": text, "entities": entities}

# ==========================================================
# GENERATE FULL DATASET
# ==========================================================
dataset = [build_sample() for _ in range(TOTAL)]
random.shuffle(dataset)

split = int(TOTAL * TRAIN_RATIO)
train_data = dataset[:split]
dev_data = dataset[split:]

with open("train.jsonl", "w", encoding="utf8") as f:
    for r in train_data:
        f.write(json.dumps(r) + "\n")

with open("dev.jsonl", "w", encoding="utf8") as f:
    for r in dev_data:
        f.write(json.dumps(r) + "\n")

print("Generated train.jsonl:", len(train_data))
print("Generated dev.jsonl  :", len(dev_data))