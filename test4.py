import random
import json
from faker import Faker

fake = Faker()
random.seed(42)

TOTAL_SAMPLES = 7000
TRAIN_RATIO = 0.80

# -------------------------------------------
# PAYMENT METHODS
# -------------------------------------------
REAL_METHODS = [
    "NEFT", "RTGS", "IMPS", "UPI", "SWIFT", "FPS",
    "SEPA", "SEPA INSTANT", "ACH", "PAYNOW", "PIX",
    "FEDNOW", "BACS", "CHAPS", "ZELLE", "WIRE", "EFT",
    "INSTANTPAY", "DIRECT DEBIT", "BANK TRANSFER",
    "MULTI DEBIT SINGLE CREDIT"
]

SYNTHETIC_METHODS = [
    "FASTPAY-X1", "PAYMODE-X31", "METHOD_ABC_FAST",
    "PAYTYPE-RANDOM", "SPECIAL_TRANSFER_900",
    "QUICKFASTPAY", "SUPER TRANSFER",
    "UNLISTED METHOD", "TESTPAY_123",
    "XPAY", "TRF_METHOD_22"
]

TYPO_METHODS = [
    "fasst payment", "fastar paymnt", "instnt trnsfer",
    "quickpaye", "achh", "uppi", "impps", "swifft"
]

ALL_METHODS = REAL_METHODS + SYNTHETIC_METHODS + TYPO_METHODS

CURRENCIES = ["INR", "USD", "EUR", "GBP", "JPY", "AED", "CAD", "AUD"]

ACCOUNT_PREFIXES = [
    "salary account", "savings account", "primary account",
    "wallet account", "corporate account", "business acct",
    "personal acct", "ACCT", "ACC", "collection account"
]

# Templates with flexible positions of fields
TEMPLATES = [
    "Transfer {amount}{currency_part} to {beneficiary}{method_part}{account_part}.",
    "Send {amount}{currency_part} to {beneficiary}{method_part}{account_part}.",
    "Execute payment of {amount}{currency_part} for {beneficiary}{method_part}{account_part}.",
    "Please pay {beneficiary} {amount}{currency_part}{method_part}{account_part}.",
    "Route {amount}{currency_part} to {beneficiary}{method_part}{account_part}.",
    "{method_upper} transfer of {amount}{currency_part} to {beneficiary}{account_part}.",
    "From {account}, send {amount}{currency_part} to {beneficiary}{method_part}.",
    "Using {method}, send {amount}{currency_part} to {beneficiary}{account_part}.",
    "Pay {beneficiary} {amount}{currency_part}{method_part}{account_part}.",
    "Move {amount}{currency_part} for {beneficiary}{method_part}{account_part}."
]

# ----------------------------------------
# OFFSET FINDER
# ----------------------------------------
def get_offsets(text, value, label):
    start = text.lower().find(value.lower())
    if start == -1:
        return None
    end = start + len(value)
    return [start, end, label]

# ----------------------------------------
# DATA GENERATION
# ----------------------------------------
dataset = []

for _ in range(TOTAL_SAMPLES):

    # Always include amount + beneficiary
    amount = str(random.randint(50, 999999))
    beneficiary = fake.first_name() + " " + fake.last_name()

    # Conditional optional fields
    include_currency = random.random() < 0.70
    include_method = random.random() < 0.70
    include_account = random.random() < 0.80

    currency = random.choice(CURRENCIES) if include_currency else ""
    method = random.choice(ALL_METHODS) if include_method else ""
    account = (
        random.choice(ACCOUNT_PREFIXES) + " " + str(random.randint(100000, 999999))
        if include_account else ""
    )

    # Build dynamic components
    currency_part = f" {currency}" if currency else ""
    method_part = f" via {method}" if method else ""
    account_part = f" from {account}" if account else ""

    method_upper = method.upper() if method else ""

    template = random.choice(TEMPLATES)

    text = template.format(
        amount=amount,
        currency_part=currency_part,
        beneficiary=beneficiary,
        method_part=method_part,
        method=method,
        method_upper=method_upper,
        account_part=account_part,
        account=account
    ).replace("  ", " ").strip()

    # Build entities list
    entities = []

    for value, label in [
        (amount, "amountHint"),
        (currency if include_currency else None, "currencyHint"),
        (beneficiary, "beneficiaryHint"),
        (method if include_method else None, "methodHint"),
        (account if include_account else None, "accountHint")
    ]:
        if value:
            offset = get_offsets(text, value, label)
            if offset:
                entities.append(offset)

    dataset.append({"text": text, "entities": entities})

# ----------------------------------------
# TRAIN / DEV SPLIT
# ----------------------------------------
train_size = int(TOTAL_SAMPLES * TRAIN_RATIO)
train_data = dataset[:train_size]
dev_data = dataset[train_size:]

with open("train.jsonl", "w", encoding="utf8") as f:
    for entry in train_data:
        f.write(json.dumps(entry) + "\n")

with open("dev.jsonl", "w", encoding="utf8") as f:
    for entry in dev_data:
        f.write(json.dumps(entry) + "\n")

print("SUCCESS!")
print(f"train.jsonl → {len(train_data)} samples")
print(f"dev.jsonl → {len(dev_data)} samples")