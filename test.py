import random
import json
from faker import Faker

fake = Faker()
random.seed(42)

# ----------------------------
# CONFIG
# ----------------------------
TOTAL_SAMPLES = 7000
TRAIN_RATIO = 0.80

PAYMENT_METHODS = [
    "NEFT", "RTGS", "IMPS", "UPI", "SWIFT", "FPS",
    "SEPA", "WIRE", "BANK TRANSFER", "MULTI DEBIT SINGLE CREDIT"
]

CURRENCIES = ["INR", "USD", "EUR", "GBP", "JPY", "AED", "CAD", "AUD"]

ACCOUNT_PREFIXES = [
    "salary account", "savings account", "primary account", "wallet account",
    "corporate account", "business acct", "personal acct", "ACCT", "ACC"
]

TEMPLATES = [
    "Transfer {amount} {currency} to {beneficiary} via {method} from {account}.",
    "Send {amount} {currency} to {beneficiary} using {method} from {account}.",
    "Execute payment of {amount} {currency} for {beneficiary} through {method} from {account}.",
    "Please pay {beneficiary} {amount} {currency} using {method} debited from {account}.",
    "Process transfer of {amount} {currency} to {beneficiary} via {method} linked to {account}.",
    "Make a payment of {amount} {currency} to {beneficiary} by {method} using {account}.",
    "Kindly route {amount} {currency} to {beneficiary} through {method} from {account}.",
    "Move {amount} {currency} for {beneficiary} using {method} from {account}.",
    "Credit {beneficiary} with {amount} {currency} via {method} from {account}.",
    "Authorize transfer of {amount} {currency} to {beneficiary} via {method} charging {account}."
]

# ------------------------------------
# FUNCTION TO CREATE ENTITY OFFSETS
# ------------------------------------
def get_offsets(text, value, label):
    """Return (start, end, label) for an entity."""
    start = text.lower().find(value.lower())
    if start == -1:
        return None
    end = start + len(value)
    return [start, end, label]


# ----------------------------
# DATA GENERATION
# ----------------------------
dataset = []

for _ in range(TOTAL_SAMPLES):
    amount = str(random.randint(100, 999999))
    currency = random.choice(CURRENCIES)
    beneficiary = fake.first_name() + " " + fake.last_name()
    method = random.choice(PAYMENT_METHODS)
    account = random.choice(ACCOUNT_PREFIXES) + " " + str(random.randint(100000, 999999))

    template = random.choice(TEMPLATES)

    text = template.format(
        amount=amount,
        currency=currency,
        beneficiary=beneficiary,
        method=method,
        account=account
    )

    entities = []

    for value, label in [
        (amount, "amountHint"),
        (currency, "currencyHint"),
        (beneficiary, "beneficiaryHint"),
        (method, "methodHint"),
        (account, "accountHint")
    ]:
        offset = get_offsets(text, value, label)
        if offset:
            entities.append(offset)

    dataset.append({"text": text, "entities": entities})


# ----------------------------
# SPLIT INTO TRAIN / DEV
# ----------------------------
train_size = int(len(dataset) * TRAIN_RATIO)
train_data = dataset[:train_size]
dev_data = dataset[train_size:]

# ----------------------------
# WRITE JSONL FILES
# ----------------------------
with open("train.jsonl", "w", encoding="utf-8") as f:
    for entry in train_data:
        f.write(json.dumps(entry) + "\n")

with open("dev.jsonl", "w", encoding="utf-8") as f:
    for entry in dev_data:
        f.write(json.dumps(entry) + "\n")

print("SUCCESS!")
print(f"Generated {len(train_data)} training samples → train.jsonl")
print(f"Generated {len(dev_data)} dev samples → dev.jsonl")