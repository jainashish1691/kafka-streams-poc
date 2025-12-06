import random
import json
from faker import Faker
fake = Faker()
random.seed(42)

TOTAL_SAMPLES = 7000
TRAIN_RATIO = 0.80

# -------------------------------------------
# ENHANCED PAYMENT METHODS (extensive list)
# -------------------------------------------
REAL_METHODS = [
    "NEFT", "RTGS", "IMPS", "UPI", "SWIFT", "FPS", "SEPA", "SEPA INSTANT",
    "ACH", "PAYNOW", "PIX", "FEDNOW", "BACS", "CHAPS", "ZELLE", "WIRE",
    "EFT", "INSTANTPAY", "DIRECT DEBIT", "BANK TRANSFER",
    "MULTI DEBIT SINGLE CREDIT"
]

SYNTHETIC_METHODS = [
    "FASTPAY-X1", "PAYMODE-X31", "METHOD_ABC_FAST", "RANDOMMETHOD99",
    "PAYTYPE-RANDOM", "SPECIAL_TRANSFER_900", "PAYMODE QUICKFAST",
    "ULTRAFASTPAY", "XPAY", "TRF_METHOD_22", "QKPAY", "ZPAY",
    "METHOD UNKNOWN", "UNLISTED METHOD", "NEWPAY 3000",
    "SUPER TRANSFER", "PAYCHANNEL_55", "TESTPAY_123"
]

TYPO_METHODS = [
    "fasst payment", "fastar paymnt", "instnt trnsfer", "quickpaye",
    "achh", "uppi", "impps", "swifft"
]

ALL_METHODS = REAL_METHODS + SYNTHETIC_METHODS + TYPO_METHODS

CURRENCIES = ["INR", "USD", "EUR", "GBP", "JPY", "AED", "CAD", "AUD"]

ACCOUNT_PREFIXES = [
    "salary account", "savings account", "primary account", "wallet account",
    "corporate account", "business acct", "personal acct", "ACCT", "ACC",
    "expense account", "collection account"
]

# -------------------------------------------------------
# MANY TEMPLATES (method anywhere in the sentence)
# -------------------------------------------------------
TEMPLATES = [
    "Transfer {amount} {currency} to {beneficiary} via {method} from {account}.",
    "Send {amount} {currency} to {beneficiary} using {method} from {account}.",
    "Use {method} to send {amount} {currency} to {beneficiary} from {account}.",
    "Execute payment of {amount} {currency} for {beneficiary} through {method} from {account}.",
    "Please pay {beneficiary} {amount} {currency} using {method} debited from {account}.",
    "Route {amount} {currency} to {beneficiary} using {method} linked to {account}.",
    "Make a payment of {amount} {currency} to {beneficiary} by {method} using {account}.",
    "Kindly transfer {amount} {currency} through {method} for {beneficiary} from {account}.",
    "Credit {beneficiary} with {amount} {currency} via {method} from {account}.",
    "Authorize {method} transfer of {amount} {currency} to {beneficiary} from {account}.",
    "{method} should be used to move {amount} {currency} to {beneficiary} from {account}.",
    "For {beneficiary}, send {amount} {currency} via {method} from {account}.",
    "Pay {amount} {currency} to {beneficiary} from {account} through {method}.",
    "Use {account} to send {amount} {currency} to {beneficiary} by {method}.",
    "Send {beneficiary} {amount} {currency} through {method} from {account}.",
    "{beneficiary} should receive {amount} {currency} via {method} from {account}.",
    "Please process {amount} {currency} using {method} for {beneficiary} debiting {account}.",
    "{method} payment of {amount} {currency} should be issued to {beneficiary} from {account}.",
    "From {account}, pay {beneficiary} {amount} {currency} through {method}.",
    "Using {method}, transfer {amount} {currency} from {account} to {beneficiary}."
]

# ----------------------------------------
# OFFSET FUNCTION
# ----------------------------------------
def get_offsets(text, value, label):
    start = text.lower().find(value.lower())
    if start == -1:
        return None
    end = start + len(value)
    return [start, end, label]

# ----------------------------------------
# GENERATE DATA
# ----------------------------------------
dataset = []

for _ in range(TOTAL_SAMPLES):
    amount = str(random.randint(50, 999999))
    currency = random.choice(CURRENCIES)

    # random names
    beneficiary = fake.first_name() + " " + fake.last_name()

    # payment method (real + synthetic + typo)
    method = random.choice(ALL_METHODS)

    # account variations
    account = random.choice(ACCOUNT_PREFIXES) + " " + str(random.randint(100000, 999999))

    text = random.choice(TEMPLATES).format(
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

# ----------------------------------------
# SPLIT TO TRAIN / DEV
# ----------------------------------------
train_size = int(TOTAL_SAMPLES * TRAIN_RATIO)
train_data = dataset[:train_size]
dev_data = dataset[train_size:]

# ----------------------------------------
# WRITE JSONL
# ----------------------------------------
with open("train.jsonl", "w", encoding="utf8") as f:
    for entry in train_data:
        f.write(json.dumps(entry) + "\n")

with open("dev.jsonl", "w", encoding="utf8") as f:
    for entry in dev_data:
        f.write(json.dumps(entry) + "\n")

print("DONE! Generated:")
print(f"train.jsonl → {len(train_data)} samples")
print(f"dev.jsonl   → {len(dev_data)} samples")