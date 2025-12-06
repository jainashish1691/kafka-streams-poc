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