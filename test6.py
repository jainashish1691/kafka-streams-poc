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
    "{method} send {amount}{curr} {benef}{