"""The secret check on everything an agent sends: message bodies and attachments.

"No secrets on the bus" used to rest on the agents' discipline alone. A
message, once written, sits in the hub's log and in every party's mirror
for good, so the check runs where the mistake is cheapest to stop: in
`Bus.send` before anything leaves the machine, and again on the hub, which
does not trust a client to have run it.

A finding names the rule and the line, never the matched text: the refusal
goes back to the model and into logs, and must not repeat the secret.

The rules aim at things that are secrets by their shape (key prefixes, PEM
blocks, JWTs) or by their name (`PASSWORD=...` in an .env). A bare
40-character base64 string is not one of them: hashes and ids in code under
review look the same, so the AWS secret key rule needs its name next to it.
"""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class Rule:
    name: str
    pattern: re.Pattern[str]


def _rule(name: str, pattern: str, flags: int = 0) -> Rule:
    return Rule(name, re.compile(pattern, flags))


# Values that are obviously placeholders, not secrets: `PASSWORD=<your password>`,
# `TOKEN=${TOKEN}`, `KEY=xxxx`, `secret: changeme`, `{{ vault_password }}`.
_PLACEHOLDER = re.compile(
    r"<[^>]*>|\$\{?[A-Za-z_][A-Za-z0-9_]*\}?|\{\{.*\}\}|%\(?[A-Za-z_]+\)?s?|[x*.#-]{3,}"
    r"|(?:your|my)[_-].*|.*(?:changeme|change_me|example|placeholder|redacted|dummy|sample).*",
    re.IGNORECASE,
)
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z_]*")
_REFERENCE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*(?:\(.*\)|\[.*\])?")
_DOTENV = re.compile(r"^\s*(?:export\s+)?[A-Z][A-Z0-9_]*=\S")

RULES: tuple[Rule, ...] = (
    _rule("agent-bus hub token", r"\bab_[A-Za-z0-9_-]{40,}"),
    _rule("AWS access key id", r"\b(?:AKIA|ASIA|ABIA|ACCA)[A-Z0-9]{16}\b"),
    _rule(
        "AWS secret access key",
        r"aws_?secret_?(?:access_?)?key\s*[\"']?\s*[:=]\s*[\"']?[A-Za-z0-9/+]{40}(?![A-Za-z0-9/+])",
        re.IGNORECASE,
    ),
    _rule("private key", r"-----BEGIN (?:[A-Z0-9]+ )*PRIVATE KEY(?: BLOCK)?-----"),
    _rule("GitHub token", r"\b(?:gh[pousr]_[A-Za-z0-9]{36,}|github_pat_[A-Za-z0-9_]{50,})"),
    _rule("GitLab token", r"\bglpat-[A-Za-z0-9_-]{20,}"),
    _rule("Slack token", r"\bxox[abposr]-[A-Za-z0-9-]{10,}"),
    _rule("Anthropic API key", r"\bsk-ant-[A-Za-z0-9_-]{20,}"),
    _rule("OpenAI API key", r"\bsk-(?:proj-|svcacct-|admin-)?[A-Za-z0-9_-]{32,}"),
    _rule("Google API key", r"\bAIza[0-9A-Za-z_-]{35}\b"),
    _rule("Stripe key", r"\b(?:sk|rk)_live_[A-Za-z0-9]{20,}"),
    _rule("JSON web token", r"\beyJ[A-Za-z0-9_-]{8,}\.eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}"),
    _rule("bearer token", r"\bBearer\s+[A-Za-z0-9._~+/-]{24,}=*", re.IGNORECASE),
    _rule("password in a URL", r"\b[a-z][a-z0-9+.-]*://[^\s:/@]+:(?![$<{%*])[^\s@/]{3,}@[^\s/]+"),
)

# `NAME=value` with a secret-sounding NAME: the shape of an .env file, an
# export, a YAML or JSON config.
_ASSIGNMENT = re.compile(
    r"""(?:^|[\s{,(])(?:export\s+)?["']?
    (?P<name>[A-Za-z0-9_.-]*(?:PASSWORD|PASSWD|PASSPHRASE|SECRET|TOKEN|API_?KEY|ACCESS_?KEY
        |PRIVATE_?KEY|CLIENT_?SECRET|CREDENTIALS?)[A-Za-z0-9_.-]*)
    ["']?\s*[:=]\s*(?P<value>"[^"\n]*"|'[^'\n]*'|[^\s,;}#]+)""",
    re.IGNORECASE | re.VERBOSE,
)
MIN_SECRET_VALUE = 8
"""Shorter values are flags and examples (`TOKEN=1`), not secrets."""


@dataclass(frozen=True)
class Finding:
    rule: str
    line: int
    where: str = "body"

    def __str__(self) -> str:
        return f"{self.where} line {self.line}: {self.rule}"


def refusal(findings: list[Finding]) -> str:
    """What the sender is told: which rule matched where, and what to do; never the match."""
    listed = "; ".join(str(f) for f in findings[:10])
    if len(findings) > 10:
        listed += f"; and {len(findings) - 10} more"
    return (
        f"refused: the message looks like it contains a secret ({listed}). Nothing was "
        "sent. Remove the secret and send again; refer to it by name or location instead "
        "(e.g. 'the token in ~/.config/x'). Tell your user if it was a real secret. If it "
        "is a false alarm, replace the value on that line with a placeholder like <redacted>."
    )


def _secret_value(raw: str, line: str) -> bool:
    """Whether the value of a secret-named assignment looks like an actual secret.

    In an .env line (`NAME=value`, no spaces) any real-looking value is one.
    Elsewhere the line is likely code, where `password=password` passes a
    variable, `token = os.environ["TOKEN"]` reads one and `PASSWORD_FIELD =
    "password_hash"` names one. There a value made of letters and
    underscores alone does not count, nor does a bare reference; a value
    with a digit or a symbol does. Paths and text with spaces never count.
    """
    quoted = raw[:1] in "\"'"
    value = raw.strip("\"'")
    if (
        len(value) < MIN_SECRET_VALUE
        or _PLACEHOLDER.fullmatch(value)
        or any(c.isspace() for c in value)
        or value.startswith(("/", "~/", "./", "../"))
    ):
        return False
    if _DOTENV.match(line):
        return True
    if _IDENTIFIER.fullmatch(value):
        return False
    if quoted:
        return True
    if any(c in value for c in "()[]{}<>`"):
        return False  # code: a call, an index, a type, markup
    if _REFERENCE.fullmatch(value) and any(c in value for c in ".(["):
        return False
    return any(not c.isalpha() for c in value)


def scan(text: str, where: str = "body") -> list[Finding]:
    """Every line of `text` that looks like it holds a secret, one finding per rule and line."""
    findings: list[Finding] = []
    for number, line in enumerate(text.splitlines(), 1):
        seen: set[str] = set()
        for rule in RULES:
            if rule.name not in seen and rule.pattern.search(line):
                seen.add(rule.name)
                findings.append(Finding(rule.name, number, where))
        for match in _ASSIGNMENT.finditer(line):
            # One finding per line is enough when a rule above already named it.
            if not seen and _secret_value(match.group("value"), line):
                seen.add("assignment")
                findings.append(
                    Finding(f"a value assigned to {match.group('name')!r}", number, where)
                )
    return findings
