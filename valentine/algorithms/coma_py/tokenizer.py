import re


def tokenize(name: str) -> list[str]:
    """
    COMA-style tokenizer that splits identifiers into lowercase tokens.

    Splitting rules:
    1. Split on delimiters: space, dot, underscore, hyphen
    2. Split camelCase/PascalCase on uppercase boundaries
    3. Group consecutive uppercase letters (e.g., "XML" stays together)
    4. Lowercase all tokens
    5. Deduplicate while preserving order

    Examples:
        "VATValue_Name" -> ["vat", "value", "name"]
        "XMLParser"     -> ["xml", "parser"]
        "firstName"     -> ["first", "name"]
        "source_title"  -> ["source", "title"]
    """
    if not name:
        return []

    # Step 1: Split on delimiters
    parts = re.split(r"[\s._\-]+", name)

    tokens = []
    for part in parts:
        if not part:
            continue
        # Step 2 & 3: Split camelCase while grouping consecutive uppercase
        # Skip splitting if >50% uppercase (likely acronym)
        upper_count = sum(1 for c in part if c.isupper())
        if len(part) > 0 and upper_count > len(part) * 0.5 and len(part) > 1:
            tokens.append(part.lower())
            continue

        camel_tokens = _split_camel_case(part)
        tokens.extend(t.lower() for t in camel_tokens if t)

    # Step 5: Deduplicate preserving order
    seen = set()
    result = []
    for t in tokens:
        if t and t not in seen:
            seen.add(t)
            result.append(t)

    return result


def _split_camel_case(s: str) -> list[str]:
    """Split a string on camelCase/PascalCase boundaries."""
    if not s:
        return []

    tokens = []
    current = [s[0]]

    for i in range(1, len(s)):
        ch = s[i]
        prev = s[i - 1]

        if ch.isupper() and prev.islower():
            # Transition: lower -> Upper (camelCase boundary)
            tokens.append("".join(current))
            current = [ch]
        elif ch.islower() and prev.isupper() and len(current) > 1:
            # Transition: UPPERlower -> split before the last upper
            tokens.append("".join(current[:-1]))
            current = [current[-1], ch]
        elif ch.isdigit() != prev.isdigit():
            # Transition: letter <-> digit
            tokens.append("".join(current))
            current = [ch]
        else:
            current.append(ch)

    if current:
        tokens.append("".join(current))

    return tokens
