"""Canonical vocabularies for idx_company_profile, shared by main.py,
shareholders_scraper.py and the local backfill script.

Three columns have their own vocabulary, so nothing is duplicated across writers:

  position  -> ALIASES           (directors / commissioners / audit_committees)
  type      -> TYPE_ALIASES      (shareholding type: position + share category mixed)
  name      -> clean_name()      ('' and '-' are placeholders, not names)

Unknown strings pass through tidied (never None) so a new IDX wording shows up in the
data instead of silently becoming empty.
"""

from enum import Enum
import re


class Position(str, Enum):
    PRESIDENT_DIRECTOR = "President Director"
    VICE_PRESIDENT_DIRECTOR = "Vice President Director"
    DIRECTOR = "Director"
    PRESIDENT_COMMISSIONER = "President Commissioner"
    VICE_PRESIDENT_COMMISSIONER = "Vice President Commissioner"
    COMMISSIONER = "Commissioner"
    HEAD_OF_AUDIT_COMMITTEE = "Head of Audit Committee"
    MEMBER_OF_AUDIT_COMMITTEE = "Member of Audit Committee"
    AUDIT_COMMITTEE = "Audit Committee"


P = Position

PLACEHOLDER = "-"

# The old scraper did `"Vice Presiden" in pos -> pos.replace(...)`, which appended a 't' to
# the already-correct "Vice President" and compounded one more per pass. Collapse any residue.
_TRAILING_T = re.compile(r"\bPresidentt+\b")

ALIASES = {
    "directors": {
        "President Director": P.PRESIDENT_DIRECTOR,
        "Presiden Direktur": P.PRESIDENT_DIRECTOR,
        "Direktur Utama": P.PRESIDENT_DIRECTOR,
        "Vice President Director": P.VICE_PRESIDENT_DIRECTOR,
        "Vice President": P.VICE_PRESIDENT_DIRECTOR,
        "Vice Presiden": P.VICE_PRESIDENT_DIRECTOR,
        "Vice Presiden Director": P.VICE_PRESIDENT_DIRECTOR,
        "Wakil Presiden Direktur": P.VICE_PRESIDENT_DIRECTOR,
        "Wakil Direktur Utama": P.VICE_PRESIDENT_DIRECTOR,
        "Director": P.DIRECTOR,
        "Direktur": P.DIRECTOR,
        "": P.DIRECTOR,
    },
    "commissioners": {
        "President Commissioner": P.PRESIDENT_COMMISSIONER,
        "President Commisioner": P.PRESIDENT_COMMISSIONER,
        "Presiden Komisaris": P.PRESIDENT_COMMISSIONER,
        "Komisaris Utama": P.PRESIDENT_COMMISSIONER,
        "Chairman Of The Board Of Supervisory": P.PRESIDENT_COMMISSIONER,
        "Vice President Commissioner": P.VICE_PRESIDENT_COMMISSIONER,
        "Vice President Commisioner": P.VICE_PRESIDENT_COMMISSIONER,
        "Vice Presiden Commissioner": P.VICE_PRESIDENT_COMMISSIONER,
        "Wakil Komisaris Utama": P.VICE_PRESIDENT_COMMISSIONER,
        "Wakil Presiden Komisaris": P.VICE_PRESIDENT_COMMISSIONER,
        "Commissioner": P.COMMISSIONER,
        "Komisaris": P.COMMISSIONER,
        "Member Of The Board Of Supervisory": P.COMMISSIONER,
        "": P.COMMISSIONER,
    },
    "audit_committees": {
        "Head": P.HEAD_OF_AUDIT_COMMITTEE,
        "Ketua": P.HEAD_OF_AUDIT_COMMITTEE,
        "Ketua Komite Audit": P.HEAD_OF_AUDIT_COMMITTEE,
        "Head Of Audit Committee": P.HEAD_OF_AUDIT_COMMITTEE,
        "Member": P.MEMBER_OF_AUDIT_COMMITTEE,
        "Anggota": P.MEMBER_OF_AUDIT_COMMITTEE,
        "Anggota Komite Audit": P.MEMBER_OF_AUDIT_COMMITTEE,
        "Member Of Audit Committee": P.MEMBER_OF_AUDIT_COMMITTEE,
        "Audit Committee": P.AUDIT_COMMITTEE,
        "": P.AUDIT_COMMITTEE,
    },
}

# Shareholding `type` vocabulary. Position aliases are checked first, so only the
# non-position entries live here.
TYPE_ALIASES = {
    "Direksi": "Director",
    "Commisioner": "Commissioner",
    "Kurang Dari 5%": "Less Than 5%",
    "Lebih Dari 5%": "More Than 5%",
    "Saham Pengendali": "Controlling Share",
    "Saham Non Pengendali": "Non Controlling Share",
    "Masyarakat Warkat": "Scrip Public Share",
    "Masyarakat Non Warkat": "Scripless Public Share",
}

# Absent values arrive as null, NaN, '' or a stringified sentinel depending on the writer.
SENTINELS = frozenset({"none", "nan", "null", "n/a", "na", "-"})


def _key(raw):
    """Tidy for lookup: collapse whitespace, undo a doubled value, repair the trailing-'t'
    typo, then title-case. Doubled must collapse before title() or 'DirectorDirector'
    becomes 'Directordirector' and no longer looks doubled."""
    value = _collapse_doubled(" ".join(raw.split()))
    return _TRAILING_T.sub("President", value.title())


def _collapse_doubled(value):
    """'DirectorDirector' -> 'Director'. Left overs from an old double-write bug."""
    half = len(value) // 2
    if len(value) > 4 and len(value) % 2 == 0 and value[:half] == value[half:]:
        return value[:half]
    return value


def _is_absent(raw):
    return (
        raw is None
        or (isinstance(raw, float) and raw != raw)  # NaN
        or (isinstance(raw, str) and (not raw.strip() or raw.strip().lower() in SENTINELS))
    )


class UnknownPosition(ValueError):
    """Raised in strict mode: a value the alias table does not know about."""


# What each column may contain once normalised.
_ROLE_VALUES = {
    "directors": (P.PRESIDENT_DIRECTOR, P.VICE_PRESIDENT_DIRECTOR, P.DIRECTOR),
    "commissioners": (
        P.PRESIDENT_COMMISSIONER,
        P.VICE_PRESIDENT_COMMISSIONER,
        P.COMMISSIONER,
    ),
    "audit_committees": (
        P.HEAD_OF_AUDIT_COMMITTEE,
        P.MEMBER_OF_AUDIT_COMMITTEE,
        P.AUDIT_COMMITTEE,
    ),
}

CANONICAL = {
    role: frozenset(p.value for p in values) for role, values in _ROLE_VALUES.items()
}


def normalise_position(raw, role: str, strict: bool = False) -> str:
    """Map a raw IDX position to its canonical value for `role`.

    Non-strings (NaN/int) are returned untouched so pandas fills stay fillable.
    Unknown strings pass through tidied; strict=True (tests, CI) raises instead.
    """
    if not isinstance(raw, str):
        return raw
    key = _key(raw)
    matched = ALIASES[role].get(key)
    if matched:
        return matched.value
    if strict:
        raise UnknownPosition(f"{raw!r} is not a known {role} position")
    return key


def normalise_type(raw) -> str:
    """Map a shareholding `type` to its canonical value.

    Absent values become PLACEHOLDER rather than null: detail rows elsewhere already use
    '-', and a null here reaches the client as a role-less blank.
    """
    if _is_absent(raw):
        return PLACEHOLDER
    if not isinstance(raw, str):
        return raw
    key = _key(raw)
    if key.lower() in SENTINELS:
        return PLACEHOLDER
    for role in ("directors", "commissioners"):
        if key in ALIASES[role]:
            return ALIASES[role][key].value
    return TYPE_ALIASES.get(key, key)


def clean_name(raw) -> str:
    """Title-case a person name, or PLACEHOLDER when it is blank / only punctuation."""
    if not isinstance(raw, str):
        return raw
    name = " ".join(raw.split()).title()
    return name if name.strip("- .") else PLACEHOLDER


def check_positions(people, role: str) -> list:
    """Return [(name, position), ...] for entries whose position is not canonical."""
    allowed = CANONICAL[role]
    bad = []
    for person in people if isinstance(people, list) else []:
        if not isinstance(person, dict):
            continue
        position = person.get("position")
        if position is None:
            continue
        # non-strings are flagged rather than compared: a dict/list position is unhashable
        # and would crash the comparison instead of being reported.
        if not isinstance(position, str) or position not in allowed:
            bad.append((person.get("name"), position))
    return bad


if __name__ == "__main__":
    # --- position: every alias lands on its target, case and spacing insensitive
    for role, table in ALIASES.items():
        for raw, expected in table.items():
            assert normalise_position(raw, role) == expected.value, (role, raw)
            assert normalise_position(f"  {raw.lower()}  ", role) == expected.value, (role, raw)

    # --- the actual defect: 1..6 accumulated 't's all repair
    for n in range(1, 7):
        assert normalise_position("Vice President" + "t" * n, "directors") == "Vice President Director", n
        assert (
            normalise_position("Vice President" + "t" * n + " Commissioner", "commissioners")
            == "Vice President Commissioner"
        ), n

    # --- idempotent, and never escapes the role's allowed set
    for role, table in ALIASES.items():
        for raw in table:
            out = normalise_position(raw, role)
            assert normalise_position(out, role) == out, (role, raw, out)
            assert "Presidentt" not in out, (role, raw, out)
            assert out in CANONICAL[role], (role, raw, out)

    for role, values in _ROLE_VALUES.items():
        for value in values:
            assert normalise_position(value.value, role, strict=True) == value.value, (role, value)

    try:
        normalise_position("Supreme Leader", "directors", strict=True)
    except UnknownPosition:
        pass
    else:
        raise AssertionError("strict mode let an unknown position through")

    # --- unknown position surfaces tidied instead of vanishing
    assert normalise_position("chief  of   stuff", "directors") == "Chief Of Stuff"
    assert normalise_position(None, "directors") is None

    # --- type: typo repair, doubled collapse, absent -> placeholder, categories untouched
    assert normalise_type("Vice Presidentt") == "Vice President Director"
    assert normalise_type("Vice Presidentt Commissioner") == "Vice President Commissioner"
    assert normalise_type("Direktur Utama") == "President Director"
    assert normalise_type("DirectorDirector") == "Director"
    assert normalise_type("More Than 5%More Than 5%") == "More Than 5%"
    assert normalise_type("President CommisionerPresident Commisioner") == "President Commissioner"
    assert normalise_type("Direksi") == "Director"
    assert normalise_type("Lebih dari 5%") == "More Than 5%"
    assert normalise_type("More Than 5%") == "More Than 5%"
    assert normalise_type("Scripless Public Share") == "Scripless Public Share"
    for absent in (None, float("nan"), "", "  ", "None", "nan", "n/a"):
        assert normalise_type(absent) == PLACEHOLDER, absent
    # unknown values come back title-cased (lookup is case-insensitive), never dropped
    assert normalise_type("More than 5%") == "More Than 5%"
    assert normalise_type("Free Float") == "Free Float"
    assert normalise_type("Public (Foreign)") == "Public (Foreign)"
    for value in ("Vice Presidentt", "More Than 5%More Than 5%", "", None):
        once = normalise_type(value)
        assert normalise_type(once) == once, (value, once)

    # --- name: title-cased, blank/dash -> placeholder
    assert clean_name("HENRY PANJAITAN") == "Henry Panjaitan"
    assert clean_name("  the kwen ie ") == "The Kwen Ie"
    assert clean_name("") == PLACEHOLDER
    assert clean_name(" - ") == PLACEHOLDER
    assert clean_name("Dr. Darmin Nasution, S.E.") == "Dr. Darmin Nasution, S.E."
    assert clean_name(None) is None
    assert clean_name(clean_name("BUDI")) == clean_name("BUDI")

    # --- validator
    assert check_positions([{"name": "A", "position": "Director"}], "directors") == []
    assert check_positions([{"name": "B", "position": "Vice Presidentt"}], "directors") == [
        ("B", "Vice Presidentt")
    ]
    assert check_positions(None, "directors") == []
    assert check_positions([{"name": "C"}], "directors") == []
    assert check_positions([{"name": "D", "position": 5}], "directors") == [("D", 5)]
    assert check_positions([{"name": "E", "position": {}}], "directors") == [("E", {})]
    assert check_positions(["not a dict"], "directors") == []

    aliases = sum(len(t) for t in ALIASES.values())
    print(f"OK {aliases} position aliases + {len(TYPE_ALIASES)} type aliases -> {len(Position)} canonical values")
    print("   allowed per column: " + ", ".join(f"{r}={len(v)}" for r, v in CANONICAL.items()))
