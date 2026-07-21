"""Pure parsers for the GDELT TSV tables and their sub-syntax fields.

No framework imports, no I/O — plain ``str``/``bytes`` in, ``dict``/``list`` out — so
the pure-logic tier drives every function directly. GDELT's rows are headerless
tab-delimited UTF-8 with occasional garbage bytes and noisy machine-coded fields, and
several columns carry their own delimited sub-syntax (``V2Tone`` a comma tuple,
``V2Enhanced*`` ``;``-separated ``name,charoffset`` lists, ``V2EnhancedLocations``
``;``-separated ``#``-subfield blocks). Every parser here is **defensive**: malformed
input is skipped or returned as ``None``, never raised — one corrupt block must not
crash a whole file (the framework's let-it-crash is for *transient* faults, not for
dirty data we can safely skip).
"""
from typing import Any


def decode_table(raw: bytes) -> str:
    """Decode a GDELT file's bytes to text, replacing the occasional garbage byte.

    GDELT is nominally UTF-8 but ships stray invalid bytes; ``errors="replace"`` keeps
    the row intact (a lone � in one field) instead of dropping data or raising.
    """
    return raw.decode("utf-8", errors="replace")


def row_to_dict(columns: tuple[str, ...], line: str) -> dict[str, str]:
    """Zip a tab-delimited line onto its column names, keeping only non-empty values.

    Positional by construction (the files are headerless — see :mod:`.schema`). A short
    row pads with empties; a long row (an unescaped tab inside the trailing free-text
    column) folds the overflow back into the last column so nothing is lost. Empty cells
    are dropped rather than stored as ``""`` — absence reads the same downstream and keeps
    the nested ``row`` record (and the ClickHouse ``JSON`` column) lean, since a GDELT row
    leaves most of its 61/16/27 columns blank.
    """
    values = line.split("\t")
    if len(values) > len(columns):  # trailing free-text column swallowed a literal tab
        values = values[: len(columns) - 1] + ["\t".join(values[len(columns) - 1:])]
    return {column: value for column, value in zip(columns, values) if value != ""}


def parse_table(raw: bytes, columns: tuple[str, ...]) -> list[dict[str, str]]:
    """Decode a whole GDELT file and shred every non-blank line into a column dict."""
    return [row_to_dict(columns, line) for line in decode_table(raw).splitlines() if line]


_TONE_FIELDS = ("tone", "positive", "negative", "polarity",
                "activity_density", "self_group_density", "word_count")


def parse_tone(value: str | None) -> dict[str, float] | None:
    """Parse GDELT's ``V2Tone`` comma tuple into its named components.

    ``tone,positive,negative,polarity,activity_density,self_group_density,word_count``.
    Returns as many components as parse cleanly (GDELT usually sends all seven);
    ``None`` if the field is absent or its leading tone value isn't numeric — a
    malformed tuple is skipped, never raised.
    """
    if not value:
        return None
    parsed: dict[str, float] = {}
    for name, part in zip(_TONE_FIELDS, value.split(",")):
        try:
            parsed[name] = float(part)
        except ValueError:
            break  # stop at the first unparseable component; keep the clean prefix
    return parsed or None


def _strip_offset(block: str) -> str:
    """Drop a trailing ``,charoffset`` from a V2-enhanced entity block, keeping the name.

    Enhanced lists append the character offset after the last comma (``Angela Merkel,1345``);
    split on the LAST comma and drop the suffix only when it is all digits, so a name that
    itself contains a comma survives. V1 lists carry no offset and pass through unchanged.
    """
    name, _, offset = block.rpartition(",")
    return name if name and offset.isdigit() else block


def parse_entities(value: str | None) -> list[str]:
    """Parse a ``;``-separated GKG entity list (persons/orgs/themes) into distinct names.

    Handles both the plain V1 form (``Name;Name``) and the V2-enhanced form
    (``Name,offset;Name,offset``) by stripping any trailing offset. Names are trimmed and
    lowercased for stable set comparison in clustering, blanks dropped, order-preserving
    de-duplicated. Never raises — a stray block just contributes a (possibly odd) token.
    """
    if not value:
        return []
    seen: dict[str, None] = {}
    for block in value.split(";"):
        name = _strip_offset(block.strip()).strip().lower()
        if name:
            seen.setdefault(name, None)
    return list(seen)


def parse_locations(value: str | None) -> list[dict[str, Any]]:
    """Parse ``V2EnhancedLocations`` into location dicts, skipping malformed blocks.

    Each ``;``-separated block is ``#``-subfielded
    ``type#fullname#countrycode#adm1#adm2#lat#lon#featureid#charoffset`` (GDELT 2.1 —
    older 2.0 blocks omit ADM2/charoffset). We read positionally but defensively: a block
    with too few subfields, or unparseable coordinates, is skipped (coordinates left out,
    not zeroed), never raised. Returns ``{type, name, country_code, adm1, lat, lon}`` with
    ``lat``/``lon`` as floats when a clean adjacent pair is present.
    """
    if not value:
        return []
    locations: list[dict[str, Any]] = []
    for block in value.split(";"):
        parts = block.split("#")
        if len(parts) < 4:
            continue  # not enough subfields to be a usable location — skip it
        location: dict[str, Any] = {
            "type": parts[0],
            "name": parts[1],
            "country_code": parts[2],
            "adm1": parts[3],
        }
        # Coordinates sit at 5/6 in the 2.1 layout (after ADM2 at 4); tolerate the 2.0
        # layout by scanning for the first adjacent pair that both parse as floats.
        for i in range(4, len(parts) - 1):
            try:
                location["lat"], location["lon"] = float(parts[i]), float(parts[i + 1])
                break
            except ValueError:
                continue
        locations.append(location)
    return locations
