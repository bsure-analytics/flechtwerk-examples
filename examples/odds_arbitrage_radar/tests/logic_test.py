"""Tier 1 — pure logic. No framework, no mocks, no network.

Drives the stages' pure cores directly: the venue normalizers (``normalize_polymarket`` /
``normalize_kalshi`` and their helpers ``best_of_book`` / ``yes_no_tokens``), the arbitrage
math (``fee`` / ``compute_margins``), and the radar fold (``run_radar``) driven as a bare
async generator over hand-built ``State``/``IncomingMessage`` — the SMARD ``run_mix`` style.
The committed fixtures are trimmed **real** captures (see ``fixtures/PROVENANCE.md``); most
cases build tiny quotes inline so the arithmetic is obvious.
"""
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from flechtwerk import Event, IncomingMessage, Message, State

from examples.odds_arbitrage_radar.arbitrage import (
    KALSHI_YES,
    POLYMARKET_YES,
    Quote,
    compute_margins,
    fee,
)
from examples.odds_arbitrage_radar.attributes import (
    COMPUTED_AT,
    DIRECTION,
    EXECUTABLE_SIZE,
    FEE_RATE,
    FETCHED_AT,
    FRESH,
    KIND,
    LEGS,
    MARGINS_TOPIC,
    NET_EDGE,
    NO_ASK,
    NO_ASK_SIZE,
    PAIR_KEY,
    SIGNALS_TOPIC,
    STATUS,
    TITLE,
    VENUE,
    YES_ASK,
    YES_ASK_SIZE,
    YES_BID,
)
from examples.odds_arbitrage_radar.kalshi import normalize_kalshi
from examples.odds_arbitrage_radar.polymarket import (
    best_of_book,
    normalize_polymarket,
    polymarket_status,
    yes_no_tokens,
)
from examples.odds_arbitrage_radar.radar import POLYMARKET, KALSHI, STALE_AFTER, run_radar

FIXTURES = Path(__file__).parent / "fixtures"
POLY_MARKET = json.loads((FIXTURES / "poly_market.json").read_text())[0]
POLY_BOOK_COLORADO = json.loads((FIXTURES / "poly_book_colorado.json").read_text())
POLY_BOOK_MILWAUKEE = json.loads((FIXTURES / "poly_book_milwaukee.json").read_text())
KALSHI_MARKET = json.loads((FIXTURES / "kalshi_market.json").read_text())["market"]

UTC = timezone.utc
PAIR = "mlb-col-mil-2026-07-24"
_T = datetime(2026, 7, 23, 20, 16, tzinfo=UTC)


def _poly_books() -> dict[str, dict]:
    """Map each fixture token id to its book (index 0 = Colorado, index 1 = Milwaukee)."""
    tokens = json.loads(POLY_MARKET["clobTokenIds"])
    return {tokens[0]: POLY_BOOK_COLORADO, tokens[1]: POLY_BOOK_MILWAUKEE}


# --- best_of_book ---

def test_best_of_book_takes_max_bid_and_min_ask_regardless_of_order() -> None:
    # Levels arrive unsorted; best bid is the highest price, best ask the lowest.
    bids = [{"price": "0.30", "size": "5"}, {"price": "0.33", "size": "9"}, {"price": "0.31", "size": "7"}]
    asks = [{"price": "0.36", "size": "4"}, {"price": "0.34", "size": "8"}, {"price": "0.35", "size": "6"}]
    assert best_of_book(bids, side="bid") == (0.33, 9.0)
    assert best_of_book(asks, side="ask") == (0.34, 8.0)


def test_best_of_book_empty_side_is_none() -> None:
    assert best_of_book([], side="bid") is None
    assert best_of_book([], side="ask") is None


# --- yes_no_tokens ---

def test_yes_no_tokens_maps_yes_outcome_at_index_1() -> None:
    tokens = json.loads(POLY_MARKET["clobTokenIds"])
    yes, no = yes_no_tokens(POLY_MARKET, "Milwaukee Brewers")   # index 1
    assert (yes, no) == (tokens[1], tokens[0])


def test_yes_no_tokens_maps_yes_outcome_at_index_0() -> None:
    tokens = json.loads(POLY_MARKET["clobTokenIds"])
    yes, no = yes_no_tokens(POLY_MARKET, "Colorado Rockies")    # index 0
    assert (yes, no) == (tokens[0], tokens[1])


def test_yes_no_tokens_rejects_unknown_outcome() -> None:
    with pytest.raises(ValueError, match="not one of"):
        yes_no_tokens(POLY_MARKET, "Milwaukee Bucks")           # NBA, not in this market


def test_yes_no_tokens_rejects_negrisk() -> None:
    with pytest.raises(ValueError, match="negRisk"):
        yes_no_tokens({**POLY_MARKET, "negRisk": True}, "Milwaukee Brewers")


def test_yes_no_tokens_rejects_non_binary() -> None:
    three = {**POLY_MARKET, "outcomes": json.dumps(["A", "B", "C"]),
             "clobTokenIds": json.dumps(["1", "2", "3"])}
    with pytest.raises(ValueError, match="not a binary market"):
        yes_no_tokens(three, "A")


# --- polymarket_status ---

def test_polymarket_status_active_requires_all_flags() -> None:
    assert polymarket_status(POLY_MARKET) == "active"


@pytest.mark.parametrize("override", [
    {"closed": True}, {"archived": True}, {"active": False}, {"acceptingOrders": False},
])
def test_polymarket_status_any_bad_flag_is_closed(override: dict) -> None:
    assert polymarket_status({**POLY_MARKET, **override}) == "closed"


# --- normalize_polymarket ---

def test_normalize_polymarket_maps_yes_to_milwaukee() -> None:
    quote = normalize_polymarket(POLY_MARKET, _poly_books(), yes_outcome="Milwaukee Brewers",
                                 fee_rate=0.05, pair_key=PAIR, fetched_at=_T)
    assert quote[KIND] == "quote" and quote[VENUE] == "polymarket" and quote[STATUS] == "active"
    assert quote[PAIR_KEY] == PAIR and quote[FEE_RATE] == 0.05 and quote[FETCHED_AT] == _T
    # YES = Milwaukee book: best ask 0.69 (min of asks), best bid 0.68 (max of bids).
    assert quote[YES_ASK] == 0.69 and quote[YES_BID] == 0.68 and quote[YES_ASK_SIZE] == 29236.24
    # NO = Colorado book: best ask 0.32.
    assert quote[NO_ASK] == 0.32 and quote[NO_ASK_SIZE] == 3246.69


def test_normalize_polymarket_swaps_sides_when_yes_is_colorado() -> None:
    quote = normalize_polymarket(POLY_MARKET, _poly_books(), yes_outcome="Colorado Rockies",
                                 fee_rate=0.05, pair_key=PAIR, fetched_at=_T)
    assert quote[YES_ASK] == 0.32 and quote[NO_ASK] == 0.69   # mirror of the previous test


def test_normalize_polymarket_one_sided_book_omits_ask() -> None:
    tokens = json.loads(POLY_MARKET["clobTokenIds"])
    books = {tokens[1]: {"bids": POLY_BOOK_MILWAUKEE["bids"], "asks": []},   # YES: no asks
             tokens[0]: POLY_BOOK_COLORADO}
    quote = normalize_polymarket(POLY_MARKET, books, yes_outcome="Milwaukee Brewers",
                                 fee_rate=0.05, pair_key=PAIR, fetched_at=_T)
    assert quote.get(YES_ASK) is None and quote.get(YES_ASK_SIZE) is None   # never a fabricated 0
    assert quote[YES_BID] == 0.68                                           # bid side still present
    assert quote[NO_ASK] == 0.32


# --- normalize_kalshi ---

def test_normalize_kalshi_parses_dollars_and_crosswise_sizes() -> None:
    quote = normalize_kalshi(KALSHI_MARKET, fee_rate=0.07, pair_key=PAIR, fetched_at=_T)
    assert quote[VENUE] == "kalshi" and quote[STATUS] == "active"
    assert quote[YES_ASK] == 0.76 and quote[NO_ASK] == 0.30
    # The NO book is the YES book's complement: no_ask == 1 - yes_bid.
    assert quote[NO_ASK] == pytest.approx(1.0 - quote[YES_BID])
    # Crosswise sizes: size at yes_ask is yes_ask_size_fp; size at no_ask is yes_bid_size_fp.
    assert quote[YES_ASK_SIZE] == 1404.06 and quote[NO_ASK_SIZE] == 997.0


def test_normalize_kalshi_zero_price_is_absent() -> None:
    # A side with no resting order reports "0.0000" — treated as absent, not a $0 ask.
    market = {**KALSHI_MARKET, "no_ask_dollars": "0.0000"}
    quote = normalize_kalshi(market, fee_rate=0.07, pair_key=PAIR, fetched_at=_T)
    assert quote.get(NO_ASK) is None and quote.get(NO_ASK_SIZE) is None
    assert quote[YES_ASK] == 0.76                                           # other side intact


def test_normalize_kalshi_non_active_is_closed() -> None:
    quote = normalize_kalshi({**KALSHI_MARKET, "status": "settled"},
                             fee_rate=0.07, pair_key=PAIR, fetched_at=_T)
    assert quote[STATUS] == "closed"


# --- fee ---

def test_fee_peaks_at_half_and_vanishes_at_extremes() -> None:
    assert fee(0.5, 0.07) == pytest.approx(0.0175)             # Kalshi's documented max, 1.75c
    assert fee(0.0, 0.07) == 0.0 and fee(1.0, 0.07) == 0.0
    assert fee(0.69, 0.05) == pytest.approx(0.05 * 0.69 * 0.31)


# --- compute_margins ---

def _q(venue: str, *, yes_ask=None, no_ask=None, yes_size=None, no_size=None, rate=0.05) -> Quote:
    return Quote(venue=venue, title="t", fee_rate=rate,
                 yes_ask=yes_ask, no_ask=no_ask, yes_ask_size=yes_size, no_ask_size=no_size)


def test_compute_margins_reproduces_the_fixture_gross_vs_net() -> None:
    # The captured Col@Mil books: Polymarket Milwaukee-YES 0.69 + Kalshi NO 0.30 = 0.99, a 1c
    # gross edge that ~2.5c of fees eats — the live gross-vs-net lesson.
    poly = _q("polymarket", yes_ask=0.69, no_ask=0.32, rate=0.05)
    kalshi = _q("kalshi", yes_ask=0.76, no_ask=0.30, rate=0.07)
    margins = {m.direction: m for m in compute_margins(poly, kalshi)}
    d1 = margins[POLYMARKET_YES]
    assert d1.gross_edge == pytest.approx(0.01) and d1.fees == pytest.approx(0.025395)
    assert d1.net_edge < 0                                      # fees eat the 1c gross edge
    assert margins[KALSHI_YES].gross_edge == pytest.approx(-0.08)


def test_compute_margins_contrived_true_arb_is_net_positive() -> None:
    poly = _q("polymarket", yes_ask=0.40, yes_size=100.0, rate=0.05)
    kalshi = _q("kalshi", no_ask=0.40, no_size=250.0, rate=0.07)
    margins = compute_margins(poly, kalshi)
    assert len(margins) == 1                                    # only the computable direction
    m = margins[0]
    assert m.direction == POLYMARKET_YES
    assert m.gross_edge == pytest.approx(0.20) and m.net_edge > 0
    assert m.executable_size == 100.0                           # min(100, 250)


def test_compute_margins_missing_ask_drops_that_direction() -> None:
    poly = _q("polymarket", yes_ask=0.40)                       # no no_ask
    kalshi = _q("kalshi", no_ask=0.40)                          # no yes_ask
    margins = compute_margins(poly, kalshi)
    assert [m.direction for m in margins] == [POLYMARKET_YES]    # kalshi_yes leg impossible


def test_compute_margins_size_absent_when_a_leg_size_missing() -> None:
    poly = _q("polymarket", yes_ask=0.40, yes_size=100.0)
    kalshi = _q("kalshi", no_ask=0.40)                          # no size
    assert compute_margins(poly, kalshi)[0].executable_size is None


# --- run_radar (bare async generator, hand-built State/messages) ---

def _quote_event(venue: str, *, yes_ask=None, no_ask=None, yes_size=None, no_size=None,
                 fee_rate=0.05, status="active", fetched_at=_T, title="Col @ Mil") -> Event:
    ev = Event({KIND: "quote", PAIR_KEY: PAIR, VENUE: venue, TITLE: title,
                STATUS: status, FEE_RATE: fee_rate, FETCHED_AT: fetched_at})
    for attr, v in ((YES_ASK, yes_ask), (NO_ASK, no_ask),
                    (YES_ASK_SIZE, yes_size), (NO_ASK_SIZE, no_size)):
        if v is not None:
            ev[attr] = v
    return ev


def _msg(value: Event) -> IncomingMessage:
    return IncomingMessage(key=PAIR, offset=0, partition=0, timestamp=None,
                           topic="odds-quotes", value=value)


async def _drive(state: State, value: Event) -> tuple[list[Message], State | None]:
    """Run ``run_radar`` over one quote; split its yields into messages and the final state."""
    messages: list[Message] = []
    new_state: State | None = None
    async for item in run_radar(state, _msg(value)):
        if isinstance(item, State):
            new_state = item
        else:
            messages.append(item)
    return messages, new_state


def _by_topic(messages: list[Message], topic: str) -> list[Event]:
    return [m.value for m in messages if m.topic == topic]


async def test_run_radar_first_leg_stores_state_and_emits_nothing() -> None:
    messages, state = await _drive(State(), _quote_event(POLYMARKET, yes_ask=0.40))
    assert messages == []                                       # only one venue — nothing to compare
    assert state is not None and POLYMARKET in state[LEGS] and KALSHI not in state[LEGS]


async def test_run_radar_second_leg_emits_margins_and_signal() -> None:
    # Seed Polymarket, then drive Kalshi against it — a contrived fresh arb.
    _, s1 = await _drive(State(), _quote_event(POLYMARKET, yes_ask=0.40, yes_size=100.0))
    messages, s2 = await _drive(s1, _quote_event(KALSHI, no_ask=0.40, no_size=250.0, fee_rate=0.07))
    margins = _by_topic(messages, MARGINS_TOPIC)
    signals = _by_topic(messages, SIGNALS_TOPIC)
    assert len(margins) == 1 and margins[0][DIRECTION] == POLYMARKET_YES
    assert margins[0][FRESH] is True and margins[0][NET_EDGE] > 0
    assert margins[0][EXECUTABLE_SIZE] == 100.0 and margins[0][COMPUTED_AT] == _T
    assert len(signals) == 1 and signals[0][NET_EDGE] > 0       # fresh + net-positive → signal
    assert s2 is not None and set(s2[LEGS]) == {POLYMARKET, KALSHI}


async def test_run_radar_stale_other_leg_flags_not_fresh_and_suppresses_signal() -> None:
    old = _T - STALE_AFTER - timedelta(seconds=1)
    _, s1 = await _drive(State(), _quote_event(POLYMARKET, yes_ask=0.40, yes_size=100.0, fetched_at=old))
    messages, _ = await _drive(s1, _quote_event(KALSHI, no_ask=0.40, no_size=250.0, fee_rate=0.07, fetched_at=_T))
    margins = _by_topic(messages, MARGINS_TOPIC)
    assert len(margins) == 1 and margins[0][FRESH] is False     # other leg too old
    assert margins[0][NET_EDGE] > 0                             # a real edge, but stale
    assert _by_topic(messages, SIGNALS_TOPIC) == []             # so: no signal


async def test_run_radar_newer_other_leg_is_fresh() -> None:
    # The trigger can be OLDER than the other leg (two producers race); a negative age is fresh.
    newer = _T + timedelta(minutes=30)
    _, s1 = await _drive(State(), _quote_event(POLYMARKET, yes_ask=0.40, yes_size=100.0, fetched_at=newer))
    messages, _ = await _drive(s1, _quote_event(KALSHI, no_ask=0.40, no_size=250.0, fee_rate=0.07, fetched_at=_T))
    assert _by_topic(messages, MARGINS_TOPIC)[0][FRESH] is True


async def test_run_radar_closed_quote_tombstones_state() -> None:
    _, s1 = await _drive(State(), _quote_event(POLYMARKET, yes_ask=0.40))
    messages, state = await _drive(s1, _quote_event(POLYMARKET, status="closed"))
    assert messages == []
    assert state is not None and not state                      # falsy State() → tombstone


async def test_run_radar_closed_with_no_state_is_noop() -> None:
    messages, state = await _drive(State(), _quote_event(KALSHI, status="closed"))
    assert messages == [] and state is None                     # nothing yielded at all
