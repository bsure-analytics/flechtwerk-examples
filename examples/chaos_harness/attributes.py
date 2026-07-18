"""Typed fields for the chaos harness.

The transformer forwards each input number and stamps it with a monotonic
sequence taken from its state. Under exactly-once delivery the output must carry
every ``n`` exactly once and a ``seq`` that never repeats or skips — even when
the transformer is SIGKILLed mid-batch.
"""
from typing import Final

from flechtwerk.attribute import Attribute, INT

N: Final = Attribute("n", INT)
"""The input value — the producer writes 0, 1, 2, … once each."""
SEQ: Final = Attribute("seq", INT)
"""The exactly-once counter the transformer assigns from its state."""
COUNT: Final = Attribute("count", INT)
"""Transformer state: how many records this key has been counted through."""
