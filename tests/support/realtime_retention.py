"""Retention declarations for tests that build a mailbox without an app lifecycle."""

from forze_kits.integrations.realtime import MailboxRetention

# ----------------------- #

UNSWEPT = MailboxRetention.unbounded(
    reason="test harness: no lifecycle, so no retention sweeper to pair with",
)
"""The opt-out a test uses when the mailbox under test is not the retention path.

Explicit rather than convenient: the builder refuses to default, and a test that quietly
defaulted would be the one place the no-unbounded-default rule does not hold.
"""
