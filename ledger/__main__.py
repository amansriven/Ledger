"""``python -m ledger``: the same as the ``ledger`` command."""

import sys

from ledger.cli import main

sys.exit(main())
