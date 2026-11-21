"""Example graphs that ship with Ledger.

    refund_agent     support ticket in, one Stripe refund out (needs the
                     ``examples`` extra: ``pip install 'ledger-runtime[examples]'``)
    toy_pipeline     5-node pipeline with a 3-way parallel fan-out
    order_pipeline   order-total pipeline whose answer can be checked

Each runs against the fake servers in ``ledger.testing`` by default. This
module imports none of them, so ``import ledger`` never needs Stripe.
"""
