"""The order-total pipeline and the real-model blame workload, offline."""

from __future__ import annotations

import random
import re
from decimal import Decimal

import pytest
from openai.types.chat import ChatCompletion

import ledger
from bench import blame_eval
from examples import order_pipeline as op
from ledger.testing import faults


class ArithmeticLLM:
    """Stands in for the model: answers each of the pipeline's prompts exactly."""

    def __init__(self):
        self.base_url = "http://stub"
        self.calls = 0

        class Completions:
            create = self._create

        class Chat:
            completions = Completions()

        self.chat = Chat()

    def _create(self, *, model, messages, **kw):
        self.calls += 1
        prompt = messages[-1]["content"]
        if prompt.startswith("Turn this customer request"):
            req = prompt.split("Request: ", 1)[1]
            items = [{"name": name, "qty": int(q)} for q, name in
                     re.findall(r"(\d+) (" + "|".join(op.CATALOG) + ")", req)]
            state = next(s for s in op.TAX if s in req)
            code = re.search(r"code (\w+)", req)
            text = ('{"items": %s, "state": "%s", "coupon": %s}' % (
                str(items).replace("'", '"'), state, f'"{code.group(1)}"' if code else "null"))
        elif prompt.startswith("Add up the subtotal"):
            total = sum(Decimal(q) * Decimal(p) for q, p in
                        re.findall(r"(\d+) x .+? at \$([\d.]+) each", prompt))
            text = '{"subtotal": %s}' % total
        elif prompt.startswith("Subtotal:"):
            sub, off, rate = re.findall(r"\d+(?:\.\d+)?", prompt)[:3]
            total = op.cents(Decimal(sub) * (1 - Decimal(off) / 100) * (1 + Decimal(rate) / 100))
            text = '{"total": %s}' % total
        else:
            amount = re.search(r"total of \$([\d.]+)", prompt).group(1)
            text = f"Thanks for your order. Your total is ${amount}."
        return ChatCompletion.model_validate({
            "id": "x", "object": "chat.completion", "created": 0, "model": model,
            "choices": [{"index": 0, "finish_reason": "stop",
                         "message": {"role": "assistant", "content": text}}],
            "usage": {"prompt_tokens": 100, "completion_tokens": 20, "total_tokens": 120},
        })


def test_the_right_total():
    order = {"items": [{"name": "blue mug", "qty": 3}, {"name": "notebook", "qty": 2}],
             "state": "California", "coupon": "SAVE10"}
    # (37.50 + 9.50) * 0.9 * 1.0725 = 45.36675
    assert op.expected_total(order) == Decimal("45.37")
    assert op.reply_is_right("Your total is $45.37.", order)
    assert not op.reply_is_right("Your total is $45.36.", order)
    assert not op.reply_is_right("Subtotal $47.00, total $45.37.", order)  # one amount only
    assert not op.reply_is_right("Your total is 45.37.", order)


def test_orders_read_naturally():
    rnd = random.Random(3)
    for _ in range(50):
        request, order = op.make_order(rnd)
        assert "sockss" not in request and order["state"] in request
        assert all(i["name"] in request for i in order["items"])


def test_perturb_changes_one_number():
    rnd = random.Random(0)
    corrupt = op.perturb(rnd)
    assert corrupt({"state": "Texas", "rate": 0.0625})["rate"] != 0.0625
    assert corrupt({"state": "Oregon", "rate": 0.0})["rate"] == 0.05
    out = corrupt({"code": "SAVE10", "percent_off": 10})
    assert out["percent_off"] > 10 and out["code"] == "SAVE10"  # amounts are scaled
    assert corrupt({"item": "notebook", "qty": 2})["qty"] in (3, 4)  # quantities go up
    text = op.perturb(random.Random(1))({"choices": [{"message": {"content": '{"total": 41.84}'}}]})
    assert text["choices"][0]["message"]["content"] != '{"total": 41.84}'


def test_the_pipeline_gets_it_right_and_a_fault_breaks_it(store):
    llm = ArithmeticLLM()
    graph = op.build_graph(ledger.wrap(llm), model="stub")
    request, order = op.make_order(random.Random(5))
    with ledger.record(store):
        out = graph.invoke({"request": request})
    assert op.reply_is_right(out["reply"], order)
    fault = faults.Fault(node="tax", kind="tool_call", corrupt=lambda v: {**v, "rate": 0.5})
    with ledger.record(store) as rec, faults.inject(fault):
        bad = graph.invoke({"request": request})
    assert not op.reply_is_right(bad["reply"], order)
    d = ledger.diagnose(rec.run_id, graph, lambda o: op.reply_is_right(o["reply"], order),
                        store=store)
    assert d.step.key == fault.step and d.step.node == "tax"


def test_the_orders_workload_end_to_end(store):
    w = blame_eval.orders_workload(ArithmeticLLM(), "stub", seed=4)
    spend = blame_eval.Spend(ledger.blame.Pricing(1.0, 2.0), cap=None)
    res = blame_eval.run_workload(w, 12, 4, 1, 0.5, ledger.blame.Budget(forks=40), store, None,
                                  ("ledger", "scan", "random"), spend)
    assert res["skipped"]["clean_failed"] == 0
    assert res["faulted"] >= 6  # some perturbations don't change the total
    m = res["methods"]
    # The stub model is exact, so reruns are clean and both counterfactual methods are right.
    assert m["ledger"]["step_accuracy"]["rate"] == m["scan"]["step_accuracy"]["rate"] == 1.0
    assert m["ledger"]["dollars"]["mean"] > 0
    assert set(spend.by) >= {"record", "ledger", "scan"}


def test_the_whole_run_stops_at_its_dollar_cap(store):
    w = blame_eval.orders_workload(ArithmeticLLM(), "stub", seed=4)
    spend = blame_eval.Spend(ledger.blame.Pricing(1000.0, 0.0), cap=0.5)  # $0.10 per call
    res = blame_eval.run_workload(w, 20, 4, 1, 0.5, ledger.blame.Budget(forks=40), store, None,
                                  ("ledger",), spend)
    assert res["attempted"] < 20 and spend.total >= 0.5


def test_the_orders_workload_needs_a_model(capsys):
    with pytest.raises(SystemExit):
        blame_eval.main(["--workloads", "orders", "--runs", "1"])
    assert "needs --model" in capsys.readouterr().err


def test_parallel_trials_draw_the_same_faults(tmp_path):
    def run(workers):
        with ledger.EventStore(tmp_path / f"w{workers}.db") as store:
            w = blame_eval.orders_workload(ArithmeticLLM(), "stub", seed=9)
            return blame_eval.run_workload(w, 10, 9, 1, 0.5, ledger.blame.Budget(forks=40),
                                           store, None, ("ledger", "random"),
                                           blame_eval.Spend(), workers)

    one, four = run(1), run(4)
    assert one["faulted"] == four["faulted"] and one["skipped"] == four["skipped"]
    assert one["methods"] == four["methods"]
