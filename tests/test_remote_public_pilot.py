import remote_public_pilot as p


def book():
    return {'bids': [[100, 5]], 'asks': [[101, 4]], 'timestamp': None}


def test_snapshot_public_data_only():
    s = p.snapshot('gate', 'ws', book())
    assert s['bids'] == [[100.0, 5.0]]
    assert s['asks'] == [[101.0, 4.0]]
    assert 'apiKey' not in s


def test_pair_quality_and_non_atomic():
    a = p.snapshot('binance', 'ws', book())
    b = p.snapshot('gate', 'ws', book())
    a['received_monotonic_ms'] = 10000
    b['received_monotonic_ms'] = 10050
    r = p.pair_record('ws', {('ws','binance'): a, ('ws','gate'): b}, 1, 10100)
    assert r['pair_quality']['usable_for_diagnostic'] is True
    assert r['pair_quality']['atomic'] is False
    assert r['actual_trades'] == 0
    assert r['realized_profit_usdt'] is None


def test_stale_snapshot_not_eligible():
    a = p.snapshot('binance', 'rest', book()); b = p.snapshot('gate', 'rest', book())
    a['received_monotonic_ms'] = 1000; b['received_monotonic_ms'] = 1001
    r = p.pair_record('rest', {('rest','binance'): a, ('rest','gate'): b}, 1, 4000)
    assert r['pair_quality']['usable_for_diagnostic'] is False


def test_report_never_claims_profit():
    r = p.report([], {}, '4.5.90')
    assert r['ready_for_adoption'] is False
    assert r['verified_fills'] == 0
    assert r['realized_profit_usdt'] is None
    assert r['paired_windows'] == 0


def test_source_contains_no_private_or_order_calls():
    import inspect
    src = inspect.getsource(p)
    for forbidden in ('create_order(', 'withdraw(', 'fetch_balance(', 'apiKey', 'secret'):
        assert forbidden not in src
