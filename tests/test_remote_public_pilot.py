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


def test_metadata_failure_diagnostics_label_each_mode_venue():
    keys = [('rest', 'binance'), ('rest', 'gate'), ('ws', 'binance'), ('ws', 'gate')]
    results = [RuntimeError('binance GET https://api.binance.com/api/v3/exchangeInfo 451'),
               {'ok': True},
               RuntimeError('binance GET https://api.binance.com/api/v3/exchangeInfo 451'),
               {'ok': True}]
    diagnostics = p.metadata_diagnostics(keys, results)
    failures = [x for x in diagnostics if x['status'] == 'ERROR']
    assert {(x['mode'], x['venue']) for x in failures} == {
        ('rest', 'binance'), ('ws', 'binance')}
    assert all(x['http_status'] == 451 for x in failures)
    assert not any('https://' in str(x) for x in diagnostics)
    assert sum(x['status'] == 'OK' for x in diagnostics) == 2


def test_market_metadata_original_failure_is_not_masked(tmp_path, monkeypatch):
    import asyncio
    import json
    import sys
    from types import ModuleType, SimpleNamespace
    from unittest.mock import AsyncMock
    import pytest

    fake_ccxt = ModuleType('ccxt')
    fake_ccxt.__path__ = []
    fake_ccxt.__version__ = '4.5.90'
    rest = ModuleType('ccxt.async_support')
    ws = ModuleType('ccxt.pro')
    clients = []

    def create(venue, mode):
        def construct(_settings):
            failure = venue == 'binance'
            client = SimpleNamespace(
                load_markets=AsyncMock(side_effect=RuntimeError('HTTP 451') if failure else None,
                                       return_value={'BTC/USDT': {'spot': True}}),
                close=AsyncMock(),
                markets={'BTC/USDT': {'spot': True, 'active': True}},
                has={'watchOrderBook': True},
            )
            clients.append(client)
            return client
        return construct

    for m, module in [('rest', rest), ('ws', ws)]:
        for venue in p.VENUES:
            setattr(module, venue, create(venue, m))
    monkeypatch.setitem(sys.modules, 'ccxt', fake_ccxt)
    monkeypatch.setitem(sys.modules, 'ccxt.async_support', rest)
    monkeypatch.setitem(sys.modules, 'ccxt.pro', ws)
    monkeypatch.chdir(tmp_path)

    with pytest.raises(p.PublicMarketMetadataError) as error:
        asyncio.run(p.main())
    assert 'PUBLIC_MARKET_METADATA_UNAVAILABLE' in str(error.value)
    result = json.loads((tmp_path / 'evidence' / 'comparison_report.json').read_text())
    assert result['failure_reason'] == 'PUBLIC_MARKET_METADATA_UNAVAILABLE'
    assert result['paired_windows'] == 0
    assert len([x for x in result['metadata_diagnostics'] if x['status'] == 'ERROR']) == 2
    assert all(client.close.await_count == 1 for client in clients)
