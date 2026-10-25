"""Manual-only 30-second public order-book timing pilot; no keys, orders or execution.

GitHub-hosted runner only. The remote-gate step must precede this script. Its
observations are non-atomic and not representative of executable arbitrage.
"""
import asyncio
from collections import Counter
from datetime import datetime, timezone
import json
import re
from pathlib import Path
import time

SYMBOL = 'BTC/USDT'
CONTRACT = 'CCXT_PUBLIC_REST_VS_WS_DIAGNOSTIC_V1'
VENUES = ('binance', 'gate')
MODES = ('rest', 'ws')
DURATION_S = 30
INTERVAL_S = 2
BOOK_DEPTH = 10


def monotonic_ms():
    return time.monotonic_ns() / 1_000_000


def p95(values):
    if not values:
        return None
    s = sorted(values)
    i = (len(s) - 1) * 0.95
    lo = int(i)
    return round(s[lo] + (s[min(lo + 1, len(s) - 1)] - s[lo]) * (i - lo), 3)



class PublicMarketMetadataError(RuntimeError):
    """Market metadata cannot be loaded from all required venue/mode clients."""


def metadata_diagnostics(client_keys, responses):
    """Identify failed CCXT public metadata requests without exposing raw URLs."""
    rows = []
    for (mode, venue), response in zip(client_keys, responses):
        if isinstance(response, BaseException):
            raw_message = str(response)
            http_match = re.search(r"(?<![0-9])(?:403|418|429|451|500|502|503|504)(?![0-9])", raw_message)
            summary = re.sub(r"https?://\S+", "<endpoint>", raw_message)
            summary = ' '.join(summary.split())[:180]
            rows.append({
                'mode': mode,
                'venue': venue,
                'status': 'ERROR',
                'exception_type': type(response).__name__,
                'http_status': int(http_match.group(0)) if http_match else None,
                'error_summary': summary,
            })
        else:
            rows.append({'mode': mode, 'venue': venue, 'status': 'OK'})
    return rows

def snapshot(venue, mode, book):
    bids = book.get('bids', [])[:BOOK_DEPTH]
    asks = book.get('asks', [])[:BOOK_DEPTH]
    if not bids or not asks:
        raise ValueError('EMPTY_ORDER_BOOK')
    if not all(len(level) >= 2 and level[0] > 0 and level[1] > 0 for level in bids + asks):
        raise ValueError('INVALID_ORDER_BOOK')
    return {
        'venue': venue,
        'mode': mode,
        'received_monotonic_ms': monotonic_ms(),
        'received_utc': datetime.now(timezone.utc).isoformat(),
        'native_timestamp_ms': book.get('timestamp'),
        'bids': [[float(p), float(q)] for p, q, *_ in bids],
        'asks': [[float(p), float(q)] for p, q, *_ in asks],
    }


def pair_record(mode, state, sequence, now_ms):
    a, b = state.get((mode, 'binance')), state.get((mode, 'gate'))
    if a is None or b is None:
        return None
    ages = [now_ms - x['received_monotonic_ms'] for x in (a, b)]
    skew = abs(a['received_monotonic_ms'] - b['received_monotonic_ms'])
    native = all(isinstance(x['native_timestamp_ms'], (int, float)) and
                 x['native_timestamp_ms'] > 0 for x in (a, b))
    eligible = all(0 <= x <= 1500 for x in ages) and skew <= 1200
    return {
        'contract': CONTRACT,
        'sample_sequence': sequence,
        'symbol': SYMBOL,
        'mode': mode,
        'observation_utc': datetime.now(timezone.utc).isoformat(),
        'pair_quality': {
            'usable_for_diagnostic': eligible,
            'max_local_book_age_ms': round(max(ages), 3),
            'pair_local_receive_skew_ms': round(skew, 3),
            'native_exchange_timestamps_both_present': native,
            'atomic': False,
        },
        'snapshots': {'binance': a, 'gate': b},
        'directions': {},  # No profit estimate or trade simulation in this pilot.
        'actual_trades': 0,
        'realized_profit_usdt': None,
        'cross_venue_books_atomic': False,
    }


def report(rows, events, ccxt_version):
    by_mode = {}
    paired = Counter()
    for row in rows:
        paired[row['sample_sequence']] += 1
    for mode in MODES:
        selected = [r for r in rows if r['mode'] == mode]
        usable = [r['pair_quality'] for r in selected if r['pair_quality']['usable_for_diagnostic']]
        by_mode[mode] = {
            'observations': len(selected),
            'timing_eligible_pairs': len(usable),
            'timing_eligible_ratio': round(len(usable) / len(selected), 4) if selected else 0.0,
            'pair_skew_ms_p95': p95([x['pair_local_receive_skew_ms'] for x in usable]),
            'max_book_age_ms_p95': p95([x['max_local_book_age_ms'] for x in usable]),
        }
    return {
        'status': 'PUBLIC_BOOK_LATENCY_RESEARCH_ONLY',
        'contract': CONTRACT,
        'ccxt_version': ccxt_version,
        'duration_sec_requested': DURATION_S,
        'paired_windows': sum(count == 2 for count in paired.values()),
        'records': len(rows),
        'mode_summary': by_mode,
        'event_counts': dict(events),
        'ready_for_adoption': False,
        'verified_executable_opportunities': 0,
        'verified_fills': 0,
        'realized_profit_usdt': None,
        'limitations': ['Non-atomic cross-venue books', 'Local receive times, not verified exchange synchronization',
                        'No account balances, private fees, precision, fills, or trades',
                        'Thirty seconds cannot validate market-data superiority'],
    }


async def main():
    import ccxt
    import ccxt.async_support as rest
    import ccxt.pro as ws

    output = Path('evidence')
    output.mkdir(exist_ok=True)
    clients = {}
    state = {}
    events = Counter()
    stopped = asyncio.Event()
    tasks = []
    rows = []
    metadata_results = []
    failure_reason = None
    try:
        for mode, module in (('rest', rest), ('ws', ws)):
            for venue in VENUES:
                clients[(mode, venue)] = getattr(module, venue)({
                    'enableRateLimit': True,
                    'options': {'defaultType': 'spot'},
                })
        responses = await asyncio.gather(*(
            asyncio.wait_for(client.load_markets(), timeout=40) for client in clients.values()),
            return_exceptions=True)
        metadata_results = metadata_diagnostics(list(clients.keys()), responses)
        if any(row['status'] == 'ERROR' for row in metadata_results):
            raise PublicMarketMetadataError('PUBLIC_MARKET_METADATA_UNAVAILABLE')
        for (mode, venue), client in clients.items():
            market = client.markets.get(SYMBOL)
            if not market or market.get('spot') is not True or market.get('active') is False:
                raise RuntimeError('SPOT_MARKET_UNAVAILABLE_' + venue)
            if mode == 'ws' and not client.has.get('watchOrderBook'):
                raise RuntimeError('WS_ORDER_BOOK_UNAVAILABLE_' + venue)

        async def poll_rest():
            while not stopped.is_set():
                started = time.monotonic()

                async def pull(venue):
                    try:
                        book = await asyncio.wait_for(
                            clients[('rest', venue)].fetch_order_book(SYMBOL, BOOK_DEPTH), timeout=12)
                        state[('rest', venue)] = snapshot(venue, 'rest', book)
                        events['rest_' + venue + '_success'] += 1
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        events['rest_' + venue + '_' + type(exc).__name__] += 1

                await asyncio.gather(*(pull(venue) for venue in VENUES))
                await asyncio.sleep(max(0.05, INTERVAL_S - (time.monotonic() - started)))

        async def consume_ws(venue):
            failures = 0
            while not stopped.is_set():
                try:
                    book = await asyncio.wait_for(
                        clients[('ws', venue)].watch_order_book(SYMBOL, BOOK_DEPTH), timeout=12)
                    state[('ws', venue)] = snapshot(venue, 'ws', book)
                    events['ws_' + venue + '_success'] += 1
                    failures = 0
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    events['ws_' + venue + '_' + type(exc).__name__] += 1
                    failures += 1
                    if failures >= 3:
                        return
                    await asyncio.sleep(failures)

        tasks = [asyncio.create_task(poll_rest())] + [asyncio.create_task(consume_ws(v)) for v in VENUES]
        start = time.monotonic()
        with (output / 'book_pairs.jsonl').open('w', encoding='utf-8') as stream:
            sequence = 0
            while time.monotonic() - start < DURATION_S:
                await asyncio.sleep(INTERVAL_S)
                sequence += 1
                for mode in MODES:
                    record = pair_record(mode, state, sequence, monotonic_ms())
                    if record is None:
                        events[mode + '_missing_pair'] += 1
                    else:
                        stream.write(json.dumps(record, separators=(',', ':'), allow_nan=False) + '\n')
                        rows.append(record)
    except Exception as exc:
        failure_reason = ('PUBLIC_MARKET_METADATA_UNAVAILABLE'
                          if isinstance(exc, PublicMarketMetadataError)
                          else type(exc).__name__)
        raise
    finally:
        stopped.set()
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.gather(*(client.close() for client in clients.values()), return_exceptions=True)
        result = report(rows, events, ccxt.__version__)
        result['metadata_diagnostics'] = metadata_results
        result['failure_reason'] = failure_reason or (
            'NO_PAIRED_TIMING_WINDOWS' if result['paired_windows'] == 0 else None)
        (output / 'comparison_report.json').write_text(json.dumps(result, indent=2, allow_nan=False) + '\n', encoding='utf-8')
        for row in metadata_results:
            label = row['mode'].upper() + '_' + row['venue'].upper()
            if row['status'] == 'ERROR':
                print('METADATA_' + label + '=ERROR:' + row['exception_type'] +
                      ':HTTP_' + str(row['http_status']) + ':' + row['error_summary'])
            else:
                print('METADATA_' + label + '=OK')
        print('FAILURE_REASON=' + str(result['failure_reason']))
        print('PROBE_STATUS=' + result['status'])
        print('CCXT_VERSION=' + ccxt.__version__)
        print('RECORDS=' + str(result['records']))
        print('PAIRED_WINDOWS=' + str(result['paired_windows']))
        print('READY_FOR_ADOPTION=false')
        print('ACTUAL_TRADES=0')
    if result['paired_windows'] == 0:
        raise RuntimeError('NO_PAIRED_TIMING_WINDOWS')


if __name__ == '__main__':
    asyncio.run(main())
