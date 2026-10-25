# Arbitrage External CCXT Lab

Isolated, research-only environment for evaluating public REST vs WebSocket market-data feeds.

**Safety boundaries:** No API keys, no real orders or transfers, and no edits to the production scanner. The initial workflow checks only the GitHub-hosted runner's public egress via a neutral IP lookup service. An independent route from the original scanner must be verified before any exchange requests are authorized.
