# Change: Merge worktrees and align price sync

## Why
We need to unify worktree changes into main and align pricing behavior with the legacy integration (SQL Server SNK_GET_PRECO, multi-table, batch prices, and unit normalization).

## What Changes
- Consolidate worktree changes into main with selective cherry-picks.
- Normalize price units (decimal vs centavos) across API and UI.
- Enforce SQL Server pricing via [sankhya].SNK_GET_PRECO.
- Support multi-table price sync and batch prices for all eligible tables.

## Impact
- Affected specs: fastchannel
- Affected code: pricing job, price UI/services, queue payloads
