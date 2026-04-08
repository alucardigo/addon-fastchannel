## Context
Multiple worktrees contain required fixes and pricing features. Legacy integration uses SQL Server SNK_GET_PRECO, multi-table pricing, and batch prices. The add-on must match legacy behavior and normalize price units.

## Goals / Non-Goals
- Goals: merge worktrees, normalize price units, enforce SQL Server pricing, support multi-table + batch prices.
- Non-Goals: change unrelated integrations or introduce new pricing rules.

## Decisions
- Decision: Cherry-pick selective commits, merge only cohesive branches.
- Decision: Use SQL Server [sankhya].SNK_GET_PRECO with GETDATE().
- Decision: Sync prices for all eligible tables (AD_TIPO_FAST/PRICE_TABLE_IDS).
- Decision: Implement batch price sync for all eligible tables.

## Risks / Trade-offs
- Risk: Unit conversion errors; mitigated by centralizing conversions and UI normalization.
- Risk: Multi-table expands load; mitigated by batching and queue limits.

## Migration Plan
- Apply schema migrations at deploy.
- Seed de-para mappings for price tables and stock mapping.

## Open Questions
- None.
