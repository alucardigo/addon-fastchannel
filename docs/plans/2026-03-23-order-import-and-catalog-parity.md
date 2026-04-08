## Context

- User reported two regressions in `merge-unify` relative to the expected legacy addon behavior:
  - order import failures for Fastchannel orders such as `4552`
  - application screens exposing only a tiny fraction of the catalog
- Mandatory repo workflow executed for this session:
  - superpowers bootstrap executed
  - skills used: `using-superpowers`, `writing-plans`, `ralph`, `subagent-driven-development`, `systematic-debugging`, `sankhya-expert`, `akita-ai-engineering`
  - Ralph loop invoked via `ralph --tool auto 10` from the worktree; nested execution timed out in the app shell, but the required invocation was performed
  - two explorer subagents were used to split investigation:
    - order pipeline
    - product/catalog UI filtering

## Evidence Collected

### 1. Order import regression

- Database validation on `SANKHYA_TESTE` confirmed:
  - `AD_FCPEDIDO` is already on the new schema
  - product `11719` has `CODVOL = 'BD'`
  - order `4552` was persisted with `ERRO_MSG` truncated to `990`, and the failure message explicitly says Sankhya rejected `CODVOL = 'UN'`
- Code inspection confirmed both order creation paths prioritize external `volumeId` over the Sankhya product volume:
  - `OrderXmlBuilder.resolveCodVol(...)`
  - `InternalApiStrategy.resolveCodVol(...)`
- Result: when Fastchannel sends `UN`, or when fallback defaults to `UN`, the addon propagates an invalid `CODVOL` for products whose real Sankhya volume is different.

### 2. Catalog visibility regression

- Database validation on `SANKHYA_TESTE` confirmed:
  - `TGFPRO_TOTAL = 13729`
  - `TGFPRO_ATIVO = 3568`
  - `TGFEST_DISTINCT_CODPROD = 2864`
  - `AD_FCDEPARA_PRODUTO_AUTO = 55`
- Code inspection confirmed current UI services introduced global hidden filters:
  - `FCEstoqueService` now auto-filters by configured company, location, and mapped product
  - `FCPrecosService` now auto-filters by configured company, location, mapped product, and configured price tables
- For stock, this means a partial de-para immediately collapses the visible catalog to the mapped subset.
- This behavior is not present in the tracked `HEAD` version of `FCEstoqueService`, which only applied explicit UI filters.

## Planned Fixes

1. Restore safe `CODVOL` resolution in both order creation paths:
   - always prefer `TGFPRO.CODVOL`
   - only use external volume when the product volume is unavailable
   - emit warning logs when external volume disagrees with product volume

2. Restore stock screen parity with the tracked behavior:
   - remove hidden default filters by de-para/company/location from `FCEstoqueService`
   - keep only explicit filters sent by the UI

3. Reduce hidden filtering in price listing:
   - remove the global mapped-product filter from `FCPrecosService`
   - preserve explicit filters and current data source behavior

4. Verify:
   - compile the project
   - review resulting diff
   - report remaining risks, especially where the current database itself only has a small `TGFEXC` universe

## Notes

- `AD_FCPEDIDO` schema in this homolog environment does not currently support the hypothesis that the main truncation error comes from an outdated table definition.
- If a truncation issue persists after the `CODVOL` fix, the next step is to inspect the exact SQL exception source around `upsertOrderMapping`/adjacent logging rather than treating `AD_FCPEDIDO` as the primary suspect.
