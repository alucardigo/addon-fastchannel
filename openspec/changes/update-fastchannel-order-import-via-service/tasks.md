## 1. Implementation
- [x] 1.1 Add AD_FCCONFIG flag for status sync enable/disable (DDL + config loader)
- [x] 1.2 Implement ServiceInvoker-based order inclusion with HTTP fallback
- [x] 1.3 Replace JAPE-based order insertion with service flow; ensure per-order error isolation
- [x] 1.4 Enforce "no product creation"; fail order if CODPROD not found
- [x] 1.5 Map legacy fields in XML (CODTIPVENDA, CODVEND, CODNAT, CODCENCUS, VLRFRETE, AD_NUMFAST)
- [x] 1.6 Block status/NF/tracking/sync when AD_FCCONFIG flag is disabled
- [x] 1.7 Update docs/tests (if any) for new flow
