# Qodana Analysis Report - 2026-02-13

## Context
- Project: `X:\IntegracaoFastchannel\APPFASTCHANNEL\.worktrees\merge-unify`
- Qodana CLI: `2025.3.1`
- Linter: `Qodana Ultimate for JVM`
- Cloud report: `https://qodana.cloud/projects/kw4JL/reports/5ZnQEN`

## Executive Summary
- Total problems: `30`
- Severity split:
  - `High` (mapped as `warning`): `27`
  - `Moderate` (mapped as `note`): `3`
- Main categories:
  - `UnusedAssignment`: `13`
  - `ConstantValue`: `9`
  - `DataFlowIssue`: `5`
  - `WhileCanBeDoWhile`: `2`
  - `IfStatementWithIdenticalBranches`: `1`

## Findings By File/Line
| Rule | Severity | File | Line | Message |
|---|---|---|---:|---|
| ConstantValue | High | `model/src/main/java/br/com/bellube/fastchannel/auth/FastchannelTokenManager.java` | 195 | Condition 'snippet != null' is always 'true' |
| DataFlowIssue | High | `model/src/main/java/br/com/bellube/fastchannel/auth/FastchannelTokenManager.java` | 266 | Variable is already assigned to this value |
| ConstantValue | High | `model/src/main/java/br/com/bellube/fastchannel/http/FastchannelHttpClient.java` | 185 | Condition 'lastException != null' is always 'true' |
| UnusedAssignment | High | `model/src/main/java/br/com/bellube/fastchannel/listener/NotaFiscalListener.java` | 159 | The value '"Pedido aprovado"' assigned to 'message' is never used |
| UnusedAssignment | High | `model/src/main/java/br/com/bellube/fastchannel/listener/NotaFiscalListener.java` | 164 | The value '"Pedido pendente"' assigned to 'message' is never used |
| UnusedAssignment | High | `model/src/main/java/br/com/bellube/fastchannel/listener/NotaFiscalListener.java` | 169 | The value '"Pedido faturado"' assigned to 'message' is never used |
| UnusedAssignment | High | `model/src/main/java/br/com/bellube/fastchannel/listener/NotaFiscalListener.java` | 174 | The value '"Pedido cancelado"' assigned to 'message' is never used |
| IfStatementWithIdenticalBranches | Moderate | `model/src/main/java/br/com/bellube/fastchannel/service/DeparaService.java` | 189 | Common parts with variables can be extracted from 'if' |
| DataFlowIssue | High | `model/src/main/java/br/com/bellube/fastchannel/service/FastchannelHeaderMappingService.java` | 140 | Variable is already assigned to this value |
| DataFlowIssue | High | `model/src/main/java/br/com/bellube/fastchannel/service/FastchannelHeaderMappingService.java` | 141 | Variable is already assigned to this value |
| DataFlowIssue | High | `model/src/main/java/br/com/bellube/fastchannel/service/FastchannelHeaderMappingService.java` | 142 | Variable is already assigned to this value |
| UnusedAssignment | High | `model/src/main/java/br/com/bellube/fastchannel/service/OrderService.java` | 606 | Variable 'exists' initializer 'false' is redundant |
| UnusedAssignment | High | `model/src/main/java/br/com/bellube/fastchannel/service/QueueService.java` | 74 | Variable 'jdbc' initializer 'null' is redundant |
| UnusedAssignment | High | `model/src/main/java/br/com/bellube/fastchannel/service/QueueService.java` | 145 | Variable 'jdbc' initializer 'null' is redundant |
| UnusedAssignment | High | `model/src/main/java/br/com/bellube/fastchannel/service/QueueService.java` | 194 | Variable 'jdbc' initializer 'null' is redundant |
| UnusedAssignment | High | `model/src/main/java/br/com/bellube/fastchannel/service/QueueService.java` | 337 | Variable 'jdbc' initializer 'null' is redundant |
| UnusedAssignment | High | `model/src/main/java/br/com/bellube/fastchannel/service/QueueService.java` | 367 | Variable 'jdbc' initializer 'null' is redundant |
| UnusedAssignment | High | `model/src/main/java/br/com/bellube/fastchannel/service/QueueService.java` | 423 | Variable 'jdbc' initializer 'null' is redundant |
| ConstantValue | High | `model/src/main/java/br/com/bellube/fastchannel/service/SankhyaServiceInvoker.java` | 94 | Condition 'response == null' is always 'false' |
| ConstantValue | High | `model/src/main/java/br/com/bellube/fastchannel/service/strategy/HttpServiceStrategy.java` | 103 | Condition 'lastError != null' is always 'true' |
| ConstantValue | High | `model/src/main/java/br/com/bellube/fastchannel/service/strategy/HttpServiceStrategy.java` | 132 | Condition 'errorBody == null' is always 'false' |
| ConstantValue | High | `model/src/main/java/br/com/bellube/fastchannel/service/strategy/HttpServiceStrategy.java` | 135 | Condition 'errorBody != null' is always 'true' |
| ConstantValue | High | `model/src/main/java/br/com/bellube/fastchannel/service/strategy/HttpServiceStrategy.java` | 143 | Condition 'response == null' is always 'false' |
| ConstantValue | High | `model/src/main/java/br/com/bellube/fastchannel/service/strategy/InternalApiStrategy.java` | 53 | Condition 'facade != null' is always 'true' |
| ConstantValue | High | `model/src/main/java/br/com/bellube/fastchannel/web/FastchannelDirectServlet.java` | 185 | Condition 'obj instanceof Timestamp' is always 'false' |
| WhileCanBeDoWhile | Moderate | `model/src/main/java/br/com/bellube/fastchannel/web/FastchannelDirectServlet.java` | 252 | Replace 'while' with 'do while' |
| WhileCanBeDoWhile | Moderate | `model/src/main/java/br/com/bellube/fastchannel/web/FastchannelDirectServlet.java` | 256 | Replace 'while' with 'do while' |
| DataFlowIssue | High | `model/src/main/java/br/com/bellube/fastchannel/web/FastchannelDirectServlet.java` | 309 | Variable is already assigned to this value |
| UnusedAssignment | High | `model/src/main/java/br/com/bellube/fastchannel/web/FCConfigService.java` | 278 | The value changed at 'idx++' is never used |
| UnusedAssignment | High | `model/src/main/java/br/com/bellube/fastchannel/web/FCPrecosService.java` | 870 | Variable 'found' initializer 'false' is redundant |

## Risk Interpretation
- Highest bug-risk group: `ConstantValue` + `DataFlowIssue` in auth/http/strategy/servlet paths.
- Lowest risk group: `WhileCanBeDoWhile` and branch dedup (`IfStatementWithIdenticalBranches`) as readability refactors.
- Repetition hotspots:
  - `QueueService.java` (`jdbc = null` redundant initializer repeated 6 times)
  - `NotaFiscalListener.java` (`message` assignments never used in 4 status branches)
