# Sankhya Native Convergence Roadmap

## Visão
Convergir o addon Fastchannel para comportamento nativo Sankhya de ponta a ponta, minimizando divergências com lançamentos feitos pela interface padrão.

## Onda 1 (concluída nesta entrega)
- Numeração oficial via `NumeracaoNotaHelper`
- Bridge unificada de invocação nativa de serviços (`ServiceInvoker` legado + `ServiceCaller` oficial)
- Documentação de engenharia reversa inicial

## Onda 2 (próxima execução)
**Alvo:** Pós-processamento de pedido

- Refatorar `OrderService.enforcePostImportParity` para priorizar:
  1. Helper/serviço nativo
  2. Jape update por entidade
  3. SQL apenas residual

### Status parcial da Onda 2 (2026-03-05)
- `TGFCAB` migrado para atualização nativa via Jape (`findByPK` + `prepareToUpdate`).
- `TGFITE` migrado para atualização nativa via Jape item a item (`ItemNota`), com SQL apenas de leitura auxiliar (`TGFEXC`/`TGFPRO`) para completar defaults.

- Revisar campos:
  - `CODVEND`, `AD_CODVENDEXEC`, `CIF_FOB`, `AD_MCAPORTAL`
  - `NUTAB`, `PRECOBASE`, `CUSTO`, `VLRCUS`, `USOPROD`

## Onda 3
**Alvo:** Normalização de autenticação e contexto

- Padronizar resolução de `MGEFrontFacade` para jobs/actions/listeners
- Instrumentar logs estruturados de falhas nativas (`service`, `module`, `status`, `fallback`) 

## Onda 4
**Alvo:** Observabilidade e testes de paridade

- Suite de cenários comparativos:
  - pedido via integração
  - pedido equivalente via interface
- Assertivas em TGFCAB/TGFITE para campos críticos

## Política de fallback
1. Nativo interno (obrigatório)
2. HTTP autenticado (último recurso)
3. Bloquear importação quando ambos falham, com mensagem diagnóstica completa

## Definição de pronto por onda
- Build Java compila sem warnings novos relevantes
- Publicação na AreaDev concluída
- Documento de mudanças técnicas atualizado
- Evidência de teste registrada (logs SQL/campos comparativos)
