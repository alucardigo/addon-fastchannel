# Fastchannel Price and Order Parity Plan

**Data:** 2026-04-08

## Contexto

O usuario reportou regressões funcionais em homologação e comportamento incompleto de sincronização entre Sankhya e Fastchannel:

- preço escalonado aparece na tela do addon, mas não sobe para a Fastchannel na sincronização;
- novas tabelas Fast mapeadas no de-para deixam de funcionar em cenários reais;
- o de-para permite repetir o mesmo `id fast` em mais de uma tabela Sankhya;
- preço e estoque precisam espelhar automaticamente o estado atual do Sankhya;
- frete com desconto ainda está sendo lançado com valor;
- número do pedido Fastchannel está indo para `OBSERVACAO`, quando deveria ir apenas para `OBSERVACAOINTERNA`;
- é necessário persistir `productDiscountCoupon` em `TGFCAB.AD_DESCONTO_FAST`;
- é necessário comparar os pedidos lançados hoje entre addon e app legado em produção.

## Evidências Confirmadas

1. `PriceService.syncPrice(...)` já envia o preço por tabela Fast correta e também chama `PriceBatchResolver` + `FastchannelPriceClient.updatePriceBatches(...)`.
2. `PriceService.syncPriceBatch(...)`, usado por fluxos em lote, não usa `resolveTableToNuTabMap()`, não sincroniza faixas escalonadas e não trata remoção/zeragem de itens fora da tabela atual.
3. `PriceFullSyncJob` depende de `syncPriceBatch(...)`; portanto o full sync atual perde exatamente o comportamento de escalonado e espelhamento.
4. `PrecoListener` só enfileira `UPDATE` simples e, em `DELETE`/inativação, hoje não garante zeragem/remoção do produto na tabela Fast correspondente.
5. `FCDeparaService.saveMappings(...)` e `DeparaService.setMapping(...)` aceitam duplicidade de `COD_EXTERNO` para `TABELA_PRECO`; não existe trava funcional nem restrição de banco para esse caso.
6. `OrderXmlBuilder` ainda usa `order.getShippingCost()` diretamente em `VLRFRETE` e inclui `Pedido Fastchannel: <id>` na observação pública.
7. `OrderService` também possui caminho nativo que grava `OBSERVACAO` pública com o id Fast e usa `shippingCost` bruto em `VLRFRETE`, apesar de já existir `getFrete(order)` com cálculo correto do frete líquido.
8. `OrderDTO` já expõe `productDiscountCoupon`, então a persistência adicional pode ser implementada sem alterar o contrato de entrada.

## Execução Obrigatória da Sessão

- Bootstrap superpowers executado.
- Ralph executado via:
  - `ralph --tool auto 1 --task "Analyze the current Fastchannel addon request in this worktree and return only a concise prioritized implementation checklist. Do not modify files. End with <promise>COMPLETE</promise>."`
- Resultado do Ralph não foi aceito como fonte de verdade para implementação porque ele interpretou parcialmente o escopo do worktree; serviu apenas como evidência do loop autônomo exigido.

## Plano de Implementação

1. Corrigir a sincronização de preços para trabalhar por tabela Fast efetiva, tanto em sync unitário quanto em sync em lote.
2. Fazer a sincronização espelhar exatamente a tabela Sankhya:
   - manter os produtos com preço atual;
   - zerar/remover produtos que saíram da tabela ativa correspondente;
   - publicar também as faixas escalonadas por tabela Fast.
3. Endurecer o de-para de `TABELA_PRECO`:
   - rejeitar duplicidade de `id fast` no backend;
   - validar também no frontend para feedback imediato;
   - manter suporte a qualquer quantidade de novas tabelas adicionadas.
4. Corrigir a criação do pedido:
   - aplicar frete líquido;
   - mover o identificador Fast apenas para observação interna;
   - adicionar `AD_DESCONTO_FAST` em `TGFCAB`;
   - persistir `productDiscountCoupon` no cabeçalho.
5. Executar validação local por build/testes direcionados e, se houver acesso configurado ao banco de produção, montar um comparativo addon vs legado para os pedidos de hoje.

## Riscos

- Espelhamento exato da tabela Fast pode exigir limpeza explícita de preços/faixas já existentes no lado Fastchannel; isso deve ser feito por tabela Fast, não globalmente por SKU.
- A trava de unicidade de `TABELA_PRECO` precisa ser compatível com os dados já existentes. Se houver duplicidade legada, a gravação nova deve falhar com mensagem clara em vez de sobrescrever silenciosamente.
- A comparação addon vs legado depende de conectividade e credenciais já configuradas no ambiente local.
