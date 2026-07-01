package br.com.bellube.fastchannel.service;

import br.com.bellube.fastchannel.dto.PriceBatchItemDTO;
import br.com.bellube.fastchannel.util.DBUtil;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Resolve precos escalonados (batches) a partir de Descontos Promocionais (TGFDES + TGFDPQ).
 *
 * Consulta diretamente as tabelas de desconto por quantidade do Sankhya,
 * calculando o preco unitario com base no desconto percentual ou valor fixo.
 */
public class PriceBatchResolver {

    private static final Logger log = Logger.getLogger(PriceBatchResolver.class.getName());

    /**
     * Teto "infinito" para a maior faixa de escalonamento ("N e acima").
     * A FC exige MaximumBatchSize; usamos um valor alto o suficiente para cobrir qualquer
     * quantidade comercial real, mantendo a semantica de "a partir de N unidades".
     */
    private static final BigDecimal TOP_TIER_MAX = new BigDecimal("999999");

    /**
     * SQL que busca descontos por quantidade para um produto, cruzando com o preco base da tabela.
     *
     * - TGFDES: cabecalho de desconto promocional (NUPROMOCAO, CODPROD, datas, versao)
     * - TGFDPQ: faixas de quantidade (QTDE, PERCDESC, TIPDESC)
     * - SNK_GET_PRECO: funcao Sankhya que retorna o preco vigente para NUTAB/CODPROD
     *
     * Filtros:
     * - Promocao vigente (DTFINAL >= hoje) OU sem data final
     * - Versao mais recente da promocao (MAX NUVERSAO)
     * - Produto especifico (CODPROD = ?)
     */
    /**
     * Busca faixas de desconto por quantidade da promocao mais recente/ativa do produto.
     *
     * Quando ha multiplas promocoes (NUPROMOCAO) para o mesmo CODPROD, seleciona apenas
     * a mais relevante: prioriza promocoes vigentes (DTFINAL >= hoje ou sem data final),
     * depois pela mais recente (DTINICIAL DESC, NUPROMOCAO DESC).
     */
    /**
     * SQL corrigido 2026-04-24 - 2 bugs:
     *
     * BUG #1 - "Atua sobre a tabela de preco" ignorado:
     *   TGFDES.CODTAB define a tabela (CODTAB sankhya) a que o desconto se aplica.
     *   O SQL antigo NAO filtrava por CODTAB, entao um desconto da CODTAB=17 era
     *   enviado para TODAS as tabelas FC (3, 4, 24, 25, ...). Agora filtramos
     *   D.CODTAB vs o CODTAB da NUTAB atual via subquery em TGFTAB. Descontos com
     *   CODTAB NULL ou 0 sao considerados validos para todas as tabelas (comportamento
     *   padrao Sankhya quando a promocao nao especifica tabela).
     *
     * BUG #2 - Ranges invertidos (qtd zerada/sobreposta):
     *   Em TGFDPQ, Q.QTDE e a quantidade ATE (limite SUPERIOR da faixa) - bate com
     *   o label "Qtd até" na tela de Descontos Promocionais do Sankhya. O SQL antigo
     *   tratava Q.QTDE como PISO e calculava um Maximum artificial (MIN proximo - 1),
     *   gerando faixas como "1 a 5" e "6 a 99999" para faixas reais de "qtd até 1"
     *   e "qtd até 6". Correcao: Q.QTDE -> Maximum; Minimum = MAX anterior + 1
     *   (ou 1 se for a primeira faixa).
     */
    private static final String BATCH_SQL =
            "SELECT " +
            "  ISNULL(( " +
            "    SELECT MAX(Q2.QTDE) + 1 FROM TGFDPQ Q2 " +
            "    WHERE Q2.NUPROMOCAO = Q.NUPROMOCAO AND Q2.QTDE < Q.QTDE " +
            "  ), 1) AS MinimumBatchSize, " +
            "  Q.QTDE AS MaximumBatchSize, " +
            "  CASE " +
            "    WHEN Q.TIPDESC = 'P' THEN " +
            "      [sankhya].SNK_GET_PRECO(?, ?, GETDATE()) * (1 - Q.PERCDESC / 100.0) " +
            "    ELSE " +
            "      Q.PERCDESC " +
            "  END AS UnitaryPriceForBatch, " +
            "  CASE WHEN D.DTFINAL IS NOT NULL AND D.DTFINAL < CAST(GETDATE() AS DATE) " +
            "    THEN 'true' ELSE 'false' END AS BatchDisabled, " +
            "  D.NUPROMOCAO, " +
            "  D.DTINICIAL, " +
            "  D.DTFINAL, " +
            "  D.CODTAB, " +
            "  Q.TIPDESC, " +
            "  Q.PERCDESC " +
            "FROM TGFDES D " +
            "INNER JOIN TGFDPQ Q ON D.NUPROMOCAO = Q.NUPROMOCAO " +
            "WHERE D.CODPROD = ? " +
            "  AND ISNULL(D.NUVERSAO, 0) = ( " +
            "    SELECT MAX(ISNULL(S.NUVERSAO, 0)) FROM TGFDES S WHERE S.NUPROMOCAO = D.NUPROMOCAO " +
            "  ) " +
            "  AND (D.CODTAB IS NULL OR D.CODTAB = 0 " +
            "       OR D.CODTAB = (SELECT TOP 1 T.CODTAB FROM TGFTAB T WHERE T.NUTAB = ?)) " +
            "  AND D.NUPROMOCAO = ( " +
            "    SELECT TOP 1 D2.NUPROMOCAO FROM TGFDES D2 " +
            "    WHERE D2.CODPROD = D.CODPROD " +
            "      AND ISNULL(D2.NUVERSAO, 0) = ( " +
            "        SELECT MAX(ISNULL(S2.NUVERSAO, 0)) FROM TGFDES S2 WHERE S2.NUPROMOCAO = D2.NUPROMOCAO " +
            "      ) " +
            "      AND (D2.CODTAB IS NULL OR D2.CODTAB = 0 " +
            "           OR D2.CODTAB = (SELECT TOP 1 T2.CODTAB FROM TGFTAB T2 WHERE T2.NUTAB = ?)) " +
            "    ORDER BY CASE WHEN D2.DTFINAL IS NULL OR D2.DTFINAL >= CAST(GETDATE() AS DATE) THEN 0 ELSE 1 END, " +
            "             D2.DTINICIAL DESC, D2.NUPROMOCAO DESC " +
            "  ) " +
            "ORDER BY Q.QTDE";

    public List<PriceBatchItemDTO> resolve(BigDecimal codProd, BigDecimal nuTab, BigDecimal priceTableId) {
        if (codProd == null) return Collections.emptyList();

        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        List<PriceBatchItemDTO> items = new ArrayList<>();

        try {
            conn = DBUtil.getConnection();
            log.info("PriceBatchResolver: CODPROD=" + codProd + " NUTAB=" + nuTab + " priceTableId=" + priceTableId);
            stmt = conn.prepareStatement(BATCH_SQL);
            // Parametros:
            // 1 = NUTAB (SNK_GET_PRECO 1o arg)
            // 2 = CODPROD (SNK_GET_PRECO 2o arg)
            // 3 = CODPROD (WHERE D.CODPROD = ?)
            // 4 = NUTAB (subquery D.CODTAB = TGFTAB.CODTAB where NUTAB=?)
            // 5 = NUTAB (mesma subquery, usada dentro do TOP 1 promocao relevante)
            BigDecimal nuTabSafe = (nuTab != null) ? nuTab : BigDecimal.ZERO;
            stmt.setBigDecimal(1, nuTabSafe);
            stmt.setBigDecimal(2, codProd);
            stmt.setBigDecimal(3, codProd);
            stmt.setBigDecimal(4, nuTabSafe);
            stmt.setBigDecimal(5, nuTabSafe);

            rs = stmt.executeQuery();
            int rowCount = 0;
            while (rs.next()) {
                rowCount++;
                BigDecimal unitPrice = rs.getBigDecimal("UnitaryPriceForBatch");
                String disabled = rs.getString("BatchDisabled");
                Object dtFinal = rs.getObject("DTFINAL");
                Object dtInicial = rs.getObject("DTINICIAL");
                log.info("PriceBatchResolver row " + rowCount + ": unitPrice=" + unitPrice
                    + " qtde=" + rs.getBigDecimal("MinimumBatchSize")
                    + " tipdesc=" + rs.getString("TIPDESC")
                    + " percdesc=" + rs.getBigDecimal("PERCDESC")
                    + " BatchDisabled=" + disabled
                    + " DTINICIAL=" + dtInicial
                    + " DTFINAL=" + dtFinal);
                if (unitPrice == null || unitPrice.compareTo(BigDecimal.ZERO) <= 0) {
                    log.warning("Batch faixa ignorada: preco invalido " + unitPrice
                        + " CODPROD=" + codProd + " qtde=" + rs.getBigDecimal("MinimumBatchSize"));
                    continue;
                }

                // [BATCH-ACTIVATION-FIX] Nao incluir batches de promocoes expiradas na lista desejada.
                // Se a promocao expirou (BatchDisabled=true), o loop de delete em updatePriceBatches
                // ira REMOVER as faixas obsoletas do FC automaticamente.
                // Apenas promocoes vigentes (BatchDisabled=false) geram batches ATIVOS no FC.
                // INCIDENTE 2026-04-14: batches eram criados com BatchDisabled=true (inativo) porque
                // TGFDES.DTFINAL estava no passado — visivel no FC como "Escalonado" mas sem efeito.
                if ("true".equalsIgnoreCase(disabled)) {
                    log.info("Batch faixa ignorada: promocao expirada CODPROD=" + codProd
                        + " qtde=" + rs.getBigDecimal("MinimumBatchSize")
                        + " DTFINAL=" + dtFinal
                        + " — faixas FC existentes serao removidas no proximo sync.");
                    continue;
                }

                PriceBatchItemDTO dto = new PriceBatchItemDTO();
                dto.setPriceTableId(priceTableId);
                // [FC-INT-QTY-FIX 2026-05-26] TGFDPQ.QTDE e float no SQL => rs.getBigDecimal devolve
                // escala 1 ("1.0","3.0") e o gson serializa "1.0". A API FC armazena Min/Max como 0
                // quando recebe notacao decimal (confirmado: 2.0->0, 2->2). Forcamos escala 0 (inteiro)
                // para o gson emitir "1","3" e a FC gravar a quantidade correta.
                dto.setMinimumBatchSize(toIntScale(rs.getBigDecimal("MinimumBatchSize")));
                dto.setMaximumBatchSize(toIntScale(rs.getBigDecimal("MaximumBatchSize")));
                // FC API espera precos em centavos (mesma unidade que SalePrice/ListPrice)
                BigDecimal unitPriceCentavos = unitPrice.movePointRight(2).setScale(0, RoundingMode.HALF_UP);
                dto.setUnitaryPriceForBatch(unitPriceCentavos);
                dto.setBatchDisabled(false); // Garantia: apenas batches ativos chegam aqui
                items.add(dto);
            }
            log.info("PriceBatchResolver: CODPROD=" + codProd + " total rows=" + rowCount + " items=" + items.size());

            if (!items.isEmpty()) {
                // [TOP-TIER-OPEN 2026-05-25] "N e acima": num escalonamento com 2+ faixas, a maior faixa
                // representa "a partir de N unidades" (aberta). Em TGFDPQ a faixa e "Qtd ATE X" (limite
                // superior); sem este ajuste a ultima faixa (ex.: ate 6) viraria Min6-Max6 e quantidades
                // 7+ ficariam SEM preco escalonado, contrariando a semantica de escalonamento.
                // Aplicado apenas com 2+ faixas para nao alterar o caso de faixa unica (cap explicito).
                if (items.size() >= 2) {
                    PriceBatchItemDTO topTier = items.get(0);
                    for (PriceBatchItemDTO it : items) {
                        if (it.getMaximumBatchSize() != null && topTier.getMaximumBatchSize() != null
                                && it.getMaximumBatchSize().compareTo(topTier.getMaximumBatchSize()) > 0) {
                            topTier = it;
                        }
                    }
                    log.info("PriceBatchResolver: faixa topo CODPROD=" + codProd
                            + " Min=" + topTier.getMinimumBatchSize()
                            + " Max=" + topTier.getMaximumBatchSize() + " estendida para Max=" + TOP_TIER_MAX
                            + " (\"N e acima\")");
                    topTier.setMaximumBatchSize(TOP_TIER_MAX);
                }
                log.info("Batch pricing encontrado para CODPROD=" + codProd + ": " + items.size() + " faixa(s)");
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao resolver batches de preco para CODPROD=" + codProd, e);
            return Collections.emptyList();
        } finally {
            DBUtil.closeAll(rs, stmt, conn);
        }

        return items;
    }

    /** Normaliza quantidade para inteiro (escala 0) — a FC grava 0 se receber notacao decimal. */
    private static BigDecimal toIntScale(BigDecimal v) {
        return v == null ? null : v.setScale(0, RoundingMode.HALF_UP);
    }

    /**
     * Verifica rapidamente se um produto tem precos escalonados configurados,
     * sem calcular os valores. Util para decidir se mostra botao na UI.
     */
    public boolean hasBatches(BigDecimal codProd) {
        if (codProd == null) return false;

        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement(
                "SELECT TOP 1 1 FROM TGFDES D " +
                "INNER JOIN TGFDPQ Q ON D.NUPROMOCAO = Q.NUPROMOCAO " +
                "WHERE D.CODPROD = ? " +
                "  AND ISNULL(D.NUVERSAO, 0) = ( " +
                "    SELECT MAX(ISNULL(S.NUVERSAO, 0)) FROM TGFDES S WHERE S.NUPROMOCAO = D.NUPROMOCAO " +
                "  )");
            stmt.setBigDecimal(1, codProd);
            rs = stmt.executeQuery();
            return rs.next();
        } catch (Exception e) {
            log.log(Level.FINE, "Erro ao verificar batches para CODPROD=" + codProd, e);
            return false;
        } finally {
            DBUtil.closeAll(rs, stmt, conn);
        }
    }
}
