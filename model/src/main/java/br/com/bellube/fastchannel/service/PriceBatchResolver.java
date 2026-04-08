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
    private static final String BATCH_SQL =
            "SELECT " +
            "  Q.QTDE AS MinimumBatchSize, " +
            "  ISNULL(( " +
            "    SELECT MIN(Q2.QTDE) FROM TGFDPQ Q2 " +
            "    WHERE Q2.NUPROMOCAO = Q.NUPROMOCAO AND Q2.QTDE > Q.QTDE " +
            "  ) - 1, 99999) AS MaximumBatchSize, " +
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
            "  Q.TIPDESC, " +
            "  Q.PERCDESC " +
            "FROM TGFDES D " +
            "INNER JOIN TGFDPQ Q ON D.NUPROMOCAO = Q.NUPROMOCAO " +
            "WHERE D.CODPROD = ? " +
            "  AND ISNULL(D.NUVERSAO, 0) = ( " +
            "    SELECT MAX(ISNULL(S.NUVERSAO, 0)) FROM TGFDES S WHERE S.NUPROMOCAO = D.NUPROMOCAO " +
            "  ) " +
            "  AND D.NUPROMOCAO = ( " +
            "    SELECT TOP 1 D2.NUPROMOCAO FROM TGFDES D2 " +
            "    WHERE D2.CODPROD = D.CODPROD " +
            "      AND ISNULL(D2.NUVERSAO, 0) = ( " +
            "        SELECT MAX(ISNULL(S2.NUVERSAO, 0)) FROM TGFDES S2 WHERE S2.NUPROMOCAO = D2.NUPROMOCAO " +
            "      ) " +
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
            // SNK_GET_PRECO parameters: NUTAB, CODPROD
            if (nuTab != null) {
                stmt.setBigDecimal(1, nuTab);
            } else {
                stmt.setBigDecimal(1, BigDecimal.ZERO);
            }
            stmt.setBigDecimal(2, codProd);
            // WHERE D.CODPROD = ?
            stmt.setBigDecimal(3, codProd);

            rs = stmt.executeQuery();
            int rowCount = 0;
            while (rs.next()) {
                rowCount++;
                BigDecimal unitPrice = rs.getBigDecimal("UnitaryPriceForBatch");
                log.info("PriceBatchResolver row " + rowCount + ": unitPrice=" + unitPrice
                    + " qtde=" + rs.getBigDecimal("MinimumBatchSize")
                    + " tipdesc=" + rs.getString("TIPDESC")
                    + " percdesc=" + rs.getBigDecimal("PERCDESC"));
                if (unitPrice == null || unitPrice.compareTo(BigDecimal.ZERO) <= 0) {
                    log.warning("Batch faixa ignorada: preco invalido " + unitPrice
                        + " CODPROD=" + codProd + " qtde=" + rs.getBigDecimal("MinimumBatchSize"));
                    continue;
                }

                PriceBatchItemDTO dto = new PriceBatchItemDTO();
                dto.setPriceTableId(priceTableId);
                dto.setMinimumBatchSize(rs.getBigDecimal("MinimumBatchSize"));
                dto.setMaximumBatchSize(rs.getBigDecimal("MaximumBatchSize"));
                // FC API espera precos em centavos (mesma unidade que SalePrice/ListPrice)
                BigDecimal unitPriceCentavos = unitPrice.movePointRight(2).setScale(0, RoundingMode.HALF_UP);
                dto.setUnitaryPriceForBatch(unitPriceCentavos);
                String disabled = rs.getString("BatchDisabled");
                dto.setBatchDisabled("true".equalsIgnoreCase(disabled));
                items.add(dto);
            }
            log.info("PriceBatchResolver: CODPROD=" + codProd + " total rows=" + rowCount + " items=" + items.size());

            if (!items.isEmpty()) {
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
