package br.com.bellube.fastchannel.service;

import br.com.bellube.fastchannel.dto.PriceBatchItemDTO;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Resolve precos escalonados (batches) via SQL Server (legacy query).
 */
public class PriceBatchResolver {

    private static final Logger log = Logger.getLogger(PriceBatchResolver.class.getName());
    private static final String SQL_RESOURCE = "sql/fastchannel/SyncBatchPricesFastChannel.sql";

    public List<PriceBatchItemDTO> resolve(BigDecimal codProd, BigDecimal nuTab, BigDecimal priceTableId) {
        if (codProd == null || nuTab == null) return Collections.emptyList();

        String sqlText = loadSql();
        if (sqlText == null || sqlText.trim().isEmpty()) return Collections.emptyList();

        String finalSql = sqlText + " WHERE R.CODPROD = :codProd AND R.NUTAB = :nuTab";
        ResultSet rs = null;
        List<PriceBatchItemDTO> items = new ArrayList<>();

        try {
            JdbcWrapper jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql(finalSql);
            sql.setNamedParameter("codProd", codProd);
            sql.setNamedParameter("nuTab", nuTab);

            rs = sql.executeQuery();
            while (rs.next()) {
                BigDecimal listPrice = rs.getBigDecimal("ListPrice");
                if (listPrice == null) continue;

                PriceBatchItemDTO dto = new PriceBatchItemDTO();
                dto.setPriceTableId(priceTableId);
                dto.setMinimumBatchSize(rs.getBigDecimal("MinimumBatchSize"));
                dto.setMaximumBatchSize(rs.getBigDecimal("MaximumBatchSize"));
                dto.setUnitaryPriceForBatch(listPrice);
                dto.setBatchDisabled(parseBoolean(rs.getObject("BatchDisabled")));
                items.add(dto);
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao resolver batches de preco", e);
            return Collections.emptyList();
        } finally {
            closeQuietly(rs);
        }

        return items;
    }

    private String loadSql() {
        try (InputStream in = PriceBatchResolver.class.getClassLoader().getResourceAsStream(SQL_RESOURCE)) {
            if (in == null) {
                log.warning("SQL de batch nao encontrado: " + SQL_RESOURCE);
                return null;
            }
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                StringBuilder sb = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    sb.append(line).append('\n');
                }
                return sb.toString();
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao carregar SQL de batch", e);
            return null;
        }
    }

    private Boolean parseBoolean(Object value) {
        if (value == null) return null;
        if (value instanceof Boolean) return (Boolean) value;
        String raw = value.toString().trim();
        if (raw.isEmpty()) return null;
        return raw.equalsIgnoreCase("S") || raw.equalsIgnoreCase("TRUE") || raw.equals("1");
    }

    private void closeQuietly(ResultSet rs) {
        if (rs != null) {
            try { rs.close(); } catch (Exception ignored) {}
        }
    }
}
