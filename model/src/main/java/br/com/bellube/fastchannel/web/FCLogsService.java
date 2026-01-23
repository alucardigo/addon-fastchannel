package br.com.bellube.fastchannel.web;

import br.com.bellube.fastchannel.util.DBUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Service para gerenciamento de logs.
 */
public class FCLogsService {

    private static final Logger log = Logger.getLogger(FCLogsService.class.getName());

    public Map<String, Object> list(Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();
        Connection conn = null;
        PreparedStatement countStmt = null;
        PreparedStatement stmt = null;
        ResultSet rsCount = null;
        ResultSet rs = null;

        try {
            conn = DBUtil.getConnection();

            StringBuilder where = new StringBuilder("1=1");
            List<Object> queryParams = new ArrayList<>();

            String nivel = getString(params, "nivel");
            if (nivel != null && !nivel.isEmpty()) {
                where.append(" AND NIVEL = ?");
                queryParams.add(nivel);
            }

            String operacao = getString(params, "operacao");
            if (operacao != null && !operacao.isEmpty()) {
                where.append(" AND OPERACAO = ?");
                queryParams.add(operacao);
            }

            String dataInicio = getString(params, "dataInicio");
            if (dataInicio != null && !dataInicio.isEmpty()) {
                where.append(" AND DH_LOG >= ?");
                queryParams.add(dataInicio);
            }

            String dataFim = getString(params, "dataFim");
            if (dataFim != null && !dataFim.isEmpty()) {
                where.append(" AND DH_LOG <= ?");
                queryParams.add(dataFim + " 23:59:59");
            }

            String busca = getString(params, "busca");
            if (busca != null && !busca.isEmpty()) {
                where.append(" AND MENSAGEM LIKE ?");
                queryParams.add("%" + busca + "%");
            }

            int page = getInt(params, "page", 1);
            int pageSize = getInt(params, "pageSize", 50);
            int offset = (page - 1) * pageSize;

            // Count total
            String countSql = "SELECT COUNT(*) AS CNT FROM AD_FCLOG WHERE " + where;
            countStmt = conn.prepareStatement(countSql);
            setParameters(countStmt, queryParams);
            rsCount = countStmt.executeQuery();
            rsCount.next();
            int total = rsCount.getInt("CNT");

            // Get page - SQL Server syntax
            String sql = "SELECT IDLOG, DH_LOG, NIVEL, OPERACAO, REFERENCIA, MENSAGEM, STACKTRACE " +
                    "FROM AD_FCLOG WHERE " + where +
                    " ORDER BY DH_LOG DESC OFFSET ? ROWS FETCH NEXT ? ROWS ONLY";

            stmt = conn.prepareStatement(sql);
            int paramIndex = setParameters(stmt, queryParams);
            stmt.setInt(paramIndex++, offset);
            stmt.setInt(paramIndex, pageSize);
            rs = stmt.executeQuery();

            List<Map<String, Object>> logs = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> logItem = new HashMap<>();
                logItem.put("id", rs.getBigDecimal("IDLOG"));
                logItem.put("dhLog", rs.getTimestamp("DH_LOG"));
                logItem.put("nivel", rs.getString("NIVEL"));
                logItem.put("operacao", rs.getString("OPERACAO"));
                logItem.put("referencia", rs.getString("REFERENCIA"));
                logItem.put("mensagem", rs.getString("MENSAGEM"));
                logItem.put("stacktrace", rs.getString("STACKTRACE"));
                logs.add(logItem);
            }

            result.put("items", logs);
            result.put("total", total);
            result.put("page", page);
            result.put("pageSize", pageSize);

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao listar logs", e);
            result.put("error", e.getMessage());
        } finally {
            DBUtil.closeAll(rs, stmt, conn);
            DBUtil.closeResultSet(rsCount);
            DBUtil.closeStatement(countStmt);
        }

        return result;
    }

    public Map<String, Object> limpar(Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();
        Connection conn = null;
        PreparedStatement stmt = null;

        try {
            conn = DBUtil.getConnection();

            int dias = getInt(params, "dias", 30);
            Calendar cal = Calendar.getInstance();
            cal.add(Calendar.DAY_OF_MONTH, -dias);
            Timestamp limite = new Timestamp(cal.getTimeInMillis());

            stmt = conn.prepareStatement("DELETE FROM AD_FCLOG WHERE DH_LOG < ?");
            stmt.setTimestamp(1, limite);
            stmt.executeUpdate();

            result.put("success", true);
            result.put("message", "Logs anteriores a " + dias + " dias removidos com sucesso");

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao limpar logs", e);
            result.put("success", false);
            result.put("message", e.getMessage());
        } finally {
            DBUtil.closeAll(null, stmt, conn);
        }

        return result;
    }

    private int setParameters(PreparedStatement stmt, List<Object> params) throws Exception {
        int index = 1;
        for (Object param : params) {
            stmt.setObject(index++, param);
        }
        return index;
    }

    private String getString(Map<String, Object> params, String key) {
        Object value = params.get(key);
        return value != null ? value.toString() : null;
    }

    private int getInt(Map<String, Object> params, String key, int defaultValue) {
        Object value = params.get(key);
        if (value == null) return defaultValue;
        if (value instanceof Number) return ((Number) value).intValue();
        try { return Integer.parseInt(value.toString()); } catch (NumberFormatException e) { return defaultValue; }
    }
}
