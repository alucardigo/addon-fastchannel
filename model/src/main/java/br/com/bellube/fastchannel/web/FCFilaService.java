package br.com.bellube.fastchannel.web;

import br.com.bellube.fastchannel.util.DBUtil;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Service para gerenciamento da fila de sincronizacao.
 */
public class FCFilaService {

    private static final Logger log = Logger.getLogger(FCFilaService.class.getName());

    public Map<String, Object> stats(Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();
        Connection conn = null;

        try {
            conn = DBUtil.getConnection();

            result.put("pending", countByStatus(conn, "PENDENTE"));
            result.put("processing", countByStatus(conn, "PROCESSANDO"));
            result.put("completed", countByStatus24h(conn, "CONCLUIDO"));
            result.put("error", countByStatus(conn, "ERRO"));

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao carregar stats", e);
            result.put("error", e.getMessage());
        } finally {
            DBUtil.closeConnection(conn);
        }

        return result;
    }

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

            String status = getString(params, "status");
            if (status != null && !status.isEmpty()) {
                where.append(" AND STATUS = ?");
                queryParams.add(status);
            }

            String tipo = getString(params, "tipo");
            if (tipo != null && !tipo.isEmpty()) {
                where.append(" AND TIPO_ENTIDADE = ?");
                queryParams.add(tipo);
            }

            String ref = getString(params, "ref");
            if (ref != null && !ref.isEmpty()) {
                where.append(" AND (REFERENCIA LIKE ? OR SKU LIKE ?)");
                queryParams.add("%" + ref + "%");
                queryParams.add("%" + ref + "%");
            }

            int page = getInt(params, "page", 1);
            int pageSize = getInt(params, "pageSize", 20);
            int offset = (page - 1) * pageSize;

            // Count total
            String countSql = "SELECT COUNT(*) AS CNT FROM AD_FCQUEUE WHERE " + where;
            countStmt = conn.prepareStatement(countSql);
            setParameters(countStmt, queryParams);
            rsCount = countStmt.executeQuery();
            rsCount.next();
            int total = rsCount.getInt("CNT");
            rsCount.close();
            countStmt.close();

            // Get page - SQL Server syntax
            String sql = "SELECT IDQUEUE, TIPO_ENTIDADE, REFERENCIA, SKU, STATUS, RETRY_COUNT, DH_CRIACAO, ULTIMO_ERRO " +
                    "FROM AD_FCQUEUE WHERE " + where +
                    " ORDER BY DH_CRIACAO DESC " +
                    " OFFSET ? ROWS FETCH NEXT ? ROWS ONLY";

            stmt = conn.prepareStatement(sql);
            int paramIndex = setParameters(stmt, queryParams);
            stmt.setInt(paramIndex++, offset);
            stmt.setInt(paramIndex, pageSize);

            rs = stmt.executeQuery();

            List<Map<String, Object>> fila = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> item = new HashMap<>();
                item.put("id", rs.getBigDecimal("IDQUEUE"));
                item.put("tipo", rs.getString("TIPO_ENTIDADE"));
                item.put("referencia", rs.getString("REFERENCIA"));
                item.put("sku", rs.getString("SKU"));
                item.put("status", rs.getString("STATUS"));
                item.put("tentativas", rs.getBigDecimal("RETRY_COUNT"));
                item.put("dhCriacao", rs.getTimestamp("DH_CRIACAO"));
                item.put("ultimoErro", rs.getString("ULTIMO_ERRO"));
                fila.add(item);
            }

            result.put("items", fila);
            result.put("total", total);
            result.put("page", page);
            result.put("pageSize", pageSize);

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao listar fila", e);
            result.put("error", e.getMessage());
        } finally {
            DBUtil.closeAll(rs, stmt, conn);
            DBUtil.closeResultSet(rsCount);
            DBUtil.closeStatement(countStmt);
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> reprocessar(Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();
        Connection conn = null;
        PreparedStatement stmt = null;

        try {
            Object idsObj = params.get("ids");
            List<Object> ids;
            if (idsObj instanceof List) {
                ids = (List<Object>) idsObj;
            } else {
                result.put("success", false);
                result.put("message", "ids obrigatorio");
                return result;
            }

            conn = DBUtil.getConnection();

            String sql = "UPDATE AD_FCQUEUE SET STATUS = 'PENDENTE', RETRY_COUNT = 0, ULTIMO_ERRO = NULL WHERE IDQUEUE = ?";
            stmt = conn.prepareStatement(sql);

            int count = 0;
            for (Object idObj : ids) {
                BigDecimal id;
                if (idObj instanceof Number) {
                    id = BigDecimal.valueOf(((Number) idObj).longValue());
                } else {
                    id = new BigDecimal(idObj.toString());
                }

                stmt.setBigDecimal(1, id);
                stmt.executeUpdate();
                count++;
            }

            result.put("success", true);
            result.put("message", count + " item(ns) marcado(s) para reprocessamento");

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao reprocessar", e);
            result.put("success", false);
            result.put("message", e.getMessage());
        } finally {
            DBUtil.closeAll(null, stmt, conn);
        }

        return result;
    }

    public Map<String, Object> limparErros(Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();
        Connection conn = null;
        PreparedStatement stmt = null;

        try {
            conn = DBUtil.getConnection();

            String sql = "DELETE FROM AD_FCQUEUE WHERE STATUS = 'ERRO' AND RETRY_COUNT >= 3";
            stmt = conn.prepareStatement(sql);
            stmt.executeUpdate();

            result.put("success", true);
            result.put("message", "Erros fatais removidos com sucesso");

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao limpar erros", e);
            result.put("success", false);
            result.put("message", e.getMessage());
        } finally {
            DBUtil.closeAll(null, stmt, conn);
        }

        return result;
    }

    private int countByStatus(Connection conn, String status) {
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement("SELECT COUNT(*) AS CNT FROM AD_FCQUEUE WHERE STATUS = ?");
            stmt.setString(1, status);
            rs = stmt.executeQuery();
            rs.next();
            return rs.getInt("CNT");
        } catch (Exception e) {
            return 0;
        } finally {
            DBUtil.closeResultSet(rs);
            DBUtil.closeStatement(stmt);
        }
    }

    private int countByStatus24h(Connection conn, String status) {
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement("SELECT COUNT(*) AS CNT FROM AD_FCQUEUE WHERE STATUS = ? AND DH_PROCESSAMENTO >= ?");
            stmt.setString(1, status);
            stmt.setTimestamp(2, new Timestamp(System.currentTimeMillis() - 24 * 60 * 60 * 1000));
            rs = stmt.executeQuery();
            rs.next();
            return rs.getInt("CNT");
        } catch (Exception e) {
            return 0;
        } finally {
            DBUtil.closeResultSet(rs);
            DBUtil.closeStatement(stmt);
        }
    }

    private int setParameters(PreparedStatement stmt, List<Object> params) throws Exception {
        int index = 1;
        for (Object param : params) {
            if (param instanceof String) {
                stmt.setString(index++, (String) param);
            } else if (param instanceof BigDecimal) {
                stmt.setBigDecimal(index++, (BigDecimal) param);
            } else if (param instanceof Integer) {
                stmt.setInt(index++, (Integer) param);
            } else if (param instanceof Long) {
                stmt.setLong(index++, (Long) param);
            } else if (param instanceof Timestamp) {
                stmt.setTimestamp(index++, (Timestamp) param);
            } else {
                stmt.setObject(index++, param);
            }
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
        try {
            return Integer.parseInt(value.toString());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
}
