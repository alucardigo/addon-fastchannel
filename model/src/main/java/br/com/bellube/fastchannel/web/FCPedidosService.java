package br.com.bellube.fastchannel.web;

import br.com.bellube.fastchannel.http.FastchannelOrdersClient;
import br.com.bellube.fastchannel.dto.OrderDTO;
import br.com.bellube.fastchannel.util.DBUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Service para gerenciamento de pedidos.
 */
public class FCPedidosService {

    private static final Logger log = Logger.getLogger(FCPedidosService.class.getName());

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
                where.append(" AND STATUS_IMPORT = ?");
                queryParams.add(status);
            }

            String orderId = getString(params, "orderId");
            if (orderId != null && !orderId.isEmpty()) {
                where.append(" AND ORDER_ID LIKE ?");
                queryParams.add("%" + orderId + "%");
            }

            String dataInicio = getString(params, "dataInicio");
            if (dataInicio != null && !dataInicio.isEmpty()) {
                where.append(" AND DH_PEDIDO >= ?");
                queryParams.add(dataInicio);
            }

            String dataFim = getString(params, "dataFim");
            if (dataFim != null && !dataFim.isEmpty()) {
                where.append(" AND DH_PEDIDO <= ?");
                queryParams.add(dataFim + " 23:59:59");
            }

            int page = getInt(params, "page", 1);
            int pageSize = getInt(params, "pageSize", 20);
            int offset = (page - 1) * pageSize;

            // Count total
            String countSql = "SELECT COUNT(*) AS CNT FROM AD_FCPEDIDO WHERE " + where;
            countStmt = conn.prepareStatement(countSql);
            setParameters(countStmt, queryParams);
            rsCount = countStmt.executeQuery();
            rsCount.next();
            int total = rsCount.getInt("CNT");

            // Get page - SQL Server syntax
            String sql = "SELECT ORDER_ID, DH_PEDIDO, NOME_CLIENTE, VALOR_TOTAL, STATUS_FC, STATUS_IMPORT, NUNOTA " +
                    "FROM AD_FCPEDIDO WHERE " + where +
                    " ORDER BY DH_IMPORTACAO DESC OFFSET ? ROWS FETCH NEXT ? ROWS ONLY";

            stmt = conn.prepareStatement(sql);
            int paramIndex = setParameters(stmt, queryParams);
            stmt.setInt(paramIndex++, offset);
            stmt.setInt(paramIndex, pageSize);
            rs = stmt.executeQuery();

            List<Map<String, Object>> pedidos = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> pedido = new HashMap<>();
                pedido.put("orderId", rs.getString("ORDER_ID"));
                pedido.put("dataPedido", rs.getTimestamp("DH_PEDIDO"));
                pedido.put("nomeCliente", rs.getString("NOME_CLIENTE"));
                pedido.put("valorTotal", rs.getBigDecimal("VALOR_TOTAL"));
                pedido.put("statusFc", rs.getString("STATUS_FC"));
                pedido.put("statusImport", rs.getString("STATUS_IMPORT"));
                pedido.put("nunota", rs.getBigDecimal("NUNOTA"));
                pedidos.add(pedido);
            }

            result.put("items", pedidos);
            result.put("total", total);
            result.put("page", page);
            result.put("pageSize", pageSize);

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao listar pedidos", e);
            result.put("error", e.getMessage());
        } finally {
            DBUtil.closeAll(rs, stmt, conn);
            DBUtil.closeResultSet(rsCount);
            DBUtil.closeStatement(countStmt);
        }

        return result;
    }

    public Map<String, Object> get(Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            String orderId = getString(params, "orderId");
            if (orderId == null || orderId.isEmpty()) {
                result.put("error", "orderId obrigatorio");
                return result;
            }

            conn = DBUtil.getConnection();

            stmt = conn.prepareStatement("SELECT * FROM AD_FCPEDIDO WHERE ORDER_ID = ?");
            stmt.setString(1, orderId);
            rs = stmt.executeQuery();

            if (rs.next()) {
                result.put("orderId", rs.getString("ORDER_ID"));
                result.put("dataPedido", rs.getTimestamp("DH_PEDIDO"));
                result.put("nomeCliente", rs.getString("NOME_CLIENTE"));
                result.put("cpfCnpj", rs.getString("CPF_CNPJ"));
                result.put("valorTotal", rs.getBigDecimal("VALOR_TOTAL"));
                result.put("frete", rs.getBigDecimal("VALOR_FRETE"));
                result.put("statusFc", rs.getString("STATUS_FC"));
                result.put("statusImport", rs.getString("STATUS_IMPORT"));
                result.put("nunota", rs.getBigDecimal("NUNOTA"));
                result.put("codparc", rs.getBigDecimal("CODPARC"));
                result.put("erro", rs.getString("ERRO_MSG"));
            } else {
                result.put("error", "Pedido nao encontrado");
            }

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao buscar pedido", e);
            result.put("error", e.getMessage());
        } finally {
            DBUtil.closeAll(rs, stmt, conn);
        }

        return result;
    }

    public Map<String, Object> reprocessar(Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();
        Connection conn = null;
        PreparedStatement checkStmt = null;
        PreparedStatement stmt = null;
        ResultSet rsCheck = null;

        try {
            String orderId = getString(params, "orderId");
            if (orderId == null || orderId.isEmpty()) {
                result.put("success", false);
                result.put("message", "orderId obrigatorio");
                return result;
            }

            conn = DBUtil.getConnection();

            // Verificar se pedido existe
            checkStmt = conn.prepareStatement("SELECT COUNT(*) AS CNT FROM AD_FCPEDIDO WHERE ORDER_ID = ?");
            checkStmt.setString(1, orderId);
            rsCheck = checkStmt.executeQuery();
            rsCheck.next();
            int exists = rsCheck.getInt("CNT");

            if (exists > 0) {
                stmt = conn.prepareStatement("UPDATE AD_FCPEDIDO SET STATUS_IMPORT = 'PENDENTE', ERRO_MSG = NULL WHERE ORDER_ID = ?");
                stmt.setString(1, orderId);
                stmt.executeUpdate();

                result.put("success", true);
                result.put("message", "Pedido marcado para reprocessamento");
            } else {
                result.put("success", false);
                result.put("message", "Pedido nao encontrado");
            }

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao reprocessar pedido", e);
            result.put("success", false);
            result.put("message", e.getMessage());
        } finally {
            DBUtil.closeAll(rsCheck, checkStmt, conn);
            DBUtil.closeStatement(stmt);
        }

        return result;
    }

    public Map<String, Object> consultarFC(Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();

        try {
            String orderId = getString(params, "orderId");
            if (orderId == null || orderId.isEmpty()) {
                result.put("error", "orderId obrigatorio");
                return result;
            }

            FastchannelOrdersClient client = new FastchannelOrdersClient();
            OrderDTO order = client.getOrder(orderId);

            if (order != null) {
                result.put("orderId", order.getOrderId());
                result.put("status", order.getStatus());
                result.put("statusDescription", order.getStatusDescription());
                result.put("total", order.getTotal());
                result.put("shippingCost", order.getShippingCost());
                result.put("createdAt", order.getCreatedAt());
                result.put("paymentMethod", order.getPaymentMethod());

                if (order.getCustomer() != null) {
                    result.put("customerName", order.getCustomer().getName());
                    result.put("customerDocument", order.getCustomer().getCpfCnpj());
                }
            } else {
                result.put("error", "Pedido nao encontrado no Fastchannel");
            }

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao consultar FC", e);
            result.put("error", e.getMessage());
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
