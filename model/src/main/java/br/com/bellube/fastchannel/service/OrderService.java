package br.com.bellube.fastchannel.service;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.config.FastchannelConstants;
import br.com.bellube.fastchannel.dto.*;
import br.com.bellube.fastchannel.http.FastchannelOrdersClient;
import br.com.bellube.fastchannel.util.DBUtil;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.core.JapeSession;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.wrapper.fluid.FluidCreateVO;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Servico de Importacao de Pedidos do Fastchannel para Sankhya.
 *
 * Processo:
 * 1. Buscar pedidos pendentes da API
 * 2. Localizar ou criar parceiro (TGFPAR)
 * 3. Criar cabecalho do pedido (TGFCAB)
 * 4. Criar itens (TGFITE)
 * 5. Registrar na AD_FCPEDIDO
 * 6. Notificar Fastchannel (marcar como sincronizado)
 */
public class OrderService {

    private static final Logger log = Logger.getLogger(OrderService.class.getName());
    private static final String STATUS_IMPORT_PENDENTE = FastchannelConstants.ORDER_IMPORT_STATUS_PENDENTE;
    private static final String STATUS_IMPORT_PROCESSANDO = FastchannelConstants.ORDER_IMPORT_STATUS_PROCESSANDO;
    private static final String STATUS_IMPORT_SUCESSO = FastchannelConstants.ORDER_IMPORT_STATUS_SUCESSO;
    private static final String STATUS_IMPORT_ERRO = FastchannelConstants.ORDER_IMPORT_STATUS_ERRO;
    private static final int ORDER_IMPORT_CLAIM_TIMEOUT_MINUTES =
            FastchannelConstants.DEFAULT_ORDER_IMPORT_CLAIM_TIMEOUT_MINUTES;
    private static volatile OrderMappingFieldSizes cachedOrderMappingFieldSizes;

    private final FastchannelConfig config;
    private final FastchannelOrdersClient ordersClient;
    private final DeparaService deparaService;
    private final LogService logService;
    private final br.com.bellube.fastchannel.service.strategy.OrderCreationOrchestrator orchestrator;
    private final FastchannelHeaderMappingService headerMappingService;

    public OrderService() {
        this.config = FastchannelConfig.getInstance();
        this.ordersClient = new FastchannelOrdersClient();
        this.deparaService = DeparaService.getInstance();
        this.logService = LogService.getInstance();
        this.orchestrator = new br.com.bellube.fastchannel.service.strategy.OrderCreationOrchestrator();
        this.headerMappingService = new FastchannelHeaderMappingService();
    }

    /**
     * Importa pedidos pendentes do Fastchannel.
     *
     * @return numero de pedidos importados com sucesso
     */
    public int importPendingOrders() {
        int imported = 0;
        int pageSize = config.getBatchSize();

        // Order import REQUER JAPE para criar notas no Sankhya.
        // Skip graceful se JAPE ainda nao inicializou.
        if (!isJapeReady()) {
            log.fine("OrderService: JAPE/mge-core indisponivel. Importacao de pedidos sera tentada no proximo ciclo.");
            return 0;
        }

        try {
            Timestamp lastSync = config.getLastOrderSync();
            log.info("Iniciando importacao de pedidos. ultima sync: " + lastSync);

            OrderImportBatchProgress progress = importPendingOrdersFromCursor(lastSync, pageSize);
            imported += progress.imported;

            if (progress.hasNewCursor(lastSync)) {
                config.updateLastOrderSync(progress.nextCursor);
                log.info("Cursor LAST_ORDER_SYNC atualizado para " + progress.nextCursor
                        + " apos varrer " + progress.totalOrdersSeen + " pedido(s) da API.");
            }

            // Nenhum fallback sem cursor - evita reimportacao massiva de pedidos historicos.
            if (imported == 0 && lastSync != null && !progress.hasNewCursor(lastSync)) {
                log.info("Nenhum pedido novo encontrado desde " + lastSync + ".");
            }

            log.info("Importacao conclu?da. " + imported + " pedidos importados.");

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro na importacao de pedidos", e);
            logService.error(LogService.OP_ORDER_IMPORT, "Erro geral na importacao", e);
        }

        return imported;
    }

    private OrderImportBatchProgress importPendingOrdersFromCursor(Timestamp lastSync, int pageSize) {
        int imported = 0;
        int page = 1;
        Timestamp nextCursor = lastSync;
        int totalOrdersSeen = 0;

        while (true) {
            List<OrderDTO> orders;
            try {
                orders = ordersClient.listOrders(lastSync, page, pageSize);
            } catch (Exception e) {
                log.log(Level.SEVERE, "Erro ao listar pedidos na API (page=" + page + ", lastSync=" + lastSync + ")", e);
                break;
            }

            if (orders == null || orders.isEmpty()) {
                break;
            }
            totalOrdersSeen += orders.size();

            for (OrderDTO order : orders) {
                OrderDTO target = order;
                nextCursor = maxTimestamp(nextCursor, order != null ? order.getCreatedAt() : null);
                try {
                    if (order.getCurrentStatusTypeId() != null && order.getCurrentStatusTypeId().intValue() == 2) {
                        log.info("Pedido " + order.getOrderId() + " ignorado por CurrentStatusTypeId=2 (nao elegivel para importacao).");
                        continue;
                    }

                    if (isOrderAlreadyImported(order.getOrderId())) {
                        log.fine("Pedido " + order.getOrderId() + " ja importado [v2]. Pulando.");
                        continue;
                    }

                    OrderDTO detailed = null;
                    try {
                        detailed = ordersClient.getOrder(order.getOrderId());
                    } catch (Exception e) {
                        log.log(Level.WARNING, "Falha ao buscar detalhes do pedido " + order.getOrderId() + ". Usando dados da listagem.", e);
                    }

                    target = detailed != null ? detailed : order;
                    nextCursor = maxTimestamp(nextCursor, target != null ? target.getCreatedAt() : null);
                    if (target.getOrderId() == null) {
                        target.setOrderId(order.getOrderId());
                    }
                    if (target.getResellerId() == null) {
                        target.setResellerId(order.getResellerId());
                    }

                    BigDecimal nuNota = importOrder(target);
                    if (nuNota != null) {
                        imported++;
                        logService.logOrderImport(target.getOrderId(), nuNota, true, null);

                        // Notificar Fastchannel (somente se sincronizacao estiver habilitada)
                        if (config.isSyncStatusEnabled()) {
                            ordersClient.markAsSynced(target.getOrderId(), nuNota.toString());
                        } else {
                            log.fine("Sincronizacao desabilitada. Pedido " + target.getOrderId() + " nao marcado como synced.");
                        }
                    }

                } catch (Exception e) {
                    Level errorLevel = isJapeUnavailableError(e) ? Level.FINE : Level.SEVERE;
                    log.log(errorLevel, "Erro ao importar pedido " + target.getOrderId(), e);
                    logService.logOrderImport(target.getOrderId(), null, false,
                            "Cliente=" + (target.getCustomer() != null ? String.valueOf(target.getCustomer().getName()) : "") +
                            " | CPF/CNPJ=" + (target.getCustomer() != null ? String.valueOf(target.getCustomer().getCpfCnpj()) : "") +
                            " | Erro=" + buildErrorDetails(e));
                }
            }

            // Se retornou menos que pageSize, nao ha mais paginas
            if (orders.size() < pageSize) {
                break;
            }
            page++;
        }

        return new OrderImportBatchProgress(imported, nextCursor, totalOrdersSeen);
    }

    /**
     * Importa um pedido especifico usando o servico nativo do Sankhya.
     *
     * @param order dados do pedido
     * @return NUNOTA criado ou null se falhar
     */
    public BigDecimal importOrder(OrderDTO order) throws Exception {
        log.info("Importando pedido: " + order.getOrderId());

        // Validar pedido
        validateOrder(order);
        normalizeOrderValues(order);

        OrderImportClaim claim = null;
        BigDecimal codParc = null;
        try {
            claim = claimOrderImport(order);
            if (claim.hasExistingNuNota()) {
                log.info("Pedido " + order.getOrderId() + " ja mapeado como NUNOTA " + claim.getExistingNuNota() + ". Reuso idempotente.");
                return claim.getExistingNuNota();
            }
            if (claim.isAlreadyProcessing()) {
                log.info("Pedido " + order.getOrderId() + " ja esta em processamento por outra execucao. Ignorando tentativa concorrente.");
                return null;
            }

            FastchannelHeaderMappingService.ResolvedHeader resolvedHeader = headerMappingService.resolve(order);
            order.setCodEmp(resolvedHeader.getCodEmp());
            order.setCodTipOper(resolvedHeader.getCodTipOper());
            order.setCodLocal(resolveCodLocalForOrder(order, resolvedHeader.getCodEmp()));

            // 1. Localizar ou criar parceiro
            codParc = resolvedHeader.getCodParc();
            if (codParc == null && order.getCustomer() != null) {
                codParc = findOrCreateParceiro(order.getCustomer(), order.getShippingAddress());
            }
            if (codParc == null) {
                codParc = getDefaultCodParc();
                log.warning("Pedido " + order.getOrderId() + ": parceiro nao resolvido por CNPJ, usando fallback CODPARC=" + codParc);
            }

            // 2. Validar que todos os produtos existem ANTES de criar o pedido
            validateAllProductsExist(order);

            // 3. Buscar parametros do pedido
            BigDecimal codTipVenda = resolvedHeader.getCodTipVenda();
            BigDecimal codVend = getCodVend(codParc);
            if (isNullOrZero(codVend)) {
                // Parceiro sem vendedor preferencial: usar CODVEND padrao da config
                codVend = config.getCodVendPadrao();
                if (isNullOrZero(codVend)) {
                    codVend = FastchannelConstants.DEFAULT_CODVEND_PADRAO;
                }
                log.info("Parceiro " + codParc + " sem CODVEND. Usando padrao: " + codVend);
                // Atualizar CODVEND no parceiro para futuras importacoes
                setCodVendParceiro(codParc, codVend);
            }
            BigDecimal codNat = resolvedHeader.getCodNat();
            BigDecimal codCenCus = resolvedHeader.getCodCenCus();

            // 4. Criar pedido via servico
            BigDecimal nuNota = importOrderViaService(order, codParc, codTipVenda, codVend, codNat, codCenCus);
            enforcePostImportParity(nuNota, order, codVend, resolvedHeader.getCodEmp());

            // 5. Registrar na AD_FCPEDIDO
            upsertOrderMapping(order, nuNota, codParc, STATUS_IMPORT_SUCESSO, null);

            log.info("Pedido " + order.getOrderId() + " importado como NUNOTA " + nuNota);
            return nuNota;

        } catch (Exception e) {
            if (order != null && !isBlank(order.getOrderId())) {
                try {
                    upsertOrderMapping(order, null, codParc, STATUS_IMPORT_ERRO, buildErrorDetails(e));
                } catch (Exception upsertEx) {
                    log.log(Level.SEVERE, "FALHA DUPLA: nao foi possivel marcar pedido " +
                            order.getOrderId() + " como ERRO na AD_FCPEDIDO. " +
                            "Pedido pode ficar travado em PROCESSANDO. " +
                            "Tentando UPDATE direto como ultimo recurso.", upsertEx);
                    forceStatusErro(order.getOrderId(), buildErrorDetails(e));
                }
            }
            log.log(Level.SEVERE, "Falha ao importar pedido " +
                    (order != null ? order.getOrderId() : "null"), e);
            throw e;
        }
    }

    private String buildErrorDetails(Throwable error) {
        if (error == null) {
            return "Erro sem detalhe";
        }
        StringBuilder sb = new StringBuilder();
        Throwable current = error;
        int depth = 0;
        while (current != null && depth < 6) {
            if (depth > 0) {
                sb.append(" <- ");
            }
            String msg = current.getMessage();
            sb.append(current.getClass().getSimpleName());
            if (msg != null && !msg.trim().isEmpty()) {
                sb.append(": ").append(msg.trim());
            }
            current = current.getCause();
            depth++;
        }
        return sb.toString();
    }

    private OrderImportClaim claimOrderImport(OrderDTO order) throws Exception {
        if (order == null || isBlank(order.getOrderId())) {
            return OrderImportClaim.claimed();
        }

        JdbcWrapper jdbc = null;
        try {
            jdbc = openJdbc();
            try {
                insertOrderMapping(jdbc, order, null, null, STATUS_IMPORT_PROCESSANDO, null);
                return OrderImportClaim.claimed();
            } catch (Exception e) {
                if (!isOrderMappingUniqueViolation(e)) {
                    throw e;
                }
            }

            OrderMappingSnapshot existing = loadOrderMapping(jdbc, order.getOrderId());
            if (existing == null) {
                throw new Exception("Registro de idempotencia do pedido " + order.getOrderId() + " nao pode ser carregado apos colisao da UK_FCPEDIDO_ORDERID.");
            }

            if (!isNullOrZero(existing.nuNota)) {
                return OrderImportClaim.reused(existing.nuNota);
            }

            if (isActiveProcessingClaim(existing)) {
                return OrderImportClaim.alreadyProcessing();
            }

            if (takeOverOrderMappingClaim(jdbc, order)) {
                return OrderImportClaim.claimed();
            }

            OrderMappingSnapshot reloaded = loadOrderMapping(jdbc, order.getOrderId());
            if (reloaded != null && !isNullOrZero(reloaded.nuNota)) {
                return OrderImportClaim.reused(reloaded.nuNota);
            }
            if (reloaded != null && isActiveProcessingClaim(reloaded)) {
                return OrderImportClaim.alreadyProcessing();
            }

            throw new Exception("Nao foi possivel assumir o processamento idempotente do pedido " + order.getOrderId() + ".");
        } finally {
            closeJdbc(jdbc);
        }
    }

    private OrderMappingSnapshot loadOrderMapping(JdbcWrapper jdbc, String orderId) throws Exception {
        if (jdbc == null || isBlank(orderId)) {
            return null;
        }
        NativeSql sql = new NativeSql(jdbc);
        ResultSet rs = null;
        try {
            sql.appendSql("SELECT TOP 1 ORDER_ID, NUNOTA, CODPARC, STATUS_IMPORT, DH_IMPORTACAO ");
            sql.appendSql("FROM AD_FCPEDIDO ");
            sql.appendSql("WHERE ORDER_ID = :orderId");
            sql.setNamedParameter("orderId", orderId);
            rs = sql.executeQuery();
            if (!rs.next()) {
                return null;
            }
            return new OrderMappingSnapshot(
                    trimToNull(rs.getString("ORDER_ID")),
                    rs.getBigDecimal("NUNOTA"),
                    rs.getBigDecimal("CODPARC"),
                    trimToNull(rs.getString("STATUS_IMPORT")),
                    rs.getTimestamp("DH_IMPORTACAO"));
        } finally {
            closeQuietly(rs);
        }
    }

    private void insertOrderMapping(JdbcWrapper jdbc, OrderDTO order, BigDecimal nuNota, BigDecimal codParc,
                                    String statusImport, String errorMsg) throws Exception {
        if (jdbc == null || order == null || isBlank(order.getOrderId())) {
            return;
        }

        NativeSql sql = new NativeSql(jdbc);
        sql.appendSql("INSERT INTO AD_FCPEDIDO ");
        sql.appendSql("(ORDER_ID, NUNOTA, CODPARC, STATUS_FC, STATUS_SKW, STATUS_IMPORT, DH_IMPORTACAO, ");
        sql.appendSql("DH_PEDIDO, NOME_CLIENTE, CPF_CNPJ, VALOR_TOTAL, VALOR_FRETE, ERRO_MSG) ");
        sql.appendSql("VALUES (:orderId, :nuNota, :codParc, :statusFc, :statusSkw, :statusImport, CURRENT_TIMESTAMP, ");
        sql.appendSql(":dhPedido, :nomeCliente, :cpfCnpj, :valorTotal, :valorFrete, :erroMsg)");

        bindOrderMappingParameters(jdbc, sql, order, nuNota, codParc, statusImport, errorMsg);
        sql.executeUpdate();
    }

    private boolean takeOverOrderMappingClaim(JdbcWrapper jdbc, OrderDTO order) throws Exception {
        if (jdbc == null || order == null || isBlank(order.getOrderId())) {
            return false;
        }

        Timestamp staleCutoff = new Timestamp(System.currentTimeMillis() - (ORDER_IMPORT_CLAIM_TIMEOUT_MINUTES * 60L * 1000L));
        NativeSql sql = new NativeSql(jdbc);
        // NUNOTA zerado porque sera re-criado; CODPARC preservado para rastreabilidade operacional
        sql.appendSql("UPDATE AD_FCPEDIDO SET ");
        sql.appendSql("NUNOTA = NULL, STATUS_FC = :statusFc, STATUS_SKW = :statusSkw, ");
        sql.appendSql("STATUS_IMPORT = :statusImport, DH_IMPORTACAO = CURRENT_TIMESTAMP, ");
        sql.appendSql("DH_PEDIDO = :dhPedido, NOME_CLIENTE = COALESCE(:nomeCliente, NOME_CLIENTE), ");
        sql.appendSql("CPF_CNPJ = COALESCE(:cpfCnpj, CPF_CNPJ), ");
        sql.appendSql("VALOR_TOTAL = :valorTotal, VALOR_FRETE = :valorFrete, ERRO_MSG = NULL ");
        sql.appendSql("WHERE ORDER_ID = :orderId ");
        sql.appendSql("AND NUNOTA IS NULL ");
        sql.appendSql("AND (UPPER(COALESCE(STATUS_IMPORT, '')) IN ('ERRO', 'PENDENTE', '') ");
        sql.appendSql("OR (UPPER(COALESCE(STATUS_IMPORT, '')) = 'PROCESSANDO' ");
        sql.appendSql("AND (DH_IMPORTACAO IS NULL OR DH_IMPORTACAO < :staleCutoff)))");

        bindOrderMappingParameters(jdbc, sql, order, null, null, STATUS_IMPORT_PROCESSANDO, null);
        sql.setNamedParameter("staleCutoff", staleCutoff);
        boolean tookOver = sql.executeUpdate();
        if (tookOver) {
            log.info("Pedido " + order.getOrderId() + " retomado por claim vencido (timeout=" + ORDER_IMPORT_CLAIM_TIMEOUT_MINUTES + "min). Reiniciando processamento.");
        }
        return tookOver;
    }

    private void bindOrderMappingParameters(JdbcWrapper jdbc, NativeSql sql, OrderDTO order, BigDecimal nuNota, BigDecimal codParc,
                                            String statusImport, String errorMsg) throws Exception {
        if (sql == null || order == null) {
            return;
        }
        OrderMappingFieldSizes fieldSizes = getOrderMappingFieldSizes();
        int statusFc = order.getStatus() > 0 ? order.getStatus() : FastchannelConstants.STATUS_APPROVED;
        sql.setNamedParameter("orderId", truncateToColumn(order.getOrderId(), fieldSizes.orderId));
        sql.setNamedParameter("nuNota", nuNota);
        sql.setNamedParameter("codParc", codParc);
        sql.setNamedParameter("statusFc", statusFc);
        sql.setNamedParameter("statusSkw", truncateToColumn(!isNullOrZero(nuNota) ? "L" : null, fieldSizes.statusSkw));
        sql.setNamedParameter("statusImport", truncateToColumn(statusImport, fieldSizes.statusImport));
        sql.setNamedParameter("dhPedido", order.getCreatedAt() != null ? order.getCreatedAt() : new Timestamp(System.currentTimeMillis()));
        sql.setNamedParameter("nomeCliente", order.getCustomer() != null
                ? truncateToColumn(order.getCustomer().getName(), fieldSizes.nomeCliente)
                : null);
        sql.setNamedParameter("cpfCnpj", order.getCustomer() != null
                ? truncateToColumn(order.getCustomer().getCpfCnpj(), fieldSizes.cpfCnpj)
                : null);
        sql.setNamedParameter("valorTotal", order.getTotal());
        sql.setNamedParameter("valorFrete", getFrete(order));
        sql.setNamedParameter("erroMsg", truncateToColumn(errorMsg, fieldSizes.erroMsg));
    }

    private boolean isActiveProcessingClaim(OrderMappingSnapshot snapshot) {
        if (snapshot == null) {
            return false;
        }
        if (!STATUS_IMPORT_PROCESSANDO.equalsIgnoreCase(trimToNull(snapshot.statusImport))) {
            return false;
        }
        if (snapshot.dhImportacao == null) {
            return true;
        }
        long ageMillis = System.currentTimeMillis() - snapshot.dhImportacao.getTime();
        return ageMillis < (ORDER_IMPORT_CLAIM_TIMEOUT_MINUTES * 60L * 1000L);
    }

    private boolean isOrderMappingUniqueViolation(Throwable error) {
        Throwable current = error;
        while (current != null) {
            String message = trimToNull(current.getMessage());
            if (message != null) {
                String normalized = message.toUpperCase();
                if (normalized.contains("UK_FCPEDIDO_ORDERID")
                        || normalized.contains("UNIQUE")
                        || normalized.contains("DUPLICATE")
                        || normalized.contains("VIOLACAO")) {
                    return true;
                }
            }
            if (current instanceof java.sql.SQLException) {
                String state = ((java.sql.SQLException) current).getSQLState();
                if ("23000".equals(state) || "23505".equals(state)) {
                    return true;
                }
            }
            current = current.getCause();
        }
        return false;
    }

    private BigDecimal findExistingMappedNuNota(JdbcWrapper jdbc, String orderId) {
        if (jdbc == null || isBlank(orderId)) {
            return null;
        }
        ResultSet rs = null;
        try {
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT TOP 1 NUNOTA ");
            sql.appendSql("FROM AD_FCPEDIDO ");
            sql.appendSql("WHERE ORDER_ID = :orderId ");
            sql.appendSql("AND NUNOTA IS NOT NULL ");
            sql.appendSql("ORDER BY DH_IMPORTACAO DESC");
            sql.setNamedParameter("orderId", orderId);
            rs = sql.executeQuery();
            if (rs.next()) {
                BigDecimal nuNota = rs.getBigDecimal("NUNOTA");
                if (!isNullOrZero(nuNota)) {
                    return nuNota;
                }
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Falha ao verificar mapeamento existente para pedido " + orderId, e);
        } finally {
            closeQuietly(rs);
        }
        return null;
    }

    /**
     * Valida que todos os produtos do pedido existem no sistema.
     * Falha rapido se algum produto nao existir.
     */
    private void validateAllProductsExist(OrderDTO order) throws Exception {
        for (OrderItemDTO item : order.getItems()) {
            BigDecimal codProd = deparaService.resolveCodProdForOrderItem(item);
            if (codProd == null) {
                throw new Exception("Produto nao encontrado para SKU: " + item.getSku() +
                                    ". Criacao de produto a partir do Fastchannel nao e permitida.");
            }
        }
    }

    /**
     * Importa pedido usando orquestrador de estrategias com fallback automatico.
     * Tenta: 1) ServiceInvoker, 2) HTTP, 3) API Interna (somente se habilitado por flag)
     */
    private BigDecimal importOrderViaService(OrderDTO order, BigDecimal codParc,
                                             BigDecimal codTipVenda, BigDecimal codVend,
                                             BigDecimal codNat, BigDecimal codCenCus) throws Exception {

        // Usar orquestrador que tenta estrategias automaticamente
        return orchestrator.createOrder(order, codParc, codTipVenda, codVend, codNat, codCenCus);
    }

    private void enforcePostImportParity(BigDecimal nuNota, OrderDTO order, BigDecimal codVend, BigDecimal codEmp) {
        if (nuNota == null) {
            return;
        }

        JapeSession.SessionHandle hnd = null;
        try {
            hnd = JapeSession.open();
            hnd.execWithTX(new JapeSession.TXBlock() {
                @Override
                public void doWithTx() throws Exception {
                    JdbcWrapper jdbc = null;
                    try {
                        jdbc = openJdbc();

                        BigDecimal vendFinal = codVend;
                        if (vendFinal == null || vendFinal.compareTo(BigDecimal.ZERO) <= 0) {
                            vendFinal = config.getCodVendPadrao();
                        }
                        if (vendFinal == null || vendFinal.compareTo(BigDecimal.ZERO) <= 0) {
                            vendFinal = FastchannelConstants.DEFAULT_CODVEND_PADRAO;
                        }
                        BigDecimal configuredNuTab = normalizeNuTabToLatestActive(jdbc, config.getNuTab());

                        applyCabecalhoParityNative(jdbc, nuNota, order, vendFinal);
                        applyItensParityNative(jdbc, nuNota, codEmp, configuredNuTab, order);
                    } finally {
                        closeJdbc(jdbc);
                    }
                }
            });
        } catch (Exception e) {
            log.log(Level.WARNING, "Falha ao aplicar paridade pos-importacao para NUNOTA " + nuNota, e);
        } finally {
            closeJapeSession(hnd);
        }
    }

    private void applyCabecalhoParityNative(JdbcWrapper jdbc, BigDecimal nuNota, OrderDTO order, BigDecimal vendFinal) throws Exception {
        JapeWrapper cabDAO = JapeFactory.dao("CabecalhoNota");
        DynamicVO cabVO = cabDAO.findByPK(nuNota);
        if (cabVO == null) {
            return;
        }

        String orderTag = order != null && !isBlank(order.getOrderId()) ? "Pedido Fastchannel: " + order.getOrderId() : null;
        BigDecimal codUsuIntegracao = resolveCodUsuIntegracao(jdbc);
        BigDecimal codParcCab = safeAsBigDecimal(cabVO, "CODPARC");
        BigDecimal codVendParceiro = resolveCodVendPreferencialByParc(codParcCab);
        BigDecimal vendTarget = !isNullOrZero(codVendParceiro) ? codVendParceiro : vendFinal;
        br.com.sankhya.jape.wrapper.fluid.FluidUpdateVO updateVO = cabDAO.prepareToUpdate(cabVO);
        boolean changed = false;

        if (hasColumn(jdbc, "TGFCAB", "CODVEND")) {
            BigDecimal current = cabVO.asBigDecimal("CODVEND");
            if (!isNullOrZero(vendTarget) && (isNullOrZero(current) || current.compareTo(vendTarget) != 0)) {
                updateVO = updateVO.set("CODVEND", vendTarget);
                changed = true;
            }
        }
        if (hasColumn(jdbc, "TGFCAB", "AD_CODVENDEXEC")) {
            BigDecimal fixedVendExec = BigDecimal.valueOf(281);
            BigDecimal current = cabVO.asBigDecimal("AD_CODVENDEXEC");
            if (isNullOrZero(current) || current.compareTo(fixedVendExec) != 0) {
                updateVO = updateVO.set("AD_CODVENDEXEC", fixedVendExec);
                changed = true;
            }
        }
        if (hasColumn(jdbc, "TGFCAB", "CODUSU") && !isNullOrZero(codUsuIntegracao)) {
            BigDecimal current = cabVO.asBigDecimal("CODUSU");
            if (isNullOrZero(current) || current.compareTo(codUsuIntegracao) != 0) {
                updateVO = updateVO.set("CODUSU", codUsuIntegracao);
                changed = true;
            }
        }
        if (hasColumn(jdbc, "TGFCAB", "CODUSUINC") && !isNullOrZero(codUsuIntegracao)) {
            BigDecimal current = cabVO.asBigDecimal("CODUSUINC");
            if (isNullOrZero(current) || current.compareTo(codUsuIntegracao) != 0) {
                updateVO = updateVO.set("CODUSUINC", codUsuIntegracao);
                changed = true;
            }
        }
        if (hasColumn(jdbc, "TGFCAB", "CIF_FOB")) {
            String current = trimToNull(cabVO.asString("CIF_FOB"));
            if (current == null || !"C".equalsIgnoreCase(current)) {
                updateVO = updateVO.set("CIF_FOB", "C");
                changed = true;
            }
        }
        if (hasColumn(jdbc, "TGFCAB", "STATUSNOTA")) {
            String current = trimToNull(cabVO.asString("STATUSNOTA"));
            if (current == null || !"P".equalsIgnoreCase(current)) {
                updateVO = updateVO.set("STATUSNOTA", "P");
                changed = true;
            }
        }
        if (hasColumn(jdbc, "TGFCAB", "PENDENTE")) {
            String current = trimToNull(cabVO.asString("PENDENTE"));
            if (current == null || !"S".equalsIgnoreCase(current)) {
                updateVO = updateVO.set("PENDENTE", "S");
                changed = true;
            }
        }
        if (hasColumn(jdbc, "TGFCAB", "APROVADO")) {
            String current = trimToNull(cabVO.asString("APROVADO"));
            if (current == null || !"N".equalsIgnoreCase(current)) {
                updateVO = updateVO.set("APROVADO", "N");
                changed = true;
            }
        }
        if (hasColumn(jdbc, "TGFCAB", "DTFATUR") && cabVO.asTimestamp("DTFATUR") != null) {
            updateVO = updateVO.set("DTFATUR", null);
            changed = true;
        }
        if (hasColumn(jdbc, "TGFCAB", "ISSRETIDO")) {
            String current = trimToNull(cabVO.asString("ISSRETIDO"));
            if (current == null || !"N".equalsIgnoreCase(current)) {
                updateVO = updateVO.set("ISSRETIDO", "N");
                changed = true;
            }
        }
        if (hasColumn(jdbc, "TGFCAB", "HISTCONFIG")) {
            String current = trimToNull(cabVO.asString("HISTCONFIG"));
            if (current == null || !"S".equalsIgnoreCase(current)) {
                updateVO = updateVO.set("HISTCONFIG", "S");
                changed = true;
            }
        }
        if (hasColumn(jdbc, "TGFCAB", "TPRETISS")) {
            String current = trimToNull(cabVO.asString("TPRETISS"));
            if (current == null || !"1".equalsIgnoreCase(current)) {
                updateVO = updateVO.set("TPRETISS", "1");
                changed = true;
            }
        }
        if (hasColumn(jdbc, "TGFCAB", "QTDVOL") && isNullOrZero(cabVO.asBigDecimal("QTDVOL"))) {
            updateVO = updateVO.set("QTDVOL", BigDecimal.ONE);
            changed = true;
        }
        if (hasColumn(jdbc, "TGFCAB", "ALIQIRF") && cabVO.asBigDecimal("ALIQIRF") == null) {
            updateVO = updateVO.set("ALIQIRF", BigDecimal.ZERO);
            changed = true;
        }
        if (hasColumn(jdbc, "TGFCAB", "BASEIRF") && cabVO.asBigDecimal("BASEIRF") == null) {
            updateVO = updateVO.set("BASEIRF", BigDecimal.ZERO);
            changed = true;
        }
        if (hasColumn(jdbc, "TGFCAB", "VLRFETHAB") && cabVO.asBigDecimal("VLRFETHAB") == null) {
            updateVO = updateVO.set("VLRFETHAB", BigDecimal.ZERO);
            changed = true;
        }
        if (hasColumn(jdbc, "TGFCAB", "VLRREPREDST") && cabVO.asBigDecimal("VLRREPREDST") == null) {
            updateVO = updateVO.set("VLRREPREDST", BigDecimal.ZERO);
            changed = true;
        }
        if (hasColumn(jdbc, "TGFCAB", "M3") && cabVO.asBigDecimal("M3") == null) {
            updateVO = updateVO.set("M3", BigDecimal.ZERO);
            changed = true;
        }
        if (hasColumn(jdbc, "TGFCAB", "SOMDESPADUNFENAC") && cabVO.asBigDecimal("SOMDESPADUNFENAC") == null) {
            updateVO = updateVO.set("SOMDESPADUNFENAC", BigDecimal.ZERO);
            changed = true;
        }
        if (hasColumn(jdbc, "TGFCAB", "HRENTSAI") && cabVO.asTimestamp("HRENTSAI") == null) {
            Timestamp dtEntSai = cabVO.asTimestamp("DTENTSAI");
            Timestamp dtMov = cabVO.asTimestamp("DTMOV");
            Timestamp dtNeg = cabVO.asTimestamp("DTNEG");
            Timestamp hrEntSai = dtEntSai != null ? dtEntSai : (dtMov != null ? dtMov : dtNeg);
            if (hrEntSai != null) {
                updateVO = updateVO.set("HRENTSAI", hrEntSai);
                changed = true;
            }
        }
        if (hasColumn(jdbc, "TGFCAB", "CODPARCTRANSP") && isNullOrZero(cabVO.asBigDecimal("CODPARCTRANSP"))) {
            BigDecimal codParcTransp = resolveCabNumericFallback(jdbc, cabVO, "CODPARCTRANSP");
            if (!isNullOrZero(codParcTransp)) {
                updateVO = updateVO.set("CODPARCTRANSP", codParcTransp);
                changed = true;
            }
        }
        if (hasColumn(jdbc, "TGFCAB", "ORDEMCARGA") && isNullOrZero(cabVO.asBigDecimal("ORDEMCARGA"))) {
            BigDecimal ordemCarga = resolveCabNumericFallback(jdbc, cabVO, "ORDEMCARGA");
            if (!isNullOrZero(ordemCarga)) {
                updateVO = updateVO.set("ORDEMCARGA", ordemCarga);
                changed = true;
            }
        }
        WeightTotals weights = resolveWeightTotals(jdbc, nuNota);
        if (weights != null) {
            if (hasColumn(jdbc, "TGFCAB", "PESO") && cabVO.asBigDecimal("PESO") == null && weights.peso != null) {
                updateVO = updateVO.set("PESO", weights.peso);
                changed = true;
            }
            if (hasColumn(jdbc, "TGFCAB", "PESOBRUTO") && cabVO.asBigDecimal("PESOBRUTO") == null && weights.pesoBruto != null) {
                updateVO = updateVO.set("PESOBRUTO", weights.pesoBruto);
                changed = true;
            }
        }
        if (hasColumn(jdbc, "TGFCAB", "TOTALCUSTOPROD")) {
            BigDecimal totalCusto = resolveTotalCusto(jdbc, nuNota);
            BigDecimal currentTotalCusto = cabVO.asBigDecimal("TOTALCUSTOPROD");
            if (totalCusto != null && (currentTotalCusto == null || currentTotalCusto.compareTo(totalCusto) != 0)) {
                updateVO = updateVO.set("TOTALCUSTOPROD", totalCusto);
                changed = true;
            }
        }
        if (hasColumn(jdbc, "TGFCAB", "TOTALCUSTOSERV") && cabVO.asBigDecimal("TOTALCUSTOSERV") == null) {
            updateVO = updateVO.set("TOTALCUSTOSERV", BigDecimal.ZERO);
            changed = true;
        }
        if (hasColumn(jdbc, "TGFCAB", "VLRSTEXTRANOTATOT") && cabVO.asBigDecimal("VLRSTEXTRANOTATOT") == null) {
            updateVO = updateVO.set("VLRSTEXTRANOTATOT", BigDecimal.ZERO);
            changed = true;
        }
        if (hasColumn(jdbc, "TGFCAB", "VLRREPREDTOTSEMDESC") && cabVO.asBigDecimal("VLRREPREDTOTSEMDESC") == null) {
            updateVO = updateVO.set("VLRREPREDTOTSEMDESC", BigDecimal.ZERO);
            changed = true;
        }
        if (hasColumn(jdbc, "TGFCAB", "SUMVLRIIOUTNOTA") && cabVO.asBigDecimal("SUMVLRIIOUTNOTA") == null) {
            updateVO = updateVO.set("SUMVLRIIOUTNOTA", BigDecimal.ZERO);
            changed = true;
        }
        if (hasColumn(jdbc, "TGFCAB", "SOMICMSNFENAC") && cabVO.asBigDecimal("SOMICMSNFENAC") == null) {
            updateVO = updateVO.set("SOMICMSNFENAC", BigDecimal.ZERO);
            changed = true;
        }
        if (hasColumn(jdbc, "TGFCAB", "SOMPISCOFNFENAC") && cabVO.asBigDecimal("SOMPISCOFNFENAC") == null) {
            updateVO = updateVO.set("SOMPISCOFNFENAC", BigDecimal.ZERO);
            changed = true;
        }
        if (hasColumn(jdbc, "TGFCAB", "AD_MCAPORTAL")) {
            String current = trimToNull(cabVO.asString("AD_MCAPORTAL"));
            if (current == null) {
                updateVO = updateVO.set("AD_MCAPORTAL", "P");
                changed = true;
            }
        }
        if (hasColumn(jdbc, "TGFCAB", "VLRFRETE")) {
            BigDecimal targetFrete = getFrete(order);
            BigDecimal currentFrete = cabVO.asBigDecimal("VLRFRETE");
            if (currentFrete == null || currentFrete.compareTo(targetFrete) != 0) {
                updateVO = updateVO.set("VLRFRETE", targetFrete);
                changed = true;
            }
            if (hasColumn(jdbc, "TGFCAB", "VLRNOTA")) {
                BigDecimal totalItens = resolveTotalItens(jdbc, nuNota);
                if (totalItens != null) {
                    BigDecimal targetVlrNota = totalItens.add(targetFrete);
                    BigDecimal currentVlrNota = cabVO.asBigDecimal("VLRNOTA");
                    if (currentVlrNota == null || currentVlrNota.compareTo(targetVlrNota) != 0) {
                        updateVO = updateVO.set("VLRNOTA", targetVlrNota);
                        changed = true;
                    }
                }
            }
        }
        if (hasColumn(jdbc, "TGFCAB", "AD_DESCONTO_FAST")) {
            BigDecimal targetDescontoFast = order != null && order.getProductDiscountCoupon() != null
                    ? order.getProductDiscountCoupon()
                    : BigDecimal.ZERO;
            BigDecimal currentDescontoFast = safeAsBigDecimal(cabVO, "AD_DESCONTO_FAST");
            if (currentDescontoFast == null || currentDescontoFast.compareTo(targetDescontoFast) != 0) {
                updateVO = updateVO.set("AD_DESCONTO_FAST", targetDescontoFast);
                changed = true;
            }
        }
        boolean hasObsInterna = hasColumn(jdbc, "TGFCAB", "OBSERVACAOINTERNA");
        // NUNCA colocar tag "Pedido Fastchannel" no OBSERVACAO (sai na nota fiscal)
        // Sempre limpar o tag do OBSERVACAO se existir de versoes anteriores
        if (hasColumn(jdbc, "TGFCAB", "OBSERVACAO") && orderTag != null) {
            String currentObs = trimToNull(cabVO.asString("OBSERVACAO"));
            String normalizedObs = removeTag(currentObs, orderTag);
            if (!equalsNullable(currentObs, normalizedObs)) {
                updateVO = updateVO.set("OBSERVACAO", normalizedObs);
                changed = true;
            }
        }
        if (hasObsInterna && orderTag != null) {
            String currentObsInt = trimToNull(cabVO.asString("OBSERVACAOINTERNA"));
            String desiredObsInterna = buildObservacaoInterna(order);
            String normalizedObsInt = trimToNull(desiredObsInterna);
            if (normalizedObsInt == null) {
                normalizedObsInt = ensureTagInObservacaoInterna(currentObsInt, orderTag);
            }
            if (!equalsNullable(currentObsInt, normalizedObsInt)) {
                updateVO = updateVO.set("OBSERVACAOINTERNA", normalizedObsInt);
                changed = true;
            }
        }

        if (changed) {
            updateVO.update();
        }
    }

    private void applyItensParityNative(JdbcWrapper jdbc, BigDecimal nuNota, BigDecimal codEmp, BigDecimal configuredNuTab, OrderDTO order) throws Exception {
        JapeWrapper iteDAO = JapeFactory.dao("ItemNota");
        Collection<DynamicVO> itens = iteDAO.find("this.NUNOTA = ?", nuNota);
        if (itens == null || itens.isEmpty()) {
            return;
        }
        Map<BigDecimal, OrderItemDTO> orderItemsBySeq = buildOrderItemsBySequencia(order);

        boolean hasNutab = hasColumn(jdbc, "TGFITE", "NUTAB");
        boolean hasPrecoBase = hasColumn(jdbc, "TGFITE", "PRECOBASE");
        boolean hasCusto = hasColumn(jdbc, "TGFITE", "CUSTO");
        boolean hasVlrCus = hasColumn(jdbc, "TGFITE", "VLRCUS");
        boolean hasUsoProd = hasColumn(jdbc, "TGFITE", "USOPROD");
        boolean hasAtualEstTerc = hasColumn(jdbc, "TGFITE", "ATUALESTTERC");
        boolean hasTerceiro = hasColumn(jdbc, "TGFITE", "TERCEIRO");
        boolean hasTerceiros = hasColumn(jdbc, "TGFITE", "TERCEIROS");
        boolean hasCodVend = hasColumn(jdbc, "TGFITE", "CODVEND");
        boolean hasCodUsu = hasColumn(jdbc, "TGFITE", "CODUSU");
        boolean hasStatusNota = hasColumn(jdbc, "TGFITE", "STATUSNOTA");
        boolean hasQtdEntregue = hasColumn(jdbc, "TGFITE", "QTDENTREGUE");
        boolean hasReserva = hasColumn(jdbc, "TGFITE", "RESERVA");
        boolean hasCodTrib = hasColumn(jdbc, "TGFITE", "CODTRIB");
        boolean hasAtualEstoque = hasColumn(jdbc, "TGFITE", "ATUALESTOQUE");
        boolean hasAliqIcms = hasColumn(jdbc, "TGFITE", "ALIQICMS");
        boolean hasAliqIpi = hasColumn(jdbc, "TGFITE", "ALIQIPI");
        boolean hasCodIpi = hasColumn(jdbc, "TGFITE", "CODIPI");
        boolean hasCsosn = hasColumn(jdbc, "TGFITE", "CSOSN");
        boolean hasSolCompra = hasColumn(jdbc, "TGFITE", "SOLCOMPRA");
        boolean hasQtdWms = hasColumn(jdbc, "TGFITE", "QTDWMS");
        boolean hasQtdFixada = hasColumn(jdbc, "TGFITE", "QTDFIXADA");
        boolean hasVlrAcrescDesc = hasColumn(jdbc, "TGFITE", "VLRACRESCDESC");
        boolean hasVlrRetencao = hasColumn(jdbc, "TGFITE", "VLRRETENCAO");
        boolean hasVlrIcmsUfDest = hasColumn(jdbc, "TGFITE", "VLRICMSUFDEST");
        boolean hasBaseStUfDest = hasColumn(jdbc, "TGFITE", "BASESTUFDEST");
        boolean hasBaseStAnt = hasColumn(jdbc, "TGFITE", "BASESTANT");
        boolean hasBaseStExtraNota = hasColumn(jdbc, "TGFITE", "BASESTEXTRANOTA");
        boolean hasAliqStExtraNota = hasColumn(jdbc, "TGFITE", "ALIQSTEXTRANOTA");
        boolean hasVlrStExtraNota = hasColumn(jdbc, "TGFITE", "VLRSTEXTRANOTA");
        BigDecimal codUsuIntegracao = resolveCodUsuIntegracao(jdbc);
        BigDecimal codVendCab = resolveCodVendCabecalho(nuNota);
        BigDecimal codTipVendaCab = resolveCodTipVendaCabecalho(nuNota);
        BigDecimal preferredNuTab = normalizeNuTabToLatestActive(jdbc, resolvePreferredNuTab(codVendCab, codTipVendaCab));

        Map<BigDecimal, ProdutoDefaults> produtoDefaultsCache = new HashMap<>();
        for (DynamicVO itemVO : itens) {
            if (itemVO == null) {
                continue;
            }

            BigDecimal codProd = itemVO.asBigDecimal("CODPROD");
            if (isNullOrZero(codProd)) {
                continue;
            }

            BigDecimal currentNutab = hasNutab ? itemVO.asBigDecimal("NUTAB") : null;
            BigDecimal nutabLookup = !isNullOrZero(preferredNuTab) ? preferredNuTab : currentNutab;
            ExcDefaults excDefaults = resolveExcDefaults(jdbc, codProd, nutabLookup, codEmp, null);
            ProdutoDefaults produtoDefaults = produtoDefaultsCache.get(codProd);
            if (produtoDefaults == null) {
                produtoDefaults = resolveProdutoDefaults(jdbc, codProd);
                produtoDefaultsCache.put(codProd, produtoDefaults);
            }

            br.com.sankhya.jape.wrapper.fluid.FluidUpdateVO updateVO = iteDAO.prepareToUpdate(itemVO);
            boolean changed = false;

            if (hasNutab && !isNullOrZero(excDefaults.nutab)
                    && (isNullOrZero(currentNutab) || currentNutab.compareTo(excDefaults.nutab) != 0)) {
                updateVO = updateVO.set("NUTAB", excDefaults.nutab);
                changed = true;
            }
            if (hasPrecoBase) {
                BigDecimal current = itemVO.asBigDecimal("PRECOBASE");
                BigDecimal vlrUnit = itemVO.asBigDecimal("VLRUNIT");
                BigDecimal targetPrecoBase = resolvePrecoBaseOrigemFromOrder(orderItemsBySeq.get(itemVO.asBigDecimal("SEQUENCIA")));
                if (isNullOrZero(targetPrecoBase)) {
                    targetPrecoBase = excDefaults.vlrVenda;
                }
                if (vlrUnit != null && (targetPrecoBase == null || targetPrecoBase.compareTo(vlrUnit) < 0)) {
                    targetPrecoBase = vlrUnit;
                }
                if (targetPrecoBase != null && (isNullOrZero(current) || current.compareTo(targetPrecoBase) != 0)) {
                    updateVO = updateVO.set("PRECOBASE", targetPrecoBase);
                    changed = true;
                }
            }
            if (hasCusto) {
                BigDecimal current = itemVO.asBigDecimal("CUSTO");
                if (isNullOrZero(current) && produtoDefaults.cusRep != null) {
                    updateVO = updateVO.set("CUSTO", produtoDefaults.cusRep);
                    changed = true;
                }
            }
            if (hasVlrCus) {
                BigDecimal current = itemVO.asBigDecimal("VLRCUS");
                if (isNullOrZero(current) && produtoDefaults.cusRep != null) {
                    updateVO = updateVO.set("VLRCUS", produtoDefaults.cusRep);
                    changed = true;
                }
            }
            if (hasUsoProd) {
                String current = trimToNull(itemVO.asString("USOPROD"));
                String targetUso = normalizeUsoProd(produtoDefaults.usoProd);
                if (targetUso != null && (current == null || !targetUso.equalsIgnoreCase(current))) {
                    updateVO = updateVO.set("USOPROD", targetUso);
                    changed = true;
                }
            }
            if (hasAtualEstTerc) {
                String current = trimToNull(itemVO.asString("ATUALESTTERC"));
                if (current == null) {
                    updateVO = updateVO.set("ATUALESTTERC", "N");
                    changed = true;
                }
            }
            if (hasSolCompra) {
                String current = trimToNull(itemVO.asString("SOLCOMPRA"));
                if (current == null || !"N".equalsIgnoreCase(current)) {
                    updateVO = updateVO.set("SOLCOMPRA", "N");
                    changed = true;
                }
            }
            if (hasTerceiro || hasTerceiros) {
                String current = hasTerceiros
                        ? trimToNull(itemVO.asString("TERCEIROS"))
                        : trimToNull(itemVO.asString("TERCEIRO"));
                if (current == null) {
                    updateVO = hasTerceiros
                            ? updateVO.set("TERCEIROS", "N")
                            : updateVO.set("TERCEIRO", "N");
                    changed = true;
                }
            }
            if (hasQtdEntregue) {
                BigDecimal current = itemVO.asBigDecimal("QTDENTREGUE");
                if (current == null || current.compareTo(BigDecimal.ZERO) != 0) {
                    updateVO = updateVO.set("QTDENTREGUE", BigDecimal.ZERO);
                    changed = true;
                }
            }
            if (hasAtualEstoque) {
                BigDecimal current = itemVO.asBigDecimal("ATUALESTOQUE");
                if (isNullOrZero(current)) {
                    updateVO = updateVO.set("ATUALESTOQUE", BigDecimal.ONE);
                    changed = true;
                }
            }
            if (hasReserva) {
                String current = trimToNull(itemVO.asString("RESERVA"));
                if (current == null || !"S".equalsIgnoreCase(current)) {
                    updateVO = updateVO.set("RESERVA", "S");
                    changed = true;
                }
            }
            if (hasStatusNota) {
                String current = trimToNull(itemVO.asString("STATUSNOTA"));
                if (current == null || !"P".equalsIgnoreCase(current)) {
                    updateVO = updateVO.set("STATUSNOTA", "P");
                    changed = true;
                }
            }
            if (hasCodVend) {
                BigDecimal current = itemVO.asBigDecimal("CODVEND");
                if (!isNullOrZero(codVendCab) && (isNullOrZero(current) || current.compareTo(codVendCab) != 0)) {
                    updateVO = updateVO.set("CODVEND", codVendCab);
                    changed = true;
                }
            }
            if (hasCodUsu && !isNullOrZero(codUsuIntegracao)) {
                BigDecimal current = itemVO.asBigDecimal("CODUSU");
                if (isNullOrZero(current) || current.compareTo(codUsuIntegracao) != 0) {
                    updateVO = updateVO.set("CODUSU", codUsuIntegracao);
                    changed = true;
                }
            }
            if (hasCodTrib && isNullOrZero(itemVO.asBigDecimal("CODTRIB"))) {
                BigDecimal codTrib = resolveCodTribByProduto(jdbc, codProd);
                if (!isNullOrZero(codTrib)) {
                    updateVO = updateVO.set("CODTRIB", codTrib);
                    changed = true;
                }
            }
            if (hasAliqIcms && itemVO.asBigDecimal("ALIQICMS") == null) {
                updateVO = updateVO.set("ALIQICMS", BigDecimal.ZERO);
                changed = true;
            }
            if (hasAliqIpi && itemVO.asBigDecimal("ALIQIPI") == null) {
                updateVO = updateVO.set("ALIQIPI", BigDecimal.ZERO);
                changed = true;
            }
            if (hasCodIpi && itemVO.asBigDecimal("CODIPI") == null) {
                updateVO = updateVO.set("CODIPI", BigDecimal.ZERO);
                changed = true;
            }
            if (hasCsosn && itemVO.asBigDecimal("CSOSN") == null) {
                updateVO = updateVO.set("CSOSN", BigDecimal.ZERO);
                changed = true;
            }
            if (hasQtdWms && itemVO.asBigDecimal("QTDWMS") == null) {
                updateVO = updateVO.set("QTDWMS", BigDecimal.ZERO);
                changed = true;
            }
            if (hasQtdFixada && itemVO.asBigDecimal("QTDFIXADA") == null) {
                updateVO = updateVO.set("QTDFIXADA", BigDecimal.ZERO);
                changed = true;
            }
            if (hasVlrAcrescDesc && itemVO.asBigDecimal("VLRACRESCDESC") == null) {
                updateVO = updateVO.set("VLRACRESCDESC", BigDecimal.ZERO);
                changed = true;
            }
            if (hasVlrRetencao && itemVO.asBigDecimal("VLRRETENCAO") == null) {
                updateVO = updateVO.set("VLRRETENCAO", BigDecimal.ZERO);
                changed = true;
            }
            if (hasVlrIcmsUfDest && itemVO.asBigDecimal("VLRICMSUFDEST") == null) {
                updateVO = updateVO.set("VLRICMSUFDEST", BigDecimal.ZERO);
                changed = true;
            }
            if (hasBaseStUfDest && itemVO.asBigDecimal("BASESTUFDEST") == null) {
                updateVO = updateVO.set("BASESTUFDEST", BigDecimal.ZERO);
                changed = true;
            }
            if (hasBaseStAnt && itemVO.asBigDecimal("BASESTANT") == null) {
                updateVO = updateVO.set("BASESTANT", BigDecimal.ZERO);
                changed = true;
            }
            if (hasBaseStExtraNota && itemVO.asBigDecimal("BASESTEXTRANOTA") == null) {
                updateVO = updateVO.set("BASESTEXTRANOTA", BigDecimal.ZERO);
                changed = true;
            }
            if (hasAliqStExtraNota && itemVO.asBigDecimal("ALIQSTEXTRANOTA") == null) {
                updateVO = updateVO.set("ALIQSTEXTRANOTA", BigDecimal.ZERO);
                changed = true;
            }
            if (hasVlrStExtraNota && itemVO.asBigDecimal("VLRSTEXTRANOTA") == null) {
                updateVO = updateVO.set("VLRSTEXTRANOTA", BigDecimal.ZERO);
                changed = true;
            }

            if (changed) {
                updateVO.update();
            }
        }
    }

    private ExcDefaults resolveExcDefaults(JdbcWrapper jdbc, BigDecimal codProd, BigDecimal currentNutab,
                                           BigDecimal codEmp, BigDecimal configuredNuTab) {
        ExcDefaults defaults = new ExcDefaults();
        ExcDefaults nativeDefaults = resolveExcDefaultsNative(codProd, currentNutab, codEmp, configuredNuTab);
        if (nativeDefaults != null && (!isNullOrZero(nativeDefaults.nutab) || nativeDefaults.vlrVenda != null)) {
            return nativeDefaults;
        }

        ResultSet rs = null;
        try {
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT TOP 1 E.NUTAB, E.VLRVENDA ");
            sql.appendSql("FROM TGFEXC E ");
            sql.appendSql("WHERE E.CODPROD = :codProd ");
            if (!isNullOrZero(currentNutab)) {
                sql.appendSql("AND E.NUTAB = :itemNutab ");
                sql.setNamedParameter("itemNutab", currentNutab);
            } else if (!isNullOrZero(configuredNuTab)) {
                sql.appendSql("AND E.NUTAB = :cfgNutab ");
                sql.setNamedParameter("cfgNutab", configuredNuTab);
            }
            if (!isNullOrZero(codEmp) && hasColumn(jdbc, "TGFEXC", "CODEMP")) {
                sql.appendSql("AND E.CODEMP = :codEmp ");
                sql.setNamedParameter("codEmp", codEmp);
            }
            sql.appendSql("ORDER BY E.NUTAB DESC");
            sql.setNamedParameter("codProd", codProd);
            rs = sql.executeQuery();
            if (rs.next()) {
                defaults.nutab = rs.getBigDecimal("NUTAB");
                defaults.vlrVenda = rs.getBigDecimal("VLRVENDA");
            }
            closeQuietly(rs);
            if ((isNullOrZero(defaults.nutab) || defaults.vlrVenda == null) && !isNullOrZero(configuredNuTab)) {
                BigDecimal codTab = resolveCodTabByNuTab(configuredNuTab);
                if (!isNullOrZero(codTab)) {
                    sql = new NativeSql(jdbc);
                    sql.appendSql("SELECT TOP 1 E.NUTAB, E.VLRVENDA ");
                    sql.appendSql("FROM TGFEXC E ");
                    sql.appendSql("INNER JOIN TGFTAB T ON T.NUTAB = E.NUTAB ");
                    sql.appendSql("WHERE E.CODPROD = :codProd ");
                    sql.appendSql("AND T.CODTAB = :codTab ");
                    if (!isNullOrZero(codEmp) && hasColumn(jdbc, "TGFEXC", "CODEMP")) {
                        sql.appendSql("AND E.CODEMP = :codEmp ");
                        sql.setNamedParameter("codEmp", codEmp);
                    }
                    if (hasColumn(jdbc, "TGFTAB", "DTVIGOR")) {
                        sql.appendSql("AND (T.DTVIGOR IS NULL OR T.DTVIGOR <= GETDATE()) ");
                    }
                    if (hasColumn(jdbc, "TGFTAB", "INATIVO")) {
                        sql.appendSql("AND (T.INATIVO IS NULL OR T.INATIVO = 'N') ");
                    }
                    sql.appendSql("ORDER BY T.DTVIGOR DESC, E.NUTAB DESC");
                    sql.setNamedParameter("codProd", codProd);
                    sql.setNamedParameter("codTab", codTab);
                    rs = sql.executeQuery();
                    if (rs.next()) {
                        defaults.nutab = rs.getBigDecimal("NUTAB");
                        defaults.vlrVenda = rs.getBigDecimal("VLRVENDA");
                    }
                }
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Falha ao resolver defaults de TGFEXC para CODPROD " + codProd, e);
        } finally {
            closeQuietly(rs);
        }
        return defaults;
    }

    private ExcDefaults resolveExcDefaultsNative(BigDecimal codProd, BigDecimal currentNutab,
                                                 BigDecimal codEmp, BigDecimal configuredNuTab) {
        if (isNullOrZero(codProd)) {
            return null;
        }

        String[] daoCandidates = {"ExcecaoTabelaPreco", "ExcecaoPreco", "TGFEXC"};
        for (String daoName : daoCandidates) {
            try {
                JapeWrapper excDAO = JapeFactory.dao(daoName);
                Collection<DynamicVO> rows = excDAO.find("this.CODPROD = ?", codProd);
                if (rows == null || rows.isEmpty()) {
                    continue;
                }

                DynamicVO selected = null;
                BigDecimal bestNutab = null;
                for (DynamicVO row : rows) {
                    if (row == null) {
                        continue;
                    }
                    BigDecimal rowNutab = safeAsBigDecimal(row, "NUTAB");
                    if (!isNullOrZero(currentNutab)) {
                        if (rowNutab == null || rowNutab.compareTo(currentNutab) != 0) {
                            continue;
                        }
                    } else if (!isNullOrZero(configuredNuTab)) {
                        if (rowNutab == null || rowNutab.compareTo(configuredNuTab) != 0) {
                            continue;
                        }
                    }

                    if (!isNullOrZero(codEmp)) {
                        BigDecimal rowCodEmp = safeAsBigDecimal(row, "CODEMP");
                        if (rowCodEmp != null && rowCodEmp.compareTo(codEmp) != 0) {
                            continue;
                        }
                    }

                    if (selected == null || (rowNutab != null && (bestNutab == null || rowNutab.compareTo(bestNutab) > 0))) {
                        selected = row;
                        bestNutab = rowNutab;
                    }
                }

                if (selected != null) {
                    ExcDefaults defaults = new ExcDefaults();
                    defaults.nutab = safeAsBigDecimal(selected, "NUTAB");
                    defaults.vlrVenda = safeAsBigDecimal(selected, "VLRVENDA");
                    return defaults;
                }
            } catch (Exception e) {
                log.log(Level.FINE, "Falha ao resolver defaults de TGFEXC via Jape (" + daoName + ")", e);
            }
        }
        return null;
    }

    private ProdutoDefaults resolveProdutoDefaults(JdbcWrapper jdbc, BigDecimal codProd) {
        ProdutoDefaults defaults = new ProdutoDefaults();
        try {
            JapeWrapper produtoDAO = JapeFactory.dao("Produto");
            DynamicVO produtoVO = produtoDAO.findByPK(codProd);
            if (produtoVO != null) {
                defaults.usoProd = trimToNull(produtoVO.asString("USOPROD"));
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Falha ao resolver defaults de TGFPRO para CODPROD " + codProd, e);
        }
        defaults.cusRep = resolveCurrentCostFromTgfCus(jdbc, codProd, null);
        return defaults;
    }

    private BigDecimal resolveCurrentCostFromTgfCus(JdbcWrapper jdbc, BigDecimal codProd, BigDecimal codEmp) {
        if (isNullOrZero(codProd)) {
            return null;
        }
        ResultSet rs = null;
        try {
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT ISNULL(MAX(CUSREP),0) AS CUSREP ");
            sql.appendSql("FROM TGFCUS ");
            sql.appendSql("WHERE CODPROD = :codProd ");
            sql.appendSql("AND DTATUAL = (SELECT MAX(DTATUAL) FROM TGFCUS CN ");
            sql.appendSql("               WHERE CN.CODPROD = :codProd ");
            sql.appendSql("               AND CN.DTATUAL <= GETDATE() ");
            if (!isNullOrZero(codEmp) && hasColumn(jdbc, "TGFCUS", "CODEMP")) {
                sql.appendSql("               AND CN.CODEMP = :codEmp) ");
                sql.appendSql("AND CODEMP = :codEmp ");
                sql.setNamedParameter("codEmp", codEmp);
            } else {
                sql.appendSql(")");
            }
            sql.setNamedParameter("codProd", codProd);
            rs = sql.executeQuery();
            if (rs.next()) {
                BigDecimal cus = rs.getBigDecimal("CUSREP");
                if (!isNullOrZero(cus)) {
                    return cus;
                }
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Falha ao resolver custo atual em TGFCUS para CODPROD " + codProd, e);
        } finally {
            closeQuietly(rs);
        }
        return null;
    }

    private BigDecimal normalizeNuTabToLatestActive(JdbcWrapper jdbc, BigDecimal nuTab) {
        if (isNullOrZero(nuTab)) {
            return null;
        }
        BigDecimal codTab = resolveCodTabByNuTab(nuTab);
        if (isNullOrZero(codTab)) {
            return nuTab;
        }
        ResultSet rs = null;
        try {
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT TOP 1 NUTAB ");
            sql.appendSql("FROM TGFTAB ");
            sql.appendSql("WHERE CODTAB = :codTab ");
            if (hasColumn(jdbc, "TGFTAB", "DTVIGOR")) {
                sql.appendSql("AND (DTVIGOR IS NULL OR DTVIGOR <= GETDATE()) ");
            }
            if (hasColumn(jdbc, "TGFTAB", "INATIVO")) {
                sql.appendSql("AND (INATIVO IS NULL OR INATIVO = 'N') ");
            }
            sql.appendSql("ORDER BY DTVIGOR DESC, NUTAB DESC");
            sql.setNamedParameter("codTab", codTab);
            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getBigDecimal("NUTAB");
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Falha ao normalizar NUTAB ativo para " + nuTab, e);
        } finally {
            closeQuietly(rs);
        }
        return nuTab;
    }

    private BigDecimal resolvePreferredNuTab(BigDecimal codVend, BigDecimal codTipVenda) {
        if (isNullOrZero(codVend)) {
            return null;
        }
        ResultSet rs = null;
        try {
            JdbcWrapper jdbc = openJdbc();
            try {
                NativeSql sql = new NativeSql(jdbc);
                sql.appendSql("SELECT TOP 1 T.NUTAB ");
                sql.appendSql("FROM TGFNPV N ");
                sql.appendSql("INNER JOIN TGFTAB T ON T.CODTAB = N.CODTAB ");
                sql.appendSql("WHERE N.CODVEND = :codVend ");
                if (!isNullOrZero(codTipVenda) && hasColumn(jdbc, "TGFNPV", "CODTIPVENDA")) {
                    sql.appendSql("AND N.CODTIPVENDA = :codTipVenda ");
                    sql.setNamedParameter("codTipVenda", codTipVenda);
                }
                if (hasColumn(jdbc, "TGFTAB", "DTVIGOR")) {
                    sql.appendSql("AND (T.DTVIGOR IS NULL OR T.DTVIGOR <= GETDATE()) ");
                }
                if (hasColumn(jdbc, "TGFTAB", "INATIVO")) {
                    sql.appendSql("AND (T.INATIVO IS NULL OR T.INATIVO = 'N') ");
                }
                sql.appendSql("ORDER BY T.DTVIGOR DESC, T.NUTAB DESC");
                sql.setNamedParameter("codVend", codVend);
                rs = sql.executeQuery();
                if (rs.next()) {
                    return rs.getBigDecimal("NUTAB");
                }
            } finally {
                closeQuietly(rs);
                closeJdbc(jdbc);
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Falha ao resolver NUTAB preferencial por TGFNPV para CODVEND " + codVend, e);
        }
        return null;
    }

    private BigDecimal resolveCodTabByNuTab(BigDecimal nuTab) {
        if (isNullOrZero(nuTab)) {
            return null;
        }
        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = openJdbc();
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT TOP 1 CODTAB FROM TGFTAB WHERE NUTAB = :nuTab");
            sql.setNamedParameter("nuTab", nuTab);
            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getBigDecimal("CODTAB");
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Falha ao resolver CODTAB para NUTAB " + nuTab, e);
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
        return null;
    }

    private BigDecimal getCodVend(BigDecimal codParc) {
        BigDecimal parceiroVend = resolveCodVendPreferencialByParc(codParc);
        if (!isNullOrZero(parceiroVend)) {
            return parceiroVend;
        }
        return null;
    }

    /**
     * Atualiza CODVEND no parceiro para que futuras importacoes nao precisem do fallback.
     */
    private void setCodVendParceiro(BigDecimal codParc, BigDecimal codVend) {
        if (isNullOrZero(codParc) || isNullOrZero(codVend)) return;
        try {
            JapeWrapper parceiroDAO = JapeFactory.dao("Parceiro");
            parceiroDAO.prepareToUpdateByPK(codParc)
                    .set("CODVEND", codVend)
                    .update();
            log.info("CODVEND " + codVend + " setado no parceiro " + codParc);
        } catch (Exception e) {
            log.log(Level.WARNING, "Nao foi possivel setar CODVEND no parceiro " + codParc, e);
        }
    }

    private BigDecimal resolveCodVendPreferencialByParc(BigDecimal codParc) {
        if (isNullOrZero(codParc)) {
            return null;
        }
        try {
            JapeWrapper parceiroDAO = JapeFactory.dao("Parceiro");
            DynamicVO parceiroVO = parceiroDAO.findByPK(codParc);
            if (parceiroVO != null) {
                BigDecimal codVend = parceiroVO.asBigDecimal("CODVEND");
                if (!isNullOrZero(codVend)) {
                    return codVend;
                }
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao buscar CODVEND do parceiro", e);
        }

        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = openJdbc();
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT TOP 1 CODVEND FROM TGFPAR WHERE CODPARC = :codParc");
            sql.setNamedParameter("codParc", codParc);
            rs = sql.executeQuery();
            if (rs.next()) {
                BigDecimal codVend = rs.getBigDecimal("CODVEND");
                if (!isNullOrZero(codVend)) {
                    return codVend;
                }
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Falha SQL ao buscar CODVEND do parceiro " + codParc, e);
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
        return null;
    }

    private BigDecimal getDefaultCodParc() {
        BigDecimal configured = config.getCodParcPadrao();
        if (!isNullOrZero(configured)) {
            return configured;
        }

        // Fallback SQL para ambientes onde campo ainda nao foi carregado por metadata/config antiga.
        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = openJdbc();
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT TOP 1 CODPARC_PADRAO FROM AD_FCCONFIG ORDER BY CODCONFIG DESC");
            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getBigDecimal("CODPARC_PADRAO");
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao buscar CODPARC_PADRAO", e);
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
        return null;
    }

    private void validateOrder(OrderDTO order) throws Exception {
        if (order.getOrderId() == null || order.getOrderId().isEmpty()) {
            throw new Exception("OrderId e obrigatorio");
        }
        if (order.getCustomer() == null) {
            throw new Exception("Cliente e obrigatorio. Pedido " + order.getOrderId()
                    + " veio sem dados de cliente da API Fastchannel.");
        }
        if (order.getItems() == null || order.getItems().isEmpty()) {
            throw new Exception("Pedido sem itens");
        }
    }

    private void normalizeOrderValues(OrderDTO order) {
        if (order == null) {
            return;
        }

        order.setSubtotal(normalizeMoney(order.getSubtotal()));
        order.setSubtotalProducts(normalizeMoney(order.getSubtotalProducts()));
        order.setShippingCost(normalizeMoney(order.getShippingCost()));
        order.setProductDiscount(normalizeMoney(order.getProductDiscount()));
        order.setProductDiscountQuota(normalizeMoney(order.getProductDiscountQuota()));
        order.setProductDiscountCoupon(normalizeMoney(order.getProductDiscountCoupon()));
        order.setProductDiscountManual(normalizeMoney(order.getProductDiscountManual()));
        order.setProductDiscountPayment(normalizeMoney(order.getProductDiscountPayment()));
        order.setProductDiscountAssociation(normalizeMoney(order.getProductDiscountAssociation()));
        order.setShippingDiscount(normalizeMoney(order.getShippingDiscount()));
        order.setShippingDiscountCoupon(normalizeMoney(order.getShippingDiscountCoupon()));
        order.setShippingDiscountManual(normalizeMoney(order.getShippingDiscountManual()));
        order.setShippingDiscountAmount(normalizeMoney(order.getShippingDiscountAmount()));
        order.setDiscount(normalizeMoney(order.getDiscount()));
        order.setTotal(normalizeMoney(order.getTotal()));
        order.setTotalOrderValue(normalizeMoney(order.getTotalOrderValue()));
        order.setOrderTotal(normalizeMoney(order.getOrderTotal()));

        if (order.getItems() != null) {
            for (OrderItemDTO item : order.getItems()) {
                if (item == null) {
                    continue;
                }
                item.setUnitPrice(normalizeMoney(item.getUnitPrice()));
                item.setAssociationDiscount(normalizeMoney(item.getAssociationDiscount()));
                item.setManualDiscount(normalizeMoney(item.getManualDiscount()));
                item.setCatalogDiscount(normalizeMoney(item.getCatalogDiscount()));
                item.setQuotaDiscount(normalizeMoney(item.getQuotaDiscount()));
                item.setCouponDiscount(normalizeMoney(item.getCouponDiscount()));
                item.setPaymentDiscount(normalizeMoney(item.getPaymentDiscount()));
                item.setDiscount(normalizeMoney(item.getDiscount()));
                item.setTotalPrice(normalizeMoney(item.getTotalPrice()));

                if (item.getTotalPrice() == null && item.getQuantity() != null && item.getUnitPrice() != null) {
                    BigDecimal total = item.getUnitPrice().multiply(item.getQuantity());
                    BigDecimal disc = item.getDiscount();
                    if (disc != null) {
                        total = total.subtract(disc);
                    }
                    item.setTotalPrice(total);
                }
            }
        }
    }

    /**
     * Localiza parceiro por CPF/CNPJ ou cria novo.
     */
    private BigDecimal findOrCreateParceiro(OrderCustomerDTO customer, OrderAddressDTO address) throws Exception {
        if (customer == null) {
            throw new Exception("Dados do cliente nao informados");
        }

        String cpfCnpj = customer.getCleanCpfCnpj();
        if (!isValidCpfCnpj(cpfCnpj)) {
            if (!isBlank(cpfCnpj)) {
                log.warning("Documento de cliente invalido no pedido. Ignorando para busca/criacao de parceiro: " + cpfCnpj);
            }
            cpfCnpj = null;
        }
        if (cpfCnpj != null && !cpfCnpj.isEmpty()) {
            BigDecimal codParc = findParceiroByCpfCnpj(cpfCnpj);
            if (codParc != null) {
                log.fine("Parceiro encontrado por CPF/CNPJ: " + codParc);
                return codParc;
            }
        }

        String email = customer.getEmail();
        if (email != null && !email.trim().isEmpty()) {
            BigDecimal codParc = findParceiroByEmail(email.trim());
            if (codParc != null) {
                log.fine("Parceiro encontrado por e-mail: " + codParc);
                return codParc;
            }
        }

        if (isBlank(cpfCnpj) && isBlank(email)) {
            BigDecimal fallback = resolveFallbackCodParc();
            if (fallback != null) {
                log.warning("Cliente sem documento/e-mail. Usando CODPARC fallback: " + fallback);
                return fallback;
            }
        }

        try {
            return createParceiro(customer, address);
        } catch (Exception e) {
            BigDecimal fallback = resolveFallbackCodParc();
            if (fallback != null) {
                log.log(Level.WARNING, "Falha ao criar parceiro. Usando CODPARC fallback: " + fallback, e);
                return fallback;
            }
            throw e;
        }
    }

    private BigDecimal findParceiroByCpfCnpj(String cpfCnpj) {
        String normalized = sanitizeDigits(cpfCnpj);
        if (normalized == null) {
            return null;
        }

        // Tentativa nativa via Jape primeiro (sem funcao SQL).
        try {
            JapeWrapper parceiroDAO = JapeFactory.dao("Parceiro");
            for (String candidate : buildDocumentCandidates(normalized)) {
                Collection<DynamicVO> parceiros = parceiroDAO.find("this.CGC_CPF = ?", candidate);
                if (parceiros != null && !parceiros.isEmpty()) {
                    for (DynamicVO parceiro : parceiros) {
                        if (parceiro != null) {
                            BigDecimal codParc = parceiro.asBigDecimal("CODPARC");
                            if (!isNullOrZero(codParc)) {
                                return codParc;
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Falha na busca nativa de parceiro por documento. Aplicando fallback SQL.", e);
        }

        // Fallback SQL apenas para normalizacao legada em banco.
        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = openJdbc();

            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT CODPARC FROM TGFPAR ");
            sql.appendSql("WHERE REPLACE(REPLACE(REPLACE(CGC_CPF, '.', ''), '-', ''), '/', '') = :cpfCnpj");

            sql.setNamedParameter("cpfCnpj", normalized);

            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getBigDecimal("CODPARC");
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao buscar parceiro", e);
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
        return null;
    }

    private BigDecimal createParceiro(OrderCustomerDTO customer, OrderAddressDTO address) throws Exception {
        log.info("Criando novo parceiro: " + customer.getName());
        final BigDecimal[] codParcRef = new BigDecimal[1];
        final BigDecimal codCid = resolveCodCidForParceiro(address);
        if (isNullOrZero(codCid)) {
            throw new Exception("CODCID nao resolvido para criacao nativa do parceiro Fastchannel.");
        }
        JapeSession.SessionHandle hnd = null;
        try {
            hnd = JapeSession.open();
            hnd.execWithTX(new JapeSession.TXBlock() {
                @Override
                public void doWithTx() throws Exception {
                    JapeWrapper parceiroDAO = JapeFactory.dao("Parceiro");
                    String cpfCnpj = sanitizeDigits(customer.getCpfCnpj());
                    if (!isValidCpfCnpj(cpfCnpj)) {
                        cpfCnpj = buildFallbackCpf(customer);
                        log.warning("Cliente sem CPF/CNPJ valido no pedido. Usando CPF fallback para cadastro de parceiro.");
                    }
                    String tipoPessoa = (cpfCnpj != null && cpfCnpj.replaceAll("\\D", "").length() > 11) ? "J" : "F";

                    // CODVEND padrao para parceiros criados via Fastchannel
                    BigDecimal codVendPadrao = config.getCodVendPadrao();
                    if (isNullOrZero(codVendPadrao)) {
                        codVendPadrao = FastchannelConstants.DEFAULT_CODVEND_PADRAO;
                    }

                    FluidCreateVO parceiroBuilder = parceiroDAO.create()
                            .set("NOMEPARC", truncate(customer.getName(), 100))
                            .set("TIPPESSOA", tipoPessoa)
                            .set("CGC_CPF", cpfCnpj)
                            .set("EMAIL", truncate(customer.getEmail(), 80))
                            .set("TELEFONE", truncate(customer.getPhone(), 15))
                            .set("CODCID", codCid)
                            .set("CODVEND", codVendPadrao)
                            .set("ATIVO", "S")
                            .set("CLIENTE", "S")
                            .set("FORNECEDOR", "N");

                    if (customer.isPJ() && customer.getCompanyName() != null) {
                        parceiroBuilder = parceiroBuilder.set("RAZAOSOCIAL", truncate(customer.getCompanyName(), 100));
                    }

                    if (customer.getStateRegistration() != null) {
                        parceiroBuilder = parceiroBuilder.set("IDENTINSCESTAD", truncate(customer.getStateRegistration(), 30));
                    }

                    DynamicVO parceiroVO = parceiroBuilder.save();
                    BigDecimal codParc = parceiroVO.asBigDecimal("CODPARC");
                    if (address != null) {
                        createEndereco(codParc, address);
                    }
                    codParcRef[0] = codParc;
                }
            });
        } finally {
            closeJapeSession(hnd);
        }

        if (codParcRef[0] == null) {
            throw new Exception("Falha ao criar parceiro para pedido Fastchannel.");
        }

        log.info("Parceiro criado: CODPARC " + codParcRef[0]);
        return codParcRef[0];
    }

    private BigDecimal findParceiroByEmail(String email) {
        String normalized = trimToNull(email);
        if (normalized == null) {
            return null;
        }

        // Tentativa nativa via Jape primeiro.
        try {
            JapeWrapper parceiroDAO = JapeFactory.dao("Parceiro");
            Collection<DynamicVO> parceiros = parceiroDAO.find("this.EMAIL = ?", normalized);
            if (parceiros != null) {
                for (DynamicVO parceiro : parceiros) {
                    if (parceiro != null) {
                        BigDecimal codParc = parceiro.asBigDecimal("CODPARC");
                        if (!isNullOrZero(codParc)) {
                            return codParc;
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Falha na busca nativa de parceiro por e-mail. Aplicando fallback SQL.", e);
        }

        // Fallback SQL case-insensitive para bases legadas.
        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = openJdbc();

            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT TOP 1 CODPARC FROM TGFPAR ");
            sql.appendSql("WHERE UPPER(LTRIM(RTRIM(EMAIL))) = UPPER(LTRIM(RTRIM(:email))) ");
            sql.appendSql("ORDER BY CODPARC DESC");
            sql.setNamedParameter("email", normalized);

            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getBigDecimal("CODPARC");
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao buscar parceiro por e-mail", e);
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
        return null;
    }

    private String buildFallbackCpfCnpj(OrderCustomerDTO customer) {
        return buildFallbackCpf(customer);
    }

    private String buildFallbackCpf(OrderCustomerDTO customer) {
        String seed = null;
        if (customer != null) {
            if (customer.getEmail() != null && !customer.getEmail().trim().isEmpty()) {
                seed = customer.getEmail().trim().toLowerCase();
            } else if (customer.getName() != null && !customer.getName().trim().isEmpty()) {
                seed = customer.getName().trim().toLowerCase();
            }
        }
        if (seed == null || seed.isEmpty()) {
            seed = "fastchannel";
        }
        long hash = Math.abs((long) seed.hashCode());
        int base = (int) (hash % 1000000000L);
        String nove = String.format("%09d", base);
        int d1 = calcCpfDigit(nove, 10);
        int d2 = calcCpfDigit(nove + d1, 11);
        return nove + d1 + d2;
    }

    private int calcCpfDigit(String digits, int weightStart) {
        int sum = 0;
        for (int i = 0; i < digits.length(); i++) {
            sum += Character.getNumericValue(digits.charAt(i)) * (weightStart - i);
        }
        int mod = 11 - (sum % 11);
        return mod >= 10 ? 0 : mod;
    }

    private String sanitizeDigits(String value) {
        if (value == null) {
            return null;
        }
        String digits = value.replaceAll("\\D", "");
        return digits.isEmpty() ? null : digits;
    }

    private boolean sameText(String left, String right) {
        String leftNorm = trimToNull(left);
        String rightNorm = trimToNull(right);
        if (leftNorm == null || rightNorm == null) {
            return false;
        }
        return leftNorm.equalsIgnoreCase(rightNorm);
    }

    private boolean isValidCpfCnpj(String value) {
        String digits = sanitizeDigits(value);
        if (digits == null) {
            return false;
        }
        if (digits.length() == 11) {
            return isValidCpf(digits);
        }
        if (digits.length() == 14) {
            return isValidCnpj(digits);
        }
        return false;
    }

    private boolean isValidCpf(String cpf) {
        if (cpf.matches("(\\d)\\1{10}")) {
            return false;
        }
        int d1 = calcCpfDigit(cpf.substring(0, 9), 10);
        int d2 = calcCpfDigit(cpf.substring(0, 9) + d1, 11);
        return cpf.equals(cpf.substring(0, 9) + d1 + d2);
    }

    private boolean isValidCnpj(String cnpj) {
        if (cnpj.matches("(\\d)\\1{13}")) {
            return false;
        }
        int[] w1 = {5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2};
        int[] w2 = {6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2};
        int d1 = calcCnpjDigit(cnpj.substring(0, 12), w1);
        int d2 = calcCnpjDigit(cnpj.substring(0, 12) + d1, w2);
        return cnpj.equals(cnpj.substring(0, 12) + d1 + d2);
    }

    private int calcCnpjDigit(String digits, int[] weights) {
        int sum = 0;
        for (int i = 0; i < digits.length(); i++) {
            sum += Character.getNumericValue(digits.charAt(i)) * weights[i];
        }
        int mod = sum % 11;
        return mod < 2 ? 0 : 11 - mod;
    }

    private void createEndereco(BigDecimal codParc, OrderAddressDTO address) {
        try {
            // Buscar ou criar cidade
            BigDecimal codCid = findOrCreateCidade(address.getCity(), address.getState());

            JapeWrapper enderecoDAO = JapeFactory.dao("Endereco");

            DynamicVO vo = enderecoDAO.create()
                    .set("CODPARC", codParc)
                    .set("NOMEEND", truncate(address.getStreet(), 100))
                    .set("NUMEND", truncate(address.getNumber(), 10))
                    .set("COMPLEMENTO", truncate(address.getComplement(), 50))
                    .set("NOMEBAI", truncate(address.getNeighborhood(), 50))
                    .set("CEP", address.getCleanZipCode())
                    .set("CODCID", codCid)
                    .set("TIPO", "CO")
                    .save();

        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao criar endereco", e);
        }
    }

    private BigDecimal findOrCreateCidade(String nomeCidade, String uf) {
        String cidade = trimToNull(nomeCidade);
        String ufNorm = trimToNull(uf);
        if (cidade == null || ufNorm == null) {
            return null;
        }

        // Tentativa nativa via Jape primeiro.
        try {
            JapeWrapper cidadeDAO = JapeFactory.dao("Cidade");
            Collection<DynamicVO> cidades = cidadeDAO.find("this.UF = ?", ufNorm);
            if (cidades != null) {
                for (DynamicVO cidadeVO : cidades) {
                    if (cidadeVO == null) {
                        continue;
                    }
                    if (!sameText(cidadeVO.asString("NOMECID"), cidade)) {
                        continue;
                    }
                    BigDecimal codCid = cidadeVO.asBigDecimal("CODCID");
                    if (!isNullOrZero(codCid)) {
                        return codCid;
                    }
                }
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Falha na busca nativa de cidade. Aplicando fallback SQL.", e);
        }

        // Fallback SQL para compatibilidade ampla.
        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = openJdbc();

            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT CODCID FROM TSICID WHERE UPPER(NOMECID) = UPPER(:nome) AND UF = :uf");
            sql.setNamedParameter("nome", cidade);
            sql.setNamedParameter("uf", ufNorm);

            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getBigDecimal("CODCID");
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao buscar cidade", e);
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }

        // Cidade nao encontrada - usar codigo padrao ou criar
        log.warning("Cidade nao encontrada: " + nomeCidade + "/" + uf);
        return null;
    }

    private BigDecimal resolveCodCidForParceiro(OrderAddressDTO address) {
        BigDecimal codCid = null;
        if (address != null) {
            codCid = findOrCreateCidade(address.getCity(), address.getState());
        }
        if (!isNullOrZero(codCid)) {
            return codCid;
        }

        BigDecimal fallbackCodParc = resolveFallbackCodParc();
        if (isNullOrZero(fallbackCodParc)) {
            return null;
        }

        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = openJdbc();
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT TOP 1 CODCID FROM TGFPAR WHERE CODPARC = :codParc AND CODCID IS NOT NULL");
            sql.setNamedParameter("codParc", fallbackCodParc);
            rs = sql.executeQuery();
            if (rs.next()) {
                BigDecimal fallbackCodCid = rs.getBigDecimal("CODCID");
                if (!isNullOrZero(fallbackCodCid)) {
                    return fallbackCodCid;
                }
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao resolver CODCID fallback para novo parceiro", e);
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
        return null;
    }

    /**
     * Cria cabecalho do pedido (TGFCAB).
     */
    private BigDecimal createCabecalho(OrderDTO order, BigDecimal codParc) throws Exception {
        JapeWrapper cabDAO = JapeFactory.dao("CabecalhoNota");

        BigDecimal codTipOper = config.getCodTipOper();
        BigDecimal codEmp = config.getCodemp();

        if (codTipOper == null) {
            throw new Exception("CODTIPOPER nao configurado para integracao Fastchannel");
        }
        if (codEmp == null) {
            throw new Exception("CODEMP nao configurado para integracao Fastchannel");
        }

        FluidCreateVO cabBuilder = cabDAO.create()
                .set("CODEMP", codEmp)
                .set("CODPARC", codParc)
                .set("CODTIPOPER", codTipOper)
                .set("DTNEG", new Timestamp(System.currentTimeMillis()))
                .set("TIPMOV", "P") // Pedido
                .set("STATUSNOTA", "P") // Pendente
                .set("AD_FASTCHANNEL_ID", truncate(order.getOrderId(), 50))
                .set("OBSERVACAO", buildObservacao(order));

        // Valores
        BigDecimal frete = getFrete(order);
        cabBuilder = cabBuilder.set("VLRFRETE", frete);
        if (order.getTotal() != null) {
            cabBuilder = cabBuilder.set("VLRNOTA", order.getTotal());
        }
        if (order.getDiscount() != null) {
            cabBuilder = cabBuilder.set("VLRDESC", order.getDiscount());
        }

        DynamicVO cabVO = cabBuilder.save();
        return cabVO.asBigDecimal("NUNOTA");
    }

    private String buildObservacao(OrderDTO order) {
        StringBuilder obs = new StringBuilder();

        if (order.getNotes() != null && !order.getNotes().isEmpty()) {
            obs.append(order.getNotes());
        }

        if (order.getShippingMethod() != null) {
            if (obs.length() > 0) {
                obs.append("\n");
            }
            obs.append("Frete: ").append(order.getShippingMethod());
        }

        return truncate(trimToNull(obs.toString()), 500);
    }

    private String buildObservacaoInterna(OrderDTO order) {
        if (order == null || isBlank(order.getOrderId())) {
            return truncate(trimToNull(order != null ? order.getSellerNotes() : null), 1000);
        }

        StringBuilder obs = new StringBuilder();
        obs.append("Pedido Fastchannel: ").append(order.getOrderId());
        if (!isBlank(order.getSellerNotes())) {
            obs.append(" | ").append(order.getSellerNotes().trim());
        }
        return truncate(obs.toString(), 1000);
    }

    /**
     * Cria itens do pedido (TGFITE).
     */
    private void createItens(BigDecimal nuNota, List<OrderItemDTO> items) throws Exception {
        JapeWrapper iteDAO = JapeFactory.dao("ItemNota");

        int sequencia = 1;
        for (OrderItemDTO item : items) {
            // Buscar CODPROD pelo SKU
            BigDecimal codProd = deparaService.resolveCodProdForOrderItem(item);
            if (codProd == null) {
                throw new Exception("Produto nao encontrado para SKU: " + item.getSku());
            }

            // Buscar CODVOL do produto
            String codVol = getVolumePadrao(codProd);

            FluidCreateVO itemBuilder = iteDAO.create()
                    .set("NUNOTA", nuNota)
                    .set("SEQUENCIA", new BigDecimal(sequencia))
                    .set("CODPROD", codProd)
                    .set("QTDNEG", item.getQuantity())
                    .set("VLRUNIT", item.getUnitPrice())
                    .set("VLRTOT", item.getTotalPrice())
                    .set("CODVOL", codVol);

            if (item.getDiscount() != null && item.getDiscount().compareTo(BigDecimal.ZERO) > 0) {
                itemBuilder = itemBuilder.set("VLRDESC", item.getDiscount());
            }

            itemBuilder.save();
            sequencia++;
        }

        log.info("Criados " + items.size() + " itens para NUNOTA " + nuNota);
    }

    private String getVolumePadrao(BigDecimal codProd) {
        if (codProd == null) {
            return "UN";
        }
        // Tentativa 1: Jape
        try {
            JapeWrapper produtoDAO = JapeFactory.dao("Produto");
            DynamicVO produtoVO = produtoDAO.findByPK(codProd);
            if (produtoVO != null) {
                String codVol = trimToNull(produtoVO.asString("CODVOL"));
                if (codVol != null) {
                    return codVol;
                }
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Jape falhou ao buscar CODVOL do produto " + codProd + ", tentando SQL direto", e);
        }
        // Tentativa 2: SQL direto
        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = openJdbc();
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT CODVOL FROM TGFPRO WHERE CODPROD = :codProd");
            sql.setNamedParameter("codProd", codProd);
            rs = sql.executeQuery();
            if (rs.next()) {
                String codVol = trimToNull(rs.getString("CODVOL"));
                if (codVol != null) {
                    log.info("[OrderService] CODVOL via SQL direto para CODPROD " + codProd + ": " + codVol);
                    return codVol;
                }
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "SQL direto tambem falhou ao buscar CODVOL do produto " + codProd, e);
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
        log.warning("[OrderService] CODVOL nao resolvido para CODPROD " + codProd + ". Fallback para 'UN'.");
        return "UN";
    }

    /**
     * Registra ou atualiza mapeamento do pedido na AD_FCPEDIDO.
     */
    private void upsertOrderMapping(OrderDTO order, BigDecimal nuNota, BigDecimal codParc,
                                    String statusImport, String errorMsg) {
        JdbcWrapper jdbc = null;
        try {
            jdbc = openJdbc();
            NativeSql update = new NativeSql(jdbc);
            update.appendSql("UPDATE AD_FCPEDIDO SET ");
            update.appendSql("NUNOTA = :nuNota, CODPARC = :codParc, STATUS_FC = :statusFc, STATUS_SKW = :statusSkw, ");
            update.appendSql("STATUS_IMPORT = :statusImport, DH_IMPORTACAO = CURRENT_TIMESTAMP, ");
            update.appendSql("DH_PEDIDO = :dhPedido, NOME_CLIENTE = COALESCE(:nomeCliente, NOME_CLIENTE), ");
            update.appendSql("CPF_CNPJ = COALESCE(:cpfCnpj, CPF_CNPJ), ");
            update.appendSql("VALOR_TOTAL = :valorTotal, VALOR_FRETE = :valorFrete, ERRO_MSG = :erroMsg ");
            update.appendSql("WHERE ORDER_ID = :orderId");
            bindOrderMappingParameters(jdbc, update, order, nuNota, codParc, statusImport, errorMsg);

            if (!update.executeUpdate()) {
                try {
                    insertOrderMapping(jdbc, order, nuNota, codParc, statusImport, errorMsg);
                } catch (Exception e) {
                    if (!isOrderMappingUniqueViolation(e)) {
                        throw e;
                    }
                    NativeSql retry = new NativeSql(jdbc);
                    retry.appendSql("UPDATE AD_FCPEDIDO SET ");
                    retry.appendSql("NUNOTA = :nuNota, CODPARC = :codParc, STATUS_FC = :statusFc, STATUS_SKW = :statusSkw, ");
                    retry.appendSql("STATUS_IMPORT = :statusImport, DH_IMPORTACAO = CURRENT_TIMESTAMP, ");
                    retry.appendSql("DH_PEDIDO = :dhPedido, NOME_CLIENTE = COALESCE(:nomeCliente, NOME_CLIENTE), ");
                    retry.appendSql("CPF_CNPJ = COALESCE(:cpfCnpj, CPF_CNPJ), ");
                    retry.appendSql("VALOR_TOTAL = :valorTotal, VALOR_FRETE = :valorFrete, ERRO_MSG = :erroMsg ");
                    retry.appendSql("WHERE ORDER_ID = :orderId");
                    bindOrderMappingParameters(jdbc, retry, order, nuNota, codParc, statusImport, errorMsg);
                    retry.executeUpdate();
                }
            }

        } catch (Exception e) {
            String oid = order != null ? order.getOrderId() : "null";
            log.log(Level.SEVERE, "ERRO CRITICO ao registrar mapeamento na AD_FCPEDIDO (orderId=" +
                    oid + ", nuNota=" + nuNota +
                    ", status=" + statusImport + "): " + e.getMessage() +
                    ". Verifique se a tabela AD_FCPEDIDO existe e tem as colunas corretas.", e);
            // Backup via JDBC direto - nao pode lancar excecao aqui
            if (oid != null && !"null".equals(oid)) {
                forceStatusUpdate(oid, nuNota, statusImport, errorMsg != null ? errorMsg : e.getMessage());
            }
            try {
                logService.error(LogService.OP_ORDER_IMPORT,
                        "upsertOrderMapping falhou para " + oid + " (status=" + statusImport + "): " + e.getMessage(), e);
            } catch (Exception logEx) {
                // ignora falha de log
            }
        } finally {
            closeJdbc(jdbc);
        }
    }

    /**
     * Ultimo recurso: UPDATE direto via JDBC puro para marcar pedido como ERRO.
     * Usado quando upsertOrderMapping falha e o pedido ficaria travado em PROCESSANDO.
     */
    private void forceStatusErro(String orderId, String errorMsg) {
        forceStatusUpdate(orderId, null, STATUS_IMPORT_ERRO, errorMsg);
    }

    /**
     * Ultimo recurso: UPDATE direto via JDBC puro para salvar status do pedido.
     * Usado quando upsertOrderMapping falha (ex: connection pool esgotado).
     * Nunca lanca excecao - apenas loga.
     */
    private void forceStatusUpdate(String orderId, BigDecimal nuNota, String status, String errorMsg) {
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = DBUtil.getConnection();
            OrderMappingFieldSizes fieldSizes = getOrderMappingFieldSizes();
            String sql = "UPDATE AD_FCPEDIDO SET STATUS_IMPORT = ?, ERRO_MSG = ?, DH_IMPORTACAO = CURRENT_TIMESTAMP" +
                    (nuNota != null ? ", NUNOTA = ?" : "") +
                    " WHERE ORDER_ID = ?";
            stmt = conn.prepareStatement(sql);
            String truncatedStatus = truncateToColumn(status, fieldSizes.statusImport);
            String truncatedMsg = truncateToColumn(errorMsg, fieldSizes.erroMsg);
            String truncatedOrderId = truncateToColumn(orderId, fieldSizes.orderId);
            int idx = 1;
            stmt.setString(idx++, truncatedStatus);
            stmt.setString(idx++, truncatedMsg);
            if (nuNota != null) {
                stmt.setBigDecimal(idx++, nuNota);
            }
            stmt.setString(idx, truncatedOrderId);
            int rows = stmt.executeUpdate();
            if (rows > 0) {
                log.info("forceStatusUpdate: pedido " + orderId + " -> " + status +
                        (nuNota != null ? " (NUNOTA=" + nuNota + ")" : "") + " via JDBC direto.");
            } else {
                log.warning("forceStatusUpdate: UPDATE retornou 0 linhas para orderId=" + orderId);
            }
        } catch (Exception e) {
            log.log(Level.SEVERE, "forceStatusUpdate FALHOU para orderId=" + orderId + " status=" + status +
                    ". Pedido FICARA travado em PROCESSANDO ate recovery automatico.", e);
        } finally {
            br.com.bellube.fastchannel.util.DBUtil.closeAll(null, stmt, conn);
        }
    }

    /**
     * Calcula o frete efetivo (bruto - descontos de frete).
     * Se o cliente tem promocao de frete gratis, retorna 0.
     */
    private BigDecimal getFrete(OrderDTO order) {
        if (order == null) return null;
        BigDecimal freteBruto = order.getShippingCost();
        if (freteBruto == null) return BigDecimal.ZERO;

        BigDecimal descontoFrete = BigDecimal.ZERO;
        if (order.getShippingDiscount() != null) {
            descontoFrete = descontoFrete.add(order.getShippingDiscount());
        }
        if (order.getShippingDiscountCoupon() != null) {
            descontoFrete = descontoFrete.add(order.getShippingDiscountCoupon());
        }
        if (order.getShippingDiscountAmount() != null) {
            descontoFrete = descontoFrete.add(order.getShippingDiscountAmount());
        }

        BigDecimal freteEfetivo = freteBruto.subtract(descontoFrete);
        if (freteEfetivo.compareTo(BigDecimal.ZERO) < 0) {
            freteEfetivo = BigDecimal.ZERO;
        }
        log.info("Frete: bruto=" + freteBruto + " descontoFrete=" + descontoFrete + " efetivo=" + freteEfetivo);
        return freteEfetivo;
    }

    /**
     * Verifica se pedido ja foi importado.
     */
    private boolean isOrderAlreadyImported(String orderId) {
        if (!config.isDuplicateCheckEnabled()) {
            log.warning("Flag DISABLE_DUPLICATE_CHECK esta ativa mas verificacao de duplicidade e obrigatoria. Ignorando flag.");
        }

        // Tentar JAPE primeiro, fallback para JDBC
        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = openJdbc();
            Timestamp processingCutoff = new Timestamp(System.currentTimeMillis() - (ORDER_IMPORT_CLAIM_TIMEOUT_MINUTES * 60L * 1000L));

            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT 1 FROM AD_FCPEDIDO ");
            sql.appendSql("WHERE ORDER_ID = :orderId ");
            sql.appendSql("AND (NUNOTA IS NOT NULL ");
            sql.appendSql("OR UPPER(COALESCE(STATUS_IMPORT, '')) IN ('SUCESSO', 'IMPORTADO') ");
            sql.appendSql("OR (UPPER(COALESCE(STATUS_IMPORT, '')) = 'PROCESSANDO' ");
            sql.appendSql("AND DH_IMPORTACAO IS NOT NULL ");
            sql.appendSql("AND DH_IMPORTACAO >= :processingCutoff))");
            sql.setNamedParameter("orderId", orderId);
            sql.setNamedParameter("processingCutoff", processingCutoff);

            rs = sql.executeQuery();
            if (rs.next()) {
                System.err.println("[FC-DIAG] " + orderId + " found in AD_FCPEDIDO -> return true");
                log.fine("Pedido " + orderId + " ja importado via AD_FCPEDIDO.");
                return true;
            }
            closeQuietly(rs);

            // Protecao anti-duplicidade com pedidos ja criados diretamente no TGFCAB
            boolean hasNumfast = hasColumn(jdbc, "TGFCAB", "AD_NUMFAST");
            boolean hasFcId = hasColumn(jdbc, "TGFCAB", "AD_FASTCHANNEL_ID");
            NativeSql cabSql = new NativeSql(jdbc);
            cabSql.appendSql("SELECT TOP 1 NUNOTA FROM TGFCAB WHERE 1=0 ");
            if (hasNumfast) {
                cabSql.appendSql("OR AD_NUMFAST = :orderId ");
            }
            if (hasFcId) {
                cabSql.appendSql("OR AD_FASTCHANNEL_ID = :orderId ");
            }
            if (!hasNumfast && !hasFcId) {
                log.fine("Pedido " + orderId + ": colunas AD_NUMFAST/AD_FASTCHANNEL_ID ausentes em TGFCAB, ignorando check.");
                return false;
            }
            cabSql.setNamedParameter("orderId", orderId);
            rs = cabSql.executeQuery();
            boolean foundInCab = rs.next();
            System.err.println("[FC-DIAG] " + orderId + " TGFCAB check: foundInCab=" + foundInCab + " hasNumfast=" + hasNumfast + " hasFcId=" + hasFcId);
            if (foundInCab) {
                log.warning("Pedido " + orderId + " ja importado via TGFCAB (hasNumfast=" + hasNumfast + ", hasFcId=" + hasFcId + ").");
            }
            return foundInCab;
        } catch (Exception e) {
            if (isJapeUnavailableError(e)) {
                log.fine("JAPE indisponivel em isOrderAlreadyImported, tentando JDBC");
                return isOrderAlreadyImportedJdbc(orderId);
            }
            log.log(Level.WARNING, "Erro ao verificar pedido existente", e);
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
        return false;
    }

    private boolean isOrderAlreadyImportedJdbc(String orderId) {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = br.com.bellube.fastchannel.util.DBUtil.getConnection();
            Timestamp processingCutoff = new Timestamp(System.currentTimeMillis() - (ORDER_IMPORT_CLAIM_TIMEOUT_MINUTES * 60L * 1000L));

            stmt = conn.prepareStatement(
                "SELECT 1 FROM AD_FCPEDIDO " +
                "WHERE ORDER_ID = ? " +
                "AND (NUNOTA IS NOT NULL " +
                "OR UPPER(COALESCE(STATUS_IMPORT, '')) IN ('SUCESSO', 'IMPORTADO') " +
                "OR (UPPER(COALESCE(STATUS_IMPORT, '')) = 'PROCESSANDO' " +
                "AND DH_IMPORTACAO IS NOT NULL AND DH_IMPORTACAO >= ?))");
            stmt.setString(1, orderId);
            stmt.setTimestamp(2, processingCutoff);

            rs = stmt.executeQuery();
            if (rs.next()) {
                log.fine("Pedido " + orderId + " ja importado (JDBC fallback).");
                return true;
            }
        } catch (Exception e2) {
            log.log(Level.FINE, "Fallback JDBC isOrderAlreadyImported falhou", e2);
        } finally {
            br.com.bellube.fastchannel.util.DBUtil.closeAll(rs, stmt, conn);
        }
        return false;
    }

    private boolean hasColumn(JdbcWrapper jdbc, String tableName, String columnName) {
        ResultSet rs = null;
        try {
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT COUNT(*) AS CNT FROM INFORMATION_SCHEMA.COLUMNS ");
            sql.appendSql("WHERE TABLE_NAME = :tableName AND COLUMN_NAME = :columnName");
            sql.setNamedParameter("tableName", tableName);
            sql.setNamedParameter("columnName", columnName);
            rs = sql.executeQuery();
            return rs.next() && rs.getInt("CNT") > 0;
        } catch (Exception e) {
            log.log(Level.FINE, "Falha ao validar coluna " + tableName + "." + columnName, e);
            return false;
        } finally {
            closeQuietly(rs);
        }
    }

    /**
     * Busca NUNOTA pelo OrderId do Fastchannel.
     */
    public BigDecimal getNuNotaByOrderId(String orderId) {
        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = openJdbc();

            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT NUNOTA FROM AD_FCPEDIDO WHERE ORDER_ID = :orderId");
            sql.setNamedParameter("orderId", orderId);

            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getBigDecimal("NUNOTA");
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao buscar NUNOTA", e);
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
        return null;
    }

    private String truncate(String str, int maxLength) {
        if (str == null) return null;
        return str.length() > maxLength ? str.substring(0, maxLength) : str;
    }

    private String truncateToColumn(String value, int maxLength) {
        if (value == null || maxLength <= 0) {
            return value;
        }
        return truncate(value, maxLength);
    }

    private Timestamp maxTimestamp(Timestamp current, Timestamp candidate) {
        if (candidate == null) {
            return current;
        }
        if (current == null || candidate.after(current)) {
            return candidate;
        }
        return current;
    }

    private OrderMappingFieldSizes getOrderMappingFieldSizes() {
        OrderMappingFieldSizes current = cachedOrderMappingFieldSizes;
        if (current != null) {
            return current;
        }

        synchronized (OrderService.class) {
            if (cachedOrderMappingFieldSizes == null) {
                cachedOrderMappingFieldSizes = loadOrderMappingFieldSizes();
            }
            return cachedOrderMappingFieldSizes;
        }
    }

    private OrderMappingFieldSizes loadOrderMappingFieldSizes() {
        OrderMappingFieldSizes defaults = OrderMappingFieldSizes.defaults();
        Connection conn = null;
        try {
            conn = DBUtil.getConnection();
            DatabaseMetaData metaData = conn.getMetaData();
            return new OrderMappingFieldSizes(
                    resolveColumnSize(metaData, "AD_FCPEDIDO", "ORDER_ID", defaults.orderId),
                    resolveColumnSize(metaData, "AD_FCPEDIDO", "STATUS_SKW", defaults.statusSkw),
                    resolveColumnSize(metaData, "AD_FCPEDIDO", "STATUS_IMPORT", defaults.statusImport),
                    resolveColumnSize(metaData, "AD_FCPEDIDO", "NOME_CLIENTE", defaults.nomeCliente),
                    resolveColumnSize(metaData, "AD_FCPEDIDO", "CPF_CNPJ", defaults.cpfCnpj),
                    resolveColumnSize(metaData, "AD_FCPEDIDO", "ERRO_MSG", defaults.erroMsg));
        } catch (Exception e) {
            log.log(Level.FINE, "Nao foi possivel ler metadata de AD_FCPEDIDO; usando limites conservadores.", e);
            return defaults;
        } finally {
            DBUtil.closeAll(null, null, conn);
        }
    }

    private int resolveColumnSize(DatabaseMetaData metaData, String table, String column, int fallback) {
        int size = resolveColumnSize(metaData, null, null, table, column);
        if (size > 0) {
            return size;
        }
        size = resolveColumnSize(metaData, null, null, table.toUpperCase(), column.toUpperCase());
        if (size > 0) {
            return size;
        }
        size = resolveColumnSize(metaData, null, null, table.toLowerCase(), column.toLowerCase());
        return size > 0 ? size : fallback;
    }

    private int resolveColumnSize(DatabaseMetaData metaData, String catalog, String schema, String table, String column) {
        ResultSet rs = null;
        try {
            rs = metaData.getColumns(catalog, schema, table, column);
            if (rs != null && rs.next()) {
                int columnSize = rs.getInt("COLUMN_SIZE");
                if (columnSize > 0) {
                    return columnSize;
                }
            }
        } catch (Exception e) {
            log.log(Level.FINEST, "Falha ao consultar metadata de " + table + "." + column, e);
        } finally {
            closeQuietly(rs);
        }
        return -1;
    }

    private boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private boolean isNullOrZero(BigDecimal value) {
        return value == null || value.compareTo(BigDecimal.ZERO) <= 0;
    }

    private static final class OrderImportClaim {
        private final BigDecimal existingNuNota;
        private final boolean alreadyProcessing;

        private OrderImportClaim(BigDecimal existingNuNota, boolean alreadyProcessing) {
            this.existingNuNota = existingNuNota;
            this.alreadyProcessing = alreadyProcessing;
        }

        private static OrderImportClaim claimed() {
            return new OrderImportClaim(null, false);
        }

        private static OrderImportClaim reused(BigDecimal nuNota) {
            return new OrderImportClaim(nuNota, false);
        }

        private static OrderImportClaim alreadyProcessing() {
            return new OrderImportClaim(null, true);
        }

        private boolean hasExistingNuNota() {
            return existingNuNota != null;
        }

        private BigDecimal getExistingNuNota() {
            return existingNuNota;
        }

        private boolean isAlreadyProcessing() {
            return alreadyProcessing;
        }
    }

    private static final class OrderMappingSnapshot {
        private final String orderId;
        private final BigDecimal nuNota;
        private final BigDecimal codParc;
        private final String statusImport;
        private final Timestamp dhImportacao;

        private OrderMappingSnapshot(String orderId, BigDecimal nuNota, BigDecimal codParc,
                                     String statusImport, Timestamp dhImportacao) {
            this.orderId = orderId;
            this.nuNota = nuNota;
            this.codParc = codParc;
            this.statusImport = statusImport;
            this.dhImportacao = dhImportacao;
        }
    }

    private String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private String removeTag(String source, String tag) {
        if (source == null || tag == null || tag.isEmpty()) {
            return source;
        }
        String updated = source.replace(tag, "");
        updated = updated.replaceAll("\\s*\\|\\s*\\|\\s*", " | ");
        updated = updated.replaceAll("^\\s*\\|\\s*|\\s*\\|\\s*$", "");
        return trimToNull(updated);
    }

    private String ensureTagInObservacaoInterna(String source, String tag) {
        if (tag == null || tag.isEmpty()) {
            return source;
        }
        String current = trimToNull(source);
        if (current == null) {
            return tag;
        }
        if (current.contains(tag)) {
            return current;
        }
        String merged = current + " | " + tag;
        return merged.length() > 1000 ? merged.substring(0, 1000) : merged;
    }

    private boolean equalsNullable(String a, String b) {
        if (a == null) {
            return b == null;
        }
        return a.equals(b);
    }

    private BigDecimal safeAsBigDecimal(DynamicVO vo, String field) {
        if (vo == null || field == null) {
            return null;
        }
        try {
            return vo.asBigDecimal(field);
        } catch (Exception ignored) {
            return null;
        }
    }

    private BigDecimal resolveFallbackCodParc() {
        BigDecimal configured = getDefaultCodParc();
        if (configured != null) {
            return configured;
        }
        log.warning("CODPARC fallback nao configurado. Nenhum parceiro arbitrario sera escolhido.");
        return null;
    }

    private BigDecimal resolveCodLocalForOrder(OrderDTO order, BigDecimal codEmp) {
        String[] candidates = {
                order != null ? order.getStorageId() : null,
                order != null && order.getStorageId() != null ? "S:" + order.getStorageId() : null,
                order != null ? order.getResellerId() : null,
                order != null && order.getResellerId() != null ? "R:" + order.getResellerId() : null,
                config.getStorageId(),
                config.getStorageId() != null ? "S:" + config.getStorageId() : null
        };

        for (String candidate : candidates) {
            if (candidate == null || candidate.trim().isEmpty()) {
                continue;
            }
            BigDecimal mapped = deparaService.getCodigoSankhya(DeparaService.TIPO_LOCAL, candidate.trim());
            if (mapped != null) {
                return mapped;
            }
            mapped = deparaService.getCodigoSankhya(DeparaService.TIPO_STOCK_STORAGE, candidate.trim());
            if (mapped != null) {
                return mapped;
            }
        }

        BigDecimal configured = config.getCodLocal();
        if (configured != null) {
            return configured;
        }

        // Fallback nativo via Jape para ambientes com configuracao parcial.
        try {
            JapeWrapper estDAO = JapeFactory.dao("Estoque");
            Collection<DynamicVO> estoques = estDAO.find("this.CODLOCAL > 0");
            BigDecimal codLocalEscolhido = null;
            BigDecimal estoqueEscolhido = null;
            if (estoques != null) {
                for (DynamicVO est : estoques) {
                    if (est == null) {
                        continue;
                    }
                    if (codEmp != null) {
                        BigDecimal codEmpEst = est.asBigDecimal("CODEMP");
                        if (codEmpEst != null && codEmpEst.compareTo(codEmp) != 0) {
                            continue;
                        }
                    }
                    BigDecimal codLocal = est.asBigDecimal("CODLOCAL");
                    if (isNullOrZero(codLocal)) {
                        continue;
                    }
                    BigDecimal estoque = est.asBigDecimal("ESTOQUE");
                    if (codLocalEscolhido == null) {
                        codLocalEscolhido = codLocal;
                        estoqueEscolhido = estoque;
                        continue;
                    }
                    if (estoque != null && (estoqueEscolhido == null || estoque.compareTo(estoqueEscolhido) > 0)) {
                        codLocalEscolhido = codLocal;
                        estoqueEscolhido = estoque;
                    }
                }
            }
            if (!isNullOrZero(codLocalEscolhido)) {
                return codLocalEscolhido;
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Falha ao resolver CODLOCAL via Jape. Aplicando fallback SQL.", e);
        }

        // Fallback SQL defensivo final.
        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = openJdbc();
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT TOP 1 CODLOCAL FROM TGFEST WHERE CODLOCAL > 0 ");
            if (codEmp != null) {
                sql.appendSql("AND CODEMP = :codEmp ");
                sql.setNamedParameter("codEmp", codEmp);
            }
            sql.appendSql("ORDER BY ESTOQUE DESC");
            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getBigDecimal("CODLOCAL");
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Nao foi possivel resolver CODLOCAL fallback no OrderService", e);
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
        // Fallback absoluto: sempre retornar 99000000 se nenhum CODLOCAL foi resolvido
        log.info("CODLOCAL nao resolvido por nenhum metodo, usando fallback fixo 99000000");
        return new BigDecimal("99000000");
    }

    private String normalizeUsoProd(String usoProd) {
        // Paridade legado: itens de pedido entram com USOPROD = R.
        return "R";
    }

    private Map<BigDecimal, OrderItemDTO> buildOrderItemsBySequencia(OrderDTO order) {
        Map<BigDecimal, OrderItemDTO> bySeq = new HashMap<>();
        if (order == null || order.getItems() == null || order.getItems().isEmpty()) {
            return bySeq;
        }
        int seq = 1;
        for (OrderItemDTO item : order.getItems()) {
            bySeq.put(new BigDecimal(seq), item);
            seq++;
        }
        return bySeq;
    }

    private BigDecimal resolvePrecoBaseOrigemFromOrder(OrderItemDTO item) {
        if (item == null) {
            return null;
        }
        BigDecimal listPrice = item.getListPrice();
        if (!isNullOrZero(listPrice)) {
            return listPrice;
        }
        BigDecimal unitPrice = item.getUnitPrice();
        if (isNullOrZero(unitPrice)) {
            return null;
        }
        BigDecimal discount = item.getDiscount();
        BigDecimal quantity = item.getQuantity();
        if (!isNullOrZero(discount) && !isNullOrZero(quantity)) {
            BigDecimal discountPerUnit = discount.divide(quantity, 6, BigDecimal.ROUND_HALF_UP);
            BigDecimal origem = unitPrice.add(discountPerUnit);
            if (!isNullOrZero(origem)) {
                return origem;
            }
        }
        return unitPrice;
    }

    private BigDecimal resolveCodUsuIntegracao(JdbcWrapper jdbc) {
        List<String> userCandidates = new ArrayList<>();
        String configured = trimToNull(config.getSankhyaUser());
        if (configured != null) {
            userCandidates.add(configured);
        }
        userCandidates.add("FAST");
        userCandidates.add("INTEGRACAOFASTCHANNEL");

        ResultSet rs = null;
        for (String user : userCandidates) {
            try {
                NativeSql sql = new NativeSql(jdbc);
                sql.appendSql("SELECT TOP 1 CODUSU FROM TSIUSU ");
                sql.appendSql("WHERE UPPER(NOMUSU)=UPPER(:user) OR UPPER(NOMEUSU)=UPPER(:user)");
                sql.setNamedParameter("user", user);
                rs = sql.executeQuery();
                if (rs.next()) {
                    BigDecimal codUsu = rs.getBigDecimal("CODUSU");
                    if (!isNullOrZero(codUsu)) {
                        return codUsu;
                    }
                }
            } catch (Exception e) {
                log.log(Level.FINE, "Falha ao resolver CODUSU de integracao para usuario " + user, e);
            } finally {
                closeQuietly(rs);
                rs = null;
            }
        }
        return null;
    }

    private BigDecimal resolveCodTipVendaCabecalho(BigDecimal nuNota) {
        if (isNullOrZero(nuNota)) {
            return null;
        }
        try {
            JapeWrapper cabDAO = JapeFactory.dao("CabecalhoNota");
            DynamicVO cabVO = cabDAO.findByPK(nuNota);
            BigDecimal codTipVenda = safeAsBigDecimal(cabVO, "CODTIPVENDA");
            if (!isNullOrZero(codTipVenda)) {
                return codTipVenda;
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Falha ao resolver CODTIPVENDA do cabecalho da nota " + nuNota, e);
        }
        return null;
    }

    private BigDecimal resolveCabNumericFallback(JdbcWrapper jdbc, DynamicVO cabVO, String field) {
        BigDecimal codParc = safeAsBigDecimal(cabVO, "CODPARC");
        BigDecimal codEmp = safeAsBigDecimal(cabVO, "CODEMP");
        BigDecimal codTipOper = safeAsBigDecimal(cabVO, "CODTIPOPER");
        ResultSet rs = null;
        try {
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT TOP 1 " + field + " AS VAL ");
            sql.appendSql("FROM TGFCAB ");
            sql.appendSql("WHERE " + field + " IS NOT NULL AND " + field + " > 0 ");
            if (!isNullOrZero(codParc) && hasColumn(jdbc, "TGFCAB", "CODPARC")) {
                sql.appendSql("AND CODPARC = :codParc ");
                sql.setNamedParameter("codParc", codParc);
            }
            if (!isNullOrZero(codEmp) && hasColumn(jdbc, "TGFCAB", "CODEMP")) {
                sql.appendSql("AND CODEMP = :codEmp ");
                sql.setNamedParameter("codEmp", codEmp);
            }
            if (!isNullOrZero(codTipOper) && hasColumn(jdbc, "TGFCAB", "CODTIPOPER")) {
                sql.appendSql("AND CODTIPOPER = :codTipOper ");
                sql.setNamedParameter("codTipOper", codTipOper);
            }
            sql.appendSql("ORDER BY NUNOTA DESC");
            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getBigDecimal("VAL");
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Falha ao resolver fallback de cabecalho para campo " + field, e);
        } finally {
            closeQuietly(rs);
        }
        return null;
    }

    private BigDecimal resolveCodTribByProduto(JdbcWrapper jdbc, BigDecimal codProd) {
        if (isNullOrZero(codProd)) {
            return null;
        }
        try {
            JapeWrapper produtoDAO = JapeFactory.dao("Produto");
            DynamicVO produtoVO = produtoDAO.findByPK(codProd);
            BigDecimal fromProduto = safeAsBigDecimal(produtoVO, "CODTRIB");
            if (!isNullOrZero(fromProduto)) {
                return fromProduto;
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Falha ao resolver CODTRIB via Jape para CODPROD " + codProd, e);
        }
        ResultSet rs = null;
        try {
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT TOP 1 CODTRIB FROM TGFPRO WHERE CODPROD = :codProd");
            sql.setNamedParameter("codProd", codProd);
            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getBigDecimal("CODTRIB");
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Falha SQL ao resolver CODTRIB para CODPROD " + codProd, e);
        } finally {
            closeQuietly(rs);
        }
        return new BigDecimal("60");
    }

    private BigDecimal resolveCodVendCabecalho(BigDecimal nuNota) {
        if (isNullOrZero(nuNota)) {
            return null;
        }
        try {
            JapeWrapper cabDAO = JapeFactory.dao("CabecalhoNota");
            DynamicVO cabVO = cabDAO.findByPK(nuNota);
            BigDecimal codVend = safeAsBigDecimal(cabVO, "CODVEND");
            if (!isNullOrZero(codVend)) {
                return codVend;
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Falha ao resolver CODVEND do cabecalho da nota " + nuNota, e);
        }
        return null;
    }

    private BigDecimal resolveTotalCusto(JdbcWrapper jdbc, BigDecimal nuNota) {
        ResultSet rs = null;
        try {
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT ISNULL(SUM(ISNULL(CUSTO, 0) * ISNULL(QTDNEG, 0)), 0) AS VLR ");
            sql.appendSql("FROM TGFITE WHERE NUNOTA = :nuNota");
            sql.setNamedParameter("nuNota", nuNota);
            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getBigDecimal("VLR");
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Falha ao calcular TOTALCUSTOPROD para NUNOTA " + nuNota, e);
        } finally {
            closeQuietly(rs);
        }
        return null;
    }

    private BigDecimal resolveTotalItens(JdbcWrapper jdbc, BigDecimal nuNota) {
        ResultSet rs = null;
        try {
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT ISNULL(SUM(ISNULL(VLRTOT, ISNULL(VLRUNIT,0) * ISNULL(QTDNEG,0))), 0) AS VLR ");
            sql.appendSql("FROM TGFITE WHERE NUNOTA = :nuNota");
            sql.setNamedParameter("nuNota", nuNota);
            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getBigDecimal("VLR");
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Falha ao calcular total de itens para NUNOTA " + nuNota, e);
        } finally {
            closeQuietly(rs);
        }
        return null;
    }

    private WeightTotals resolveWeightTotals(JdbcWrapper jdbc, BigDecimal nuNota) {
        ResultSet rs = null;
        try {
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT ");
            sql.appendSql("ISNULL(SUM(ISNULL(P.PESOLIQ, 0) * ISNULL(I.QTDNEG, 0)), 0) AS PESO, ");
            sql.appendSql("ISNULL(SUM(ISNULL(P.PESOBRUTO, 0) * ISNULL(I.QTDNEG, 0)), 0) AS PESOBRUTO ");
            sql.appendSql("FROM TGFITE I ");
            sql.appendSql("INNER JOIN TGFPRO P ON P.CODPROD = I.CODPROD ");
            sql.appendSql("WHERE I.NUNOTA = :nuNota");
            sql.setNamedParameter("nuNota", nuNota);
            rs = sql.executeQuery();
            if (rs.next()) {
                WeightTotals totals = new WeightTotals();
                totals.peso = rs.getBigDecimal("PESO");
                totals.pesoBruto = rs.getBigDecimal("PESOBRUTO");
                return totals;
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Falha ao calcular PESO/PESOBRUTO para NUNOTA " + nuNota, e);
        } finally {
            closeQuietly(rs);
        }
        return null;
    }

    private void closeQuietly(ResultSet rs) {
        if (rs != null) {
            try { rs.close(); } catch (Exception ignored) {}
        }
    }

    private void closeQuietly(AutoCloseable closeable) {
        if (closeable != null) {
            try { closeable.close(); } catch (Exception ignored) {}
        }
    }

    private static volatile boolean japeReady = false;

    /**
     * Retorna true se JAPE/mge-core esta disponivel.
     * Usado para skip graceful durante warmup do WildFly.
     */
    static boolean isJapeReady() {
        if (japeReady) return true;
        try {
            EntityFacadeFactory.getCoreFacade();
            japeReady = true;
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private JdbcWrapper openJdbc() throws Exception {
        JdbcWrapper jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();
        jdbc.openSession();
        if (!japeReady) {
            japeReady = true;
            log.info("OrderService: JAPE/mge-core disponivel.");
        }
        return jdbc;
    }

    private static boolean isJapeUnavailableError(Exception e) {
        Throwable current = e;
        while (current != null) {
            String msg = current.getMessage();
            if (msg != null && msg.contains("Erro ao inicializar datasource para provider mge-core")) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private void closeJdbc(JdbcWrapper jdbc) {
        if (jdbc != null) {
            try {
                jdbc.closeSession();
            } catch (Exception e) {
                log.log(Level.FINE, "Erro ao fechar JdbcWrapper", e);
            }
        }
    }

    private void closeJapeSession(JapeSession.SessionHandle hnd) {
        if (hnd != null) {
            try {
                JapeSession.close(hnd);
            } catch (Exception e) {
                log.log(Level.FINE, "Erro ao fechar sessao Jape", e);
            }
        }
    }

    private BigDecimal normalizeMoney(BigDecimal value) {
        if (value == null) return null;
        if (value.scale() <= 0) {
            return value.movePointLeft(2);
        }
        return value;
    }

    private List<String> buildDocumentCandidates(String normalizedCpfCnpj) {
        Set<String> ordered = new LinkedHashSet<>();
        ordered.add(normalizedCpfCnpj);

        if (normalizedCpfCnpj.length() == 11) {
            ordered.add(formatCpf(normalizedCpfCnpj));
        } else if (normalizedCpfCnpj.length() == 14) {
            ordered.add(formatCnpj(normalizedCpfCnpj));
        }
        return new ArrayList<>(ordered);
    }

    private String formatCpf(String cpfDigits) {
        if (cpfDigits == null || cpfDigits.length() != 11) {
            return cpfDigits;
        }
        return cpfDigits.substring(0, 3) + "."
                + cpfDigits.substring(3, 6) + "."
                + cpfDigits.substring(6, 9) + "-"
                + cpfDigits.substring(9);
    }

    private String formatCnpj(String cnpjDigits) {
        if (cnpjDigits == null || cnpjDigits.length() != 14) {
            return cnpjDigits;
        }
        return cnpjDigits.substring(0, 2) + "."
                + cnpjDigits.substring(2, 5) + "."
                + cnpjDigits.substring(5, 8) + "/"
                + cnpjDigits.substring(8, 12) + "-"
                + cnpjDigits.substring(12);
    }

    private static final class ExcDefaults {
        private BigDecimal nutab;
        private BigDecimal vlrVenda;
    }

    private static final class ProdutoDefaults {
        private BigDecimal cusRep;
        private String usoProd;
    }

    private static final class OrderImportBatchProgress {
        private final int imported;
        private final Timestamp nextCursor;
        private final int totalOrdersSeen;

        private OrderImportBatchProgress(int imported, Timestamp nextCursor, int totalOrdersSeen) {
            this.imported = imported;
            this.nextCursor = nextCursor;
            this.totalOrdersSeen = totalOrdersSeen;
        }

        private boolean hasNewCursor(Timestamp previousCursor) {
            return nextCursor != null && (previousCursor == null || nextCursor.after(previousCursor));
        }
    }

    private static final class OrderMappingFieldSizes {
        private final int orderId;
        private final int statusSkw;
        private final int statusImport;
        private final int nomeCliente;
        private final int cpfCnpj;
        private final int erroMsg;

        private OrderMappingFieldSizes(int orderId, int statusSkw, int statusImport,
                                       int nomeCliente, int cpfCnpj, int erroMsg) {
            this.orderId = orderId;
            this.statusSkw = statusSkw;
            this.statusImport = statusImport;
            this.nomeCliente = nomeCliente;
            this.cpfCnpj = cpfCnpj;
            this.erroMsg = erroMsg;
        }

        private static OrderMappingFieldSizes defaults() {
            return new OrderMappingFieldSizes(100, 5, 20, 120, 20, 990);
        }
    }

    private static final class WeightTotals {
        private BigDecimal peso;
        private BigDecimal pesoBruto;
    }
}
