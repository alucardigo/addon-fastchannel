package br.com.bellube.fastchannel.service;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.config.FastchannelConstants;
import br.com.bellube.fastchannel.dto.*;
import br.com.bellube.fastchannel.http.FastchannelOrdersClient;
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
 * Servi?o de Importa??o de Pedidos do Fastchannel para Sankhya.
 *
 * Processo:
 * 1. Buscar pedidos pendentes da API
 * 2. Localizar ou criar parceiro (TGFPAR)
 * 3. Criar cabe?alho do pedido (TGFCAB)
 * 4. Criar itens (TGFITE)
 * 5. Registrar na AD_FCPEDIDO
 * 6. Notificar Fastchannel (marcar como sincronizado)
 */
public class OrderService {

    private static final Logger log = Logger.getLogger(OrderService.class.getName());

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
     * @return n?mero de pedidos importados com sucesso
     */
    public int importPendingOrders() {
        int imported = 0;
        int pageSize = config.getBatchSize();

        try {
            Timestamp lastSync = config.getLastOrderSync();
            log.info("Iniciando importa??o de pedidos. ?ltima sync: " + lastSync);

            imported += importPendingOrdersFromCursor(lastSync, pageSize);

            // Fallback: se o cursor estiver adiantado/fora de fuso e houver pedidos antigos nao sincronizados,
            // varre novamente sem CreatedAfter.
            if (imported == 0 && lastSync != null) {
                log.warning("Nenhum pedido retornado com CreatedAfter. Executando fallback sem cursor para buscar pedidos nao sincronizados.");
                imported += importPendingOrdersFromCursor(null, pageSize);
            }

            // Atualizar cursor de sincroniza??o
            if (imported > 0) {
                config.updateLastOrderSync(new Timestamp(System.currentTimeMillis()));
            }

            log.info("Importa??o conclu?da. " + imported + " pedidos importados.");

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro na importa??o de pedidos", e);
            logService.error(LogService.OP_ORDER_IMPORT, "Erro geral na importa??o", e);
        }

        return imported;
    }

    private int importPendingOrdersFromCursor(Timestamp lastSync, int pageSize) {
        int imported = 0;
        int page = 1;

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

            for (OrderDTO order : orders) {
                OrderDTO target = order;
                try {
                    if (isOrderAlreadyImported(order.getOrderId())) {
                        log.fine("Pedido " + order.getOrderId() + " j? importado. Pulando.");
                        continue;
                    }

                    OrderDTO detailed = null;
                    try {
                        detailed = ordersClient.getOrder(order.getOrderId());
                    } catch (Exception e) {
                        log.log(Level.WARNING, "Falha ao buscar detalhes do pedido " + order.getOrderId() + ". Usando dados da listagem.", e);
                    }

                    target = detailed != null ? detailed : order;
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
                    log.log(Level.SEVERE, "Erro ao importar pedido " + target.getOrderId(), e);
                    logService.logOrderImport(target.getOrderId(), null, false,
                            "Cliente=" + (target.getCustomer() != null ? String.valueOf(target.getCustomer().getName()) : "") +
                            " | CPF/CNPJ=" + (target.getCustomer() != null ? String.valueOf(target.getCustomer().getCpfCnpj()) : "") +
                            " | Erro=" + buildErrorDetails(e));
                }
            }

            // Se retornou menos que pageSize, n?o h? mais p?ginas
            if (orders.size() < pageSize) {
                break;
            }
            page++;
        }

        return imported;
    }

    /**
     * Importa um pedido espec?fico usando o servico nativo do Sankhya.
     *
     * @param order dados do pedido
     * @return NUNOTA criado ou null se falhar
     */
    public BigDecimal importOrder(OrderDTO order) throws Exception {
        log.info("Importando pedido: " + order.getOrderId());

        // Validar pedido
        validateOrder(order);
        normalizeOrderValues(order);

        BigDecimal codParc = null;
        try {
            FastchannelHeaderMappingService.ResolvedHeader resolvedHeader = headerMappingService.resolve(order);
            order.setCodEmp(resolvedHeader.getCodEmp());
            order.setCodTipOper(resolvedHeader.getCodTipOper());
            order.setCodLocal(resolveCodLocalForOrder(order, resolvedHeader.getCodEmp()));

            // 1. Localizar ou criar parceiro
            codParc = resolvedHeader.getCodParc();
            if (codParc == null) {
                codParc = getDefaultCodParc();
            }
            if (codParc == null) {
                codParc = findOrCreateParceiro(order.getCustomer(), order.getShippingAddress());
            }

            // 2. Validar que todos os produtos existem ANTES de criar o pedido
            validateAllProductsExist(order);

            // 3. Buscar parametros do pedido
            BigDecimal codTipVenda = resolvedHeader.getCodTipVenda();
            BigDecimal codVend = getCodVend(codParc);
            if (isNullOrZero(codVend)) {
                throw new Exception("Parceiro " + codParc + " sem vendedor preferencial (TGFPAR.CODVEND).");
            }
            BigDecimal codNat = resolvedHeader.getCodNat();
            BigDecimal codCenCus = resolvedHeader.getCodCenCus();

            // 4. Criar pedido via servico
            BigDecimal nuNota = importOrderViaService(order, codParc, codTipVenda, codVend, codNat, codCenCus);
            enforcePostImportParity(nuNota, order, codVend, resolvedHeader.getCodEmp());

            // 5. Registrar na AD_FCPEDIDO
            upsertOrderMapping(order, nuNota, codParc, "SUCESSO", null);

            log.info("Pedido " + order.getOrderId() + " importado como NUNOTA " + nuNota);
            return nuNota;

        } catch (Exception e) {
            upsertOrderMapping(order, null, codParc, "ERRO", buildErrorDetails(e));
            log.log(Level.SEVERE, "Falha ao importar pedido " + order.getOrderId(), e);
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
                        applyItensParityNative(jdbc, nuNota, codEmp, configuredNuTab);
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
            BigDecimal current = cabVO.asBigDecimal("AD_CODVENDEXEC");
            BigDecimal target = new BigDecimal("281");
            if (isNullOrZero(current) || current.compareTo(target) != 0) {
                updateVO = updateVO.set("AD_CODVENDEXEC", new BigDecimal("281"));
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
        if (hasColumn(jdbc, "TGFCAB", "TOTALCUSTOPROD") && cabVO.asBigDecimal("TOTALCUSTOPROD") == null) {
            BigDecimal totalCusto = resolveTotalCusto(jdbc, nuNota);
            if (totalCusto != null) {
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
            BigDecimal frete = cabVO.asBigDecimal("VLRFRETE");
            if (frete == null) {
                updateVO = updateVO.set("VLRFRETE", BigDecimal.ZERO);
                changed = true;
            }
        }
        if (hasColumn(jdbc, "TGFCAB", "OBSERVACAO") && orderTag != null) {
            String currentObs = trimToNull(cabVO.asString("OBSERVACAO"));
            String normalizedObs = removeTag(currentObs, orderTag);
            if (!equalsNullable(currentObs, normalizedObs)) {
                updateVO = updateVO.set("OBSERVACAO", normalizedObs);
                changed = true;
            }
        }
        if (hasColumn(jdbc, "TGFCAB", "OBSERVACAOINTERNA") && orderTag != null) {
            String currentObsInt = trimToNull(cabVO.asString("OBSERVACAOINTERNA"));
            String normalizedObsInt = ensureTagInObservacaoInterna(currentObsInt, orderTag);
            if (!equalsNullable(currentObsInt, normalizedObsInt)) {
                updateVO = updateVO.set("OBSERVACAOINTERNA", normalizedObsInt);
                changed = true;
            }
        }

        if (changed) {
            updateVO.update();
        }
    }

    private void applyItensParityNative(JdbcWrapper jdbc, BigDecimal nuNota, BigDecimal codEmp, BigDecimal configuredNuTab) throws Exception {
        JapeWrapper iteDAO = JapeFactory.dao("ItemNota");
        Collection<DynamicVO> itens = iteDAO.find("this.NUNOTA = ?", nuNota);
        if (itens == null || itens.isEmpty()) {
            return;
        }

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
                if (excDefaults.vlrVenda != null && (isNullOrZero(current) || current.compareTo(excDefaults.vlrVenda) != 0)) {
                    updateVO = updateVO.set("PRECOBASE", excDefaults.vlrVenda);
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

        // Fallback SQL para ambientes onde campo ainda não foi carregado por metadata/config antiga.
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
            throw new Exception("OrderId ? obrigat?rio");
        }
        if (order.getCustomer() == null) {
            throw new Exception("Cliente ? obrigat?rio");
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
            throw new Exception("Dados do cliente n?o informados");
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

                    FluidCreateVO parceiroBuilder = parceiroDAO.create()
                            .set("NOMEPARC", truncate(customer.getName(), 100))
                            .set("TIPPESSOA", tipoPessoa)
                            .set("CGC_CPF", cpfCnpj)
                            .set("EMAIL", truncate(customer.getEmail(), 80))
                            .set("TELEFONE", truncate(customer.getPhone(), 15))
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
            log.log(Level.WARNING, "Erro ao criar endere?o", e);
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
            Collection<DynamicVO> cidades = cidadeDAO.find("UPPER(this.NOMECID) = UPPER(?) AND this.UF = ?", cidade, ufNorm);
            if (cidades != null) {
                for (DynamicVO cidadeVO : cidades) {
                    if (cidadeVO != null) {
                        BigDecimal codCid = cidadeVO.asBigDecimal("CODCID");
                        if (!isNullOrZero(codCid)) {
                            return codCid;
                        }
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

        // Cidade n?o encontrada - usar c?digo padr?o ou criar
        log.warning("Cidade n?o encontrada: " + nomeCidade + "/" + uf);
        return null;
    }

    /**
     * Cria cabe?alho do pedido (TGFCAB).
     */
    private BigDecimal createCabecalho(OrderDTO order, BigDecimal codParc) throws Exception {
        JapeWrapper cabDAO = JapeFactory.dao("CabecalhoNota");

        BigDecimal codTipOper = config.getCodTipOper();
        BigDecimal codEmp = config.getCodemp();

        if (codTipOper == null) {
            throw new Exception("CODTIPOPER n?o configurado para integra??o Fastchannel");
        }
        if (codEmp == null) {
            throw new Exception("CODEMP n?o configurado para integra??o Fastchannel");
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
        obs.append("Pedido Fastchannel: ").append(order.getOrderId());

        if (order.getNotes() != null && !order.getNotes().isEmpty()) {
            obs.append("\n").append(order.getNotes());
        }

        if (order.getShippingMethod() != null) {
            obs.append("\nFrete: ").append(order.getShippingMethod());
        }

        return truncate(obs.toString(), 500);
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
                throw new Exception("Produto n?o encontrado para SKU: " + item.getSku());
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
            log.log(Level.WARNING, "Erro ao buscar CODVOL", e);
        }
        return "UN";
    }

    /**
     * Registra mapeamento do pedido na AD_FCPEDIDO.
     */
    private void registerOrderMapping(String orderId, BigDecimal nuNota, BigDecimal codParc) {
        JdbcWrapper jdbc = null;
        try {
            jdbc = openJdbc();

            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("INSERT INTO AD_FCPEDIDO ");
            sql.appendSql("(ORDER_ID, NUNOTA, CODPARC, STATUS_FC, STATUS_SKW, DH_IMPORTACAO) ");
            sql.appendSql("VALUES (:orderId, :nuNota, :codParc, :statusFc, :statusSkw, CURRENT_TIMESTAMP)");

            sql.setNamedParameter("orderId", orderId);
            sql.setNamedParameter("nuNota", nuNota);
            sql.setNamedParameter("codParc", codParc);
            sql.setNamedParameter("statusFc", FastchannelConstants.STATUS_APPROVED);
            sql.setNamedParameter("statusSkw", "L"); // Liberado

            sql.executeUpdate();

        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao registrar mapeamento de pedido", e);
        }
    }

    /**
     * Registra ou atualiza mapeamento do pedido na AD_FCPEDIDO.
     */
    private void upsertOrderMapping(OrderDTO order, BigDecimal nuNota, BigDecimal codParc,
                                    String statusImport, String errorMsg) {
        JdbcWrapper jdbc = null;
        try {
            jdbc = openJdbc();

            boolean exists = false;
            ResultSet rs = null;
            try {
                NativeSql check = new NativeSql(jdbc);
                check.appendSql("SELECT 1 FROM AD_FCPEDIDO WHERE ORDER_ID = :orderId");
                check.setNamedParameter("orderId", order.getOrderId());
                rs = check.executeQuery();
                exists = rs.next();
            } finally {
                closeQuietly(rs);
            }

            NativeSql sql = new NativeSql(jdbc);
            if (exists) {
                sql.appendSql("UPDATE AD_FCPEDIDO SET ");
                sql.appendSql("NUNOTA = :nuNota, CODPARC = :codParc, STATUS_FC = :statusFc, STATUS_SKW = :statusSkw, ");
                sql.appendSql("STATUS_IMPORT = :statusImport, DH_IMPORTACAO = CURRENT_TIMESTAMP, ");
                // Preserva nome/documento previamente registrados quando a API vier sem customer no payload
                sql.appendSql("DH_PEDIDO = :dhPedido, NOME_CLIENTE = COALESCE(:nomeCliente, NOME_CLIENTE), ");
                sql.appendSql("CPF_CNPJ = COALESCE(:cpfCnpj, CPF_CNPJ), ");
                sql.appendSql("VALOR_TOTAL = :valorTotal, VALOR_FRETE = :valorFrete, ERRO_MSG = :erroMsg ");
                sql.appendSql("WHERE ORDER_ID = :orderId");
            } else {
                sql.appendSql("INSERT INTO AD_FCPEDIDO ");
                sql.appendSql("(ORDER_ID, NUNOTA, CODPARC, STATUS_FC, STATUS_SKW, STATUS_IMPORT, DH_IMPORTACAO, ");
                sql.appendSql("DH_PEDIDO, NOME_CLIENTE, CPF_CNPJ, VALOR_TOTAL, VALOR_FRETE, ERRO_MSG) ");
                sql.appendSql("VALUES (:orderId, :nuNota, :codParc, :statusFc, :statusSkw, :statusImport, CURRENT_TIMESTAMP, ");
                sql.appendSql(":dhPedido, :nomeCliente, :cpfCnpj, :valorTotal, :valorFrete, :erroMsg)");
            }

            sql.setNamedParameter("orderId", order.getOrderId());
            sql.setNamedParameter("nuNota", nuNota);
            sql.setNamedParameter("codParc", codParc);
            int statusFc = order.getStatus() > 0 ? order.getStatus() : FastchannelConstants.STATUS_APPROVED;
            sql.setNamedParameter("statusFc", statusFc);
            sql.setNamedParameter("statusSkw", "L"); // Liberado
            sql.setNamedParameter("statusImport", statusImport);
            sql.setNamedParameter("dhPedido", order.getCreatedAt() != null ? order.getCreatedAt() : new Timestamp(System.currentTimeMillis()));
            sql.setNamedParameter("nomeCliente", order.getCustomer() != null ? truncate(order.getCustomer().getName(), 120) : null);
            sql.setNamedParameter("cpfCnpj", order.getCustomer() != null ? truncate(order.getCustomer().getCpfCnpj(), 20) : null);
            sql.setNamedParameter("valorTotal", order.getTotal());
            sql.setNamedParameter("valorFrete", getFrete(order));
            sql.setNamedParameter("erroMsg", truncate(errorMsg, 2000));

            sql.executeUpdate();

        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao registrar mapeamento de pedido", e);
        } finally {
            closeJdbc(jdbc);
        }
    }

    private BigDecimal getFrete(OrderDTO order) {
        if (order == null) return null;
        return order.getShippingCost();
    }

    /**
     * Verifica se pedido j? foi importado.
     */
    private boolean isOrderAlreadyImported(String orderId) {
        if (!config.isDuplicateCheckEnabled()) {
            log.warning("Verificacao de duplicidade desabilitada por flag (fastchannel.disableDuplicateCheck=true).");
            return false;
        }
        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = openJdbc();

            NativeSql sql = new NativeSql(jdbc);
            // Considera como importado somente quando ja existe NUNOTA (ou status final de sucesso).
            // Registros em ERRO/PENDENTE devem ser reprocessados.
            sql.appendSql("SELECT 1 FROM AD_FCPEDIDO ");
            sql.appendSql("WHERE ORDER_ID = :orderId ");
            sql.appendSql("AND (NUNOTA IS NOT NULL OR UPPER(COALESCE(STATUS_IMPORT, '')) IN ('SUCESSO', 'IMPORTADO'))");
            sql.setNamedParameter("orderId", orderId);

            rs = sql.executeQuery();
            if (rs.next()) {
                return true;
            }
            closeQuietly(rs);

            // Protecao anti-duplicidade com pedidos ja criados diretamente no TGFCAB
            NativeSql cabSql = new NativeSql(jdbc);
            cabSql.appendSql("SELECT TOP 1 NUNOTA FROM TGFCAB WHERE 1=0 ");
            if (hasColumn(jdbc, "TGFCAB", "AD_NUMFAST")) {
                cabSql.appendSql("OR AD_NUMFAST = :orderId ");
            }
            if (hasColumn(jdbc, "TGFCAB", "AD_FASTCHANNEL_ID")) {
                cabSql.appendSql("OR AD_FASTCHANNEL_ID = :orderId ");
            }
            if (hasColumn(jdbc, "TGFCAB", "OBSERVACAOINTERNA")) {
                cabSql.appendSql("OR OBSERVACAOINTERNA LIKE :orderIdLike ");
            }
            if (hasColumn(jdbc, "TGFCAB", "OBSERVACAO")) {
                cabSql.appendSql("OR OBSERVACAO LIKE :orderIdLike ");
            }
            cabSql.setNamedParameter("orderId", orderId);
            cabSql.setNamedParameter("orderIdLike", "%" + orderId + "%");
            rs = cabSql.executeQuery();
            return rs.next();
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao verificar pedido existente", e);
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
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

    private boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private boolean isNullOrZero(BigDecimal value) {
        return value == null || value.compareTo(BigDecimal.ZERO) <= 0;
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

        // Tentativa nativa via Jape primeiro.
        try {
            JapeWrapper parceiroDAO = JapeFactory.dao("Parceiro");
            Collection<DynamicVO> parceiros = parceiroDAO.find("this.CLIENTE = ?", "S");
            BigDecimal menorCodParc = null;
            if (parceiros != null) {
                for (DynamicVO parceiro : parceiros) {
                    if (parceiro == null) {
                        continue;
                    }
                    BigDecimal codParc = parceiro.asBigDecimal("CODPARC");
                    if (isNullOrZero(codParc)) {
                        continue;
                    }
                    if (menorCodParc == null || codParc.compareTo(menorCodParc) < 0) {
                        menorCodParc = codParc;
                    }
                }
            }
            if (menorCodParc != null) {
                return menorCodParc;
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao resolver parceiro fallback", e);
        }
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

        // Fallback nativo via Jape para ambientes com configuração parcial.
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
        return null;
    }

    private String normalizeUsoProd(String usoProd) {
        String normalized = trimToNull(usoProd);
        if (normalized == null) {
            return "R";
        }
        if ("V".equalsIgnoreCase(normalized)) {
            return "R";
        }
        return normalized;
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

    private JdbcWrapper openJdbc() throws Exception {
        JdbcWrapper jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();
        jdbc.openSession();
        return jdbc;
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

    private static final class WeightTotals {
        private BigDecimal peso;
        private BigDecimal pesoBruto;
    }
}
