package br.com.bellube.fastchannel.service.strategy;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.config.FastchannelConstants;
import br.com.bellube.fastchannel.dto.OrderDTO;
import br.com.bellube.fastchannel.dto.OrderItemDTO;
import br.com.bellube.fastchannel.service.DeparaService;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.core.JapeSession;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.jape.util.JapeSessionContext;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;
import br.com.sankhya.jape.wrapper.fluid.FluidCreateVO;
import br.com.sankhya.modelcore.util.NumeracaoNotaHelper;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Estrategia PREFERENCIAL: Usa API interna do Sankhya com JapeWrapper.
 * Similar ao VendaPixAdianta, usa APIs oficiais diretamente.
 */
public class InternalApiStrategy implements OrderCreationStrategy {

    private static final Logger log = Logger.getLogger(InternalApiStrategy.class.getName());
    private static final Map<String, Boolean> CAB_FIELD_SUPPORT = new ConcurrentHashMap<>();
    private static final Map<String, Boolean> CAB_FIELD_REQUIRED = new ConcurrentHashMap<>();
    private static final Map<String, Boolean> ITEM_FIELD_SUPPORT = new ConcurrentHashMap<>();

    private final FastchannelConfig config;
    private final DeparaService deparaService;

    public InternalApiStrategy() {
        this.config = FastchannelConfig.getInstance();
        this.deparaService = DeparaService.getInstance();
    }

    @Override
    public String getStrategyName() {
        return "InternalAPI";
    }

    @Override
    public boolean isAvailable() {
        try {
            // Verificar se consegue obter EntityFacade
            EntityFacade facade = EntityFacadeFactory.getCoreFacade();
            return facade != null;
        } catch (Exception e) {
            log.log(Level.WARNING, "InternalAPI nao disponivel", e);
            return false;
        }
    }

    @Override
    public BigDecimal createOrder(OrderDTO order, BigDecimal codParc,
                                  BigDecimal codTipVenda, BigDecimal codVend,
                                  BigDecimal codNat, BigDecimal codCenCus) throws Exception {

        log.info("[InternalAPI] Criando pedido " + order.getOrderId() + " usando API interna");

        JapeSession.SessionHandle hnd = null;
        try {
            EntityFacadeFactory.getCoreFacade();
            hnd = JapeSession.open();
            ensureRequiredSessionProperties();

            final BigDecimal[] nuNotaRef = new BigDecimal[1];
            hnd.execWithTX(new JapeSession.TXBlock() {
                @Override
                public void doWithTx() throws Exception {
                    // 1. Criar cabecalho
                    HeaderCreateResult header = createCabecalho(order, codParc, codTipVenda, codVend, codNat, codCenCus);

                    // 2. Criar itens
                    createItens(header.nuNota, order, header.codLocal, header.codEmp, header.codVend, header.codTipVenda);
                    enrichCabecalhoAndItensLegacyParity(header.nuNota, order, codParc, header.codVend, header.codEmp, order.getCodTipOper());

                    nuNotaRef[0] = header.nuNota;
                }
            });

            if (nuNotaRef[0] == null) {
                throw new Exception("NUNOTA nao gerado via API interna");
            }

            log.info("[InternalAPI] Pedido " + order.getOrderId() + " criado como NUNOTA " + nuNotaRef[0]);
            return nuNotaRef[0];

        } catch (Exception e) {
            log.log(Level.SEVERE, "[InternalAPI] Erro ao criar pedido via API interna", e);
            throw new Exception("Falha na criacao via API interna: " + e.getMessage(), e);
        } finally {
            if (hnd != null) {
                try {
                    JapeSession.close(hnd);
                } catch (Exception ignored) {}
            }
        }
    }

    private HeaderCreateResult createCabecalho(OrderDTO order, BigDecimal codParc,
                                               BigDecimal codTipVenda, BigDecimal codVend,
                                               BigDecimal codNat, BigDecimal codCenCus) throws Exception {

        JapeWrapper cabDAO = JapeFactory.dao("CabecalhoNota");

        // Resolucao via De-Para (prioridade: ResellerId > Config)
        String externalId = order.getResellerId();

        BigDecimal codEmp = order.getCodEmp();
        if (codEmp == null) {
            codEmp = deparaService.getCodEmp(externalId);
        }
        if (codEmp == null) {
            codEmp = config.getCodemp();
        }

        BigDecimal codTipOper = order.getCodTipOper();
        if (codTipOper == null) {
            codTipOper = deparaService.getCodTipOper(externalId);
        }
        if (codTipOper == null) {
            codTipOper = config.getCodTipOper();
        }
        codTipOper = preferTop403(codTipOper);

        // Tenta resolver TIPNEG se nao informado
        if (codTipVenda == null) {
            codTipVenda = deparaService.getCodTipVenda(externalId);
        }

        if (codTipOper == null) {
            throw new Exception("CODTIPOPER nao configurado (verifique De-Para ou Config)");
        }
        if (codEmp == null) {
            throw new Exception("CODEMP nao configurado (verifique De-Para ou Config)");
        }

        // Mantem valores resolvidos no pedido para uso das estrategias de fallback.
        order.setCodEmp(codEmp);
        order.setCodTipOper(codTipOper);

        BigDecimal codLocal = resolveCodLocal(order, codEmp, codTipOper);
        order.setCodLocal(codLocal);

        TopDefaults topDefaults = loadTopDefaults(codTipOper);
        if (codTipVenda == null) {
            codTipVenda = topDefaults.codTipVenda;
        }
        // Regra de negocio: CODVEND deve refletir o vendedor preferencial do parceiro.
        BigDecimal codVendParceiro = resolveCodVendByParc(codParc);
        if (!isNullOrZero(codVendParceiro)) {
            codVend = codVendParceiro;
        }
        if (isNullOrZero(codVend)) {
            throw new Exception("Parceiro " + codParc + " sem vendedor preferencial (TGFPAR.CODVEND).");
        }
        if (codNat == null) {
            codNat = topDefaults.codNat;
        }
        if (codCenCus == null) {
            codCenCus = topDefaults.codCenCus;
        }
        if (codNat == null) {
            codNat = resolveAnyActiveNatureza();
        }

        if (isNullOrZero(codCenCus)) {
            codCenCus = resolveDefaultCenCusFromUser();
        }
        if (isNullOrZero(codCenCus)) {
            codCenCus = resolveAnyActiveCenCus();
        }

        if (isNullOrZero(codCenCus)) {
            throw new Exception("CODCENCUS nao resolvido para TOP " + codTipOper +
                    " (configure em AD_FCCONFIG/DEPARA ou na TOP vigente)");
        }
        if (isNullOrZero(codNat)) {
            throw new Exception("CODNAT nao resolvido para TOP " + codTipOper +
                    " (configure em AD_FCCONFIG/DEPARA ou na TOP vigente)");
        }

        log.info("[InternalAPI] Header resolved order=" + order.getOrderId()
                + " CODEMP=" + codEmp
                + " CODTIPOPER=" + codTipOper
                + " CODTIPVENDA=" + codTipVenda
                + " CODVEND=" + codVend
                + " CODNAT=" + codNat
                + " CODCENCUS=" + codCenCus
                + " CODLOCAL=" + codLocal);

        Timestamp dhTipOper = resolveDhTipOper(codTipOper);
        Timestamp dhTipVenda = codTipVenda != null ? resolveDhTipVenda(codTipVenda) : null;
        BigDecimal codUsu = resolveCodUsuLogado();
        FluidCreateVO cabBuilder = cabDAO.create()
                .set("CODEMP", codEmp)
                .set("CODPARC", codParc)
                .set("CODTIPOPER", codTipOper)
                .set("DTNEG", new Timestamp(System.currentTimeMillis()))
                .set("TIPMOV", "P"); // Pedido

        if (supportsCabField("AD_NUMFAST")) {
            cabBuilder = cabBuilder.set("AD_NUMFAST", truncate(order.getOrderId(), 50));
        }
        if (!isNullOrZero(codUsu) && supportsCabField("CODUSU")) {
            cabBuilder = cabBuilder.set("CODUSU", codUsu);
        }
        if (!isNullOrZero(codUsu) && supportsCabField("CODUSUINC")) {
            cabBuilder = cabBuilder.set("CODUSUINC", codUsu);
        }
        if (supportsCabField("STATUSNOTA")) {
            cabBuilder = cabBuilder.set("STATUSNOTA", "P");
        }
        if (supportsCabField("PENDENTE")) {
            cabBuilder = cabBuilder.set("PENDENTE", "S");
        }
        if (supportsCabField("APROVADO")) {
            cabBuilder = cabBuilder.set("APROVADO", "N");
        }
        if (supportsCabField("ISSRETIDO")) {
            cabBuilder = cabBuilder.set("ISSRETIDO", "N");
        }
        if (supportsCabField("HISTCONFIG")) {
            cabBuilder = cabBuilder.set("HISTCONFIG", "S");
        }
        if (supportsCabField("TPRETISS")) {
            cabBuilder = cabBuilder.set("TPRETISS", "1");
        }
        if (supportsCabField("QTDVOL")) {
            cabBuilder = cabBuilder.set("QTDVOL", BigDecimal.ONE);
        }
        if (supportsCabField("VLRFRETECPL")) {
            cabBuilder = cabBuilder.set("VLRFRETECPL", BigDecimal.ZERO);
        }
        if (supportsCabField("SUMVLRIIOUTNOTA")) {
            cabBuilder = cabBuilder.set("SUMVLRIIOUTNOTA", BigDecimal.ZERO);
        }
        if (supportsCabField("SOMICMSNFENAC")) {
            cabBuilder = cabBuilder.set("SOMICMSNFENAC", BigDecimal.ZERO);
        }
        if (supportsCabField("SOMPISCOFNFENAC")) {
            cabBuilder = cabBuilder.set("SOMPISCOFNFENAC", BigDecimal.ZERO);
        }
        if (supportsCabField("VLRSTEXTRANOTATOT")) {
            cabBuilder = cabBuilder.set("VLRSTEXTRANOTATOT", BigDecimal.ZERO);
        }
        if (supportsCabField("VLRREPREDTOTSEMDESC")) {
            cabBuilder = cabBuilder.set("VLRREPREDTOTSEMDESC", BigDecimal.ZERO);
        }

        // Campos opcionais
        if (codTipVenda != null) {
            cabBuilder = cabBuilder.set("CODTIPVENDA", codTipVenda);
        }
        if (dhTipOper != null && supportsCabField("DHTIPOPER")) {
            cabBuilder = cabBuilder.set("DHTIPOPER", dhTipOper);
        }
        if (dhTipVenda != null && supportsCabField("DHTIPVENDA")) {
            cabBuilder = cabBuilder.set("DHTIPVENDA", dhTipVenda);
        }
        if (codVend != null) {
            cabBuilder = cabBuilder.set("CODVEND", codVend);
            if (supportsCabField("AD_CODVENDEXEC")) {
                cabBuilder = cabBuilder.set("AD_CODVENDEXEC", new BigDecimal("281"));
            }
        }
        if (codNat != null) {
            cabBuilder = cabBuilder.set("CODNAT", codNat);
        }
        if (codCenCus != null) {
            cabBuilder = cabBuilder.set("CODCENCUS", codCenCus);
        }
        if (supportsCabField("NUMNOTA")) {
            cabBuilder = cabBuilder.set("NUMNOTA", BigDecimal.ZERO);
            log.fine("[InternalAPI] NUMNOTA inicial definido como 0 para ativacao de eventos na confirmacao.");
        }
        if (!isNullOrZero(codLocal) && supportsCabField("CODLOCAL")) {
            cabBuilder = cabBuilder.set("CODLOCAL", codLocal);
        }
        // Valores
        if (order.getTotal() != null) {
            cabBuilder = cabBuilder.set("VLRNOTA", order.getTotal());
        }
        if (order.getDiscount() != null && order.getDiscount().compareTo(BigDecimal.ZERO) > 0 && supportsCabField("VLRDESC")) {
            cabBuilder = cabBuilder.set("VLRDESC", order.getDiscount());
        }
        // Sempre enviar VLRFRETE (mesmo quando zero) - compatibilidade com legado
        BigDecimal frete = order.getShippingCost() != null ? order.getShippingCost() : BigDecimal.ZERO;
        cabBuilder = cabBuilder.set("VLRFRETE", frete);
        if (supportsCabField("CIF_FOB")) {
            cabBuilder = cabBuilder.set("CIF_FOB", "C");
        }
        if (supportsCabField("AD_MCAPORTAL")) {
            cabBuilder = cabBuilder.set("AD_MCAPORTAL", "P");
        }

        // Observacao publica sem numero Fast
        String obs = buildObservacao(order);
        if (obs != null && !obs.isEmpty()) {
            cabBuilder = cabBuilder.set("OBSERVACAO", obs);
        }
        if (supportsCabField("OBSERVACAOINTERNA")) {
            String obsInterna = buildObservacaoInterna(order);
            if (!isBlank(obsInterna)) {
                cabBuilder = cabBuilder.set("OBSERVACAOINTERNA", obsInterna);
            }
        }

        DynamicVO cabVO = cabBuilder.save();
        BigDecimal nuNota = cabVO.asBigDecimal("NUNOTA");

        HeaderCreateResult result = new HeaderCreateResult();
        result.nuNota = nuNota;
        result.codLocal = codLocal;
        result.codEmp = codEmp;
        result.codVend = codVend;
        result.codTipVenda = codTipVenda;
        return result;
    }

    private TopDefaults loadTopDefaults(BigDecimal codTipOper) {
        TopDefaults defaults = new TopDefaults();
        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            // Caminho nativo (Jape) primeiro.
            DynamicVO topVO = findTipoOperacaoByCodTipOper(codTipOper);
            if (topVO != null) {
                defaults.codTipVenda = safeAsBigDecimal(topVO, "CODTIPVENDA");
                defaults.codVend = safeAsBigDecimal(topVO, "CODVEND");
                defaults.codNat = safeAsBigDecimal(topVO, "CODNAT");
                defaults.codCenCus = safeAsBigDecimal(topVO, "CODCENCUS");
            }

            List<String> fields = new ArrayList<>();
            if (hasTableColumn("TGFTOP", "CODTIPVENDA")) fields.add("CODTIPVENDA");
            if (hasTableColumn("TGFTOP", "CODVEND")) fields.add("CODVEND");
            if (hasTableColumn("TGFTOP", "CODNAT")) fields.add("CODNAT");
            if (hasTableColumn("TGFTOP", "CODCENCUS")) fields.add("CODCENCUS");

            if (!fields.isEmpty() && (defaults.codCenCus == null || defaults.codNat == null
                    || defaults.codTipVenda == null || defaults.codVend == null)) {
                jdbc = openJdbc();
                NativeSql sql = new NativeSql(jdbc);
                sql.appendSql("SELECT TOP 1 ");
                sql.appendSql(String.join(", ", fields));
                sql.appendSql("FROM TGFTOP ");
                sql.appendSql("WHERE CODTIPOPER = :codTipOper ");
                sql.appendSql("ORDER BY DHALTER DESC");
                sql.setNamedParameter("codTipOper", codTipOper);
                rs = sql.executeQuery();
                if (rs.next()) {
                    if (containsField(fields, "CODTIPVENDA")) defaults.codTipVenda = rs.getBigDecimal("CODTIPVENDA");
                    if (containsField(fields, "CODVEND")) defaults.codVend = rs.getBigDecimal("CODVEND");
                    if (containsField(fields, "CODNAT")) defaults.codNat = rs.getBigDecimal("CODNAT");
                    if (containsField(fields, "CODCENCUS")) defaults.codCenCus = rs.getBigDecimal("CODCENCUS");
                }
            }
            if (defaults.codCenCus == null || defaults.codNat == null || defaults.codTipVenda == null || defaults.codVend == null) {
                fillFromRecentCabecalho(codTipOper, defaults);
            }
            if (defaults.codCenCus == null) {
                defaults.codCenCus = resolveDefaultCenCusFromUser();
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Nao foi possivel carregar defaults da TOP " + codTipOper, e);
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
        return defaults;
    }

    private void fillFromRecentCabecalho(BigDecimal codTipOper, TopDefaults defaults) {
        // Caminho nativo (Jape) primeiro.
        try {
            JapeWrapper cabDAO = JapeFactory.dao("CabecalhoNota");
            Collection<DynamicVO> notas = cabDAO.find("this.CODTIPOPER = ?", codTipOper);
            DynamicVO selected = null;
            BigDecimal bestNuNota = null;
            if (notas != null) {
                for (DynamicVO nota : notas) {
                    if (nota == null) {
                        continue;
                    }
                    BigDecimal codCenCus = safeAsBigDecimal(nota, "CODCENCUS");
                    BigDecimal codNat = safeAsBigDecimal(nota, "CODNAT");
                    BigDecimal codTipVenda = safeAsBigDecimal(nota, "CODTIPVENDA");
                    BigDecimal codVend = safeAsBigDecimal(nota, "CODVEND");
                    if (codCenCus == null && codNat == null && codTipVenda == null && codVend == null) {
                        continue;
                    }
                    BigDecimal nuNota = safeAsBigDecimal(nota, "NUNOTA");
                    if (selected == null || (nuNota != null && (bestNuNota == null || nuNota.compareTo(bestNuNota) > 0))) {
                        selected = nota;
                        bestNuNota = nuNota;
                    }
                }
            }
            if (selected != null) {
                if (defaults.codTipVenda == null) {
                    defaults.codTipVenda = safeAsBigDecimal(selected, "CODTIPVENDA");
                }
                if (defaults.codVend == null) {
                    defaults.codVend = safeAsBigDecimal(selected, "CODVEND");
                }
                if (defaults.codNat == null) {
                    defaults.codNat = safeAsBigDecimal(selected, "CODNAT");
                }
                if (defaults.codCenCus == null) {
                    defaults.codCenCus = safeAsBigDecimal(selected, "CODCENCUS");
                }
                if (defaults.codCenCus != null && defaults.codNat != null
                        && defaults.codTipVenda != null && defaults.codVend != null) {
                    return;
                }
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Nao foi possivel carregar fallback de TGFCAB via Jape para TOP " + codTipOper, e);
        }

        // Fallback SQL.
        JdbcWrapper jdbc = null;
        NativeSql sql = null;
        ResultSet rs = null;
        try {
            jdbc = openJdbc();
            sql = new NativeSql(jdbc);
            sql.appendSql("SELECT TOP 1 CODCENCUS, CODNAT, CODTIPVENDA, CODVEND ");
            sql.appendSql("FROM TGFCAB ");
            sql.appendSql("WHERE CODTIPOPER = :codTipOper ");
            sql.appendSql("AND (CODCENCUS IS NOT NULL OR CODNAT IS NOT NULL OR CODTIPVENDA IS NOT NULL OR CODVEND IS NOT NULL) ");
            sql.appendSql("ORDER BY NUNOTA DESC");
            sql.setNamedParameter("codTipOper", codTipOper);
            rs = sql.executeQuery();
            if (rs.next()) {
                if (defaults.codTipVenda == null) {
                    defaults.codTipVenda = rs.getBigDecimal("CODTIPVENDA");
                }
                if (defaults.codVend == null) {
                    defaults.codVend = rs.getBigDecimal("CODVEND");
                }
                if (defaults.codNat == null) {
                    defaults.codNat = rs.getBigDecimal("CODNAT");
                }
                if (defaults.codCenCus == null) {
                    defaults.codCenCus = rs.getBigDecimal("CODCENCUS");
                }
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Nao foi possivel carregar fallback de TGFCAB para TOP " + codTipOper, e);
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
    }

    private Timestamp resolveDhTipOper(BigDecimal codTipOper) {
        if (codTipOper == null) return null;
        DynamicVO topVO = findTipoOperacaoByCodTipOper(codTipOper);
        Timestamp viaJape = safeAsTimestamp(topVO, "DHALTER");
        if (viaJape != null) {
            return viaJape;
        }

        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = openJdbc();
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT TOP 1 DHALTER ");
            sql.appendSql("FROM TGFTOP ");
            sql.appendSql("WHERE CODTIPOPER = :codTipOper ");
            sql.appendSql("ORDER BY DHALTER DESC");
            sql.setNamedParameter("codTipOper", codTipOper);
            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getTimestamp("DHALTER");
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Nao foi possivel resolver DHTIPOPER para TOP " + codTipOper, e);
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
        return null;
    }

    private Timestamp resolveDhTipVenda(BigDecimal codTipVenda) {
        if (codTipVenda == null) return null;
        DynamicVO tpvVO = findTipoVendaByCodTipVenda(codTipVenda);
        Timestamp viaJape = safeAsTimestamp(tpvVO, "DHALTER");
        if (viaJape != null) {
            return viaJape;
        }

        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = openJdbc();
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT TOP 1 DHALTER ");
            sql.appendSql("FROM TGFTPV ");
            sql.appendSql("WHERE CODTIPVENDA = :codTipVenda ");
            sql.appendSql("ORDER BY DHALTER DESC");
            sql.setNamedParameter("codTipVenda", codTipVenda);
            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getTimestamp("DHALTER");
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Nao foi possivel resolver DHTIPVENDA para TIPVENDA " + codTipVenda, e);
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
        return null;
    }

    private BigDecimal resolveDefaultCenCusFromUser() {
        String user = config.getSankhyaUser();
        if (user == null || user.trim().isEmpty()) {
            return null;
        }

        // Caminho nativo (Jape) primeiro.
        try {
            JapeWrapper usuDAO = JapeFactory.dao("Usuario");
            Collection<DynamicVO> byNomUsu = usuDAO.find("UPPER(this.NOMUSU) = UPPER(?)", user);
            BigDecimal codCenCus = extractCodCenCusPad(byNomUsu);
            if (!isNullOrZero(codCenCus)) {
                return codCenCus;
            }
            Collection<DynamicVO> byNomeUsu = usuDAO.find("UPPER(this.NOMEUSU) = UPPER(?)", user);
            codCenCus = extractCodCenCusPad(byNomeUsu);
            if (!isNullOrZero(codCenCus)) {
                return codCenCus;
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Nao foi possivel resolver CODCENCUSPAD via Jape para usuario " + user, e);
        }

        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = openJdbc();
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT TOP 1 CODCENCUSPAD ");
            sql.appendSql("FROM TSIUSU ");
            sql.appendSql("WHERE UPPER(NOMEUSU)=UPPER(:nomeUsu) ");
            sql.appendSql("AND CODCENCUSPAD IS NOT NULL");
            sql.setNamedParameter("nomeUsu", user);
            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getBigDecimal("CODCENCUSPAD");
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Nao foi possivel resolver CODCENCUSPAD do usuario " + user, e);
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
        return null;
    }

    private BigDecimal resolveAnyActiveCenCus() {
        // Caminho nativo (Jape) primeiro: reutiliza historico de cabecalhos.
        try {
            JapeWrapper cabDAO = JapeFactory.dao("CabecalhoNota");
            Collection<DynamicVO> notas = cabDAO.find("this.CODCENCUS > 0");
            DynamicVO selected = null;
            BigDecimal bestNuNota = null;
            if (notas != null) {
                for (DynamicVO nota : notas) {
                    if (nota == null) {
                        continue;
                    }
                    BigDecimal nuNota = safeAsBigDecimal(nota, "NUNOTA");
                    if (selected == null || (nuNota != null && (bestNuNota == null || nuNota.compareTo(bestNuNota) > 0))) {
                        selected = nota;
                        bestNuNota = nuNota;
                    }
                }
            }
            BigDecimal codCenCus = safeAsBigDecimal(selected, "CODCENCUS");
            if (!isNullOrZero(codCenCus)) {
                return codCenCus;
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Nao foi possivel resolver fallback de CODCENCUS via Jape", e);
        }

        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = openJdbc();
            if (hasTableColumn("TGFCAB", "CODCENCUS")) {
                NativeSql sql = new NativeSql(jdbc);
                sql.appendSql("SELECT TOP 1 CODCENCUS ");
                sql.appendSql("FROM TGFCAB ");
                sql.appendSql("WHERE CODCENCUS > 0 ");
                sql.appendSql("ORDER BY NUNOTA DESC");
                rs = sql.executeQuery();
                if (rs.next()) {
                    BigDecimal value = rs.getBigDecimal("CODCENCUS");
                    if (!isNullOrZero(value)) {
                        return value;
                    }
                }
                closeQuietly(rs);
            }

            if (hasTableColumn("TSICUS", "CODCENCUS")) {
                NativeSql sql = new NativeSql(jdbc);
                sql.appendSql("SELECT TOP 1 CODCENCUS ");
                sql.appendSql("FROM TSICUS ");
                sql.appendSql("WHERE CODCENCUS > 0 ");
                if (hasTableColumn("TSICUS", "ATIVO")) {
                    sql.appendSql("AND ATIVO = 'S' ");
                }
                sql.appendSql("ORDER BY CODCENCUS");
                rs = sql.executeQuery();
                if (rs.next()) {
                    BigDecimal value = rs.getBigDecimal("CODCENCUS");
                    if (!isNullOrZero(value)) {
                        return value;
                    }
                }
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Nao foi possivel resolver fallback de CODCENCUS", e);
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
        return null;
    }

    private BigDecimal resolveAnyActiveNatureza() {
        // Caminho nativo (Jape) primeiro: reutiliza historico de cabecalhos.
        try {
            JapeWrapper cabDAO = JapeFactory.dao("CabecalhoNota");
            Collection<DynamicVO> notas = cabDAO.find("this.CODNAT > 0");
            DynamicVO selected = null;
            BigDecimal bestNuNota = null;
            if (notas != null) {
                for (DynamicVO nota : notas) {
                    if (nota == null) {
                        continue;
                    }
                    BigDecimal nuNota = safeAsBigDecimal(nota, "NUNOTA");
                    if (selected == null || (nuNota != null && (bestNuNota == null || nuNota.compareTo(bestNuNota) > 0))) {
                        selected = nota;
                        bestNuNota = nuNota;
                    }
                }
            }
            BigDecimal codNat = safeAsBigDecimal(selected, "CODNAT");
            if (!isNullOrZero(codNat)) {
                return codNat;
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Nao foi possivel resolver fallback de CODNAT via Jape", e);
        }

        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = openJdbc();
            NativeSql sql = new NativeSql(jdbc);

            if (hasTableColumn("TGFCAB", "CODNAT")) {
                sql.appendSql("SELECT TOP 1 CODNAT ");
                sql.appendSql("FROM TGFCAB ");
                sql.appendSql("WHERE CODNAT > 0 ");
                sql.appendSql("ORDER BY NUNOTA DESC");
                rs = sql.executeQuery();
                if (rs.next()) {
                    BigDecimal value = rs.getBigDecimal("CODNAT");
                    if (!isNullOrZero(value)) {
                        return value;
                    }
                }
                closeQuietly(rs);
            }

            if (hasTableColumn("TGFNAT", "CODNAT")) {
                sql = new NativeSql(jdbc);
                sql.appendSql("SELECT TOP 1 CODNAT ");
                sql.appendSql("FROM TGFNAT ");
                sql.appendSql("WHERE CODNAT > 0 ");
                sql.appendSql("ORDER BY CODNAT");
                rs = sql.executeQuery();
                if (rs.next()) {
                    BigDecimal value = rs.getBigDecimal("CODNAT");
                    if (!isNullOrZero(value)) {
                        return value;
                    }
                }
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Nao foi possivel resolver fallback de CODNAT", e);
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
        return null;
    }

    private BigDecimal resolveCodLocal(OrderDTO order, BigDecimal codEmp, BigDecimal codTipOper) {
        BigDecimal byExternal = resolveCodLocalByExternalIds(order);
        if (!isNullOrZero(byExternal)) {
            return byExternal;
        }

        BigDecimal configured = config.getCodLocal();
        if (!isNullOrZero(configured)) {
            return configured;
        }

        if (isStrictCodLocalResolution()) {
            return null;
        }

        BigDecimal byRecentCab = resolveCodLocalFromRecentCab(codTipOper, codEmp);
        if (!isNullOrZero(byRecentCab)) {
            return byRecentCab;
        }

        BigDecimal byStock = resolveCodLocalFromOrderStock(order, codEmp);
        if (!isNullOrZero(byStock)) {
            return byStock;
        }

        BigDecimal anyLocal = resolveAnyActiveLocal(codEmp);
        return isNullOrZero(anyLocal) ? null : anyLocal;
    }

    private BigDecimal resolveCodLocalByExternalIds(OrderDTO order) {
        if (order == null) {
            return null;
        }

        List<String> candidates = new ArrayList<>();
        appendIfNotBlank(candidates, "RESERVA");
        appendIfNotBlank(candidates, "RESERVE");
        appendIfNotBlank(candidates, order.getStorageId());
        appendIfNotBlank(candidates, "S:" + order.getStorageId());
        appendIfNotBlank(candidates, order.getResellerId());
        appendIfNotBlank(candidates, "R:" + order.getResellerId());
        appendIfNotBlank(candidates, config.getStorageId());
        appendIfNotBlank(candidates, "S:" + config.getStorageId());

        for (String externalId : candidates) {
            BigDecimal mapped = deparaService.getCodigoSankhya(DeparaService.TIPO_STOCK_STORAGE, externalId);
            if (!isNullOrZero(mapped)) {
                return mapped;
            }

            mapped = deparaService.getCodigoSankhya(DeparaService.TIPO_LOCAL, externalId);
            if (!isNullOrZero(mapped)) {
                return mapped;
            }

            mapped = deparaService.getCodigoSankhya(DeparaService.TIPO_STOCK_RESELLER, externalId);
            if (!isNullOrZero(mapped)) {
                return mapped;
            }
        }
        return null;
    }

    private void appendIfNotBlank(List<String> list, String value) {
        if (value == null) return;
        String trimmed = value.trim();
        if (!trimmed.isEmpty()) {
            list.add(trimmed);
        }
    }

    private BigDecimal resolveCodLocalFromRecentCab(BigDecimal codTipOper, BigDecimal codEmp) {
        // Caminho nativo (Jape) primeiro.
        try {
            JapeWrapper cabDAO = JapeFactory.dao("CabecalhoNota");
            Collection<DynamicVO> notas = cabDAO.find("this.CODLOCAL > 0");
            DynamicVO selected = null;
            BigDecimal bestNuNota = null;
            if (notas != null) {
                for (DynamicVO nota : notas) {
                    if (nota == null) {
                        continue;
                    }
                    if (!isNullOrZero(codTipOper)) {
                        BigDecimal notaTop = safeAsBigDecimal(nota, "CODTIPOPER");
                        if (notaTop != null && notaTop.compareTo(codTipOper) != 0) {
                            continue;
                        }
                    }
                    if (!isNullOrZero(codEmp)) {
                        BigDecimal notaEmp = safeAsBigDecimal(nota, "CODEMP");
                        if (notaEmp != null && notaEmp.compareTo(codEmp) != 0) {
                            continue;
                        }
                    }
                    BigDecimal nuNota = safeAsBigDecimal(nota, "NUNOTA");
                    if (selected == null || (nuNota != null && (bestNuNota == null || nuNota.compareTo(bestNuNota) > 0))) {
                        selected = nota;
                        bestNuNota = nuNota;
                    }
                }
            }
            BigDecimal codLocal = safeAsBigDecimal(selected, "CODLOCAL");
            if (!isNullOrZero(codLocal)) {
                return codLocal;
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Nao foi possivel resolver CODLOCAL via TGFCAB/Jape", e);
        }

        // Fallback SQL.
        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            if (!hasTableColumn("TGFCAB", "CODLOCAL")) {
                return null;
            }

            jdbc = openJdbc();
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT TOP 1 CODLOCAL ");
            sql.appendSql("FROM TGFCAB ");
            sql.appendSql("WHERE CODLOCAL > 0 ");
            if (!isNullOrZero(codTipOper) && hasTableColumn("TGFCAB", "CODTIPOPER")) {
                sql.appendSql("AND CODTIPOPER = :codTipOper ");
                sql.setNamedParameter("codTipOper", codTipOper);
            }
            if (!isNullOrZero(codEmp) && hasTableColumn("TGFCAB", "CODEMP")) {
                sql.appendSql("AND CODEMP = :codEmp ");
                sql.setNamedParameter("codEmp", codEmp);
            }
            sql.appendSql("ORDER BY NUNOTA DESC");
            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getBigDecimal("CODLOCAL");
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Nao foi possivel resolver CODLOCAL via TGFCAB", e);
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
        return null;
    }

    private BigDecimal resolveCodLocalFromOrderStock(OrderDTO order, BigDecimal codEmp) {
        if (order == null || order.getItems() == null || order.getItems().isEmpty()) {
            return null;
        }

        for (OrderItemDTO item : order.getItems()) {
            BigDecimal codProd = deparaService.resolveCodProdForOrderItem(item);
            if (isNullOrZero(codProd)) {
                continue;
            }

            BigDecimal codLocal = resolveCodLocalWithStock(codProd, codEmp);
            if (!isNullOrZero(codLocal)) {
                return codLocal;
            }
        }
        return null;
    }

    private BigDecimal resolveCodLocalWithStock(BigDecimal codProd, BigDecimal codEmp) {
        // Caminho nativo (Jape) primeiro.
        try {
            JapeWrapper estDAO = JapeFactory.dao("Estoque");
            Collection<DynamicVO> estoques = estDAO.find("this.CODPROD = ? AND this.CODLOCAL > 0", codProd);
            BigDecimal selectedLocal = null;
            BigDecimal bestSaldo = null;
            if (estoques != null) {
                for (DynamicVO est : estoques) {
                    if (est == null) {
                        continue;
                    }
                    if (!isNullOrZero(codEmp)) {
                        BigDecimal estEmp = safeAsBigDecimal(est, "CODEMP");
                        if (estEmp != null && estEmp.compareTo(codEmp) != 0) {
                            continue;
                        }
                    }
                    BigDecimal saldo = safeAsBigDecimal(est, "ESTOQUE");
                    if (saldo != null && saldo.compareTo(BigDecimal.ZERO) <= 0) {
                        continue;
                    }
                    BigDecimal local = safeAsBigDecimal(est, "CODLOCAL");
                    if (isNullOrZero(local)) {
                        continue;
                    }
                    if (selectedLocal == null || (saldo != null && (bestSaldo == null || saldo.compareTo(bestSaldo) > 0))) {
                        selectedLocal = local;
                        bestSaldo = saldo;
                    }
                }
            }
            if (!isNullOrZero(selectedLocal)) {
                return selectedLocal;
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Nao foi possivel resolver CODLOCAL por estoque via Jape para CODPROD " + codProd, e);
        }

        // Fallback SQL.
        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            if (!hasTableColumn("TGFEST", "CODLOCAL")) {
                return null;
            }

            jdbc = openJdbc();
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT TOP 1 E.CODLOCAL ");
            sql.appendSql("FROM TGFEST E ");
            sql.appendSql("WHERE E.CODPROD = :codProd ");
            sql.appendSql("AND E.CODLOCAL > 0 ");
            if (hasTableColumn("TGFEST", "ESTOQUE")) {
                sql.appendSql("AND E.ESTOQUE > 0 ");
            }
            if (!isNullOrZero(codEmp) && hasTableColumn("TGFEST", "CODEMP")) {
                sql.appendSql("AND E.CODEMP = :codEmp ");
                sql.setNamedParameter("codEmp", codEmp);
            }
            sql.appendSql("ORDER BY E.ESTOQUE DESC");
            sql.setNamedParameter("codProd", codProd);
            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getBigDecimal("CODLOCAL");
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Nao foi possivel resolver CODLOCAL por estoque do produto " + codProd, e);
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
        return null;
    }

    private BigDecimal resolveAnyActiveLocal(BigDecimal codEmp) {
        // Caminho nativo (Jape) primeiro.
        try {
            JapeWrapper estDAO = JapeFactory.dao("Estoque");
            Collection<DynamicVO> estoques = estDAO.find("this.CODLOCAL > 0");
            BigDecimal bestCodLocal = null;
            BigDecimal bestSaldo = null;
            if (estoques != null) {
                for (DynamicVO est : estoques) {
                    if (est == null) {
                        continue;
                    }
                    BigDecimal local = safeAsBigDecimal(est, "CODLOCAL");
                    if (isNullOrZero(local)) {
                        continue;
                    }
                    if (!isNullOrZero(codEmp)) {
                        BigDecimal estCodEmp = safeAsBigDecimal(est, "CODEMP");
                        if (estCodEmp != null && estCodEmp.compareTo(codEmp) != 0) {
                            continue;
                        }
                    }
                    BigDecimal saldo = safeAsBigDecimal(est, "ESTOQUE");
                    if (saldo == null) {
                        saldo = BigDecimal.ZERO;
                    }
                    if (bestCodLocal == null || saldo.compareTo(bestSaldo) > 0) {
                        bestCodLocal = local;
                        bestSaldo = saldo;
                    }
                }
            }
            if (!isNullOrZero(bestCodLocal)) {
                return bestCodLocal;
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Nao foi possivel resolver fallback geral de CODLOCAL via Jape", e);
        }

        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = openJdbc();

            if (hasTableColumn("TGFEST", "CODLOCAL")) {
                NativeSql sql = new NativeSql(jdbc);
                sql.appendSql("SELECT TOP 1 E.CODLOCAL ");
                sql.appendSql("FROM TGFEST E ");
                sql.appendSql("WHERE E.CODLOCAL > 0 ");
                if (hasTableColumn("TGFEST", "ESTOQUE")) {
                    sql.appendSql("AND E.ESTOQUE > 0 ");
                }
                if (!isNullOrZero(codEmp) && hasTableColumn("TGFEST", "CODEMP")) {
                    sql.appendSql("AND E.CODEMP = :codEmp ");
                    sql.setNamedParameter("codEmp", codEmp);
                }
                sql.appendSql("GROUP BY E.CODLOCAL ");
                sql.appendSql("ORDER BY SUM(E.ESTOQUE) DESC");
                rs = sql.executeQuery();
                if (rs.next()) {
                    BigDecimal codLocal = rs.getBigDecimal("CODLOCAL");
                    if (!isNullOrZero(codLocal)) {
                        return codLocal;
                    }
                }
                closeQuietly(rs);
            }

            if (hasTableColumn("TGFLOC", "CODLOCAL")) {
                NativeSql sql = new NativeSql(jdbc);
                sql.appendSql("SELECT TOP 1 CODLOCAL ");
                sql.appendSql("FROM TGFLOC ");
                sql.appendSql("WHERE CODLOCAL > 0 ");
                if (hasTableColumn("TGFLOC", "ATIVO")) {
                    sql.appendSql("AND ATIVO = 'S' ");
                }
                sql.appendSql("ORDER BY CODLOCAL");
                rs = sql.executeQuery();
                if (rs.next()) {
                    return rs.getBigDecimal("CODLOCAL");
                }
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Nao foi possivel resolver fallback geral de CODLOCAL", e);
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
        return null;
    }

    private void createItens(BigDecimal nuNota, OrderDTO order, BigDecimal codLocal, BigDecimal codEmp,
                             BigDecimal codVend, BigDecimal codTipVenda) throws Exception {
        JapeWrapper iteDAO = JapeFactory.dao("ItemNota");
        boolean strictCodLocal = isStrictCodLocalResolution();
        BigDecimal codUsu = resolveCodUsuLogado();

        if (isNullOrZero(codLocal)) {
            log.warning("[InternalAPI] CODLOCAL nao resolvido para pedido " + order.getOrderId()
                    + ". Itens serao gravados sem local explicito.");
        } else {
            log.info("[InternalAPI] Itens do pedido " + order.getOrderId() + " usando CODLOCAL " + codLocal);
        }

        int sequencia = 1;
        for (OrderItemDTO item : order.getItems()) {
            BigDecimal codProd = deparaService.resolveCodProdForOrderItem(item);
            if (codProd == null) {
                throw new Exception("Produto nao encontrado para SKU: " + item.getSku());
            }

            BigDecimal quantity = sanitizeQuantity(item, order);
            BigDecimal unitPrice = sanitizeUnitPrice(item, quantity, order);
            String codVol = resolveCodVol(item, codProd);
            BigDecimal itemCodLocal = resolveItemCodLocal(codLocal, codProd, codEmp, quantity, item.getGradeControlId());
            ItemPricingData pricing = resolveItemPricingData(codProd, codEmp, codVend, codTipVenda);
            if (strictCodLocal && isNullOrZero(itemCodLocal)) {
                log.warning("[InternalAPI] Strict CODLOCAL ativo, mas sem saldo/mapeamento para item pedido="
                        + order.getOrderId() + " SKU=" + item.getSku() + " CODPROD=" + codProd
                        + " QTD=" + quantity + ". Item sera gravado sem CODLOCAL explicito.");
            }

            FluidCreateVO itemBuilder = iteDAO.create()
                    .set("NUNOTA", nuNota)
                    .set("SEQUENCIA", new BigDecimal(sequencia))
                    .set("CODPROD", codProd)
                    .set("QTDNEG", quantity)
                    .set("VLRUNIT", unitPrice)
                    .set("CODVOL", codVol);

            // ORIGPROD: usar valor cadastrado no produto (compatibilidade com legado)
            String origProd = getOrigProd(codProd);
            if (origProd == null || origProd.isEmpty()) {
                origProd = "0"; // Fallback: 0 = Nacional (padrao Sankhya)
            }
            itemBuilder = itemBuilder.set("ORIGPROD", origProd);

            if (supportsItemField("VLRTOT")) {
                itemBuilder = itemBuilder.set("VLRTOT", unitPrice.multiply(quantity));
            }
            if (pricing != null && !isNullOrZero(pricing.nuTab) && supportsItemField("NUTAB")) {
                itemBuilder = itemBuilder.set("NUTAB", pricing.nuTab);
            }
            if (pricing != null && pricing.precoBase != null && supportsItemField("PRECOBASE")) {
                itemBuilder = itemBuilder.set("PRECOBASE", pricing.precoBase);
            }
            if (pricing != null && pricing.custo != null && supportsItemField("CUSTO")) {
                itemBuilder = itemBuilder.set("CUSTO", pricing.custo);
            }
            if (pricing != null && pricing.custo != null && supportsItemField("VLRCUS")) {
                itemBuilder = itemBuilder.set("VLRCUS", pricing.custo);
            }
            if (pricing != null && supportsItemField("USOPROD")) {
                String usoProd = normalizeUsoProdForPedido(pricing.usoProd);
                if (!isBlank(usoProd)) {
                    itemBuilder = itemBuilder.set("USOPROD", usoProd);
                }
            }
            if (supportsItemField("ATUALESTTERC")) {
                itemBuilder = itemBuilder.set("ATUALESTTERC", "N");
            }
            if (supportsItemField("TERCEIROS")) {
                itemBuilder = itemBuilder.set("TERCEIROS", "N");
            } else if (supportsItemField("TERCEIRO")) {
                itemBuilder = itemBuilder.set("TERCEIRO", "N");
            }

            if (!isNullOrZero(codEmp) && supportsItemField("CODEMP")) {
                itemBuilder = itemBuilder.set("CODEMP", codEmp);
            }
            if (!isNullOrZero(codVend) && supportsItemField("CODVEND")) {
                itemBuilder = itemBuilder.set("CODVEND", codVend);
            }
            if (!isNullOrZero(codUsu) && supportsItemField("CODUSU")) {
                itemBuilder = itemBuilder.set("CODUSU", codUsu);
            }
            if (supportsItemField("STATUSNOTA")) {
                itemBuilder = itemBuilder.set("STATUSNOTA", "P");
            }
            if (supportsItemField("QTDENTREGUE")) {
                itemBuilder = itemBuilder.set("QTDENTREGUE", BigDecimal.ZERO);
            }
            if (supportsItemField("ATUALESTOQUE")) {
                itemBuilder = itemBuilder.set("ATUALESTOQUE", BigDecimal.ONE);
            }
            if (supportsItemField("RESERVA")) {
                itemBuilder = itemBuilder.set("RESERVA", "S");
            }
            if (supportsItemField("CODTRIB")) {
                BigDecimal codTrib = resolveCodTrib(codProd);
                if (!isNullOrZero(codTrib)) {
                    itemBuilder = itemBuilder.set("CODTRIB", codTrib);
                }
            }
            if (!isNullOrZero(itemCodLocal) && supportsItemField("CODLOCAL")) {
                itemBuilder = itemBuilder.set("CODLOCAL", itemCodLocal);
            }
            if (!isNullOrZero(itemCodLocal) && supportsItemField("CODLOCALORIG")) {
                itemBuilder = itemBuilder.set("CODLOCALORIG", itemCodLocal);
            }
            if (!isBlank(item.getGradeControlId()) && supportsItemField("CONTROLE")) {
                itemBuilder = itemBuilder.set("CONTROLE", item.getGradeControlId().trim());
            }

            if (item.getDiscount() != null && item.getDiscount().compareTo(BigDecimal.ZERO) > 0) {
                BigDecimal totalSemDesc = unitPrice.multiply(quantity);
                if (totalSemDesc.compareTo(BigDecimal.ZERO) > 0) {
                    BigDecimal vlrDesc = item.getDiscount();
                    if (supportsItemField("VLRDESC")) {
                        itemBuilder = itemBuilder.set("VLRDESC", vlrDesc);
                    }
                    if (vlrDesc.compareTo(totalSemDesc) >= 0) {
                        log.warning("[InternalAPI] VLRDESC >= VLRTOT para SKU " + item.getSku()
                                + " no pedido " + order.getOrderId()
                                + ". PERCDESC sera ignorado para evitar falhas de calculo interno.");
                    } else {
                        BigDecimal percDesc = vlrDesc
                            .divide(totalSemDesc, 4, BigDecimal.ROUND_HALF_UP)
                            .multiply(new BigDecimal("100"));
                        if (supportsItemField("PERCDESC")
                                && percDesc.compareTo(BigDecimal.ZERO) > 0
                                && percDesc.compareTo(new BigDecimal("100")) < 0) {
                            itemBuilder = itemBuilder.set("PERCDESC", percDesc);
                        }
                    }
                }
            }

            itemBuilder.save();
            sequencia++;
        }

        log.info("[InternalAPI] Criados " + order.getItems().size() + " itens para NUNOTA " + nuNota);
    }

    private BigDecimal resolveItemCodLocal(BigDecimal preferredCodLocal, BigDecimal codProd, BigDecimal codEmp,
                                           BigDecimal quantity, String controle) {
        if (isStrictCodLocalResolution()) {
            if (!isNullOrZero(preferredCodLocal)
                    && hasSufficientStock(codProd, codEmp, preferredCodLocal, quantity, controle)) {
                return preferredCodLocal;
            }
            return null;
        }

        if (!isNullOrZero(preferredCodLocal)
                && hasSufficientStock(codProd, codEmp, preferredCodLocal, quantity, controle)) {
            return preferredCodLocal;
        }

        BigDecimal alternative = resolveCodLocalWithStock(codProd, codEmp);
        if (!isNullOrZero(alternative) && hasSufficientStock(codProd, codEmp, alternative, quantity, controle)) {
            if (isNullOrZero(preferredCodLocal) || preferredCodLocal.compareTo(alternative) != 0) {
                log.info("[InternalAPI] CODLOCAL ajustado por item CODPROD=" + codProd
                        + " de " + preferredCodLocal + " para " + alternative);
            }
            return alternative;
        }

        if (!isNullOrZero(preferredCodLocal)) {
            log.warning("[InternalAPI] CODLOCAL " + preferredCodLocal + " sem saldo suficiente para CODPROD="
                    + codProd + " QTD=" + quantity + ". Item sera gravado sem CODLOCAL explicito.");
        }
        return null;
    }

    private boolean isStrictCodLocalResolution() {
        String configured = System.getProperty("fastchannel.order.strictCodLocal");
        if (configured == null || configured.trim().isEmpty()) {
            configured = System.getenv("FASTCHANNEL_ORDER_STRICT_CODLOCAL");
        }
        if (configured == null || configured.trim().isEmpty()) {
            // Modo resiliente por padrão: tenta fallback por TGFCAB/TGFEST/TGFLOC
            // e evita bloquear importacao quando CODLOCAL nao vem no de-para.
            return false;
        }
        return Boolean.parseBoolean(configured);
    }

    private boolean hasSufficientStock(BigDecimal codProd, BigDecimal codEmp, BigDecimal codLocal,
                                       BigDecimal quantity, String controle) {
        if (isNullOrZero(codProd) || isNullOrZero(codLocal)) {
            return false;
        }

        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            if (!hasTableColumn("TGFEST", "ESTOQUE")) {
                return true;
            }

            jdbc = openJdbc();
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT ISNULL(SUM(E.ESTOQUE),0) AS QTD ");
            sql.appendSql("FROM TGFEST E ");
            sql.appendSql("WHERE E.CODPROD = :codProd ");
            sql.appendSql("AND E.CODLOCAL = :codLocal ");
            if (!isNullOrZero(codEmp) && hasTableColumn("TGFEST", "CODEMP")) {
                sql.appendSql("AND E.CODEMP = :codEmp ");
                sql.setNamedParameter("codEmp", codEmp);
            }
            if (!isBlank(controle) && hasTableColumn("TGFEST", "CONTROLE")) {
                sql.appendSql("AND E.CONTROLE = :controle ");
                sql.setNamedParameter("controle", controle.trim());
            }
            sql.setNamedParameter("codProd", codProd);
            sql.setNamedParameter("codLocal", codLocal);
            rs = sql.executeQuery();
            if (rs.next()) {
                BigDecimal saldo = rs.getBigDecimal("QTD");
                BigDecimal required = quantity != null ? quantity : BigDecimal.ONE;
                return saldo != null && saldo.compareTo(required) >= 0;
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Nao foi possivel validar saldo para CODPROD=" + codProd
                    + " CODLOCAL=" + codLocal, e);
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
        return false;
    }

    private void enrichCabecalhoAndItensLegacyParity(BigDecimal nuNota, OrderDTO order, BigDecimal codParc,
                                                     BigDecimal codVend, BigDecimal codEmp, BigDecimal codTipOper) {
        if (isNullOrZero(nuNota)) {
            return;
        }

        try {
            JapeWrapper cabDAO = JapeFactory.dao("CabecalhoNota");
            DynamicVO cabVO = cabDAO.findByPK(nuNota);
            if (cabVO == null) {
                return;
            }

            BigDecimal codUsu = resolveCodUsuLogado();
            br.com.sankhya.jape.wrapper.fluid.FluidUpdateVO updateVO = cabDAO.prepareToUpdate(cabVO);
            boolean changed = false;

            if (!isNullOrZero(codUsu) && supportsCabField("CODUSU") && isNullOrZero(cabVO.asBigDecimal("CODUSU"))) {
                updateVO = updateVO.set("CODUSU", codUsu);
                changed = true;
            }
            if (!isNullOrZero(codUsu) && supportsCabField("CODUSUINC") && isNullOrZero(cabVO.asBigDecimal("CODUSUINC"))) {
                updateVO = updateVO.set("CODUSUINC", codUsu);
                changed = true;
            }
            if (!isNullOrZero(codVend) && supportsCabField("CODVEND") && isNullOrZero(cabVO.asBigDecimal("CODVEND"))) {
                updateVO = updateVO.set("CODVEND", codVend);
                changed = true;
            }
            if (!isNullOrZero(codVend) && supportsCabField("AD_CODVENDEXEC") && isNullOrZero(cabVO.asBigDecimal("AD_CODVENDEXEC"))) {
                updateVO = updateVO.set("AD_CODVENDEXEC", new BigDecimal("281"));
                changed = true;
            }
            if (supportsCabField("STATUSNOTA")) {
                String current = trimToNull(cabVO.asString("STATUSNOTA"));
                if (current == null || !"P".equalsIgnoreCase(current)) {
                    updateVO = updateVO.set("STATUSNOTA", "P");
                    changed = true;
                }
            }
            if (supportsCabField("PENDENTE")) {
                String current = trimToNull(cabVO.asString("PENDENTE"));
                if (current == null || !"S".equalsIgnoreCase(current)) {
                    updateVO = updateVO.set("PENDENTE", "S");
                    changed = true;
                }
            }
            if (supportsCabField("DTFATUR") && cabVO.asTimestamp("DTFATUR") != null) {
                updateVO = updateVO.set("DTFATUR", null);
                changed = true;
            }
            if (supportsCabField("APROVADO")) {
                String current = trimToNull(cabVO.asString("APROVADO"));
                if (current == null || !"N".equalsIgnoreCase(current)) {
                    updateVO = updateVO.set("APROVADO", "N");
                    changed = true;
                }
            }
            if (supportsCabField("ISSRETIDO")) {
                String current = trimToNull(cabVO.asString("ISSRETIDO"));
                if (current == null || !"N".equalsIgnoreCase(current)) {
                    updateVO = updateVO.set("ISSRETIDO", "N");
                    changed = true;
                }
            }
            if (supportsCabField("HISTCONFIG")) {
                String current = trimToNull(cabVO.asString("HISTCONFIG"));
                if (current == null || !"S".equalsIgnoreCase(current)) {
                    updateVO = updateVO.set("HISTCONFIG", "S");
                    changed = true;
                }
            }
            if (supportsCabField("TPRETISS")) {
                String current = trimToNull(cabVO.asString("TPRETISS"));
                if (current == null || !"1".equalsIgnoreCase(current)) {
                    updateVO = updateVO.set("TPRETISS", "1");
                    changed = true;
                }
            }
            if (supportsCabField("QTDVOL") && isNullOrZero(cabVO.asBigDecimal("QTDVOL"))) {
                updateVO = updateVO.set("QTDVOL", BigDecimal.ONE);
                changed = true;
            }
            if (supportsCabField("CODPARCTRANSP") && isNullOrZero(cabVO.asBigDecimal("CODPARCTRANSP"))) {
                BigDecimal transportadora = resolveCodParcTransp(codParc, codEmp, codTipOper);
                if (!isNullOrZero(transportadora)) {
                    updateVO = updateVO.set("CODPARCTRANSP", transportadora);
                    changed = true;
                }
            }
            if (supportsCabField("ORDEMCARGA") && isNullOrZero(cabVO.asBigDecimal("ORDEMCARGA"))) {
                BigDecimal ordemCarga = resolveOrdemCarga(codParc, codEmp, codTipOper);
                if (!isNullOrZero(ordemCarga)) {
                    updateVO = updateVO.set("ORDEMCARGA", ordemCarga);
                    changed = true;
                }
            }

            WeightTotals weights = resolveWeightTotalsByNota(nuNota);
            if (weights != null) {
                if (supportsCabField("PESO") && cabVO.asBigDecimal("PESO") == null && weights.peso != null) {
                    updateVO = updateVO.set("PESO", weights.peso);
                    changed = true;
                }
                if (supportsCabField("PESOBRUTO") && cabVO.asBigDecimal("PESOBRUTO") == null && weights.pesoBruto != null) {
                    updateVO = updateVO.set("PESOBRUTO", weights.pesoBruto);
                    changed = true;
                }
            }

            BigDecimal totalCustoProd = resolveTotalCustoByNota(nuNota);
            if (supportsCabField("TOTALCUSTOPROD") && cabVO.asBigDecimal("TOTALCUSTOPROD") == null && totalCustoProd != null) {
                updateVO = updateVO.set("TOTALCUSTOPROD", totalCustoProd);
                changed = true;
            }
            if (supportsCabField("TOTALCUSTOSERV") && cabVO.asBigDecimal("TOTALCUSTOSERV") == null) {
                updateVO = updateVO.set("TOTALCUSTOSERV", BigDecimal.ZERO);
                changed = true;
            }
            if (supportsCabField("VLRSTEXTRANOTATOT") && cabVO.asBigDecimal("VLRSTEXTRANOTATOT") == null) {
                updateVO = updateVO.set("VLRSTEXTRANOTATOT", BigDecimal.ZERO);
                changed = true;
            }
            if (supportsCabField("VLRREPREDTOTSEMDESC") && cabVO.asBigDecimal("VLRREPREDTOTSEMDESC") == null) {
                updateVO = updateVO.set("VLRREPREDTOTSEMDESC", BigDecimal.ZERO);
                changed = true;
            }
            if (supportsCabField("SUMVLRIIOUTNOTA") && cabVO.asBigDecimal("SUMVLRIIOUTNOTA") == null) {
                updateVO = updateVO.set("SUMVLRIIOUTNOTA", BigDecimal.ZERO);
                changed = true;
            }
            if (supportsCabField("SOMICMSNFENAC") && cabVO.asBigDecimal("SOMICMSNFENAC") == null) {
                updateVO = updateVO.set("SOMICMSNFENAC", BigDecimal.ZERO);
                changed = true;
            }
            if (supportsCabField("SOMPISCOFNFENAC") && cabVO.asBigDecimal("SOMPISCOFNFENAC") == null) {
                updateVO = updateVO.set("SOMPISCOFNFENAC", BigDecimal.ZERO);
                changed = true;
            }

            if (changed) {
                updateVO.update();
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "[InternalAPI] Falha ao reforcar paridade de cabecalho pos-itens para NUNOTA " + nuNota, e);
        }
    }

    private String normalizeUsoProdForPedido(String usoProd) {
        String normalized = trimToNull(usoProd);
        if (normalized == null) {
            return "R";
        }
        if ("V".equalsIgnoreCase(normalized)) {
            return "R";
        }
        return normalized;
    }

    private BigDecimal resolveCodTrib(BigDecimal codProd) {
        if (isNullOrZero(codProd)) {
            return null;
        }
        try {
            JapeWrapper produtoDAO = JapeFactory.dao("Produto");
            DynamicVO produtoVO = produtoDAO.findByPK(codProd);
            if (produtoVO != null) {
                BigDecimal codTrib = safeAsBigDecimal(produtoVO, "CODTRIB");
                if (!isNullOrZero(codTrib)) {
                    return codTrib;
                }
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Nao foi possivel resolver CODTRIB via Jape para CODPROD " + codProd, e);
        }
        return new BigDecimal("60");
    }

    private BigDecimal resolveCodParcTransp(BigDecimal codParc, BigDecimal codEmp, BigDecimal codTipOper) {
        if (!isNullOrZero(codParc)) {
            try {
                JapeWrapper parcDAO = JapeFactory.dao("Parceiro");
                DynamicVO parcVO = parcDAO.findByPK(codParc);
                if (parcVO != null) {
                    BigDecimal fromPartner = safeAsBigDecimal(parcVO, "CODPARCTRANSP");
                    if (!isNullOrZero(fromPartner)) {
                        return fromPartner;
                    }
                }
            } catch (Exception e) {
                log.log(Level.FINE, "Nao foi possivel resolver CODPARCTRANSP no parceiro " + codParc, e);
            }
        }
        return resolveFromRecentCab(codParc, codEmp, codTipOper, "CODPARCTRANSP");
    }

    private BigDecimal resolveOrdemCarga(BigDecimal codParc, BigDecimal codEmp, BigDecimal codTipOper) {
        return resolveFromRecentCab(codParc, codEmp, codTipOper, "ORDEMCARGA");
    }

    private BigDecimal resolveFromRecentCab(BigDecimal codParc, BigDecimal codEmp, BigDecimal codTipOper, String field) {
        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = openJdbc();
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT TOP 1 " + field + " AS VAL ");
            sql.appendSql("FROM TGFCAB WHERE " + field + " IS NOT NULL ");
            sql.appendSql("AND " + field + " > 0 ");
            if (!isNullOrZero(codParc) && hasTableColumn("TGFCAB", "CODPARC")) {
                sql.appendSql("AND CODPARC = :codParc ");
                sql.setNamedParameter("codParc", codParc);
            }
            if (!isNullOrZero(codEmp) && hasTableColumn("TGFCAB", "CODEMP")) {
                sql.appendSql("AND CODEMP = :codEmp ");
                sql.setNamedParameter("codEmp", codEmp);
            }
            if (!isNullOrZero(codTipOper) && hasTableColumn("TGFCAB", "CODTIPOPER")) {
                sql.appendSql("AND CODTIPOPER = :codTipOper ");
                sql.setNamedParameter("codTipOper", codTipOper);
            }
            sql.appendSql("ORDER BY NUNOTA DESC");
            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getBigDecimal("VAL");
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Falha ao resolver campo legado " + field + " via TGFCAB", e);
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
        return null;
    }

    private BigDecimal resolveTotalCustoByNota(BigDecimal nuNota) {
        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = openJdbc();
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT ISNULL(SUM(ISNULL(CUSTO, 0) * ISNULL(QTDNEG, 0)), 0) AS VLR ");
            sql.appendSql("FROM TGFITE WHERE NUNOTA = :nuNota");
            sql.setNamedParameter("nuNota", nuNota);
            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getBigDecimal("VLR");
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Falha ao calcular TOTALCUSTOPROD da nota " + nuNota, e);
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
        return null;
    }

    private WeightTotals resolveWeightTotalsByNota(BigDecimal nuNota) {
        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = openJdbc();
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
            log.log(Level.FINE, "Falha ao calcular peso/peso bruto da nota " + nuNota, e);
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
        return null;
    }

    private String resolveCodVol(OrderItemDTO item, BigDecimal codProd) {
        if (item != null && !isBlank(item.getVolumeId())) {
            return item.getVolumeId().trim();
        }
        return getVolumePadrao(codProd);
    }

    private BigDecimal resolveCodVendByParc(BigDecimal codParc) {
        if (isNullOrZero(codParc)) {
            return null;
        }
        try {
            JapeWrapper parceiroDAO = JapeFactory.dao("Parceiro");
            DynamicVO parceiroVO = parceiroDAO.findByPK(codParc);
            if (parceiroVO != null) {
                BigDecimal codVend = safeAsBigDecimal(parceiroVO, "CODVEND");
                if (!isNullOrZero(codVend)) {
                    return codVend;
                }
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Nao foi possivel resolver CODVEND do parceiro " + codParc, e);
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
            log.log(Level.FINE, "Nao foi possivel resolver CODVEND via SQL do parceiro " + codParc, e);
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
        return null;
    }

    private String getOrigProd(BigDecimal codProd) {
        if (isNullOrZero(codProd)) {
            return null;
        }
        try {
            JapeWrapper produtoDAO = JapeFactory.dao("Produto");
            DynamicVO produtoVO = produtoDAO.findByPK(codProd);
            if (produtoVO != null) {
                String val = trimToNull(produtoVO.asString("ORIGPROD"));
                if (val != null) {
                    return val;
                }
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao buscar ORIGPROD do produto " + codProd, e);
        }
        return null;
    }

    private String getVolumePadrao(BigDecimal codProd) {
        if (isNullOrZero(codProd)) {
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
            log.log(Level.WARNING, "Erro ao buscar CODVOL do produto " + codProd, e);
        }
        return "UN";
    }

    private BigDecimal sanitizeQuantity(OrderItemDTO item, OrderDTO order) throws Exception {
        BigDecimal quantity = item != null ? item.getQuantity() : null;
        if (quantity == null || quantity.compareTo(BigDecimal.ZERO) <= 0) {
            throw new Exception("Quantidade invalida para SKU " + (item != null ? item.getSku() : null)
                    + " no pedido " + (order != null ? order.getOrderId() : null));
        }
        return quantity;
    }

    private BigDecimal sanitizeUnitPrice(OrderItemDTO item, BigDecimal quantity, OrderDTO order) {
        BigDecimal unitPrice = item != null ? item.getUnitPrice() : null;
        if (unitPrice != null && unitPrice.compareTo(BigDecimal.ZERO) > 0) {
            return unitPrice;
        }

        BigDecimal totalPrice = item != null ? item.getTotalPrice() : null;
        if (totalPrice != null && totalPrice.compareTo(BigDecimal.ZERO) > 0
                && quantity != null && quantity.compareTo(BigDecimal.ZERO) > 0) {
            BigDecimal calculated = totalPrice.divide(quantity, 6, BigDecimal.ROUND_HALF_UP);
            if (calculated.compareTo(BigDecimal.ZERO) > 0) {
                log.warning("[InternalAPI] VLRUNIT ajustado por TotalProductCost para SKU "
                        + item.getSku() + " no pedido " + order.getOrderId() + ": " + calculated);
                return calculated;
            }
        }

        log.warning("[InternalAPI] VLRUNIT ausente/zerado para SKU "
                + (item != null ? item.getSku() : null)
                + " no pedido " + (order != null ? order.getOrderId() : null)
                + ". Aplicando fallback 0.000001 para evitar falha de calculo interno.");
        return new BigDecimal("0.000001");
    }

    private boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private String buildObservacao(OrderDTO order) {
        StringBuilder obs = new StringBuilder();
        if (order.getNotes() != null && !order.getNotes().isEmpty()) {
            obs.append(order.getNotes());
        }

        if (order.getShippingMethod() != null) {
            if (obs.length() > 0) {
                obs.append(" | ");
            }
            obs.append("Frete: ").append(order.getShippingMethod());
        }

        String result = obs.toString();
        return result.length() > 500 ? result.substring(0, 500) : result;
    }

    private String buildObservacaoInterna(OrderDTO order) {
        StringBuilder obs = new StringBuilder();
        obs.append("Pedido Fastchannel: ").append(order.getOrderId());
        if (order.getSellerNotes() != null && !order.getSellerNotes().trim().isEmpty()) {
            obs.append(" | ").append(order.getSellerNotes().trim());
        }
        String result = obs.toString();
        return result.length() > 1000 ? result.substring(0, 1000) : result;
    }

    private ItemPricingData resolveItemPricingData(BigDecimal codProd, BigDecimal codEmp,
                                                   BigDecimal codVend, BigDecimal codTipVenda) {
        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = openJdbc();
            ItemPricingData data = new ItemPricingData();
            BigDecimal preferredNuTab = resolvePreferredNuTab(codVend, codTipVenda);
            if (!isNullOrZero(preferredNuTab)) {
                data.nuTab = preferredNuTab;
            }

            ItemPricingData nativeExc = resolveExcPricingNative(codProd, codEmp, preferredNuTab);
            if (nativeExc != null) {
                if (!isNullOrZero(nativeExc.nuTab)) {
                    data.nuTab = nativeExc.nuTab;
                }
                data.precoBase = nativeExc.precoBase;
            } else {
                NativeSql sqlExc = new NativeSql(jdbc);
                sqlExc.appendSql("SELECT TOP 1 NUTAB, VLRVENDA ");
                sqlExc.appendSql("FROM TGFEXC ");
                sqlExc.appendSql("WHERE CODPROD = :codProd ");
                if (!isNullOrZero(preferredNuTab)) {
                    sqlExc.appendSql("AND NUTAB = :nuTab ");
                    sqlExc.setNamedParameter("nuTab", preferredNuTab);
                }
                if (!isNullOrZero(codEmp) && hasTableColumn("TGFEXC", "CODEMP")) {
                    sqlExc.appendSql("AND CODEMP = :codEmp ");
                    sqlExc.setNamedParameter("codEmp", codEmp);
                }
                sqlExc.appendSql("ORDER BY DTALTER DESC");
                sqlExc.setNamedParameter("codProd", codProd);
                rs = sqlExc.executeQuery();
                if (rs.next()) {
                    data.nuTab = rs.getBigDecimal("NUTAB");
                    data.precoBase = rs.getBigDecimal("VLRVENDA");
                }
                closeQuietly(rs);
                if ((isNullOrZero(data.nuTab) || data.precoBase == null) && !isNullOrZero(preferredNuTab)) {
                    BigDecimal preferredCodTab = resolveCodTabByNuTab(preferredNuTab);
                    if (!isNullOrZero(preferredCodTab)) {
                        sqlExc = new NativeSql(jdbc);
                        sqlExc.appendSql("SELECT TOP 1 E.NUTAB, E.VLRVENDA ");
                        sqlExc.appendSql("FROM TGFEXC E ");
                        sqlExc.appendSql("INNER JOIN TGFTAB T ON T.NUTAB = E.NUTAB ");
                        sqlExc.appendSql("WHERE E.CODPROD = :codProd ");
                        sqlExc.appendSql("AND T.CODTAB = :codTab ");
                        if (hasTableColumn("TGFTAB", "DTVIGOR")) {
                            sqlExc.appendSql("AND (T.DTVIGOR IS NULL OR T.DTVIGOR <= GETDATE()) ");
                        }
                        if (hasTableColumn("TGFTAB", "INATIVO")) {
                            sqlExc.appendSql("AND (T.INATIVO IS NULL OR T.INATIVO = 'N') ");
                        }
                        if (!isNullOrZero(codEmp) && hasTableColumn("TGFEXC", "CODEMP")) {
                            sqlExc.appendSql("AND E.CODEMP = :codEmp ");
                            sqlExc.setNamedParameter("codEmp", codEmp);
                        }
                        sqlExc.appendSql("ORDER BY T.DTVIGOR DESC, E.NUTAB DESC");
                        sqlExc.setNamedParameter("codProd", codProd);
                        sqlExc.setNamedParameter("codTab", preferredCodTab);
                        rs = sqlExc.executeQuery();
                        if (rs.next()) {
                            data.nuTab = rs.getBigDecimal("NUTAB");
                            data.precoBase = rs.getBigDecimal("VLRVENDA");
                        }
                        closeQuietly(rs);
                    }
                }
                if (isNullOrZero(preferredNuTab) && (isNullOrZero(data.nuTab) || data.precoBase == null)) {
                    sqlExc = new NativeSql(jdbc);
                    sqlExc.appendSql("SELECT TOP 1 NUTAB, VLRVENDA ");
                    sqlExc.appendSql("FROM TGFEXC ");
                    sqlExc.appendSql("WHERE CODPROD = :codProd ");
                    if (!isNullOrZero(codEmp) && hasTableColumn("TGFEXC", "CODEMP")) {
                        sqlExc.appendSql("AND CODEMP = :codEmp ");
                        sqlExc.setNamedParameter("codEmp", codEmp);
                    }
                    sqlExc.appendSql("ORDER BY NUTAB DESC");
                    sqlExc.setNamedParameter("codProd", codProd);
                    rs = sqlExc.executeQuery();
                    if (rs.next()) {
                        if (isNullOrZero(data.nuTab)) {
                            data.nuTab = rs.getBigDecimal("NUTAB");
                        }
                        if (data.precoBase == null) {
                            data.precoBase = rs.getBigDecimal("VLRVENDA");
                        }
                    }
                    closeQuietly(rs);
                }
            }
            if (!isNullOrZero(data.nuTab)) {
                data.nuTab = normalizeNuTabToLatestActive(data.nuTab);
            }

            try {
                JapeWrapper produtoDAO = JapeFactory.dao("Produto");
                DynamicVO produtoVO = produtoDAO.findByPK(codProd);
                if (produtoVO != null) {
                    data.usoProd = trimToNull(produtoVO.asString("USOPROD"));
                }
            } catch (Exception nativeErr) {
                NativeSql sqlProd = new NativeSql(jdbc);
                sqlProd.appendSql("SELECT USOPROD FROM TGFPRO WHERE CODPROD = :codProd");
                sqlProd.setNamedParameter("codProd", codProd);
                rs = sqlProd.executeQuery();
                if (rs.next()) {
                    data.usoProd = trimToNull(rs.getString("USOPROD"));
                }
            }
            data.custo = resolveCurrentCost(codProd, codEmp);
            return data;
        } catch (Exception e) {
            log.log(Level.FINE, "Nao foi possivel resolver NUTAB/CUSTO/PRECOBASE para CODPROD " + codProd, e);
            return null;
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
    }

    private ItemPricingData resolveExcPricingNative(BigDecimal codProd, BigDecimal codEmp, BigDecimal preferredNuTab) {
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
                    if (!isNullOrZero(preferredNuTab)) {
                        if (rowNutab == null || rowNutab.compareTo(preferredNuTab) != 0) {
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
                    ItemPricingData result = new ItemPricingData();
                    result.nuTab = safeAsBigDecimal(selected, "NUTAB");
                    result.precoBase = safeAsBigDecimal(selected, "VLRVENDA");
                    return result;
                }
            } catch (Exception e) {
                log.log(Level.FINE, "Falha ao resolver TGFEXC via Jape (" + daoName + ")", e);
            }
        }
        return null;
    }

    private BigDecimal resolvePreferredNuTab(BigDecimal codVend, BigDecimal codTipVenda) {
        if (isNullOrZero(codVend)) {
            return null;
        }
        BigDecimal nativeNuTab = resolvePreferredNuTabNative(codVend, codTipVenda);
        if (!isNullOrZero(nativeNuTab)) {
            return nativeNuTab;
        }
        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = openJdbc();
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT TOP 1 T.NUTAB ");
            sql.appendSql("FROM TGFNPV N ");
            sql.appendSql("INNER JOIN TGFTAB T ON T.CODTAB = N.CODTAB ");
            sql.appendSql("WHERE N.CODVEND = :codVend ");
            if (hasTableColumn("TGFNPV", "ATIVO")) {
                sql.appendSql("AND (N.ATIVO IS NULL OR N.ATIVO = 'S') ");
            }
            if (hasTableColumn("TGFNPV", "INATIVO")) {
                sql.appendSql("AND (N.INATIVO IS NULL OR N.INATIVO = 'N') ");
            }
            if (!isNullOrZero(codTipVenda) && hasTableColumn("TGFNPV", "CODTIPVENDA")) {
                sql.appendSql("AND N.CODTIPVENDA = :codTipVenda ");
                sql.setNamedParameter("codTipVenda", codTipVenda);
            }
            if (hasTableColumn("TGFTAB", "DTVIGOR")) {
                sql.appendSql("AND (T.DTVIGOR IS NULL OR T.DTVIGOR <= GETDATE()) ");
            }
            if (hasTableColumn("TGFTAB", "INATIVO")) {
                sql.appendSql("AND (T.INATIVO IS NULL OR T.INATIVO = 'N') ");
            }
            sql.appendSql("ORDER BY T.DTVIGOR DESC, T.NUTAB DESC");
            sql.setNamedParameter("codVend", codVend);
            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getBigDecimal("NUTAB");
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Nao foi possivel resolver NUTAB por TGFNPV para CODVEND " + codVend, e);
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
        return null;
    }

    private BigDecimal resolvePreferredNuTabNative(BigDecimal codVend, BigDecimal codTipVenda) {
        String[] daoCandidates = {"NegociacaoVendedor", "TGFNPV"};
        for (String daoName : daoCandidates) {
            try {
                JapeWrapper npvDAO = JapeFactory.dao(daoName);
                Collection<DynamicVO> rows = npvDAO.find("this.CODVEND = ?", codVend);
                if (rows == null || rows.isEmpty()) {
                    continue;
                }
                BigDecimal selectedNutab = null;
                Timestamp selectedDtVigor = null;
                for (DynamicVO row : rows) {
                    if (row == null) {
                        continue;
                    }
                    String rowAtivo = safeAsString(row, "ATIVO");
                    if ("N".equalsIgnoreCase(trimToNull(rowAtivo))) {
                        continue;
                    }
                    String rowInativo = safeAsString(row, "INATIVO");
                    if ("S".equalsIgnoreCase(trimToNull(rowInativo))) {
                        continue;
                    }
                    if (!isNullOrZero(codTipVenda)) {
                        BigDecimal rowTipVenda = safeAsBigDecimal(row, "CODTIPVENDA");
                        if (rowTipVenda != null && rowTipVenda.compareTo(codTipVenda) != 0) {
                            continue;
                        }
                    }
                    BigDecimal codTab = safeAsBigDecimal(row, "CODTAB");
                    if (isNullOrZero(codTab)) {
                        continue;
                    }
                    DynamicVO tabVO = resolveLatestActiveTabByCodTabNative(codTab);
                    if (tabVO == null) {
                        continue;
                    }
                    BigDecimal rowNutab = safeAsBigDecimal(tabVO, "NUTAB");
                    Timestamp rowDtVigor = safeAsTimestamp(tabVO, "DTVIGOR");
                    if (isNullOrZero(rowNutab)) {
                        continue;
                    }
                    if (selectedNutab == null
                            || (rowDtVigor != null && (selectedDtVigor == null || rowDtVigor.after(selectedDtVigor)))
                            || (rowDtVigor != null && selectedDtVigor != null && rowDtVigor.equals(selectedDtVigor)
                            && rowNutab.compareTo(selectedNutab) > 0)) {
                        selectedNutab = rowNutab;
                        selectedDtVigor = rowDtVigor;
                    }
                }
                if (!isNullOrZero(selectedNutab)) {
                    return selectedNutab;
                }
            } catch (Exception e) {
                log.log(Level.FINE, "Falha ao resolver NUTAB via Jape (" + daoName + ")", e);
            }
        }
        return null;
    }

    private DynamicVO resolveLatestActiveTabByCodTabNative(BigDecimal codTab) {
        if (isNullOrZero(codTab)) {
            return null;
        }
        String[] daoCandidates = {"TabelaPreco", "TGFTAB"};
        for (String daoName : daoCandidates) {
            try {
                JapeWrapper tabDAO = JapeFactory.dao(daoName);
                Collection<DynamicVO> rows = tabDAO.find("this.CODTAB = ?", codTab);
                if (rows == null || rows.isEmpty()) {
                    continue;
                }
                DynamicVO selected = null;
                Timestamp bestDtVigor = null;
                BigDecimal bestNutab = null;
                Timestamp now = new Timestamp(System.currentTimeMillis());
                for (DynamicVO row : rows) {
                    if (row == null) {
                        continue;
                    }
                    String inativo = safeAsString(row, "INATIVO");
                    if ("S".equalsIgnoreCase(trimToNull(inativo))) {
                        continue;
                    }
                    Timestamp dtVigor = safeAsTimestamp(row, "DTVIGOR");
                    if (dtVigor != null && dtVigor.after(now)) {
                        continue;
                    }
                    BigDecimal nuTab = safeAsBigDecimal(row, "NUTAB");
                    if (isNullOrZero(nuTab)) {
                        continue;
                    }
                    if (selected == null
                            || (dtVigor != null && (bestDtVigor == null || dtVigor.after(bestDtVigor)))
                            || (dtVigor != null && bestDtVigor != null && dtVigor.equals(bestDtVigor)
                            && nuTab.compareTo(bestNutab) > 0)
                            || (dtVigor == null && bestDtVigor == null && (bestNutab == null || nuTab.compareTo(bestNutab) > 0))) {
                        selected = row;
                        bestDtVigor = dtVigor;
                        bestNutab = nuTab;
                    }
                }
                if (selected != null) {
                    return selected;
                }
            } catch (Exception e) {
                log.log(Level.FINE, "Falha ao resolver TGFTAB via Jape (" + daoName + ")", e);
            }
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
            log.log(Level.FINE, "Nao foi possivel resolver CODTAB para NUTAB " + nuTab, e);
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
        return null;
    }

    private BigDecimal normalizeNuTabToLatestActive(BigDecimal nuTab) {
        if (isNullOrZero(nuTab)) {
            return null;
        }
        BigDecimal codTab = resolveCodTabByNuTab(nuTab);
        if (isNullOrZero(codTab)) {
            return nuTab;
        }
        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = openJdbc();
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT TOP 1 NUTAB ");
            sql.appendSql("FROM TGFTAB ");
            sql.appendSql("WHERE CODTAB = :codTab ");
            if (hasTableColumn("TGFTAB", "DTVIGOR")) {
                sql.appendSql("AND (DTVIGOR IS NULL OR DTVIGOR <= GETDATE()) ");
            }
            if (hasTableColumn("TGFTAB", "INATIVO")) {
                sql.appendSql("AND (INATIVO IS NULL OR INATIVO = 'N') ");
            }
            sql.appendSql("ORDER BY DTVIGOR DESC, NUTAB DESC");
            sql.setNamedParameter("codTab", codTab);
            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getBigDecimal("NUTAB");
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Nao foi possivel normalizar NUTAB " + nuTab, e);
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
        return nuTab;
    }

    private BigDecimal resolveCurrentCost(BigDecimal codProd, BigDecimal codEmp) {
        if (isNullOrZero(codProd)) {
            return null;
        }
        BigDecimal nativeCost = resolveCurrentCostNative(codProd, codEmp);
        if (!isNullOrZero(nativeCost)) {
            return nativeCost;
        }
        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = openJdbc();
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT ISNULL(MAX(CUSREP),0) AS CUSREP ");
            sql.appendSql("FROM TGFCUS ");
            sql.appendSql("WHERE CODPROD = :codProd ");
            sql.appendSql("AND DTATUAL = (SELECT MAX(DTATUAL) FROM TGFCUS CN ");
            sql.appendSql("               WHERE CN.CODPROD = :codProd ");
            sql.appendSql("               AND CN.DTATUAL <= GETDATE() ");
            if (!isNullOrZero(codEmp) && hasTableColumn("TGFCUS", "CODEMP")) {
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
            log.log(Level.FINE, "Nao foi possivel resolver custo atual em TGFCUS para CODPROD " + codProd, e);
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
        return null;
    }

    private BigDecimal resolveCurrentCostNative(BigDecimal codProd, BigDecimal codEmp) {
        String[] daoCandidates = {"Custo", "TGFCUS"};
        for (String daoName : daoCandidates) {
            try {
                JapeWrapper cusDAO = JapeFactory.dao(daoName);
                Collection<DynamicVO> rows = cusDAO.find("this.CODPROD = ?", codProd);
                if (rows == null || rows.isEmpty()) {
                    continue;
                }
                BigDecimal bestCus = null;
                Timestamp bestDt = null;
                Timestamp now = new Timestamp(System.currentTimeMillis());
                for (DynamicVO row : rows) {
                    if (row == null) {
                        continue;
                    }
                    if (!isNullOrZero(codEmp)) {
                        BigDecimal rowEmp = safeAsBigDecimal(row, "CODEMP");
                        if (rowEmp != null && rowEmp.compareTo(codEmp) != 0) {
                            continue;
                        }
                    }
                    Timestamp dtAtual = safeAsTimestamp(row, "DTATUAL");
                    if (dtAtual != null && dtAtual.after(now)) {
                        continue;
                    }
                    BigDecimal cusRep = safeAsBigDecimal(row, "CUSREP");
                    if (isNullOrZero(cusRep)) {
                        continue;
                    }
                    if (bestCus == null
                            || (dtAtual != null && (bestDt == null || dtAtual.after(bestDt)))
                            || (dtAtual != null && bestDt != null && dtAtual.equals(bestDt) && cusRep.compareTo(bestCus) > 0)) {
                        bestCus = cusRep;
                        bestDt = dtAtual;
                    }
                }
                if (!isNullOrZero(bestCus)) {
                    return bestCus;
                }
            } catch (Exception e) {
                log.log(Level.FINE, "Falha ao resolver TGFCUS via Jape (" + daoName + ")", e);
            }
        }
        return null;
    }

    private BigDecimal preferTop403(BigDecimal currentTop) {
        BigDecimal preferred = FastchannelConstants.DEFAULT_TOP_PEDIDO;
        if (existsTopForPedido(preferred)) {
            return preferred;
        }
        return currentTop;
    }

    private boolean existsTopForPedido(BigDecimal codTipOper) {
        if (isNullOrZero(codTipOper)) {
            return false;
        }

        DynamicVO topVO = findTipoOperacaoByCodTipOper(codTipOper);
        if (topVO != null) {
            String tipMov = safeAsString(topVO, "TIPMOV");
            if ("P".equalsIgnoreCase(tipMov)) {
                return true;
            }
        }

        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = openJdbc();
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT TOP 1 CODTIPOPER FROM TGFTOP ");
            sql.appendSql("WHERE CODTIPOPER = :codTipOper AND TIPMOV = 'P'");
            sql.setNamedParameter("codTipOper", codTipOper);
            rs = sql.executeQuery();
            return rs.next();
        } catch (Exception e) {
            log.log(Level.FINE, "Nao foi possivel validar TOP " + codTipOper, e);
            return false;
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
    }

    private String truncate(String str, int maxLength) {
        if (str == null) return null;
        return str.length() > maxLength ? str.substring(0, maxLength) : str;
    }

    private boolean supportsCabField(String field) {
        if (field == null || field.trim().isEmpty()) return false;
        String normalizedField = field.trim().toUpperCase();
        Boolean cached = CAB_FIELD_SUPPORT.get(normalizedField);
        if (cached != null) return cached;

        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = openJdbc();
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT COUNT(*) AS CNT ");
            sql.appendSql("FROM INFORMATION_SCHEMA.COLUMNS ");
            sql.appendSql("WHERE TABLE_NAME = 'TGFCAB' AND COLUMN_NAME = :columnName");
            sql.setNamedParameter("columnName", normalizedField);
            rs = sql.executeQuery();
            boolean supported = rs.next() && rs.getInt("CNT") > 0;
            CAB_FIELD_SUPPORT.put(normalizedField, supported);
            return supported;
        } catch (Exception e) {
            log.log(Level.WARNING, "Nao foi possivel validar campo TGFCAB." + normalizedField, e);
            CAB_FIELD_SUPPORT.put(normalizedField, false);
            return false;
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
    }

    private boolean supportsItemField(String field) {
        if (field == null || field.trim().isEmpty()) return false;
        String normalizedField = field.trim().toUpperCase();
        Boolean cached = ITEM_FIELD_SUPPORT.get(normalizedField);
        if (cached != null) return cached;

        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = openJdbc();
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT COUNT(*) AS CNT ");
            sql.appendSql("FROM INFORMATION_SCHEMA.COLUMNS ");
            sql.appendSql("WHERE TABLE_NAME = 'TGFITE' AND COLUMN_NAME = :columnName");
            sql.setNamedParameter("columnName", normalizedField);
            rs = sql.executeQuery();
            boolean supported = rs.next() && rs.getInt("CNT") > 0;
            ITEM_FIELD_SUPPORT.put(normalizedField, supported);
            return supported;
        } catch (Exception e) {
            log.log(Level.WARNING, "Nao foi possivel validar campo TGFITE." + normalizedField, e);
            ITEM_FIELD_SUPPORT.put(normalizedField, false);
            return false;
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
    }

    private boolean isCabFieldRequired(String field) {
        if (field == null || field.trim().isEmpty()) return false;
        String normalizedField = field.trim().toUpperCase();
        Boolean cached = CAB_FIELD_REQUIRED.get(normalizedField);
        if (cached != null) return cached;

        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = openJdbc();
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT IS_NULLABLE ");
            sql.appendSql("FROM INFORMATION_SCHEMA.COLUMNS ");
            sql.appendSql("WHERE TABLE_NAME = 'TGFCAB' AND COLUMN_NAME = :columnName");
            sql.setNamedParameter("columnName", normalizedField);
            rs = sql.executeQuery();
            boolean required = rs.next() && "NO".equalsIgnoreCase(rs.getString("IS_NULLABLE"));
            CAB_FIELD_REQUIRED.put(normalizedField, required);
            return required;
        } catch (Exception e) {
            log.log(Level.WARNING, "Nao foi possivel validar obrigatoriedade TGFCAB." + normalizedField, e);
            CAB_FIELD_REQUIRED.put(normalizedField, false);
            return false;
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
    }

    private BigDecimal resolveNumNotaBySankhyaRules(BigDecimal codUsu, BigDecimal codEmp, BigDecimal codParc, BigDecimal codTipOper, Timestamp dtNeg) {
        TopNumeracaoConfig numCfg = loadTopNumeracaoConfig(codTipOper);
        if (numCfg == null || isBlank(numCfg.baseNumeracao) || isBlank(numCfg.tipoNumeracao)) {
            log.warning("[InternalAPI] TOP " + codTipOper + " sem configuracao completa de numeracao (BASENUMERACAO/TIPONUMERACAO).");
            return null;
        }

        try {
            NumeracaoNotaHelper helper = new NumeracaoNotaHelper();
            NumeracaoNotaHelper.ParamNota param = new NumeracaoNotaHelper.ParamNota();
            param.setNumNota(BigDecimal.ZERO);
            param.setCodEmp(codEmp);
            param.setCodParc(codParc);
            param.setDatNeg(dtNeg != null ? dtNeg : new Timestamp(System.currentTimeMillis()));
            param.setCodTop(codTipOper);
            param.setCodModNF(numCfg.codModNF != null ? numCfg.codModNF : BigDecimal.ZERO);
            param.setCodModDoc(numCfg.codModDoc != null ? numCfg.codModDoc : BigDecimal.ZERO);
            param.setBase(numCfg.baseNumeracao);
            param.setTipoNumeracao(numCfg.tipoNumeracao);
            param.setValidaData(!isBlank(numCfg.validaData) ? numCfg.validaData : "N");
            param.setSerie(" ");
            param.setAtualizaLivros(numCfg.atualizaLivros);
            param.setNuNota(BigDecimal.ZERO);
            return helper.gerarNumero(param, codUsu != null ? codUsu : BigDecimal.ZERO);
        } catch (Exception e) {
            log.log(Level.WARNING, "[InternalAPI] Falha ao gerar NUMNOTA via NumeracaoNotaHelper para TOP " + codTipOper, e);
            return null;
        }
    }

    private TopNumeracaoConfig loadTopNumeracaoConfig(BigDecimal codTipOper) {
        if (isNullOrZero(codTipOper)) {
            return null;
        }

        // Caminho nativo (Jape) primeiro.
        try {
            DynamicVO topVO = findTipoOperacaoByCodTipOper(codTipOper);
            if (topVO != null) {
                TopNumeracaoConfig cfg = new TopNumeracaoConfig();
                cfg.codModNF = safeAsBigDecimal(topVO, "CODMODNF");
                cfg.codModDoc = safeAsBigDecimal(topVO, "CODMODDOC");
                cfg.baseNumeracao = safeAsString(topVO, "BASENUMERACAO");
                cfg.tipoNumeracao = safeAsString(topVO, "TIPONUMERACAO");
                cfg.validaData = safeAsString(topVO, "VALIDADATA");
                String atualLivFis = safeAsString(topVO, "ATUALLIVFIS");
                String atualLivIss = safeAsString(topVO, "ATUALLIVISS");
                cfg.atualizaLivros = (atualLivFis != null && !"N".equalsIgnoreCase(atualLivFis))
                        || (atualLivIss != null && !"N".equalsIgnoreCase(atualLivIss));
                if (!isBlank(cfg.baseNumeracao) && !isBlank(cfg.tipoNumeracao)) {
                    return cfg;
                }
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Nao foi possivel carregar numeracao da TOP via Jape para " + codTipOper, e);
        }

        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = openJdbc();
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT TOP 1 ");
            sql.appendSql("CODMODNF, CODMODDOC, BASENUMERACAO, TIPONUMERACAO, VALIDADATA, ATUALLIVFIS, ATUALLIVISS ");
            sql.appendSql("FROM TGFTOP ");
            sql.appendSql("WHERE CODTIPOPER = :codTipOper ");
            sql.appendSql("ORDER BY DHALTER DESC");
            sql.setNamedParameter("codTipOper", codTipOper);
            rs = sql.executeQuery();
            if (rs.next()) {
                TopNumeracaoConfig cfg = new TopNumeracaoConfig();
                cfg.codModNF = rs.getBigDecimal("CODMODNF");
                cfg.codModDoc = rs.getBigDecimal("CODMODDOC");
                cfg.baseNumeracao = rs.getString("BASENUMERACAO");
                cfg.tipoNumeracao = rs.getString("TIPONUMERACAO");
                cfg.validaData = rs.getString("VALIDADATA");
                String atualLivFis = rs.getString("ATUALLIVFIS");
                String atualLivIss = rs.getString("ATUALLIVISS");
                cfg.atualizaLivros = (atualLivFis != null && !"N".equalsIgnoreCase(atualLivFis))
                        || (atualLivIss != null && !"N".equalsIgnoreCase(atualLivIss));
                return cfg;
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Nao foi possivel carregar configuracao de numeracao da TOP " + codTipOper, e);
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
        return null;
    }

    private void ensureRequiredSessionProperties() {
        BigDecimal codUsu = resolveCodUsuLogado();
        try {
            Object existing = JapeSessionContext.getProperty("usuario_logado");
            if (existing == null) {
                JapeSessionContext.putProperty("usuario_logado", codUsu);
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Nao foi possivel ajustar JapeSessionContext.usuario_logado", e);
        }

        try {
            Object existing = JapeSession.getProperty("usuario_logado");
            if (existing == null) {
                JapeSession.putProperty("usuario_logado", codUsu);
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Nao foi possivel ajustar JapeSession.usuario_logado", e);
        }
    }

    private BigDecimal resolveCodUsuLogado() {
        List<String> users = new ArrayList<>();
        String configuredUser = trimToNull(config.getSankhyaUser());
        if (!isBlank(configuredUser)) {
            users.add(configuredUser);
        }
        users.add("FAST");
        users.add("INTEGRACAOFASTCHANNEL");

        if (users.isEmpty()) {
            return BigDecimal.ZERO;
        }

        for (String user : users) {
            BigDecimal codUsu = resolveCodUsuByName(user);
            if (!isNullOrZero(codUsu)) {
                return codUsu;
            }
        }
        return BigDecimal.ZERO;
    }

    private BigDecimal resolveCodUsuByName(String user) {
        if (user == null || user.trim().isEmpty()) {
            return null;
        }

        // Caminho nativo (Jape) primeiro.
        try {
            JapeWrapper usuDAO = JapeFactory.dao("Usuario");
            Collection<DynamicVO> byNomUsu = usuDAO.find("UPPER(this.NOMUSU) = UPPER(?)", user);
            BigDecimal codUsu = extractCodUsu(byNomUsu);
            if (!isNullOrZero(codUsu)) {
                return codUsu;
            }
            Collection<DynamicVO> byNomeUsu = usuDAO.find("UPPER(this.NOMEUSU) = UPPER(?)", user);
            codUsu = extractCodUsu(byNomeUsu);
            if (!isNullOrZero(codUsu)) {
                return codUsu;
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Nao foi possivel resolver CODUSU via Jape para usuario " + user, e);
        }

        if (hasTableColumn("TSIUSU", "NOMUSU")) {
            BigDecimal codUsu = findCodUsu("SELECT TOP 1 CODUSU FROM TSIUSU WHERE UPPER(NOMUSU)=UPPER(?)", user);
            if (codUsu != null) {
                return codUsu;
            }
        }

        if (hasTableColumn("TSIUSU", "NOMEUSU")) {
            BigDecimal codUsu = findCodUsu("SELECT TOP 1 CODUSU FROM TSIUSU WHERE UPPER(NOMEUSU)=UPPER(?)", user);
            if (codUsu != null) {
                return codUsu;
            }
        }

        return null;
    }

    private BigDecimal findCodUsu(String sqlText, String user) {
        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = openJdbc();
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql(sqlText.replace("?", ":userName"));
            sql.setNamedParameter("userName", user);
            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getBigDecimal(1);
            }
        } catch (Exception e) {
            log.log(Level.FINE, "Nao foi possivel resolver CODUSU para usuario " + user, e);
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
        return null;
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
                log.log(Level.FINE, "Erro ao fechar session do JdbcWrapper", e);
            }
        }
    }

    private void closeQuietly(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (Exception ignored) {}
        }
    }

    private boolean isNullOrZero(BigDecimal value) {
        return value == null || value.compareTo(BigDecimal.ZERO) <= 0;
    }

    private boolean hasTableColumn(String tableName, String columnName) {
        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = openJdbc();
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT COUNT(*) AS CNT ");
            sql.appendSql("FROM INFORMATION_SCHEMA.COLUMNS ");
            sql.appendSql("WHERE TABLE_NAME = :tableName AND COLUMN_NAME = :columnName");
            sql.setNamedParameter("tableName", tableName);
            sql.setNamedParameter("columnName", columnName);
            rs = sql.executeQuery();
            return rs.next() && rs.getInt("CNT") > 0;
        } catch (Exception e) {
            log.log(Level.FINE, "Nao foi possivel validar coluna " + tableName + "." + columnName, e);
            return false;
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
    }

    private boolean containsField(List<String> fields, String name) {
        for (String field : fields) {
            if (name.equalsIgnoreCase(field)) {
                return true;
            }
        }
        return false;
    }

    private DynamicVO findTipoOperacaoByCodTipOper(BigDecimal codTipOper) {
        if (isNullOrZero(codTipOper)) {
            return null;
        }
        try {
            JapeWrapper topDAO = JapeFactory.dao("TipoOperacao");
            Collection<DynamicVO> tops = topDAO.find("this.CODTIPOPER = ?", codTipOper);
            if (tops == null || tops.isEmpty()) {
                return null;
            }
            DynamicVO selected = null;
            Timestamp selectedDh = null;
            for (DynamicVO top : tops) {
                if (top == null) {
                    continue;
                }
                Timestamp dh = safeAsTimestamp(top, "DHALTER");
                if (selected == null || (dh != null && (selectedDh == null || dh.after(selectedDh)))) {
                    selected = top;
                    selectedDh = dh;
                }
            }
            return selected;
        } catch (Exception e) {
            log.log(Level.FINE, "Nao foi possivel resolver TipoOperacao via Jape para CODTIPOPER " + codTipOper, e);
            return null;
        }
    }

    private DynamicVO findTipoVendaByCodTipVenda(BigDecimal codTipVenda) {
        if (isNullOrZero(codTipVenda)) {
            return null;
        }
        try {
            JapeWrapper tpvDAO = JapeFactory.dao("TipoVenda");
            return tpvDAO.findByPK(codTipVenda);
        } catch (Exception e) {
            log.log(Level.FINE, "Nao foi possivel resolver TipoVenda via Jape para CODTIPVENDA " + codTipVenda, e);
            return null;
        }
    }

    private BigDecimal extractCodCenCusPad(Collection<DynamicVO> usuarios) {
        if (usuarios == null || usuarios.isEmpty()) {
            return null;
        }
        for (DynamicVO usu : usuarios) {
            BigDecimal cod = safeAsBigDecimal(usu, "CODCENCUSPAD");
            if (!isNullOrZero(cod)) {
                return cod;
            }
        }
        return null;
    }

    private BigDecimal extractCodUsu(Collection<DynamicVO> usuarios) {
        if (usuarios == null || usuarios.isEmpty()) {
            return null;
        }
        for (DynamicVO usu : usuarios) {
            BigDecimal cod = safeAsBigDecimal(usu, "CODUSU");
            if (!isNullOrZero(cod)) {
                return cod;
            }
        }
        return null;
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

    private Timestamp safeAsTimestamp(DynamicVO vo, String field) {
        if (vo == null || field == null) {
            return null;
        }
        try {
            return vo.asTimestamp(field);
        } catch (Exception ignored) {
            return null;
        }
    }

    private String safeAsString(DynamicVO vo, String field) {
        if (vo == null || field == null) {
            return null;
        }
        try {
            String value = vo.asString(field);
            if (value == null) {
                return null;
            }
            String trimmed = value.trim();
            return trimmed.isEmpty() ? null : trimmed;
        } catch (Exception ignored) {
            return null;
        }
    }

    private static final class TopDefaults {
        private BigDecimal codTipVenda;
        private BigDecimal codVend;
        private BigDecimal codNat;
        private BigDecimal codCenCus;
    }

    private static final class ItemPricingData {
        private BigDecimal nuTab;
        private BigDecimal precoBase;
        private BigDecimal custo;
        private String usoProd;
    }

    private static final class TopNumeracaoConfig {
        private BigDecimal codModNF;
        private BigDecimal codModDoc;
        private String baseNumeracao;
        private String tipoNumeracao;
        private String validaData;
        private boolean atualizaLivros;
    }

    private static final class HeaderCreateResult {
        private BigDecimal nuNota;
        private BigDecimal codLocal;
        private BigDecimal codEmp;
        private BigDecimal codVend;
        private BigDecimal codTipVenda;
    }

    private static final class WeightTotals {
        private BigDecimal peso;
        private BigDecimal pesoBruto;
    }
}
