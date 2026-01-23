package br.com.bellube.fastchannel.service;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.config.FastchannelConstants;
import br.com.bellube.fastchannel.dto.*;
import br.com.bellube.fastchannel.http.FastchannelOrdersClient;
import br.com.sankhya.jape.EntityFacade;
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
import java.util.List;
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

    public OrderService() {
        this.config = FastchannelConfig.getInstance();
        this.ordersClient = new FastchannelOrdersClient();
        this.deparaService = DeparaService.getInstance();
        this.logService = LogService.getInstance();
    }

    /**
     * Importa pedidos pendentes do Fastchannel.
     *
     * @return n?mero de pedidos importados com sucesso
     */
    public int importPendingOrders() {
        int imported = 0;
        int page = 1;
        int pageSize = config.getBatchSize();

        try {
            Timestamp lastSync = config.getLastOrderSync();
            log.info("Iniciando importa??o de pedidos. ?ltima sync: " + lastSync);

            while (true) {
                List<OrderDTO> orders = ordersClient.listOrders(lastSync, page, pageSize);

                if (orders == null || orders.isEmpty()) {
                    break;
                }

                for (OrderDTO order : orders) {
                    try {
                        if (isOrderAlreadyImported(order.getOrderId())) {
                            log.fine("Pedido " + order.getOrderId() + " j? importado. Pulando.");
                            continue;
                        }

                        BigDecimal nuNota = importOrder(order);
                        if (nuNota != null) {
                            imported++;
                            logService.logOrderImport(order.getOrderId(), nuNota, true, null);

                            // Notificar Fastchannel
                            ordersClient.markAsSynced(order.getOrderId(), nuNota.toString());
                        }

                    } catch (Exception e) {
                        log.log(Level.SEVERE, "Erro ao importar pedido " + order.getOrderId(), e);
                        logService.logOrderImport(order.getOrderId(), null, false, e.getMessage());
                    }
                }

                // Se retornou menos que pageSize, n?o h? mais p?ginas
                if (orders.size() < pageSize) {
                    break;
                }

                page++;
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

    /**
     * Importa um pedido espec?fico.
     *
     * @param order dados do pedido
     * @return NUNOTA criado ou null se falhar
     */
    public BigDecimal importOrder(OrderDTO order) throws Exception {
        log.info("Importando pedido: " + order.getOrderId());

        // Validar pedido
        validateOrder(order);

        EntityFacade entityFacade = EntityFacadeFactory.getCoreFacade();
        JdbcWrapper jdbc = entityFacade.getJdbcWrapper();

        try {
            // 1. Localizar ou criar parceiro
            BigDecimal codParc = findOrCreateParceiro(order.getCustomer(), order.getShippingAddress());

            // 2. Criar cabe?alho do pedido
            BigDecimal nuNota = createCabecalho(order, codParc);

            // 3. Criar itens
            createItens(nuNota, order.getItems());

            // 4. Registrar na AD_FCPEDIDO
            registerOrderMapping(order.getOrderId(), nuNota, codParc);

            log.info("Pedido " + order.getOrderId() + " importado como NUNOTA " + nuNota);
            return nuNota;

        } catch (Exception e) {
            log.log(Level.SEVERE, "Falha ao importar pedido " + order.getOrderId(), e);
            throw e;
        }
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

    /**
     * Localiza parceiro por CPF/CNPJ ou cria novo.
     */
    private BigDecimal findOrCreateParceiro(OrderCustomerDTO customer, OrderAddressDTO address) throws Exception {
        if (customer == null) {
            throw new Exception("Dados do cliente n?o informados");
        }

        String cpfCnpj = customer.getCleanCpfCnpj();
        if (cpfCnpj == null || cpfCnpj.isEmpty()) {
            throw new Exception("CPF/CNPJ do cliente n?o informado");
        }

        // Tentar localizar por CPF/CNPJ
        BigDecimal codParc = findParceiroByCpfCnpj(cpfCnpj);
        if (codParc != null) {
            log.fine("Parceiro encontrado: " + codParc);
            return codParc;
        }

        // Criar novo parceiro
        return createParceiro(customer, address);
    }

    private BigDecimal findParceiroByCpfCnpj(String cpfCnpj) {
        ResultSet rs = null;
        try {
            JdbcWrapper jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();

            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT CODPARC FROM TGFPAR ");
            sql.appendSql("WHERE REPLACE(REPLACE(REPLACE(CGC_CPF, '.', ''), '-', ''), '/', '') = :cpfCnpj");

            sql.setNamedParameter("cpfCnpj", cpfCnpj);

            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getBigDecimal("CODPARC");
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao buscar parceiro", e);
        } finally {
            closeQuietly(rs);
        }
        return null;
    }

    private BigDecimal createParceiro(OrderCustomerDTO customer, OrderAddressDTO address) throws Exception {
        log.info("Criando novo parceiro: " + customer.getName());

        JapeWrapper parceiroDAO = JapeFactory.dao("Parceiro");

        FluidCreateVO parceiroBuilder = parceiroDAO.create()
                .set("NOMEPARC", truncate(customer.getName(), 100))
                .set("TIPPESSOA", customer.isPJ() ? "J" : "F")
                .set("CGC_CPF", customer.getCpfCnpj())
                .set("EMAIL", truncate(customer.getEmail(), 80))
                .set("TELEFONE", truncate(customer.getPhone(), 15))
                .set("ATIVO", "S")
                .set("CLIENTE", "S")
                .set("FORNECEDOR", "N");

        // Raz?o Social para PJ
        if (customer.isPJ() && customer.getCompanyName() != null) {
            parceiroBuilder = parceiroBuilder.set("RAZAOSOCIAL", truncate(customer.getCompanyName(), 100));
        }

        // Inscri??o Estadual
        if (customer.getStateRegistration() != null) {
            parceiroBuilder = parceiroBuilder.set("IDENTINSCESTAD", truncate(customer.getStateRegistration(), 30));
        }

        DynamicVO parceiroVO = parceiroBuilder.save();
        BigDecimal codParc = parceiroVO.asBigDecimal("CODPARC");

        // Criar endere?o se informado
        if (address != null) {
            createEndereco(codParc, address);
        }

        log.info("Parceiro criado: CODPARC " + codParc);
        return codParc;
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
        ResultSet rs = null;
        try {
            JdbcWrapper jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();

            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT CODCID FROM TSICID WHERE UPPER(NOMECID) = UPPER(:nome) AND UF = :uf");
            sql.setNamedParameter("nome", nomeCidade);
            sql.setNamedParameter("uf", uf);

            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getBigDecimal("CODCID");
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao buscar cidade", e);
        } finally {
            closeQuietly(rs);
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
                .set("STATUSNOTA", "L") // Liberado
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
            BigDecimal codProd = deparaService.getCodProdBySkuOrEan(item.getSku());
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
        ResultSet rs = null;
        try {
            JdbcWrapper jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();

            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT CODVOL FROM TGFPRO WHERE CODPROD = :codProd");
            sql.setNamedParameter("codProd", codProd);

            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getString("CODVOL");
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao buscar CODVOL", e);
        } finally {
            closeQuietly(rs);
        }
        return "UN";
    }

    /**
     * Registra mapeamento do pedido na AD_FCPEDIDO.
     */
    private void registerOrderMapping(String orderId, BigDecimal nuNota, BigDecimal codParc) {
        try {
            JdbcWrapper jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();

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
     * Verifica se pedido j? foi importado.
     */
    private boolean isOrderAlreadyImported(String orderId) {
        ResultSet rs = null;
        try {
            JdbcWrapper jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();

            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT 1 FROM AD_FCPEDIDO WHERE ORDER_ID = :orderId");
            sql.setNamedParameter("orderId", orderId);

            rs = sql.executeQuery();
            return rs.next();
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao verificar pedido existente", e);
        } finally {
            closeQuietly(rs);
        }
        return false;
    }

    /**
     * Busca NUNOTA pelo OrderId do Fastchannel.
     */
    public BigDecimal getNuNotaByOrderId(String orderId) {
        ResultSet rs = null;
        try {
            JdbcWrapper jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();

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
        }
        return null;
    }

    private String truncate(String str, int maxLength) {
        if (str == null) return null;
        return str.length() > maxLength ? str.substring(0, maxLength) : str;
    }

    private void closeQuietly(ResultSet rs) {
        if (rs != null) {
            try { rs.close(); } catch (Exception ignored) {}
        }
    }
}

