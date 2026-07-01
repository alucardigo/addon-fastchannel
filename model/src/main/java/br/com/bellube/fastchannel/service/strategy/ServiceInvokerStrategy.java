package br.com.bellube.fastchannel.service.strategy;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.dto.OrderDTO;
import br.com.bellube.fastchannel.service.OrderXmlBuilder;
import br.com.bellube.fastchannel.service.nativeapi.SankhyaNativeServiceCaller;
import br.com.sankhya.jape.core.JapeSession;
import br.com.sankhya.jape.util.JapeSessionContext;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

import java.math.BigDecimal;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Estrategia FALLBACK 1: usa invocacao nativa de servicos Sankhya
 * (ServiceInvoker legado ou modelcore ServiceCaller oficial).
 *
 * <p><b>[FIX 2026-04-28]</b> Wrapper JapeSession.open() adicionado.
 * Anterior: createOrder() chamava nativeCaller.invoke() sem JapeSession aberta.
 * Resultado: o servlet /mgecom/service.sbr lanca "Gerenciador de sessao
 * nao foi iniciado" (4638 + 2415 ocorrencias em 10h de PROD em 28/04/2026)
 * porque o MGEFrontFacadeBean.ejbCreate (servidor) precisa de sessao ativa
 * pra inicializar o gerenciador EJB.
 *
 * <p>Padrao identico ao InternalApiStrategy.createOrder e ao
 * FastchannelAutoProvisioning.runInJapeSession (ja funcionais ha meses).
 * Fail-open por design: se JapeSession.open() falhar (ex.: classloader
 * isolation no boot inicial), seguimos sem session - prefer falhar com erro
 * Sankhya proprio do que abortar antes da invocacao.
 */
public class ServiceInvokerStrategy implements OrderCreationStrategy {

    private static final Logger log = Logger.getLogger(ServiceInvokerStrategy.class.getName());
    private static final String SERVICE_NAME = "CACSP.incluirNota";

    private final OrderXmlBuilder xmlBuilder;
    private final SankhyaNativeServiceCaller nativeCaller;
    private final FastchannelConfig config;

    public ServiceInvokerStrategy() {
        this.xmlBuilder = new OrderXmlBuilder();
        this.nativeCaller = new SankhyaNativeServiceCaller();
        this.config = FastchannelConfig.getInstance();
    }

    @Override
    public String getStrategyName() {
        return "ServiceInvoker";
    }

    @Override
    public boolean isAvailable() {
        return nativeCaller.isAvailable();
    }

    @Override
    public BigDecimal createOrder(OrderDTO order, BigDecimal codParc,
                                  BigDecimal codTipVenda, BigDecimal codVend,
                                  BigDecimal codNat, BigDecimal codCenCus) throws Exception {

        log.info("[ServiceInvoker] Criando pedido " + order.getOrderId() + " via bridge nativo Sankhya");

        JapeSession.SessionHandle hnd = null;
        try {
            // [FIX 2026-04-28] Garantir sessao ativa antes de tocar modelcore.
            // Sem isso, ServiceCaller -> MGEFrontFacadeBean.ejbCreate quebra com
            // "Gerenciador de sessao nao foi iniciado" (4638 erros/10h em PROD).
            try {
                EntityFacadeFactory.getCoreFacade();
                hnd = JapeSession.open();
                ensureRequiredSessionProperties();
            } catch (Throwable openT) {
                log.log(Level.FINE, "[ServiceInvoker] JapeSession.open() falhou, tentando sem session ("
                        + openT.getClass().getSimpleName() + ")", openT);
            }

            String requestXml = xmlBuilder.buildIncluirNotaXml(order, codParc, codTipVenda, codVend, codNat, codCenCus);
            String responseXml = nativeCaller.invoke(SERVICE_NAME, requestXml, config.getSankhyaUser(), config.getSankhyaPassword());

            if (responseXml == null || responseXml.trim().isEmpty()) {
                throw new Exception("Resposta vazia ao invocar " + SERVICE_NAME);
            }

            BigDecimal nuNota = parseNuNotaFromResponse(responseXml);
            if (nuNota == null) {
                throw new Exception("NUNOTA nao encontrado na resposta do servico");
            }

            log.info("[ServiceInvoker] Pedido " + order.getOrderId() + " criado como NUNOTA " + nuNota);
            return nuNota;

        } catch (Exception e) {
            log.log(Level.SEVERE, "[ServiceInvoker] Erro ao criar pedido", e);
            throw new Exception("Falha no ServiceInvoker: " + e.getMessage(), e);
        } finally {
            if (hnd != null) {
                try { JapeSession.close(hnd); } catch (Throwable ignored) {}
            }
        }
    }

    /**
     * Garante propriedades minimas no JapeSessionContext que o modelcore
     * ServiceCaller / MGEFrontFacade esperam para inicializar.
     * Sem isso, o EJB ejbCreate falha com NullPointer/IllegalState.
     */
    private void ensureRequiredSessionProperties() {
        try {
            String user = config.getSankhyaUser();
            if (user != null && !user.trim().isEmpty()) {
                if (JapeSessionContext.getProperty("usuario_logado") == null) {
                    JapeSessionContext.putProperty("usuario_logado", user);
                }
                if (JapeSessionContext.getProperty("usuarioLogado") == null) {
                    JapeSessionContext.putProperty("usuarioLogado", user);
                }
            }
            if (JapeSessionContext.getProperty("origem_chamada") == null) {
                JapeSessionContext.putProperty("origem_chamada", "FastchannelAddon");
            }
        } catch (Throwable t) {
            log.log(Level.FINE, "[ServiceInvoker] ensureRequiredSessionProperties: " + t.getMessage(), t);
        }
    }

    private BigDecimal parseNuNotaFromResponse(String responseXml) {
        try {
            java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("<NUNOTA>(\\d+)</NUNOTA>");
            java.util.regex.Matcher matcher = pattern.matcher(responseXml);
            if (matcher.find()) {
                return new BigDecimal(matcher.group(1));
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao parsear NUNOTA", e);
        }
        return null;
    }
}
