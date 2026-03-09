package br.com.bellube.fastchannel.action;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.http.FastchannelStockClient;
import br.com.bellube.fastchannel.service.DeparaService;
import br.com.bellube.fastchannel.service.LogService;
import br.com.bellube.fastchannel.service.PriceService;
import br.com.bellube.fastchannel.service.StockResolver;
import br.com.sankhya.extensions.actionbutton.AcaoRotinaJava;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Sincronizacao manual de catalogo (estoque + preco) para todos os produtos.
 */
public class SincronizarProdutosAction implements AcaoRotinaJava {

    private static final Logger log = Logger.getLogger(SincronizarProdutosAction.class.getName());

    @Override
    public void doAction(ContextoAcao contexto) throws Exception {
        StringBuilder resultado = new StringBuilder();
        FastchannelConfig config = FastchannelConfig.getInstance();
        if (!config.isAtivo()) {
            contexto.setMensagemRetorno("[ERRO] Integração Fastchannel não está ativa.");
            return;
        }

        DeparaService depara = DeparaService.getInstance();
        StockResolver stockResolver = new StockResolver();
        FastchannelStockClient stockClient = new FastchannelStockClient();
        PriceService priceService = new PriceService();

        int total = 0;
        int syncedStock = 0;
        int syncedPrice = 0;
        int errors = 0;

        JdbcWrapper jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();
        ResultSet rs = null;
        try {
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT P.CODPROD, P.ATIVO, P.REFFORN, M.AD_FASTREF ");
            sql.appendSql("FROM TGFPRO P ");
            sql.appendSql("LEFT JOIN TGFMAR M ON M.CODIGO = P.CODMARCA ");
            sql.appendSql("WHERE P.CODPROD IS NOT NULL ");
            sql.appendSql("ORDER BY P.CODPROD");
            rs = sql.executeQuery();

            while (rs.next()) {
                total++;
                BigDecimal codProd = rs.getBigDecimal("CODPROD");
                String ativo = rs.getString("ATIVO");
                String adFastRef = rs.getString("AD_FASTREF");
                String refForn = rs.getString("REFFORN");
                String sku = "S".equalsIgnoreCase(adFastRef) && refForn != null && !refForn.trim().isEmpty()
                        ? refForn.trim()
                        : depara.getSkuForStock(codProd);

                if (sku == null || sku.trim().isEmpty()) {
                    continue;
                }

                try {
                    BigDecimal qty = "S".equalsIgnoreCase(ativo)
                            ? stockResolver.resolve(codProd, config.getCodemp(), config.getCodLocal())
                            : BigDecimal.ZERO;
                    stockClient.updateStock(sku, qty != null ? qty : BigDecimal.ZERO);
                    syncedStock++;
                } catch (Exception e) {
                    errors++;
                    log.log(Level.WARNING, "Erro ao sincronizar estoque para SKU " + sku, e);
                }

                try {
                    priceService.syncPrice(codProd, sku);
                    syncedPrice++;
                } catch (Exception e) {
                    errors++;
                    log.log(Level.WARNING, "Erro ao sincronizar preco para SKU " + sku, e);
                }
            }
        } finally {
            if (rs != null) {
                try { rs.close(); } catch (Exception ignored) {}
            }
        }

        resultado.append("Sincronização de produtos concluída.\n")
                .append("Total analisado: ").append(total).append("\n")
                .append("Estoque sincronizado: ").append(syncedStock).append("\n")
                .append("Preço sincronizado: ").append(syncedPrice).append("\n")
                .append("Erros: ").append(errors).append("\n");
        LogService.getInstance().info(LogService.OP_GENERAL, "Sincronizar produtos", resultado.toString());
        contexto.setMensagemRetorno(resultado.toString());
    }
}

