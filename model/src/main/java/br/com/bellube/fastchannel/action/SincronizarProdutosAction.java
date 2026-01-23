package br.com.bellube.fastchannel.action;

import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.service.DeparaService;
import br.com.bellube.fastchannel.service.LogService;
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
 * A??o para sincronizar mapeamento de produtos.
 * Cria registros de De-Para para todos os produtos com REFERENCIA.
 */
public class SincronizarProdutosAction implements AcaoRotinaJava {

    private static final Logger log = Logger.getLogger(SincronizarProdutosAction.class.getName());

    @Override
    public void doAction(ContextoAcao contexto) throws Exception {
        StringBuilder resultado = new StringBuilder();

        try {
            // Verificar se integra??o est? ativa
            FastchannelConfig config = FastchannelConfig.getInstance();
            if (!config.isAtivo()) {
                resultado.append("[ERRO] Integra??o n?o est? ativa!");
                contexto.setMensagemRetorno(resultado.toString());
                return;
            }

            resultado.append("=== Sincroniza??o de Mapeamento de Produtos ===\n\n");
            resultado.append("Buscando produtos ativos com REFERENCIA...\n\n");

            JdbcWrapper jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();
            ResultSet rs = null;

            try {
                // Buscar todos os produtos ativos com REFERENCIA
                NativeSql sql = new NativeSql(jdbc);
                sql.appendSql("SELECT CODPROD, REFERENCIA FROM TGFPRO ");
                sql.appendSql("WHERE ATIVO = 'S' AND REFERENCIA IS NOT NULL AND REFERENCIA <> '' ");
                sql.appendSql("ORDER BY CODPROD");

                rs = sql.executeQuery();

                DeparaService deparaService = DeparaService.getInstance();
                int total = 0;
                int criados = 0;
                int atualizados = 0;
                int erros = 0;

                while (rs.next()) {
                    total++;
                    BigDecimal codProd = rs.getBigDecimal("CODPROD");
                    String referencia = rs.getString("REFERENCIA");

                    try {
                        // Verificar se j? existe
                        String existingSku = deparaService.getSku(codProd);

                        if (existingSku == null) {
                            // Criar novo mapeamento
                            deparaService.setMapping(DeparaService.TIPO_PRODUTO, codProd, referencia);
                            criados++;
                        } else if (!existingSku.equals(referencia)) {
                            // Atualizar mapeamento existente
                            deparaService.setMapping(DeparaService.TIPO_PRODUTO, codProd, referencia);
                            atualizados++;
                        }
                        // Se igual, n?o faz nada

                    } catch (Exception e) {
                        erros++;
                        log.log(Level.WARNING, "Erro ao mapear produto " + codProd, e);
                    }
                }

                resultado.append("Total de produtos analisados: ").append(total).append("\n");
                resultado.append("Novos mapeamentos criados: ").append(criados).append("\n");
                resultado.append("Mapeamentos atualizados: ").append(atualizados).append("\n");
                resultado.append("Erros: ").append(erros).append("\n\n");

                if (criados > 0 || atualizados > 0) {
                    resultado.append("[SUCESSO] Sincroniza??o conclu?da!\n");
                    LogService.getInstance().info(LogService.OP_GENERAL,
                            "Sincroniza??o De-Para produtos",
                            "Criados: " + criados + ", Atualizados: " + atualizados);
                } else {
                    resultado.append("[INFO] Nenhuma altera??o necess?ria.\n");
                }

            } finally {
                if (rs != null) {
                    try { rs.close(); } catch (Exception ignored) {}
                }
            }

        } catch (Exception e) {
            resultado.append("\n[ERRO] Falha na sincroniza??o: ").append(e.getMessage());
            log.log(Level.SEVERE, "Erro na sincroniza??o de produtos", e);
        }

        contexto.setMensagemRetorno(resultado.toString());
    }
}

