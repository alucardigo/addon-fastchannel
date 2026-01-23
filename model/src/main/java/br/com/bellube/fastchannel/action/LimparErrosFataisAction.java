package br.com.bellube.fastchannel.action;

import br.com.bellube.fastchannel.config.FastchannelConstants;
import br.com.bellube.fastchannel.service.LogService;
import br.com.sankhya.extensions.actionbutton.AcaoRotinaJava;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A??o para limpar itens com erro fatal da fila.
 * Remove itens que n?o ser?o mais reprocessados.
 */
public class LimparErrosFataisAction implements AcaoRotinaJava {

    private static final Logger log = Logger.getLogger(LimparErrosFataisAction.class.getName());

    @Override
    public void doAction(ContextoAcao contexto) throws Exception {
        StringBuilder resultado = new StringBuilder();

        try {
            resultado.append("=== Limpeza de Itens com Erro Fatal ===\n\n");

            JdbcWrapper jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();

            // Contar itens antes
            NativeSql countSql = new NativeSql(jdbc);
            countSql.appendSql("SELECT COUNT(*) AS TOTAL FROM AD_FCQUEUE WHERE STATUS = :status");
            countSql.setNamedParameter("status", FastchannelConstants.QUEUE_STATUS_ERRO_FATAL);

            java.sql.ResultSet rs = countSql.executeQuery();
            int count = 0;
            if (rs.next()) {
                count = rs.getInt("TOTAL");
            }
            rs.close();

            if (count == 0) {
                resultado.append("[INFO] N?o h? itens com erro fatal para limpar.\n");
                contexto.setMensagemRetorno(resultado.toString());
                return;
            }

            resultado.append("Itens com erro fatal encontrados: ").append(count).append("\n\n");

            // Remover itens
            NativeSql deleteSql = new NativeSql(jdbc);
            deleteSql.appendSql("DELETE FROM AD_FCQUEUE WHERE STATUS = :status");
            deleteSql.setNamedParameter("status", FastchannelConstants.QUEUE_STATUS_ERRO_FATAL);

            deleteSql.executeUpdate();

            resultado.append("[SUCESSO] ").append(count).append(" itens removidos da fila.\n");

            LogService.getInstance().info(LogService.OP_QUEUE_PROCESS,
                    "Limpeza de erros fatais", count + " itens removidos");

        } catch (Exception e) {
            resultado.append("\n[ERRO] Falha na limpeza: ").append(e.getMessage());
            log.log(Level.SEVERE, "Erro na limpeza de erros fatais", e);
        }

        contexto.setMensagemRetorno(resultado.toString());
    }
}

