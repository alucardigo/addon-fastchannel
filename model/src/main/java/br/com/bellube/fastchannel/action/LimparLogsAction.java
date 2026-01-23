package br.com.bellube.fastchannel.action;

import br.com.sankhya.extensions.actionbutton.AcaoRotinaJava;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A??o para limpar logs antigos da integra??o.
 * Remove logs com mais de 30 dias.
 */
public class LimparLogsAction implements AcaoRotinaJava {

    private static final Logger log = Logger.getLogger(LimparLogsAction.class.getName());
    private static final int DIAS_PARA_MANTER = 30;

    @Override
    public void doAction(ContextoAcao contexto) throws Exception {
        StringBuilder resultado = new StringBuilder();

        try {
            resultado.append("=== Limpeza de Logs Antigos ===\n\n");
            resultado.append("Removendo logs com mais de ").append(DIAS_PARA_MANTER).append(" dias...\n\n");

            JdbcWrapper jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();

            // Contar logs antes
            NativeSql countSql = new NativeSql(jdbc);
            countSql.appendSql("SELECT COUNT(*) AS TOTAL FROM AD_FCLOG ");
            countSql.appendSql("WHERE DH_REGISTRO < DATEADD(DAY, -:dias, CURRENT_TIMESTAMP)");
            countSql.setNamedParameter("dias", DIAS_PARA_MANTER);

            java.sql.ResultSet rs = countSql.executeQuery();
            int count = 0;
            if (rs.next()) {
                count = rs.getInt("TOTAL");
            }
            rs.close();

            if (count == 0) {
                resultado.append("[INFO] N?o h? logs antigos para limpar.\n");
                contexto.setMensagemRetorno(resultado.toString());
                return;
            }

            resultado.append("Logs antigos encontrados: ").append(count).append("\n\n");

            // Remover logs
            NativeSql deleteSql = new NativeSql(jdbc);
            deleteSql.appendSql("DELETE FROM AD_FCLOG ");
            deleteSql.appendSql("WHERE DH_REGISTRO < DATEADD(DAY, -:dias, CURRENT_TIMESTAMP)");
            deleteSql.setNamedParameter("dias", DIAS_PARA_MANTER);

            deleteSql.executeUpdate();

            resultado.append("[SUCESSO] ").append(count).append(" logs removidos.\n");

            log.info("Limpeza de logs: " + count + " registros removidos");

        } catch (Exception e) {
            resultado.append("\n[ERRO] Falha na limpeza: ").append(e.getMessage());
            log.log(Level.SEVERE, "Erro na limpeza de logs", e);
        }

        contexto.setMensagemRetorno(resultado.toString());
    }
}

