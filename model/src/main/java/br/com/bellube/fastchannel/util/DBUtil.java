package br.com.bellube.fastchannel.util;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Utilitario para acesso direto ao banco de dados via JNDI.
 * Usado quando o EntityFacade nao esta disponivel fora do contexto Sankhya.
 */
public class DBUtil {

    private static final Logger log = Logger.getLogger(DBUtil.class.getName());
    private static final String JNDI_NAME = "java:/MGEDS";
    private static DataSource dataSource;

    /**
     * Obtem uma conexao do datasource MGEDS.
     */
    public static Connection getConnection() throws Exception {
        if (dataSource == null) {
            Context ctx = new InitialContext();
            dataSource = (DataSource) ctx.lookup(JNDI_NAME);
            log.info("DataSource MGEDS obtido via JNDI");
        }
        return dataSource.getConnection();
    }

    /**
     * Fecha conexao de forma segura.
     */
    public static void closeConnection(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (Exception e) {
                log.log(Level.WARNING, "Erro ao fechar conexao", e);
            }
        }
    }

    /**
     * Fecha statement de forma segura.
     */
    public static void closeStatement(Statement stmt) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (Exception e) {
                log.log(Level.WARNING, "Erro ao fechar statement", e);
            }
        }
    }

    /**
     * Fecha ResultSet de forma segura.
     */
    public static void closeResultSet(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (Exception e) {
                log.log(Level.WARNING, "Erro ao fechar resultset", e);
            }
        }
    }

    /**
     * Fecha todos os recursos de forma segura.
     */
    public static void closeAll(ResultSet rs, Statement stmt, Connection conn) {
        closeResultSet(rs);
        closeStatement(stmt);
        closeConnection(conn);
    }
}
