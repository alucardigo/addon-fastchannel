package br.com.bellube.fastchannel.util;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public final class DbColumnSupport {

    private DbColumnSupport() {
    }

    public static boolean hasColumn(ResultSet rs, String column) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int count = meta.getColumnCount();
        for (int i = 1; i <= count; i++) {
            String label = meta.getColumnLabel(i);
            if (column.equalsIgnoreCase(label)) return true;
            String name = meta.getColumnName(i);
            if (column.equalsIgnoreCase(name)) return true;
        }
        return false;
    }

    public static boolean hasColumn(Connection conn, String table, String column) throws SQLException {
        if (conn == null) return false;
        DatabaseMetaData meta = conn.getMetaData();
        if (hasColumn(meta, table, column)) return true;
        String upperTable = table != null ? table.toUpperCase() : null;
        if (upperTable != null && !upperTable.equals(table) && hasColumn(meta, upperTable, column)) return true;
        String lowerTable = table != null ? table.toLowerCase() : null;
        if (lowerTable != null && !lowerTable.equals(table)) return hasColumn(meta, lowerTable, column);
        return false;
    }

    private static boolean hasColumn(DatabaseMetaData meta, String table, String column) throws SQLException {
        try (ResultSet rs = meta.getColumns(null, null, table, null)) {
            while (rs.next()) {
                String name = rs.getString("COLUMN_NAME");
                if (column.equalsIgnoreCase(name)) return true;
            }
        }
        return false;
    }
}
