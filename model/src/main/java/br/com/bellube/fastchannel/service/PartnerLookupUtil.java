package br.com.bellube.fastchannel.service;

import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.sql.ResultSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

final class PartnerLookupUtil {

    private static final Logger log = Logger.getLogger(PartnerLookupUtil.class.getName());

    private PartnerLookupUtil() {
    }

    static BigDecimal findCodParcByDocument(String document) {
        String normalized = sanitizeDigits(document);
        if (normalized == null) {
            return null;
        }

        // Fluxo nativo primeiro.
        try {
            JapeWrapper parceiroDAO = JapeFactory.dao("Parceiro");
            for (String candidate : buildDocumentCandidates(normalized)) {
                Collection<DynamicVO> parceiros = parceiroDAO.find("this.CGC_CPF = ?", candidate);
                if (parceiros == null) {
                    continue;
                }
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
            log.log(Level.FINE, "Falha na busca nativa de CODPARC por documento. Aplicando fallback SQL.", e);
        }

        // Fallback SQL para documentos armazenados com máscara.
        JdbcWrapper jdbc = null;
        ResultSet rs = null;
        try {
            jdbc = openJdbc();
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("SELECT CODPARC FROM TGFPAR ");
            sql.appendSql("WHERE REPLACE(REPLACE(REPLACE(CGC_CPF, '.', ''), '-', ''), '/', '') = :doc");
            sql.setNamedParameter("doc", normalized);
            rs = sql.executeQuery();
            if (rs.next()) {
                return rs.getBigDecimal("CODPARC");
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Erro ao buscar CODPARC por documento", e);
        } finally {
            closeQuietly(rs);
            closeJdbc(jdbc);
        }
        return null;
    }

    static BigDecimal findCodVendByParc(BigDecimal codParc) {
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
        return null;
    }

    private static String sanitizeDigits(String value) {
        if (value == null) {
            return null;
        }
        String digits = value.replaceAll("\\D", "");
        return digits.isEmpty() ? null : digits;
    }

    private static List<String> buildDocumentCandidates(String normalizedCpfCnpj) {
        Set<String> ordered = new LinkedHashSet<>();
        ordered.add(normalizedCpfCnpj);
        if (normalizedCpfCnpj.length() == 11) {
            ordered.add(formatCpf(normalizedCpfCnpj));
        } else if (normalizedCpfCnpj.length() == 14) {
            ordered.add(formatCnpj(normalizedCpfCnpj));
        }
        return new ArrayList<>(ordered);
    }

    private static String formatCpf(String cpfDigits) {
        if (cpfDigits == null || cpfDigits.length() != 11) {
            return cpfDigits;
        }
        return cpfDigits.substring(0, 3) + "."
                + cpfDigits.substring(3, 6) + "."
                + cpfDigits.substring(6, 9) + "-"
                + cpfDigits.substring(9);
    }

    private static String formatCnpj(String cnpjDigits) {
        if (cnpjDigits == null || cnpjDigits.length() != 14) {
            return cnpjDigits;
        }
        return cnpjDigits.substring(0, 2) + "."
                + cnpjDigits.substring(2, 5) + "."
                + cnpjDigits.substring(5, 8) + "/"
                + cnpjDigits.substring(8, 12) + "-"
                + cnpjDigits.substring(12);
    }

    private static boolean isNullOrZero(BigDecimal value) {
        return value == null || value.compareTo(BigDecimal.ZERO) <= 0;
    }

    private static JdbcWrapper openJdbc() throws Exception {
        JdbcWrapper jdbc = EntityFacadeFactory.getCoreFacade().getJdbcWrapper();
        jdbc.openSession();
        return jdbc;
    }

    private static void closeJdbc(JdbcWrapper jdbc) {
        if (jdbc != null) {
            try {
                jdbc.closeSession();
            } catch (Exception e) {
                log.log(Level.FINE, "Erro ao fechar session do JdbcWrapper", e);
            }
        }
    }

    private static void closeQuietly(ResultSet rs) {
        if (rs != null) {
            try { rs.close(); } catch (Exception ignored) {}
        }
    }
}
