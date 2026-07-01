package br.com.bellube.fastchannel.util;

import java.util.regex.Pattern;

/**
 * Utilitario de sanitizacao de logs para mascarar dados sensiveis.
 *
 * Mascara:
 *   - JSESSIONID / mgeSession (token sessao WildFly/Sankhya)
 *   - Authorization / Bearer tokens
 *   - password / senha / apikey em form/query/json
 *   - credenciais em XML mge-login (NOMEUSU/INTERNO)
 *
 * Uso:
 *   log.info("[fc-debug] " + LogSanitizer.sanitize(qs));
 *   log.fine("[HTTP] XML: " + LogSanitizer.sanitizeXml(requestXml));
 *
 * Ver: MELHORIAS_POS_PUBLISH_2026_04_17.md P1.1
 */
public final class LogSanitizer {

    private LogSanitizer() {
    }

    private static final String MASK = "***";

    // Query-string: mgeSession=ABC -> mgeSession=***
    private static final Pattern QS_SECRETS = Pattern.compile(
            "(?i)(mgeSession|JSESSIONID|Authorization|Bearer|password|senha|apikey|token|secret|access_token|refresh_token)=([^&\\s]+)");

    // Header style: Authorization: Bearer xxx
    private static final Pattern HEADER_BEARER = Pattern.compile("(?i)(Bearer\\s+)[A-Za-z0-9_\\-\\.=]+");

    // XML mge-login credenciais
    private static final Pattern XML_CRED_NOMUSU = Pattern.compile(
            "(?is)(<NOMUSU>)([^<]*)(</NOMUSU>)");
    private static final Pattern XML_CRED_INTERNO = Pattern.compile(
            "(?is)(<INTERNO>)([^<]*)(</INTERNO>)");
    private static final Pattern XML_CRED_SENHA = Pattern.compile(
            "(?is)(<SENHA>)([^<]*)(</SENHA>)");

    // JSON "password": "xxx" / "token": "xxx"
    private static final Pattern JSON_SECRET = Pattern.compile(
            "(?i)(\"(?:password|senha|token|apikey|secret|access_token|refresh_token|mgeSession|authorization|bearer)\"\\s*:\\s*\")([^\"]+)(\")");

    /**
     * Sanitiza string generica (query string, URL, json-like).
     */
    public static String sanitize(String input) {
        if (input == null || input.isEmpty()) {
            return input;
        }
        String out = input;
        out = QS_SECRETS.matcher(out).replaceAll("$1=" + MASK);
        out = HEADER_BEARER.matcher(out).replaceAll("$1" + MASK);
        out = JSON_SECRET.matcher(out).replaceAll("$1" + MASK + "$3");
        return out;
    }

    /**
     * Sanitiza XML (remove credenciais mge-login/senha).
     */
    public static String sanitizeXml(String xml) {
        if (xml == null || xml.isEmpty()) {
            return xml;
        }
        String out = xml;
        out = XML_CRED_NOMUSU.matcher(out).replaceAll("$1" + MASK + "$3");
        out = XML_CRED_INTERNO.matcher(out).replaceAll("$1" + MASK + "$3");
        out = XML_CRED_SENHA.matcher(out).replaceAll("$1" + MASK + "$3");
        // Tambem aplica sanitizer generico (JSESSIONID em URL, Bearer em header inline)
        out = sanitize(out);
        return out;
    }

    /**
     * Mascara valor exibindo apenas primeiros/ultimos 4 chars, ex: "abcd...wxyz".
     * Util para session IDs onde presenca importa mas valor nao deve vazar.
     */
    public static String maskToken(String token) {
        if (token == null || token.isEmpty()) {
            return "<null>";
        }
        if (token.length() <= 8) {
            return MASK;
        }
        return token.substring(0, 4) + "..." + token.substring(token.length() - 4);
    }
}
