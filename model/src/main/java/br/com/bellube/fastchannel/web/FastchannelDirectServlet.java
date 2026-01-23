package br.com.bellube.fastchannel.web;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Servlet direto para chamadas do frontend HTML5.
 * Roteia requisicoes para os Services correspondentes.
 */
public class FastchannelDirectServlet extends HttpServlet {

    private static final Logger log = Logger.getLogger(FastchannelDirectServlet.class.getName());
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

    // Registro de services
    private static final Map<String, ServiceInfo> services = new HashMap<>();

    static {
        // Dashboard
        services.put("FCDashboardSP.snapshot", new ServiceInfo(FCDashboardService.class, "snapshot"));

        // Config
        services.put("FCConfigSP.get", new ServiceInfo(FCConfigService.class, "get"));
        services.put("FCConfigSP.save", new ServiceInfo(FCConfigService.class, "save"));

        // Admin
        services.put("FCAdminSP.testarConexao", new ServiceInfo(FCAdminService.class, "testarConexao"));
        services.put("FCAdminSP.importarPedidos", new ServiceInfo(FCAdminService.class, "importarPedidos"));
        services.put("FCAdminSP.processarFila", new ServiceInfo(FCAdminService.class, "processarFila"));

        // Pedidos
        services.put("FCPedidosSP.list", new ServiceInfo(FCPedidosService.class, "list"));
        services.put("FCPedidosSP.get", new ServiceInfo(FCPedidosService.class, "get"));
        services.put("FCPedidosSP.reprocessar", new ServiceInfo(FCPedidosService.class, "reprocessar"));
        services.put("FCPedidosSP.consultarFC", new ServiceInfo(FCPedidosService.class, "consultarFC"));

        // Fila
        services.put("FCFilaSP.stats", new ServiceInfo(FCFilaService.class, "stats"));
        services.put("FCFilaSP.list", new ServiceInfo(FCFilaService.class, "list"));
        services.put("FCFilaSP.reprocessar", new ServiceInfo(FCFilaService.class, "reprocessar"));
        services.put("FCFilaSP.limparErros", new ServiceInfo(FCFilaService.class, "limparErros"));

        // Logs
        services.put("FCLogsSP.list", new ServiceInfo(FCLogsService.class, "list"));
        services.put("FCLogsSP.limpar", new ServiceInfo(FCLogsService.class, "limpar"));
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        resp.setContentType("application/json");
        resp.setCharacterEncoding("UTF-8");

        String serviceName = req.getParameter("serviceName");
        boolean hasMgeSession = req.getParameter("mgeSession") != null;
        String requestPath = req.getRequestURI();
        log.info("fc-direct request method=" + req.getMethod()
                + " path=" + requestPath
                + " service=" + (serviceName == null ? "<null>" : serviceName)
                + " mgeSession=" + (hasMgeSession ? "present" : "missing"));

        // CORS headers
        resp.setHeader("Access-Control-Allow-Origin", "*");
        resp.setHeader("Access-Control-Allow-Methods", "POST, GET, OPTIONS");
        resp.setHeader("Access-Control-Allow-Headers", "Content-Type");

        PrintWriter out = resp.getWriter();

        try {
            if (serviceName == null || serviceName.isEmpty()) {
                sendError(resp, out, 400, "serviceName obrigatorio");
                return;
            }

            ServiceInfo serviceInfo = services.get(serviceName);
            if (serviceInfo == null) {
                sendError(resp, out, 404, "Service nao encontrado: " + serviceName);
                return;
            }

            // Ler payload
            Map<String, Object> payload = readPayload(req);

            // Invocar service
            Object service = serviceInfo.serviceClass.getDeclaredConstructor().newInstance();
            Method method = serviceInfo.serviceClass.getMethod(serviceInfo.methodName, Map.class);
            Object result = method.invoke(service, payload);

            // Retornar resultado
            out.print(toJson(result));

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro no service", e);
            sendError(resp, out, 500, e.getMessage());
        }
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        doPost(req, resp);
    }

    @Override
    protected void doOptions(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        resp.setHeader("Access-Control-Allow-Origin", "*");
        resp.setHeader("Access-Control-Allow-Methods", "POST, GET, OPTIONS");
        resp.setHeader("Access-Control-Allow-Headers", "Content-Type");
        resp.setStatus(HttpServletResponse.SC_OK);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> readPayload(HttpServletRequest req) throws IOException {
        StringBuilder sb = new StringBuilder();
        BufferedReader reader = req.getReader();
        String line;
        while ((line = reader.readLine()) != null) {
            sb.append(line);
        }

        String body = sb.toString().trim();
        if (body.isEmpty() || body.equals("{}")) {
            return new HashMap<>();
        }

        return parseJson(body);
    }

    private void sendError(HttpServletResponse resp, PrintWriter out, int code, String message) {
        resp.setStatus(code);
        out.print("{\"error\":true,\"message\":\"" + escapeJson(message) + "\"}");
    }

    // Simple JSON serializer
    @SuppressWarnings("unchecked")
    private String toJson(Object obj) {
        if (obj == null) {
            return "null";
        }
        if (obj instanceof String) {
            return "\"" + escapeJson((String) obj) + "\"";
        }
        if (obj instanceof Number) {
            return obj.toString();
        }
        if (obj instanceof Boolean) {
            return obj.toString();
        }
        if (obj instanceof Date) {
            return "\"" + dateFormat.format((Date) obj) + "\"";
        }
        if (obj instanceof Timestamp) {
            return "\"" + dateFormat.format((Timestamp) obj) + "\"";
        }
        if (obj instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) obj;
            StringBuilder sb = new StringBuilder("{");
            boolean first = true;
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                if (!first) sb.append(",");
                sb.append("\"").append(escapeJson(entry.getKey())).append("\":");
                sb.append(toJson(entry.getValue()));
                first = false;
            }
            sb.append("}");
            return sb.toString();
        }
        if (obj instanceof List) {
            List<?> list = (List<?>) obj;
            StringBuilder sb = new StringBuilder("[");
            boolean first = true;
            for (Object item : list) {
                if (!first) sb.append(",");
                sb.append(toJson(item));
                first = false;
            }
            sb.append("]");
            return sb.toString();
        }
        return "\"" + escapeJson(obj.toString()) + "\"";
    }

    private String escapeJson(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }

    // Simple JSON parser
    @SuppressWarnings("unchecked")
    private Map<String, Object> parseJson(String json) {
        Map<String, Object> result = new HashMap<>();
        if (json == null || json.isEmpty()) return result;

        json = json.trim();
        if (!json.startsWith("{") || !json.endsWith("}")) return result;

        json = json.substring(1, json.length() - 1).trim();
        if (json.isEmpty()) return result;

        int i = 0;
        while (i < json.length()) {
            // Skip whitespace
            while (i < json.length() && Character.isWhitespace(json.charAt(i))) i++;
            if (i >= json.length()) break;

            // Parse key
            if (json.charAt(i) != '"') break;
            i++;
            int keyStart = i;
            while (i < json.length() && json.charAt(i) != '"') i++;
            String key = json.substring(keyStart, i);
            i++; // skip closing quote

            // Skip to colon
            while (i < json.length() && json.charAt(i) != ':') i++;
            i++; // skip colon

            // Skip whitespace
            while (i < json.length() && Character.isWhitespace(json.charAt(i))) i++;

            // Parse value
            Object value = null;
            if (i < json.length()) {
                char c = json.charAt(i);
                if (c == '"') {
                    // String
                    i++;
                    StringBuilder sb = new StringBuilder();
                    while (i < json.length() && json.charAt(i) != '"') {
                        if (json.charAt(i) == '\\' && i + 1 < json.length()) {
                            i++;
                            sb.append(json.charAt(i));
                        } else {
                            sb.append(json.charAt(i));
                        }
                        i++;
                    }
                    value = sb.toString();
                    i++; // skip closing quote
                } else if (c == '[') {
                    // Array
                    int depth = 1;
                    int arrStart = i;
                    i++;
                    while (i < json.length() && depth > 0) {
                        if (json.charAt(i) == '[') depth++;
                        else if (json.charAt(i) == ']') depth--;
                        i++;
                    }
                    String arrJson = json.substring(arrStart, i);
                    value = parseJsonArray(arrJson);
                } else if (c == '{') {
                    // Nested object
                    int depth = 1;
                    int objStart = i;
                    i++;
                    while (i < json.length() && depth > 0) {
                        if (json.charAt(i) == '{') depth++;
                        else if (json.charAt(i) == '}') depth--;
                        i++;
                    }
                    String objJson = json.substring(objStart, i);
                    value = parseJson(objJson);
                } else if (c == 't' || c == 'f') {
                    // Boolean
                    int start = i;
                    while (i < json.length() && Character.isLetter(json.charAt(i))) i++;
                    value = Boolean.parseBoolean(json.substring(start, i));
                } else if (c == 'n') {
                    // Null
                    i += 4;
                    value = null;
                } else if (c == '-' || Character.isDigit(c)) {
                    // Number
                    int start = i;
                    while (i < json.length() && (Character.isDigit(json.charAt(i)) || json.charAt(i) == '.' || json.charAt(i) == '-' || json.charAt(i) == 'e' || json.charAt(i) == 'E')) i++;
                    String numStr = json.substring(start, i);
                    if (numStr.contains(".")) {
                        value = Double.parseDouble(numStr);
                    } else {
                        value = Long.parseLong(numStr);
                    }
                }
            }

            result.put(key, value);

            // Skip to comma or end
            while (i < json.length() && json.charAt(i) != ',' && json.charAt(i) != '}') i++;
            if (i < json.length() && json.charAt(i) == ',') i++;
        }

        return result;
    }

    private List<Object> parseJsonArray(String json) {
        List<Object> result = new ArrayList<>();
        if (json == null || json.isEmpty()) return result;

        json = json.trim();
        if (!json.startsWith("[") || !json.endsWith("]")) return result;

        json = json.substring(1, json.length() - 1).trim();
        if (json.isEmpty()) return result;

        int i = 0;
        while (i < json.length()) {
            // Skip whitespace
            while (i < json.length() && Character.isWhitespace(json.charAt(i))) i++;
            if (i >= json.length()) break;

            char c = json.charAt(i);
            Object value = null;

            if (c == '"') {
                // String
                i++;
                StringBuilder sb = new StringBuilder();
                while (i < json.length() && json.charAt(i) != '"') {
                    if (json.charAt(i) == '\\' && i + 1 < json.length()) {
                        i++;
                        sb.append(json.charAt(i));
                    } else {
                        sb.append(json.charAt(i));
                    }
                    i++;
                }
                value = sb.toString();
                i++; // skip closing quote
            } else if (c == '-' || Character.isDigit(c)) {
                // Number
                int start = i;
                while (i < json.length() && (Character.isDigit(json.charAt(i)) || json.charAt(i) == '.' || json.charAt(i) == '-')) i++;
                String numStr = json.substring(start, i);
                if (numStr.contains(".")) {
                    value = Double.parseDouble(numStr);
                } else {
                    value = Long.parseLong(numStr);
                }
            } else if (c == '{') {
                // Object
                int depth = 1;
                int objStart = i;
                i++;
                while (i < json.length() && depth > 0) {
                    if (json.charAt(i) == '{') depth++;
                    else if (json.charAt(i) == '}') depth--;
                    i++;
                }
                value = parseJson(json.substring(objStart, i));
            }

            if (value != null) {
                result.add(value);
            }

            // Skip to comma or end
            while (i < json.length() && json.charAt(i) != ',' && json.charAt(i) != ']') i++;
            if (i < json.length() && json.charAt(i) == ',') i++;
        }

        return result;
    }

    private static class ServiceInfo {
        Class<?> serviceClass;
        String methodName;

        ServiceInfo(Class<?> serviceClass, String methodName) {
            this.serviceClass = serviceClass;
            this.methodName = methodName;
        }
    }
}
