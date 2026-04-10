package br.com.bellube.fastchannel.service;

import br.com.bellube.fastchannel.config.FastchannelConfig;

import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.mail.*;
import javax.mail.internet.*;

/**
 * Servico de notificacao por email para erros criticos.
 * Envia alertas quando ocorrem falhas na importacao de pedidos
 * ou no processamento da fila de sincronizacao.
 */
public class NotificationService {

    private static final Logger log = Logger.getLogger(NotificationService.class.getName());
    private static NotificationService instance;

    private NotificationService() {}

    public static synchronized NotificationService getInstance() {
        if (instance == null) {
            instance = new NotificationService();
        }
        return instance;
    }

    /**
     * Notifica erro na importacao de pedido.
     */
    public void notifyOrderImportError(String orderId, String errorMsg) {
        sendNotification(
            "Erro na importacao de pedido FastChannel #" + orderId,
            buildOrderErrorBody(orderId, errorMsg)
        );
    }

    /**
     * Notifica erro critico no processamento.
     */
    public void notifyCriticalError(String operation, String errorMsg) {
        sendNotification(
            "Erro critico FastChannel - " + operation,
            buildCriticalErrorBody(operation, errorMsg)
        );
    }

    /**
     * Notifica erro fatal na fila (max retries excedido).
     */
    public void notifyQueueFatalError(String entityType, String entityKey, String errorMsg) {
        sendNotification(
            "Erro fatal na fila FastChannel - " + entityType + " " + entityKey,
            buildQueueErrorBody(entityType, entityKey, errorMsg)
        );
    }

    private void sendNotification(String subject, String body) {
        try {
            FastchannelConfig config = FastchannelConfig.getInstance();
            if (!config.isEmailHabilitado()) {
                log.fine("Notificacao por email desabilitada.");
                return;
            }

            String recipients = config.getEmailNotificacao();
            if (recipients == null || recipients.trim().isEmpty()) {
                log.fine("Nenhum destinatario de email configurado.");
                return;
            }

            // Tenta usar a API de email do Sankhya primeiro
            try {
                sendViaSankhyaEmail(recipients, subject, body);
                log.info("Notificacao enviada para: " + recipients);
                return;
            } catch (Exception e) {
                log.log(Level.FINE, "Sankhya email API nao disponivel, tentando SMTP direto", e);
            }

            // Fallback: SMTP direto via javax.mail
            sendViaSmtp(recipients, subject, body);
            log.info("Notificacao SMTP enviada para: " + recipients);

        } catch (Exception e) {
            log.log(Level.WARNING, "Falha ao enviar notificacao por email", e);
            // Nao propaga - notificacao e best-effort
        }
    }

    private void sendViaSankhyaEmail(String recipients, String subject, String body) throws Exception {
        // Tenta usar br.com.sankhya.modelcore.util.MailUtils se disponivel
        Class<?> mailUtilsClass = Class.forName("br.com.sankhya.modelcore.util.MailUtils");
        java.lang.reflect.Method sendMethod = mailUtilsClass.getMethod(
            "sendMail", String.class, String.class, String.class, boolean.class);
        sendMethod.invoke(null, recipients, subject, body, true);
    }

    private void sendViaSmtp(String recipients, String subject, String body) throws Exception {
        FastchannelConfig config = FastchannelConfig.getInstance();
        String smtpHost = config.getSmtpHost();
        if (smtpHost == null || smtpHost.trim().isEmpty()) {
            smtpHost = "localhost";
        }

        Properties props = new Properties();
        props.put("mail.smtp.host", smtpHost);
        props.put("mail.smtp.port", "25");
        props.put("mail.smtp.auth", "false");

        Session session = Session.getInstance(props);
        MimeMessage message = new MimeMessage(session);
        message.setFrom(new InternetAddress("fastchannel@bellube.com.br"));

        String[] recipientList = recipients.split(",");
        for (String r : recipientList) {
            message.addRecipient(Message.RecipientType.TO, new InternetAddress(r.trim()));
        }

        message.setSubject(subject);
        message.setContent(body, "text/html; charset=UTF-8");
        Transport.send(message);
    }

    private String buildOrderErrorBody(String orderId, String errorMsg) {
        return "<html><body>" +
            "<h2>Erro na Importacao de Pedido FastChannel</h2>" +
            "<p><strong>Order ID:</strong> " + escapeHtml(orderId) + "</p>" +
            "<p><strong>Erro:</strong> " + escapeHtml(errorMsg) + "</p>" +
            "<p><strong>Acao:</strong> Verifique o log de integracao no addon FastChannel.</p>" +
            "<hr><p style='color:#999'>Addon FastChannel - Notificacao Automatica</p>" +
            "</body></html>";
    }

    private String buildCriticalErrorBody(String operation, String errorMsg) {
        return "<html><body>" +
            "<h2>Erro Critico FastChannel</h2>" +
            "<p><strong>Operacao:</strong> " + escapeHtml(operation) + "</p>" +
            "<p><strong>Erro:</strong> " + escapeHtml(errorMsg) + "</p>" +
            "<hr><p style='color:#999'>Addon FastChannel - Notificacao Automatica</p>" +
            "</body></html>";
    }

    private String buildQueueErrorBody(String entityType, String entityKey, String errorMsg) {
        return "<html><body>" +
            "<h2>Erro Fatal na Fila FastChannel</h2>" +
            "<p><strong>Tipo:</strong> " + escapeHtml(entityType) + "</p>" +
            "<p><strong>Chave:</strong> " + escapeHtml(entityKey) + "</p>" +
            "<p><strong>Erro:</strong> " + escapeHtml(errorMsg) + "</p>" +
            "<p><strong>Acao:</strong> Limite de tentativas excedido. Intervencao manual necessaria.</p>" +
            "<hr><p style='color:#999'>Addon FastChannel - Notificacao Automatica</p>" +
            "</body></html>";
    }

    private String escapeHtml(String s) {
        if (s == null) return "";
        return s.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;")
                .replace("'", "&#39;");
    }
}
