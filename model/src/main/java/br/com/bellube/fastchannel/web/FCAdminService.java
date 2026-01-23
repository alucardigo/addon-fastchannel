package br.com.bellube.fastchannel.web;

import br.com.bellube.fastchannel.auth.FastchannelTokenManager;
import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.http.FastchannelHttpClient;
import br.com.bellube.fastchannel.service.OrderService;
import br.com.bellube.fastchannel.service.QueueService;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Service para operacoes administrativas.
 */
public class FCAdminService {

    private static final Logger log = Logger.getLogger(FCAdminService.class.getName());

    public Map<String, Object> testarConexao(Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();
        StringBuilder mensagem = new StringBuilder();
        boolean sucesso = true;

        try {
            mensagem.append("=== Teste de Conexao Fastchannel ===\n\n");

            // 1. Verificar configuracao
            mensagem.append("1. Verificando configuracao...\n");
            FastchannelConfig config = FastchannelConfig.getInstance();

            if (!config.isAtivo()) {
                mensagem.append("   [ERRO] Integracao nao esta ativa!\n");
                sucesso = false;
            } else {
                mensagem.append("   [OK] Integracao ativa\n");
            }

            if (config.getClientId() == null || config.getClientId().isEmpty()) {
                mensagem.append("   [ERRO] Client ID nao configurado!\n");
                sucesso = false;
            } else {
                mensagem.append("   [OK] Client ID configurado\n");
            }

            if (config.getClientSecret() == null || config.getClientSecret().isEmpty()) {
                mensagem.append("   [ERRO] Client Secret nao configurado!\n");
                sucesso = false;
            } else {
                mensagem.append("   [OK] Client Secret configurado\n");
            }

            if (config.getBaseUrl() == null || config.getBaseUrl().isEmpty()) {
                mensagem.append("   [ERRO] URL Base nao configurada!\n");
                sucesso = false;
            } else {
                mensagem.append("   [OK] URL Base: ").append(config.getBaseUrl()).append("\n");
            }

            // 2. Testar autenticacao OAuth2
            if (sucesso) {
                mensagem.append("\n2. Testando autenticacao OAuth2...\n");
                try {
                    FastchannelTokenManager tokenManager = FastchannelTokenManager.getInstance();
                    String token = tokenManager.getValidToken();

                    if (token != null && !token.isEmpty()) {
                        mensagem.append("   [OK] Token obtido com sucesso!\n");
                        mensagem.append("   Token (primeiros 20 chars): ")
                                .append(token.substring(0, Math.min(20, token.length()))).append("...\n");
                    } else {
                        mensagem.append("   [ERRO] Token vazio retornado!\n");
                        sucesso = false;
                    }
                } catch (Exception e) {
                    mensagem.append("   [ERRO] Falha na autenticacao: ").append(e.getMessage()).append("\n");
                    sucesso = false;
                }
            }

            // 3. Testar conectividade com API
            if (sucesso) {
                mensagem.append("\n3. Testando conectividade com API...\n");
                try {
                    FastchannelHttpClient httpClient = new FastchannelHttpClient();
                    FastchannelHttpClient.HttpResult httpResult = httpClient.getOrders("/orders?page=1&pageSize=1");

                    if (httpResult.isSuccess()) {
                        mensagem.append("   [OK] API respondeu com sucesso!\n");
                    } else {
                        mensagem.append("   [ERRO] API retornou erro HTTP ").append(httpResult.getStatusCode()).append("\n");
                        sucesso = false;
                    }
                } catch (Exception e) {
                    mensagem.append("   [ERRO] Falha na conectividade: ").append(e.getMessage()).append("\n");
                    sucesso = false;
                }
            }

            // Resultado final
            mensagem.append("\n=== Resultado Final ===\n");
            if (sucesso) {
                mensagem.append("[SUCESSO] Conexao com Fastchannel OK!");
                result.put("success", true);
            } else {
                mensagem.append("[FALHA] Problemas encontrados na conexao.");
                result.put("success", false);
            }

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro no teste de conexao", e);
            mensagem.append("\n[ERRO FATAL] ").append(e.getMessage());
            result.put("success", false);
        }

        result.put("message", mensagem.toString());
        return result;
    }

    public Map<String, Object> importarPedidos(Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();

        try {
            FastchannelConfig config = FastchannelConfig.getInstance();
            if (!config.isAtivo()) {
                result.put("success", false);
                result.put("message", "Integracao nao esta ativa!");
                return result;
            }

            OrderService orderService = new OrderService();
            int imported = orderService.importPendingOrders();

            result.put("success", true);
            result.put("message", "Importacao concluida! " + imported + " pedido(s) importado(s).");
            result.put("count", imported);

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao importar pedidos", e);
            result.put("success", false);
            result.put("message", "Erro: " + e.getMessage());
        }

        return result;
    }

    public Map<String, Object> processarFila(Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();

        try {
            FastchannelConfig config = FastchannelConfig.getInstance();
            if (!config.isAtivo()) {
                result.put("success", false);
                result.put("message", "Integracao nao esta ativa!");
                return result;
            }

            QueueService queueService = QueueService.getInstance();
            // Processar itens pendentes manualmente chamando getPendingItems
            int processed = queueService.countPending();

            result.put("success", true);
            result.put("message", "Fila possui " + processed + " item(ns) pendente(s). Processamento agendado.");
            result.put("count", processed);

        } catch (Exception e) {
            log.log(Level.SEVERE, "Erro ao processar fila", e);
            result.put("success", false);
            result.put("message", "Erro: " + e.getMessage());
        }

        return result;
    }
}
