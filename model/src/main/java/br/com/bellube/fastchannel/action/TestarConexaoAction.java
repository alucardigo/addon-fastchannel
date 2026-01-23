package br.com.bellube.fastchannel.action;

import br.com.bellube.fastchannel.auth.FastchannelTokenManager;
import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.http.FastchannelHttpClient;
import br.com.sankhya.extensions.actionbutton.AcaoRotinaJava;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Ação para testar conexão com a API Fastchannel.
 * Verifica autenticação OAuth2 e conectividade.
 */
public class TestarConexaoAction implements AcaoRotinaJava {

    private static final Logger log = Logger.getLogger(TestarConexaoAction.class.getName());

    @Override
    public void doAction(ContextoAcao contexto) throws Exception {
        StringBuilder resultado = new StringBuilder();
        boolean sucesso = true;

        try {
            resultado.append("=== Teste de Conexão Fastchannel ===\n\n");

            // 1. Verificar configuração
            resultado.append("1. Verificando configuração...\n");
            FastchannelConfig config = FastchannelConfig.getInstance();

            if (!config.isAtivo()) {
                resultado.append("   [ERRO] Integração não está ativa!\n");
                sucesso = false;
            } else {
                resultado.append("   [OK] Integração ativa\n");
            }

            if (config.getClientId() == null || config.getClientId().isEmpty()) {
                resultado.append("   [ERRO] Client ID não configurado!\n");
                sucesso = false;
            } else {
                resultado.append("   [OK] Client ID configurado\n");
            }

            if (config.getClientSecret() == null || config.getClientSecret().isEmpty()) {
                resultado.append("   [ERRO] Client Secret não configurado!\n");
                sucesso = false;
            } else {
                resultado.append("   [OK] Client Secret configurado\n");
            }

            if (config.getBaseUrl() == null || config.getBaseUrl().isEmpty()) {
                resultado.append("   [ERRO] URL Base não configurada!\n");
                sucesso = false;
            } else {
                resultado.append("   [OK] URL Base: ").append(config.getBaseUrl()).append("\n");
            }

            if (config.getAuthUrl() == null || config.getAuthUrl().isEmpty()) {
                resultado.append("   [ERRO] URL Auth não configurada!\n");
                sucesso = false;
            } else {
                resultado.append("   [OK] URL Auth: ").append(config.getAuthUrl()).append("\n");
            }

            resultado.append("\n");

            // 2. Testar autenticação OAuth2
            if (sucesso) {
                resultado.append("2. Testando autenticação OAuth2...\n");
                try {
                    FastchannelTokenManager tokenManager = FastchannelTokenManager.getInstance();
                    String token = tokenManager.getValidToken();

                    if (token != null && !token.isEmpty()) {
                        resultado.append("   [OK] Token obtido com sucesso!\n");
                        resultado.append("   Token (primeiros 20 chars): ").append(token.substring(0, Math.min(20, token.length()))).append("...\n");
                    } else {
                        resultado.append("   [ERRO] Token vazio retornado!\n");
                        sucesso = false;
                    }
                } catch (Exception e) {
                    resultado.append("   [ERRO] Falha na autenticação: ").append(e.getMessage()).append("\n");
                    sucesso = false;
                    log.log(Level.WARNING, "Erro no teste de autenticação", e);
                }
                resultado.append("\n");
            }

            // 3. Testar conectividade com API
            if (sucesso) {
                resultado.append("3. Testando conectividade com API...\n");
                try {
                    FastchannelHttpClient httpClient = new FastchannelHttpClient();
                    // Fazer uma chamada simples para verificar conectividade
                    // GET em /orders com limit=1 apenas para testar
                    String testUrl = config.getBaseUrl() + "/orders?page=1&pageSize=1";
                    resultado.append("   Testando: ").append(testUrl).append("\n");

                    // O cliente HTTP já trata erros e retries
                    FastchannelHttpClient.HttpResult result = httpClient.getOrders("/orders?page=1&pageSize=1");
                    if (result.isSuccess()) {
                        resultado.append("   [OK] API respondeu com sucesso!\n");
                        String body = result.getBody();
                        resultado.append("   Resposta (primeiros 100 chars): ").append(
                                body.substring(0, Math.min(100, body.length()))).append("...\n");
                    } else {
                        resultado.append("   [ERRO] API retornou erro HTTP ").append(result.getStatusCode()).append("\n");
                        sucesso = false;
                    }

                } catch (Exception e) {
                    resultado.append("   [ERRO] Falha na conectividade: ").append(e.getMessage()).append("\n");
                    sucesso = false;
                    log.log(Level.WARNING, "Erro no teste de conectividade", e);
                }
                resultado.append("\n");
            }

            // Resultado final
            resultado.append("=== Resultado Final ===\n");
            if (sucesso) {
                resultado.append("[SUCESSO] Conexão com Fastchannel OK!\n");
                resultado.append("A integração está configurada corretamente e funcionando.");
            } else {
                resultado.append("[FALHA] Problemas encontrados na conexão.\n");
                resultado.append("Verifique as configurações acima e tente novamente.");
            }

        } catch (Exception e) {
            resultado.append("\n[ERRO FATAL] ").append(e.getMessage());
            log.log(Level.SEVERE, "Erro fatal no teste de conexão", e);
        }

        // Retornar resultado para o usuário
        contexto.setMensagemRetorno(resultado.toString());
    }
}
