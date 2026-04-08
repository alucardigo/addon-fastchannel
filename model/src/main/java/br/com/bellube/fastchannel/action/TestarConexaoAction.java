package br.com.bellube.fastchannel.action;

import br.com.bellube.fastchannel.auth.FastchannelTokenManager;
import br.com.bellube.fastchannel.config.FastchannelConfig;
import br.com.bellube.fastchannel.http.FastchannelHttpClient;
import br.com.sankhya.extensions.actionbutton.AcaoRotinaJava;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Acao para testar conexao com a API Fastchannel.
 * Verifica autenticacao OAuth2 e conectividade.
 */
public class TestarConexaoAction implements AcaoRotinaJava {

    private static final Logger log = Logger.getLogger(TestarConexaoAction.class.getName());

    @Override
    public void doAction(ContextoAcao contexto) throws Exception {
        StringBuilder resultado = new StringBuilder();
        boolean sucesso = true;

        try {
            resultado.append("=== Teste de Conexao Fastchannel ===\n\n");

            // 1. Verificar configuracao
            resultado.append("1. Verificando configuracao...\n");
            FastchannelConfig config = FastchannelConfig.getInstance();

            if (!config.isAtivo()) {
                resultado.append("   [AVISO] Integracao esta desativada (nao bloqueia conexao)\n");
            } else {
                resultado.append("   [OK] Integracao ativa\n");
            }

            if (config.getClientId() == null || config.getClientId().isEmpty()) {
                resultado.append("   [ERRO] Client ID nao configurado!\n");
                sucesso = false;
            } else {
                resultado.append("   [OK] Client ID configurado\n");
            }

            if (config.getClientSecret() == null || config.getClientSecret().isEmpty()) {
                resultado.append("   [ERRO] Client Secret nao configurado!\n");
                sucesso = false;
            } else {
                resultado.append("   [OK] Client Secret configurado\n");
            }

            if (config.getBaseUrl() == null || config.getBaseUrl().isEmpty()) {
                resultado.append("   [ERRO] URL Base nao configurada!\n");
                sucesso = false;
            } else {
                resultado.append("   [OK] URL Base: ").append(config.getBaseUrl()).append("\n");
            }

            if (config.getAuthUrl() == null || config.getAuthUrl().isEmpty()) {
                resultado.append("   [ERRO] URL Auth nao configurada!\n");
                sucesso = false;
            } else {
                resultado.append("   [OK] URL Auth: ").append(config.getAuthUrl()).append("\n");
            }

            resultado.append("\n");

            // 2. Testar autenticacao OAuth2
            if (sucesso) {
                resultado.append("2. Testando autenticacao OAuth2...\n");
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
                    resultado.append("   [ERRO] Falha na autenticacao: ").append(e.getMessage()).append("\n");
                    sucesso = false;
                    log.log(Level.WARNING, "Erro no teste de autenticacao", e);
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
                    String testUrl = config.getBaseUrl() + "/orders?PageNumber=1&PageSize=1";
                    resultado.append("   Testando: ").append(testUrl).append("\n");

                    // O cliente HTTP ja trata erros e retries
                    FastchannelHttpClient.HttpResult result = httpClient.getOrders("/orders?PageNumber=1&PageSize=1");
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
                resultado.append("[SUCESSO] Conexao com Fastchannel OK!\n");
                resultado.append("A integracao esta configurada corretamente e funcionando.");
            } else {
                resultado.append("[FALHA] Problemas encontrados na conexao.\n");
                resultado.append("Verifique as configuracoes acima e tente novamente.");
            }

        } catch (Exception e) {
            resultado.append("\n[ERRO FATAL] ").append(e.getMessage());
            log.log(Level.SEVERE, "Erro fatal no teste de conexao", e);
        }

        // Retornar resultado para o usuario
        contexto.setMensagemRetorno(resultado.toString());
    }
}
