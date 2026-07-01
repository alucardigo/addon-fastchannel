package br.com.bellube.fastchannel.exception;

/**
 * Erro fatal de cliente (4xx excluindo 401/403/429) — request invalido, payload mal formado,
 * recurso nao encontrado (404), conflito (409), etc.
 * NAO e retryable — requer intervencao (corrigir payload ou logica).
 */
public class FastchannelFatalException extends FastchannelException {

    public FastchannelFatalException(String message, int httpStatus) {
        super(message, httpStatus);
    }

    public FastchannelFatalException(String message, int httpStatus, Throwable cause) {
        super(message, httpStatus, cause);
    }
}
