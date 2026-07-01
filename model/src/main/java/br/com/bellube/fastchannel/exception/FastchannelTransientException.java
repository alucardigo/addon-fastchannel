package br.com.bellube.fastchannel.exception;

/**
 * Erro transiente — 5xx, timeout, connection reset, falha de rede.
 * Retryable com exponential backoff.
 */
public class FastchannelTransientException extends FastchannelException {

    public FastchannelTransientException(String message) {
        super(message);
    }

    public FastchannelTransientException(String message, Throwable cause) {
        super(message, cause);
    }

    public FastchannelTransientException(String message, int httpStatus, Throwable cause) {
        super(message, httpStatus, cause);
    }
}
