package br.com.bellube.fastchannel.exception;

/**
 * Erro de autenticacao/autorizacao (401, 403, token expirado, credenciais invalidas).
 * NAO e retryable via backoff — deve disparar refresh de token.
 */
public class FastchannelAuthException extends FastchannelException {

    public FastchannelAuthException(String message, int httpStatus) {
        super(message, httpStatus);
    }

    public FastchannelAuthException(String message, int httpStatus, Throwable cause) {
        super(message, httpStatus, cause);
    }
}
