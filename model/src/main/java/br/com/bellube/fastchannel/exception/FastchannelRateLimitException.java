package br.com.bellube.fastchannel.exception;

/**
 * Erro HTTP 429 — rate limit atingido no gateway Fastchannel.
 * Retryable com respeito ao header Retry-After, se presente.
 */
public class FastchannelRateLimitException extends FastchannelException {

    private final Long retryAfterSeconds;

    public FastchannelRateLimitException(String message, Long retryAfter) {
        super(message, 429);
        this.retryAfterSeconds = retryAfter;
    }

    public FastchannelRateLimitException(String message, Long retryAfter, Throwable cause) {
        super(message, 429, cause);
        this.retryAfterSeconds = retryAfter;
    }

    /**
     * @return segundos do header Retry-After ou null se nao informado.
     */
    public Long getRetryAfterSeconds() {
        return retryAfterSeconds;
    }
}
