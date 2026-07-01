package br.com.bellube.fastchannel.exception;

/**
 * Excecao base para erros da integracao Fastchannel.
 * Todas as excecoes tipadas do addon herdam desta.
 *
 * Mantem compatibilidade com callers existentes por herdar de RuntimeException
 * (nao obriga try/catch adicional).
 */
public class FastchannelException extends RuntimeException {

    private final Integer httpStatus;

    public FastchannelException(String message) {
        super(message);
        this.httpStatus = null;
    }

    public FastchannelException(String message, Throwable cause) {
        super(message, cause);
        this.httpStatus = null;
    }

    public FastchannelException(String message, int httpStatus) {
        super(message);
        this.httpStatus = httpStatus;
    }

    public FastchannelException(String message, int httpStatus, Throwable cause) {
        super(message, cause);
        this.httpStatus = httpStatus;
    }

    /**
     * @return HTTP status code associado ou null se nao aplicavel.
     */
    public Integer getHttpStatus() {
        return httpStatus;
    }
}
