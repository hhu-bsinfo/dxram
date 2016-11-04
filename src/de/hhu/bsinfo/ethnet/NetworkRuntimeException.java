package de.hhu.bsinfo.ethnet;

/**
 * A netork runtime exception
 *
 * @author Stefan Nothaas, stefan.nothass@hhu.de, 01.02.2016
 */
class NetworkRuntimeException extends RuntimeException {
    private static final long serialVersionUID = -1801173917259116729L;

    /**
     * Creates an instance of NetworkRuntimeException
     *
     * @param p_message
     *     the message
     */
    NetworkRuntimeException(final String p_message) {
        super(p_message);
    }

    /**
     * Creates an instance of NetworkRuntimeException
     *
     * @param p_message
     *     the message
     * @param p_cause
     *     the cause
     */
    NetworkRuntimeException(final String p_message, final Throwable p_cause) {
        super(p_message, p_cause);
    }
}
