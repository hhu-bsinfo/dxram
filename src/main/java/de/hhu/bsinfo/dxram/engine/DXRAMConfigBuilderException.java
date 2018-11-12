package de.hhu.bsinfo.dxram.engine;

/**
 * Exception thrown when building a configuration fails
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 12.11.2018
 */
public class DXRAMConfigBuilderException extends Exception {
    /**
     * Constructor
     *
     * @param p_msg
     *         Exception message
     */
    public DXRAMConfigBuilderException(final String p_msg) {
        super(p_msg);
    }

    /**
     * Constructor
     *
     * @param p_e
     *         Exception to wrap
     */
    public DXRAMConfigBuilderException(final Exception p_e) {
        super(p_e);
    }

    /**
     * Constructor
     *
     * @param p_msg
     *         Exception message
     * @param p_e
     *         Exception to wrap
     */
    public DXRAMConfigBuilderException(final String p_msg, final Exception p_e) {
        super(p_msg, p_e);
    }
}
