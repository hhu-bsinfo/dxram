package de.hhu.bsinfo.dxterm;

import java.io.Serializable;

/**
 * Stdin data sent from the client to the terminal on stdin request
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.05.2017
 */
public class TerminalStdinData implements Serializable {
    private String m_text;

    /**
     * Constructor
     *
     * @param p_text
     *         Text read from stdin or null on error
     */
    public TerminalStdinData(final String p_text) {
        m_text = p_text;
    }

    /**
     * Text read from stdin or null on error
     */
    public String getText() {
        return m_text;
    }
}
