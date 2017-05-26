package de.hhu.bsinfo.dxterm;

/**
 * Stdin for terminal sessions. Redirects any input requested from a client to the server
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.05.2017
 */
public class TerminalServerStdin {
    private TerminalSession m_session;

    /**
     * Constructor
     *
     * @param p_session
     *         Terminal session to attach to
     */
    public TerminalServerStdin(final TerminalSession p_session) {
        m_session = p_session;
    }

    /**
     * Read a line from the client
     *
     * @return Valid string if reading a line was successful, null on error
     */
    public String readLine() {
        m_session.write(new TerminalReqStdin());
        Object obj = m_session.read();

        if (obj instanceof TerminalStdinData) {
            return ((TerminalStdinData) obj).getText();
        } else {
            return null;
        }
    }
}
