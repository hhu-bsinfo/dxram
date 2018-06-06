package de.hhu.bsinfo.dxterm;

import java.io.Serializable;

/**
 * Request a list of completion suggestions for the arguments of a command (client to server)
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.05.2017
 */
class TerminalReqCmdArgComp implements Serializable {
    private final int m_argument;
    private final TerminalCommandString m_cmdStr;

    /**
     * Constructor
     *
     * @param p_arg
     *         Current argument position of the terminal cursor
     * @param p_cmdStr
     *         Current (and probably incomplete) terminal command string
     */
    TerminalReqCmdArgComp(final int p_arg, final TerminalCommandString p_cmdStr) {
        m_argument = p_arg;
        m_cmdStr = p_cmdStr;
    }

    /**
     * Get the current argument position of the cursor
     */
    int getArgumentPos() {
        return m_argument;
    }

    /**
     * Current (and probably incomplete) terminal command string
     */
    TerminalCommandString getCmdStr() {
        return m_cmdStr;
    }
}
