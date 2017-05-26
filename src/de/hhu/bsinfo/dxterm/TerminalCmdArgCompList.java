package de.hhu.bsinfo.dxterm;

import java.io.Serializable;
import java.util.List;

/**
 * Response to the client with suggestions on argument completion
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.05.2017
 */
class TerminalCmdArgCompList implements Serializable {
    private List<String> m_completions;

    /**
     * Constructor
     *
     * @param p_comps
     *         Completion suggestions
     */
    TerminalCmdArgCompList(final List<String> p_comps) {
        m_completions = p_comps;
    }

    /**
     * Get the completion suggestions
     */
    List<String> getCompletions() {
        return m_completions;
    }
}
