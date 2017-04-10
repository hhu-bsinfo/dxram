package de.hhu.bsinfo.dxram.term;

/**
 * Base class for all terminal commands
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.04.2017
 */
public abstract class AbstractTerminalCommand {
    private String m_name;

    /**
     * Constructor
     *
     * @param p_name
     *     Name of the command
     */
    protected AbstractTerminalCommand(final String p_name) {
        m_name = p_name;
    }

    /**
     * Get the command's name
     *
     * @return Name
     */
    public String getName() {
        return m_name;
    }

    /**
     * Get a string describing the command and how to use it
     *
     * @return Help string for the command
     */
    public abstract String getHelp();

    /**
     * Execute the command
     *
     * @param p_args
     *     Arguments provided to the command
     * @param p_ctx
     *     Terminal context to access services and other useful things
     */
    public abstract void exec(final String[] p_args, final TerminalCommandContext p_ctx);
}
