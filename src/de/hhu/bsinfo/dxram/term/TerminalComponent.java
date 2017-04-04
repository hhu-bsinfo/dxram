package de.hhu.bsinfo.dxram.term;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.DXRAMComponentOrder;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;

/**
 * Terminal component providing terminal commands for the TerminalService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 04.04.2017
 */
public class TerminalComponent extends AbstractDXRAMComponent {
    private static final Logger LOGGER = LogManager.getFormatterLogger(TerminalComponent.class.getSimpleName());

    private Map<String, TerminalCommand> m_commands = new HashMap<String, TerminalCommand>();

    /**
     * Constructor
     */
    public TerminalComponent() {
        super(DXRAMComponentOrder.Init.TERMINAL, DXRAMComponentOrder.Shutdown.TERMINAL);
    }

    /**
     * Register a terminal command to make it callable from the terminal
     *
     * @param p_cmd
     *     Terminal command to register
     */
    public void registerTerminalCommand(final TerminalCommand p_cmd) {
        // #if LOGGER >= DEBUG
        LOGGER.debug("Registering terminal command: %s", p_cmd.getName());
        // #endif /* LOGGER >= DEBUG */

        if (m_commands.putIfAbsent(p_cmd.getName(), p_cmd) != null) {
            // #if LOGGER >= ERROR
            LOGGER.error("Registering command %s failed, name already used", p_cmd.getName());
            // #endif /* LOGGER >= ERROR */
        }
    }

    /**
     * Get a terminal command
     *
     * @param p_name
     *     Name of the command
     * @return If found, valid reference otherwise null
     */
    TerminalCommand getCommand(final String p_name) {
        return m_commands.get(p_name);
    }

    /**
     * Get a list of terminal commands available
     */
    Collection<String> getListOfCommands() {
        return m_commands.keySet();
    }

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {

    }

    @Override
    protected boolean initComponent(final DXRAMContext.EngineSettings p_engineEngineSettings) {
        return true;
    }

    @Override
    protected boolean shutdownComponent() {
        m_commands.clear();

        return true;
    }
}
