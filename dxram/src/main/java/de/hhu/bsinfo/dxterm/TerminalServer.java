/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxterm;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Terminal server class handling command evaluation
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class TerminalServer {
    private static final Logger LOGGER = LogManager.getFormatterLogger(TerminalServer.class.getSimpleName());

    private final short m_nodeId;
    private Map<String, AbstractTerminalCommand> m_commands = new HashMap<String, AbstractTerminalCommand>();

    /**
     * Constructor
     *
     * @param p_nodeId
     *         Node id of the DXRAM peer the server is running on
     */
    public TerminalServer(final short p_nodeId) {
        m_nodeId = p_nodeId;
    }

    /**
     * Get the node id of the DXRAM peer the server is running on
     */
    public short getNodeId() {
        return m_nodeId;
    }

    /**
     * Register a terminal command to make it callable from the terminal
     *
     * @param p_cmd
     *         Terminal command to register
     */
    public void registerTerminalCommand(final AbstractTerminalCommand p_cmd) {

        LOGGER.debug("Registering terminal command: %s", p_cmd.getName());


        if (m_commands.putIfAbsent(p_cmd.getName(), p_cmd) != null) {

            LOGGER.error("Registering command %s, class %s failed, name already used", p_cmd.getName(), p_cmd.getClass().getSimpleName());

        }
    }

    /**
     * Get a list of terminal commands available
     */
    public List<String> getTerminalCommandList() {
        Collection<AbstractTerminalCommand> cmds = m_commands.values();
        List<String> cmdNames = new ArrayList<>();

        for (AbstractTerminalCommand tmp : cmds) {
            cmdNames.add(tmp.getName());
        }

        Collections.sort(cmdNames);

        return cmdNames;
    }

    /**
     * Evaluate a terminal command
     *
     * @param p_session
     *         Session of the client requesting evaluation
     * @param p_cmdStr
     *         The client's command string to evaluate
     * @param p_services
     *         Service accessor for the terminal command
     */
    public void evaluate(final TerminalSession p_session, final TerminalCommandString p_cmdStr, final TerminalServiceAccessor p_services) {
        TerminalServerStdout stdout = new TerminalServerStdout(p_session);
        TerminalServerStdin stdin = new TerminalServerStdin(p_session);

        if ("list".equals(p_cmdStr.getName())) {
            List<String> cmdNames = getTerminalCommandList();

            StringBuilder strBuilder = new StringBuilder();

            for (String tmp : cmdNames) {
                strBuilder.append(tmp);
                strBuilder.append(' ');
            }

            stdout.println(strBuilder.toString());

            return;
        }

        if ("help".equals(p_cmdStr.getName())) {
            AbstractTerminalCommand cmd = m_commands.get(p_cmdStr.getArgString(0, ""));
            if (cmd == null) {
                stdout.printlnErr("Can't find help for command");
                return;
            }

            stdout.println(cmd.getHelp());
            return;
        }

        AbstractTerminalCommand cmd = m_commands.get(p_cmdStr.getName());
        if (cmd == null) {
            stdout.printlnErr("Unknown command");
            return;
        }

        cmd.exec(p_cmdStr, stdout, stdin, p_services);
    }

    /**
     * Get a list of completion suggestions for an argument of a command
     *
     * @param p_argumentPos
     *         Current argument position of the cursor
     * @param p_cmdStr
     *         Current (and probably incomplete) cmd string from the terminal
     * @param p_services
     *         Service accessor of dxram
     * @return List of argument suggestions for command
     */
    public List<String> getCommandArgumentCompletionList(final int p_argumentPos, final TerminalCommandString p_cmdStr,
            final TerminalServiceAccessor p_services) {
        AbstractTerminalCommand cmd = m_commands.get(p_cmdStr.getName());
        if (cmd == null) {
            return Collections.emptyList();
        }

        return cmd.getArgumentCompletionSuggestions(p_argumentPos, p_cmdStr, p_services);
    }
}
