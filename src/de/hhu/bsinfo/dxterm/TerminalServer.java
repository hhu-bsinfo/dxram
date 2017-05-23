/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
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

public class TerminalServer {
    private static final Logger LOGGER = LogManager.getFormatterLogger(TerminalServer.class.getSimpleName());

    private final short m_nodeId;
    private Map<String, AbstractTerminalCommand> m_commands = new HashMap<String, AbstractTerminalCommand>();

    public TerminalServer(final short p_nodeId) {
        m_nodeId = p_nodeId;
    }

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
        // #if LOGGER >= DEBUG
        LOGGER.debug("Registering terminal command: %s", p_cmd.getName());
        // #endif /* LOGGER >= DEBUG */

        if (m_commands.putIfAbsent(p_cmd.getName(), p_cmd) != null) {
            // #if LOGGER >= ERROR
            LOGGER.error("Registering command %s, class %s failed, name already used", p_cmd.getName(), p_cmd.getClass().getSimpleName());
            // #endif /* LOGGER >= ERROR */
        }
    }

    public List<String> getTerminalCommandList() {
        Collection<AbstractTerminalCommand> cmds = m_commands.values();
        List<String> cmdNames = new ArrayList<>();

        for (AbstractTerminalCommand tmp : cmds) {
            cmdNames.add(tmp.getName());
        }

        Collections.sort(cmdNames);

        return cmdNames;
    }

    public void evaluate(final TerminalSession p_session, final TerminalCommandString p_cmdStr, final TerminalServiceAccessor p_services) {
        TerminalServerStdout stdout = new TerminalServerStdout(p_session);

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

        cmd.exec(p_cmdStr, stdout, p_services);
    }
}
