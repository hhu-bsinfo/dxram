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

import java.util.List;

/**
 * Base class for all terminal commands (server side)
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.04.2017
 */
public abstract class AbstractTerminalCommand {
    private String m_name;

    /**
     * Constructor
     *
     * @param p_name
     *         Name of the command
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
     * @param p_cmd
     *         Terminal command with arguments
     * @param p_stdout
     *         Stdout for terminal commands to write output to
     * @param p_stdin
     *         Stdin for terminal commands to read input
     * @param p_services
     *         Accessor to dxram services
     */
    public abstract void exec(final TerminalCommandString p_cmd, final TerminalServerStdout p_stdout, final TerminalServerStdin p_stdin,
            final TerminalServiceAccessor p_services);

    /**
     * Return suggestions for argument completion
     *
     * @param p_argumentPos
     *         Current argument to be completed
     * @param p_cmdStr
     *         Current (and probably incomplete) command string from the terminal
     * @param p_services
     *         Accessor to dxram services
     * @return List of suggestions on how to complete the argument
     */
    public abstract List<String> getArgumentCompletionSuggestions(final int p_argumentPos, final TerminalCommandString p_cmdStr,
            final TerminalServiceAccessor p_services);
}
