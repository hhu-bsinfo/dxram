/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

package de.hhu.bsinfo.dxterm.cmd;

import java.util.Collections;
import java.util.List;

import de.hhu.bsinfo.dxutils.stats.StatisticsRecorder;
import de.hhu.bsinfo.dxram.stats.StatisticsService;
import de.hhu.bsinfo.dxterm.AbstractTerminalCommand;
import de.hhu.bsinfo.dxterm.TerminalCommandString;
import de.hhu.bsinfo.dxterm.TerminalServerStdin;
import de.hhu.bsinfo.dxterm.TerminalServerStdout;
import de.hhu.bsinfo.dxterm.TerminalServiceAccessor;

/**
 * Prints all statistics
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.04.2017
 */
public class TcmdStatsprint extends AbstractTerminalCommand {
    public TcmdStatsprint() {
        super("statsprint");
    }

    @Override
    public String getHelp() {
        return "Prints all statistics\n" + "Usage: statsprint [className]\n" + "  className: Filter statistics by class name, optional";
    }

    @Override
    public void exec(final TerminalCommandString p_cmd, final TerminalServerStdout p_stdout, final TerminalServerStdin p_stdin,
            final TerminalServiceAccessor p_services) {
        String className = p_cmd.getArgString(0, null);

        StatisticsService statistics = p_services.getService(StatisticsService.class);

        if (className == null) {
            for (StatisticsRecorder rec : statistics.getRecorders()) {
                p_stdout.println(rec.toString());
            }
        } else {
            StatisticsRecorder rec = statistics.getRecorder(className);
            if (rec == null) {
                p_stdout.printlnErr("No such recorder available");
            } else {
                p_stdout.println(rec.toString());
            }
        }
    }

    @Override
    public List<String> getArgumentCompletionSuggestions(final int p_argumentPos, final TerminalCommandString p_cmdStr,
            final TerminalServiceAccessor p_services) {
        return Collections.emptyList();
    }
}
