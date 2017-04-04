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

package de.hhu.bsinfo.dxram.term.cmd;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.List;

import de.hhu.bsinfo.dxram.term.TerminalCommand;
import de.hhu.bsinfo.dxram.term.TerminalCommandContext;
import de.hhu.bsinfo.dxram.term.TerminalService;

/**
 * Load and run a text file with terminal commands
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.04.2017
 */
public class TcmdScriptrun extends TerminalCommand {
    public TcmdScriptrun() {
        super("scriptrun");
    }

    @Override
    public String getHelp() {
        return "Load and run a text file with terminal commands\n" + "Usage: termscriptrun <file>\n" + "  file: Path to script file to load and run";
    }

    @Override
    public void exec(final String[] p_args, final TerminalCommandContext p_ctx) {
        String file = p_ctx.getArgString(p_args, 0, null);

        if (file == null) {
            p_ctx.printlnErr("No scriptfile specified");
            return;
        }

        List<String> stringList;

        try {
            stringList = Files.readAllLines(new File(file).toPath(), Charset.defaultCharset());
        } catch (final IOException e) {
            p_ctx.printflnErr("Reading script file %s failed: %s", file, e);
            return;
        }

        TerminalService term = p_ctx.getService(TerminalService.class);

        for (String str : stringList) {
            term.evaluate(str);
        }
    }
}
