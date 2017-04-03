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

package de.hhu.bsinfo.dxram.term;

import jline.ArgumentCompletor;
import jline.ConsoleReader;
import jline.History;
import jline.SimpleCompletor;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.gson.annotations.Expose;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.term.cmd.TcmdLoggerlevel;
import de.hhu.bsinfo.dxram.term.cmd.TcmdLoginfo;
import de.hhu.bsinfo.dxram.term.cmd.TcmdNameget;
import de.hhu.bsinfo.dxram.term.cmd.TcmdNamelist;
import de.hhu.bsinfo.dxram.term.cmd.TcmdNamereg;
import de.hhu.bsinfo.dxram.term.cmd.TcmdNodeinfo;
import de.hhu.bsinfo.dxram.term.cmd.TcmdNodelist;
import de.hhu.bsinfo.dxram.term.cmd.TcmdNodeshutdown;
import de.hhu.bsinfo.dxram.term.cmd.TcmdNodewait;
import de.hhu.bsinfo.dxram.term.cmd.TcmdStatsprint;
import de.hhu.bsinfo.dxram.term.cmd.TcmdStatsrecorders;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.ethnet.NodeID;

/**
 * Service providing an interactive terminal running on a DXRAM instance.
 * Allows access to implemented services, triggering commands, getting information
 * about current or remote DXRAM instances.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 11.03.2016
 */
public class TerminalService extends AbstractDXRAMService {
    private static final Logger LOGGER = LogManager.getFormatterLogger(TerminalService.class.getSimpleName());

    // configuration values
    @Expose
    private String m_autostartScript = "";
    @Expose
    private String m_historyFilePath = ".dxram_term_history";

    // dependent components
    private AbstractBootComponent m_boot;

    private Map<String, TerminalCommand> m_commands = new HashMap<String, TerminalCommand>();
    private TerminalCommandContext m_commandCtx;
    private ConsoleReader m_consoleReader;
    private ArgumentCompletor m_argCompletor;

    private volatile boolean m_loop = true;

    /**
     * Constructor
     */
    public TerminalService() {
        super("term");
    }

    /**
     * Run the terminal loop.
     * Only returns if terminal was exited.
     */
    public void loop() {
        if (m_boot.getNodeRole() != NodeRole.TERMINAL) {
            System.out.println("A Terminal node must have the NodeRole \"terminal\". Aborting");
            return;
        }

        // register commands for auto completion
        m_argCompletor = new ArgumentCompletor(new SimpleCompletor(m_commands.keySet().toArray(new String[m_commands.size()])));

        m_consoleReader.addCompletor(m_argCompletor);

        // #if LOGGER >= INFO
        LOGGER.info("Running terminal...");
        // #endif /* LOGGER >= INFO */

        System.out.println("Running on node " + NodeID.toHexString(m_boot.getNodeID()) + ", role " + m_boot.getNodeRole());
        System.out.println("Type '?' or 'help' to print the help message");

        // auto start script file
        if (!m_autostartScript.isEmpty()) {
            String[] cmds = readAutostartScript(m_autostartScript);
            System.out.println("Running auto start script " + m_autostartScript);
            runAutostartScript(cmds);
        }

        while (m_loop) {
            String command;
            try {
                command = m_consoleReader.readLine('$' + NodeID.toHexString(m_boot.getNodeID()) + "> ");
            } catch (IOException e) {
                LOGGER.error("Readline failed", e);
                continue;
            }

            evaluate(command);
        }

        // #if LOGGER >= INFO
        LOGGER.info("Exiting terminal...");
        // #endif /* LOGGER >= INFO */
    }

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
        m_boot = p_componentAccessor.getComponent(AbstractBootComponent.class);
    }

    @Override
    protected boolean startService(final DXRAMContext.EngineSettings p_engineEngineSettings) {
        if (m_boot.getNodeRole() == NodeRole.TERMINAL) {
            try {
                m_consoleReader = new ConsoleReader();
                m_consoleReader.setBellEnabled(false);
            } catch (IOException e) {
                LOGGER.error("Initializing ConsoleReader failed", e);
                return false;
            }

            loadHistoryFromFile(m_historyFilePath);
        }

        m_commandCtx = new TerminalCommandContext(getServiceAccessor());
        registerTerminalCommands();

        return true;
    }

    @Override
    protected boolean shutdownService() {
        return true;
    }

    @Override
    protected boolean isServiceAccessor() {
        // access the services to provide a context for the terminal commands
        return true;
    }

    /**
     * Evaluate the text entered in the terminal.
     *
     * @param p_text
     *     Text to evaluate
     */
    private void evaluate(final String p_text) {

        // skip empty
        if (p_text.isEmpty()) {
            return;
        }

        if (p_text.startsWith("?")) {
            printHelp();
        } else if ("exit".equals(p_text)) {
            m_loop = false;
        } else if ("clear".equals(p_text)) {
            // ANSI escape codes (clear screen, move cursor to first row and first column)
            System.out.print("\033[H\033[2J");
            System.out.flush();
        } else {
            evaluateCommand(p_text);
        }
    }

    /**
     * Evaluate the terminal command
     *
     * @param p_text
     *     Text to evaluate as terminal command
     */
    private void evaluateCommand(final String p_text) {
        // Remove leading and trailing white spaces and replace multiple space with one space
        String text = p_text.trim().replaceAll(" +", " ");

        if ("list".equals(text)) {
            listCommands();
            return;
        }

        if (text.startsWith("help")) {
            if ("help".equals(text)) {
                printHelp();
            } else {
                String[] tokens = text.split(" ");
                if (tokens.length > 1) {
                    TerminalCommand cmd = m_commands.get(tokens[1]);
                    if (cmd != null) {
                        System.out.println(cmd.getHelp());
                    } else {
                        m_commandCtx.printlnErr("Could not find help for terminal command '" + tokens[1] + '\'');
                    }

                    return;
                }
            }
        }

        // separate by space but keep strings, e.g. "this is a test" as a single string and remove the quotes
        List<String> list = new ArrayList<String>();
        Matcher m = Pattern.compile("([^\"]\\S*|\".+?\")\\s*").matcher(p_text);
        while (m.find()) {
            list.add(m.group(1).replace("\"", ""));
        }

        if (!list.isEmpty()) {
            TerminalCommand cmd = m_commands.get(list.get(0));
            if (cmd != null) {
                try {
                    cmd.exec(Arrays.copyOfRange(list.toArray(new String[list.size()]), 1, list.size()), m_commandCtx);
                } catch (final Exception e) {
                    m_commandCtx.printflnErr("Exception executing command: %s", e);
                }
            } else {
                m_commandCtx.printlnErr("Invalid terminal command '" + list.get(0) + '\'');
            }
        }
    }

    /**
     * Load terminal command history from a file.
     *
     * @param p_file
     *     File to load the history from and append new commands to.
     */
    private void loadHistoryFromFile(final String p_file) {
        try {
            m_consoleReader.setHistory(new History(new File(p_file)));
        } catch (final FileNotFoundException e) {
            // #if LOGGER >= DEBUG
            LOGGER.debug("No history found: %s", p_file);
            // #endif /* LOGGER >= DEBUG */
        } catch (final IOException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Reading history %s failed: %s", p_file, e);
            // #endif /* LOGGER >= ERROR */
        }
    }

    /**
     * Register terminal commands
     */
    private void registerTerminalCommands() {
        registerTerminalCommand(new TcmdLoggerlevel());
        registerTerminalCommand(new TcmdLoginfo());
        registerTerminalCommand(new TcmdNameget());
        registerTerminalCommand(new TcmdNamelist());
        registerTerminalCommand(new TcmdNamereg());
        registerTerminalCommand(new TcmdNodeinfo());
        registerTerminalCommand(new TcmdNodelist());
        registerTerminalCommand(new TcmdNodeshutdown());
        registerTerminalCommand(new TcmdNodewait());
        registerTerminalCommand(new TcmdStatsprint());
        registerTerminalCommand(new TcmdStatsrecorders());
    }

    /**
     * Register a terminal command to make it callable from the terminal
     *
     * @param p_cmd
     *     Terminal command to register
     */
    private void registerTerminalCommand(final TerminalCommand p_cmd) {
        // #if LOGGER >= DEBUG
        LOGGER.debug("Registering terminal command: %s", p_cmd.getName());
        // #endif /* LOGGER >= DEBUG */

        m_commands.put(p_cmd.getName(), p_cmd);
    }

    /**
     * Print the terminal help message
     */
    private void printHelp() {
        System.out.println(
            "Type '?' or 'help' to print this message\n" + "> To list all built in terminal commands: 'list'\n" + "> 'clear' to clear the screen\n" +
                "> 'exit' to close and shut down the current terminal\n" + "> Get help about a terminal command, example: 'help nodelist'\n" +
                "> Execute built in terminal commands, e.g. 'nodelist' or with arguments separated by spaces 'nodelist \"peer\"'");
    }

    /**
     * Print a list of available terminal commands
     */
    private void listCommands() {
        String str = "";

        for (String item : m_commands.keySet()) {
            str += item + ", ";
        }

        System.out.println(str.substring(0, str.length() - 2));
    }

    private String[] readAutostartScript(final String p_path) {
        List<String> stringList;

        try {
            stringList = Files.readAllLines(new File(p_path).toPath(), Charset.defaultCharset());
        } catch (final IOException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Reading autostart script file %s failed: %s", p_path, e);
            // #endif /* LOGGER >= ERROR */

            return new String[0];
        }

        return stringList.toArray(new String[stringList.size()]);
    }

    private void runAutostartScript(final String[] p_lines) {
        for (String line : p_lines) {
            evaluate(line);
        }
    }
}
