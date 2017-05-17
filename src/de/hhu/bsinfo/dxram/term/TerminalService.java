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

import jline.console.ConsoleReader;
import jline.console.UserInterruptException;
import jline.console.completer.ArgumentCompleter;
import jline.console.completer.FileNameCompleter;
import jline.console.completer.StringsCompleter;
import jline.console.history.FileHistory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.gson.annotations.Expose;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.term.tcmd.TcmdScriptrun;
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
    /**
     * Path to a text file with terminal commands to run right after the terminal started
     */
    @Expose
    private String m_autostartScript = "";

    /**
     * Path to a file to store the history of the terminal to
     */
    @Expose
    private String m_historyFilePath = ".dxram_term_history";

    // component dependencies
    private AbstractBootComponent m_boot;
    private TerminalComponent m_terminal;

    private TerminalCommandContext m_commandCtx;
    private ConsoleReader m_consoleReader;
    private FileHistory m_history;
    private ArgumentCompleter m_argCompletor;

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
        Collection<String> cmds = m_terminal.getListOfCommands();
        m_argCompletor = new ArgumentCompleter(new StringsCompleter(cmds.toArray(new String[cmds.size()])), new FileNameCompleter());

        //m_consoleReader.addCompleter();
        m_consoleReader.addCompleter(m_argCompletor);
        // handle ctrl + c
        m_consoleReader.setHandleUserInterrupt(true);

        // #if LOGGER >= INFO
        LOGGER.info("Running terminal...");
        // #endif /* LOGGER >= INFO */

        System.out.println("Running on node " + NodeID.toHexString(m_boot.getNodeID()) + ", role " + m_boot.getNodeRole());
        System.out.println("Type '?' or 'help' to print the help message");

        // auto start script file
        if (!m_autostartScript.isEmpty()) {
            String[] lines = readAutostartScript(m_autostartScript);
            System.out.println("Running auto start script " + m_autostartScript);
            runAutostartScript(lines);
        }

        while (m_loop) {
            String command;
            try {
                command = m_consoleReader.readLine('$' + NodeID.toHexString(m_boot.getNodeID()) + "> ");
            } catch (final IOException e) {
                LOGGER.error("Readline failed", e);
                continue;
            } catch (final UserInterruptException e) {
                // ctrl + c
                command = "";
            }

            evaluate(command);
        }

        // #if LOGGER >= INFO
        LOGGER.info("Exiting terminal...");
        // #endif /* LOGGER >= INFO */
    }

    /**
     * Evaluate the text entered in the terminal.
     *
     * @param p_text
     *     Text to evaluate
     */
    public void evaluate(final String p_text) {

        // skip empty
        if (p_text.isEmpty()) {
            return;
        }

        if (p_text.startsWith("?")) {
            printHelp();
        } else if ("exit".equals(p_text) || "quit".equals(p_text)) {
            m_loop = false;
        } else if ("clear".equals(p_text)) {
            // ANSI escape codes (clear screen, move cursor to first row and first column)
            System.out.print("\033[HOUR\033[2J");
            System.out.flush();
        } else if (p_text.startsWith("#")) {
            // comment, do nothing
        } else {
            evaluateCommand(p_text);
        }
    }

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
        m_boot = p_componentAccessor.getComponent(AbstractBootComponent.class);
        m_terminal = p_componentAccessor.getComponent(TerminalComponent.class);
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
        if (m_boot.getNodeRole() == NodeRole.TERMINAL) {
            // flush history
            try {
                m_history.flush();
            } catch (final IOException ignored) {
            }

        }

        return true;
    }

    @Override
    protected boolean isServiceAccessor() {
        // access the services to provide a context for the terminal commands
        return true;
    }

    /**
     * Read an autostart script from a afile
     *
     * @param p_path
     *     File to read
     * @return Lines read from file
     */
    private static String[] readAutostartScript(final String p_path) {
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

    /**
     * Print the terminal help message
     */
    private static void printHelp() {
        System.out.println(
            "Type '?' or 'help' to print this message\n" + "> To list all built in terminal commands: 'list'\n" + "> 'clear' to clear the screen\n" +
                "> 'exit' to close and shut down the current terminal\n" + "> Get help about a terminal command, example: 'help nodelist'\n" +
                "> Execute built in terminal commands, e.g. 'nodelist' or with arguments separated by spaces 'nodelist \"peer\"'");
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
                    AbstractTerminalCommand cmd = m_terminal.getCommand(tokens[1]);
                    if (cmd != null) {
                        System.out.println(cmd.getHelp());
                    } else {
                        TerminalCommandContext.printlnErr("Could not find help for terminal command '" + tokens[1] + '\'');
                    }

                    return;
                }
            }
        }

        // separate by space but keep strings, e.g. "this is a test" as a single string and remove the quotes
        List<String> list = new ArrayList<String>();
        Matcher matcher = Pattern.compile("([^\"]\\S*|\".+?\")\\s*").matcher(p_text);
        while (matcher.find()) {
            list.add(matcher.group(1).replace("\"", ""));
        }

        if (!list.isEmpty()) {
            AbstractTerminalCommand cmd = m_terminal.getCommand(list.get(0));
            if (cmd != null) {
                try {
                    cmd.exec(Arrays.copyOfRange(list.toArray(new String[list.size()]), 1, list.size()), m_commandCtx);
                } catch (final Exception e) {
                    TerminalCommandContext.printflnErr("Exception executing command: %s", e);
                }
            } else {
                TerminalCommandContext.printlnErr("Invalid terminal command '" + list.get(0) + '\'');
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
        File file = new File(p_file);

        if (!file.exists()) {
            try {
                if (!file.createNewFile()) {
                    // #if LOGGER >= ERROR
                    LOGGER.error("Creating new history file %s failed", p_file);
                    // #endif /* LOGGER >= ERROR */
                }
            } catch (final IOException ignored) {
            }
        }

        try {
            m_history = new FileHistory(new File(p_file));
            m_consoleReader.setHistory(m_history);
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
        m_terminal.registerTerminalCommand(new TcmdScriptrun());
    }

    /**
     * Print a list of terminal commands available
     */
    private void listCommands() {
        String str = "";
        Collection<String> cmds = m_terminal.getListOfCommands();

        for (String item : cmds) {
            str += item + ", ";
        }

        System.out.println(str.substring(0, str.length() - 2));
    }

    /**
     * Run the commands provided by an autostart script
     *
     * @param p_lines
     *     Array of lines with one command each read from script
     */
    private void runAutostartScript(final String[] p_lines) {
        for (String line : p_lines) {
            evaluate(line);
        }
    }
}
