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
import java.util.regex.Pattern;

import com.google.gson.annotations.Expose;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.ethnet.NodeID;

/**
 * Service providing an interactive terminal running on a DXRAM instance.
 * Allows access to implemented services, triggering commands, getting information
 * about current or remote DXRAM instances. The command line interface is basically a java script interpreter with
 * a few built in special commands
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 11.03.2016
 */
public class TerminalService extends AbstractDXRAMService {

    private static final Logger LOGGER = LogManager.getFormatterLogger(TerminalService.class.getSimpleName());

    // configuration values
    @Expose
    private String m_autostartScript = "";

    // dependent components
    private AbstractBootComponent m_boot;
    private TerminalComponent m_terminal;

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
        byte[] arr;

        if (m_boot.getNodeRole() != NodeRole.TERMINAL) {
            System.out.println("A Terminal node must have the NodeRole \"terminal\". Aborting");
            return;
        }

        // register commands for auto completion
        m_argCompletor = new ArgumentCompletor(
            new SimpleCompletor(m_terminal.getRegisteredCommands().keySet().toArray(new String[m_terminal.getRegisteredCommands().size()])));

        m_consoleReader.addCompletor(m_argCompletor);

        // #if LOGGER >= INFO
        LOGGER.info("Running terminal...");
        // #endif /* LOGGER >= INFO */

        System.out.println("Running on node " + NodeID.toHexString(m_boot.getNodeID()) + ", role " + m_boot.getNodeRole());
        System.out.println("Type '?' or 'help' to print the help message");

        // auto start script file
        if (!m_autostartScript.isEmpty()) {
            System.out.println("Running auto start script " + m_autostartScript);
            if (!m_terminal.getScriptContext().load(m_autostartScript)) {
                System.out.println("Running auto start script failed");
            } else {
                System.out.println("Running auto start script complete");
            }
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

    /**
     * Load a script file into the terminal context
     *
     * @param p_path
     *     Path to the java script file to load
     * @return True if successful, false otherwise.
     */
    public boolean load(final String p_path) {

        return m_terminal.getScriptContext().load(p_path);
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

            loadHistoryFromFile(".dxram_term_history");
        }

        return true;
    }

    @Override
    protected boolean shutdownService() {
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
            m_terminal.getScriptTerminalContext().help();
        } else if ("exit".equals(p_text)) {
            m_loop = false;
        } else if ("clear".equals(p_text)) {
            // ANSI escape codes (clear screen, move cursor to first row and first column)
            System.out.print("\033[H\033[2J");
            System.out.flush();
        } else {
            eveluateCommand(p_text);
        }
    }

    /**
     * Evaluate the terminal command
     *
     * @param p_text
     *     Text to evaluate as terminal command
     */
    private void eveluateCommand(final String p_text) {
        // Remove leading and trailing white spaces and replace multiple space with one space
        String text = p_text.trim().replaceAll(" +", " ");

        if ("list".equals(text)) {
            m_terminal.getScriptContext().eval("dxterm.list()");
            return;
        }
        if ("reload".equals(text)) {
            m_terminal.getScriptContext().eval("dxterm.reload()");
            return;
        }
        if (text.startsWith("help")) {
            if ("help".equals(text)) {
                m_terminal.getScriptTerminalContext().help();
            } else {
                String[] tokens = text.split(" ");
                if (tokens.length > 1) {
                    String cmd = tokens[1].replaceAll("\\(\\)", "");
                    de.hhu.bsinfo.dxram.script.ScriptContext scriptCtx = m_terminal.getRegisteredCommands().get(cmd);
                    if (scriptCtx != null) {
                        m_terminal.getScriptContext().eval("dxterm.cmd(\"" + cmd + "\").help()");
                    } else {
                        System.out.println("Could not find help for terminal command '" + cmd + '\'');
                    }

                    return;
                }
            }
        }

        if (Pattern.matches("[a-z]++ [a-zA-Z0-9\"]++", text)) {
            // Type1: command arg1 arg2

            // Replace first space after command with (
            text = text.replaceAll("([a-z]++) ", "$1(");
            // Replace all spaces outside of double quotes with ,
            text = text.replaceAll("\\s+(?=((\\\\[\\\\\"]|[^\\\\\"])*\"(\\\\[\\\\\"]|[^\\\\\"])*\")*(\\\\[\\\\\"]|[^\\\\\"])*$)", ",");
            // Add an closing bracket
            text += ")";
        } else {
            // Type2: command(arg1,arg2)

            // Remove all spaces outside of double quotes
            text = text.replaceAll("\\s+(?=((\\\\[\\\\\"]|[^\\\\\"])*\"(\\\\[\\\\\"]|[^\\\\\"])*\")*(\\\\[\\\\\"]|[^\\\\\"])*$)", "");
            if (!text.contains("(")) {
                // Add brackets
                text += "()";
            } else if (text.contains("(") && !text.contains(")")) {
                // Add closing bracket
                text += ")";
            }
        }

        // resolve terminal cmd "macros"
        String[] tokensFunc = text.split("\\(");
        if (tokensFunc.length > 1) {
            // resolve cmd call
            de.hhu.bsinfo.dxram.script.ScriptContext scriptCtx = m_terminal.getRegisteredCommands().get(tokensFunc[0]);
            if (scriptCtx != null) {
                // assemble long call
                String call = "dxterm.cmd(\"" + tokensFunc[0] + "\").exec(";

                // prepare parameters
                if (tokensFunc[1].length() > 1) {
                    call += tokensFunc[1];
                } else {
                    call += ")";
                }

                m_terminal.getScriptContext().eval(call);
            } else {
                // Command is not a registered command -> execute unchanged javascript code
                m_terminal.getScriptContext().eval(p_text);
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
            // TODO fix
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
}
