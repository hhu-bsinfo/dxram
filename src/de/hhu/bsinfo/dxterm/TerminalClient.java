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

import jline.console.ConsoleReader;
import jline.console.UserInterruptException;
import jline.console.completer.ArgumentCompleter;
import jline.console.completer.FileNameCompleter;
import jline.console.completer.StringsCompleter;
import jline.console.history.FileHistory;

import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Standalone thin client application to login to a terminal server running on a DXRAM peer
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public final class TerminalClient implements TerminalSession.Listener {
    private static final Logger LOGGER = LogManager.getFormatterLogger(TerminalClient.class.getSimpleName());

    private String m_autostartScript = "";
    private String m_historyFilePath = ".dxram_term_hist";

    private ConsoleReader m_consoleReader;
    private FileHistory m_history;

    private String[] m_localCommands = new String[] {"?", "help", "quit", "exit", "clear", "run"};

    private TerminalSession m_session;

    /**
     * Constructor
     *
     * @param p_hostname
     *         Hostname running the DXRAM peer with a terminal server
     * @param p_port
     *         Port of the terminal server to login to
     * @throws TerminalException
     *         If either creating the terminal environment or connecting to the server failed
     */
    private TerminalClient(final String p_hostname, final int p_port) throws TerminalException {
        try {
            m_consoleReader = new ConsoleReader();
            m_consoleReader.setBellEnabled(false);
        } catch (final IOException e) {
            throw new TerminalException("Initializing ConsoleReader failed", e);
        }

        loadHistoryFromFile(m_historyFilePath);

        // handle ctrl + c
        m_consoleReader.setHandleUserInterrupt(true);

        try {
            m_session = new TerminalSession((byte) 0, new Socket(p_hostname, p_port), this);
        } catch (final IOException e) {
            throw new TerminalException(e);
        }
    }

    /**
     * Main entry point for terminal client
     *
     * @param p_args
     *         Program arguments
     */
    public static void main(final String[] p_args) {
        if (p_args.length < 1) {
            System.out.println("Usage: TerminalClient <server hostname> [port]");
            return;
        }

        int port = 22222;
        if (p_args.length > 1) {
            port = Integer.parseInt(p_args[1]);
        }

        TerminalClient client = null;
        try {
            client = new TerminalClient(p_args[0], port);
            client.run();
        } catch (final TerminalException e) {
            System.out.println(e.getMessage());
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }

    /**
     * Run function of the terminal client
     *
     * @throws TerminalException
     *         On error (connection list, readline error, ...)
     */
    public void run() throws TerminalException {
        System.out.println("Connecting...");
        String prompt = login();

        System.out.println("Connected");
        System.out.println("Type '?' or 'help' to print the help message");

        // auto start script file
        if (!m_autostartScript.isEmpty()) {
            String[] lines = readAutostartScript(m_autostartScript);
            System.out.println(">>> Running auto start script " + m_autostartScript);

            for (String line : lines) {
                if (!evaluate(line)) {
                    System.out.println("Aborting command evaluation");
                    return;
                }
            }
        }

        boolean loop = true;

        while (loop) {
            String command;
            try {
                command = m_consoleReader.readLine('$' + prompt + "> ");
            } catch (final IOException e) {
                System.out.println("Readline failed: " + e.getMessage());
                continue;
            } catch (final UserInterruptException e) {
                // ctrl + c
                command = "";
            }

            loop = evaluate(command);
        }
    }

    /**
     * Close the terminal client and clean up
     */
    public void close() {
        // flush history
        try {
            m_history.flush();
        } catch (final IOException ignored) {
        }
    }

    @Override
    public void sessionClosed(final TerminalSession p_session) {
        // always own session
    }

    /**
     * Login to the connected server
     *
     * @return Command line prompt to display after login
     */
    private String login() {
        Object object = m_session.read();

        if (object instanceof TerminalLogin) {
            TerminalLogin login = (TerminalLogin) object;

            // TODO implement a completer for remote commands
            // add completers for commands
            List<String> list = login.getCmdNames();
            Collections.addAll(list, m_localCommands);
            m_consoleReader.addCompleter(new ArgumentCompleter(new StringsCompleter(list), new FileNameCompleter()));

            return login.getSessionId() + "/0x" + String.format("%04x", login.getNodeId()).toUpperCase();
        }

        return "LOGIN ERROR";
    }

    /**
     * Evaluate a terminal command
     *
     * @param p_cmd
     *         TerminalCommandString to evaluate
     * @return True on success, false on error
     */
    private boolean evaluateCommand(final TerminalCommandString p_cmd) {
        if (!m_session.write(p_cmd)) {
            System.out.println("Sending command failed, most likely connection loss");
            return false;
        }

        Object object;

        while (true) {
            object = m_session.read();

            if (object instanceof TerminalStdoutData) {
                TerminalStdoutData data = (TerminalStdoutData) object;

                changeConsoleColor(data.getColor(), data.getBackground(), data.getStyle());
                System.out.print(data.getText());
                changeConsoleColor(TerminalColor.DEFAULT, TerminalColor.DEFAULT, TerminalStyle.NORMAL);
            } else if (object instanceof TerminalCommandDone) {
                break;
            } else {
                System.out.println("Lost connection to server, closing");

                // lost connection
                m_session.close();
                return false;
            }
        }

        return true;
    }

    /**
     * Quit the terminal session (logout)
     */
    private void quit() {
        if (!m_session.write(new TerminalLogout())) {
            // #if LOGGER == ERROR
            LOGGER.error("Sending logout to server failed");
            // #endif /* LOGGER == ERROR */
        }

        m_session.close();
    }

    /**
     * Evaluate the text entered on the terminal
     *
     * @param p_text
     *         Text to evaluate
     * @return True if evaluation was successful and continue with command processing, false on error or quit.
     */
    private boolean evaluate(final String p_text) {
        // skip empty
        if (p_text.isEmpty()) {
            return true;
        }

        TerminalCommandString cmd = new TerminalCommandString(p_text);

        // skip comments
        if (cmd.isComment()) {
            return true;
        }

        // catch a few commands that are local, only
        switch (cmd.getName()) {
            case "?":
            case "help":
                if (cmd.getArgc() == 0) {
                    printHelp();
                    return true;
                }

                // help on command, handle on server
                break;

            case "exit":
            case "quit":
                quit();
                return false;

            case "clear":
                // ANSI escape codes (clear screen, move cursor to first row and first column)
                System.out.print("\033[HOUR\033[2J");
                System.out.flush();
                return true;

            case "run":
                return runScript(cmd);

            default:
                break;
        }

        return evaluateCommand(cmd);
    }

    /**
     * Load terminal command history from a file.
     *
     * @param p_file
     *         File to load the history from and append new commands to.
     */
    private void loadHistoryFromFile(final String p_file) throws TerminalException {
        File file = new File(p_file);

        if (!file.exists()) {
            try {
                if (!file.createNewFile()) {
                    throw new TerminalException("Creating new history file " + p_file + " failed");
                }
            } catch (final IOException ignored) {
            }
        }

        try {
            m_history = new FileHistory(new File(p_file));
            m_consoleReader.setHistory(m_history);
        } catch (final IOException e) {
            throw new TerminalException("Reading history " + p_file + " failed", e);
        }
    }

    /**
     * Run a terminal script (list of terminal commands)
     *
     * @param p_cmd
     *         TerminalCommandString identified as run script command
     * @return True if running script was successful, false on error
     */
    private boolean runScript(final TerminalCommandString p_cmd) {
        if (p_cmd.getArgc() < 1) {
            System.out.println("Missing path to script file to run");
            return true;
        }

        List<String> stringList;

        try {
            stringList = Files.readAllLines(new File(p_cmd.getArgs()[0]).toPath(), Charset.defaultCharset());
        } catch (final IOException e) {
            System.out.println("Loading script file " + p_cmd.getArgs()[0] + " failed: " + e.getMessage());
            return true;
        }

        System.out.println("Running terminal cmd script " + p_cmd.getArgs()[0] + "...");

        for (String line : stringList) {
            if (!evaluate(line)) {
                System.out.println("Evaluating command failed, aborting");
                return false;
            }
        }

        System.out.println("Finished running script " + p_cmd.getArgs()[0]);
        return true;
    }

    /**
     * Read an autostart script from a afile
     *
     * @param p_path
     *         File to read
     * @return Lines read from file
     */
    private static String[] readAutostartScript(final String p_path) throws TerminalException {
        List<String> stringList;

        try {
            stringList = Files.readAllLines(new File(p_path).toPath(), Charset.defaultCharset());
        } catch (final IOException e) {
            throw new TerminalException("Reading autostart script file " + p_path + " failed", e);
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
     * Change the color of stdout.
     *
     * @param p_color
     *         Text color.
     * @param p_backgroundColor
     *         Shell background color
     * @param p_style
     *         Text style.
     */
    private static void changeConsoleColor(final TerminalColor p_color, final TerminalColor p_backgroundColor, final TerminalStyle p_style) {
        if (p_backgroundColor != TerminalColor.DEFAULT) {
            System.out.printf("\033[%d;%d;%dm", p_style.ordinal(), p_color.ordinal() + 30, p_backgroundColor.ordinal() + 40);
        } else if (p_color != TerminalColor.DEFAULT) {
            System.out.printf("\033[%d;%dm", p_style.ordinal(), p_color.ordinal() + 30);
        } else {
            System.out.printf("\033[%dm", p_style.ordinal());
        }
    }
}
