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

/**
 * Stdout for terminal server sessions. This redirects any output written to the remote client's stdout
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class TerminalServerStdout {
    private TerminalSession m_session;

    /**
     * Constructor
     *
     * @param p_session
     *         Terminal session to attach to
     */
    public TerminalServerStdout(final TerminalSession p_session) {
        m_session = p_session;
    }

    /**
     * Print to the console
     *
     * @param p_str
     *         String to print
     */
    public void print(final String p_str) {
        m_session.write(new TerminalStdoutData(p_str));
    }

    /**
     * Print to the console + newline
     *
     * @param p_str
     *         String to print
     */
    public void println(final String p_str) {
        m_session.write(new TerminalStdoutData(p_str + '\n'));
    }

    /**
     * Print to the console using a c-style formated string and arguments
     *
     * @param p_format
     *         Formatting for the string
     * @param p_args
     *         Optional arguments
     */
    public void printf(final String p_format, final Object... p_args) {
        m_session.write(new TerminalStdoutData(String.format(p_format, p_args)));
    }

    /**
     * Print an error message to the console
     *
     * @param p_str
     *         String to print
     */
    public void printErr(final String p_str) {
        m_session.write(new TerminalStdoutData(p_str, TerminalColor.RED, TerminalColor.DEFAULT, TerminalStyle.NORMAL));
    }

    /**
     * Print an error message to the console + newline
     *
     * @param p_str
     *         String to print
     */
    public void printlnErr(final String p_str) {
        m_session.write(new TerminalStdoutData(p_str + '\n', TerminalColor.RED, TerminalColor.DEFAULT, TerminalStyle.NORMAL));
    }

    /**
     * Print an error to the console using a c-style formated string and arguments
     *
     * @param p_format
     *         Formating for the string
     * @param p_args
     *         Optional arguments
     */
    public void printfErr(final String p_format, final Object... p_args) {
        m_session.write(new TerminalStdoutData(String.format(p_format, p_args), TerminalColor.RED, TerminalColor.DEFAULT, TerminalStyle.NORMAL));
    }

    /**
     * Print an error to the console using a c-style formated string and arguments + newline
     *
     * @param p_format
     *         Formating for the string
     * @param p_args
     *         Optional arguments
     */
    public void printflnErr(final String p_format, final Object... p_args) {
        m_session.write(new TerminalStdoutData(String.format(p_format + '\n', p_args), TerminalColor.RED, TerminalColor.DEFAULT, TerminalStyle.NORMAL));
    }

    /**
     * Print to the console using a c-style formated string and arguments + newline
     *
     * @param p_format
     *         Formating for the string
     * @param p_args
     *         Optional arguments
     */
    public void printfln(final String p_format, final Object... p_args) {
        m_session.write(new TerminalStdoutData(String.format(p_format + '\n', p_args)));
    }
}
