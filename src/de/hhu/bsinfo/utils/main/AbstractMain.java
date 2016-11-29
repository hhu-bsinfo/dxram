/*
 * Copyright (C) 2016 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

package de.hhu.bsinfo.utils.main;

import de.hhu.bsinfo.utils.ManifestHelper;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentListParser;
import de.hhu.bsinfo.utils.args.DefaultArgumentListParser;

/**
 * Framework for application execution with easier to handle argument list.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public abstract class AbstractMain {
    private String m_description = "";
    private ArgumentList m_arguments = new ArgumentList();
    private ArgumentListParser m_argumentsParser = new DefaultArgumentListParser();

    /**
     * Constructor
     *
     * @param p_description
     *         Short description for the application
     */
    protected AbstractMain(final String p_description) {
        m_description = p_description;
    }

    /**
     * Constructor
     *
     * @param p_argumentsParser
     *         Provide a different parser for the program arguments.
     * @param p_description
     *         Short description for the application
     */
    public AbstractMain(final ArgumentListParser p_argumentsParser, final String p_description) {
        m_argumentsParser = p_argumentsParser;
        m_description = p_description;
    }

    /**
     * Print build date and user (if available).
     * Requires the application to be built as a jar package.
     */
    public void printBuildDateAndUser() {
        String buildDate = getBuildDate();
        if (buildDate != null) {
            System.out.println("BuildDate: " + buildDate);
        }
        String buildUser = getBuildUser();
        if (buildUser != null) {
            System.out.println("BuildUser: " + buildUser);
        }
    }

    /**
     * Get the build date of the application. Has to be built as a jar file.
     *
     * @return If build date is available returns it as a string, null otherwise.
     */
    public String getBuildDate() {
        return ManifestHelper.getProperty(getClass(), "BuildDate");
    }

    /**
     * Get the user who built this application. Has to be built as a jar file.
     *
     * @return Name of the user who built this application if available, null otherwise.
     */
    public String getBuildUser() {
        return ManifestHelper.getProperty(getClass(), "BuildUser");
    }

    /**
     * Execute this application.
     *
     * @param p_args
     *         Arguments from Java main entry point.
     */
    public void run(final String[] p_args) {
        registerDefaultProgramArguments(m_arguments);
        m_argumentsParser.parseArguments(p_args, m_arguments);

        if (!m_arguments.checkArguments()) {
            String usage = m_arguments.createUsageDescription(getClass().getName());
            System.out.println(getClass().getName() + ": " + m_description + "\n");
            System.out.println(usage);
            System.exit(-1);
        }

        int exitCode = main(m_arguments);

        System.exit(exitCode);
    }

    /**
     * Implement this and provide default arguments the application expects.
     *
     * @param p_arguments
     *         Argument list for the application.
     */
    protected abstract void registerDefaultProgramArguments(ArgumentList p_arguments);

    /**
     * Implement this and treat it as your main function.
     *
     * @param p_arguments
     *         Arguments for the application.
     * @return Application exit code.
     */
    protected abstract int main(ArgumentList p_arguments);
}
