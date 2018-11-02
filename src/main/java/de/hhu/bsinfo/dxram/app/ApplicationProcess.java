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

package de.hhu.bsinfo.dxram.app;

import org.jetbrains.annotations.NotNull;

/**
 * @author Filip Krakowski, filip.krakowski@hhu.de, 02.11.18
 */
public class ApplicationProcess {

    private final int m_id;
    private final AbstractApplication m_application;
    private final long m_startTime;

    private static final String ARGS_DELIMITER = " ";

    public ApplicationProcess(final int p_id, final @NotNull AbstractApplication p_application) {
        m_id = p_id;
        m_application = p_application;
        m_startTime = System.currentTimeMillis();
    }

    /**
     * Returns the arguments provided to the application.
     *
     * @return The arguments provided to the application.
     */
    public String getArguments() {
        return m_application.getArguments();
    }

    /**
     * Returns this application's name.
     *
     * @return This application's name.
     */
    public String getName() {
        return m_application.getApplicationName();
    }

    /**
     * Returns this application's id.
     *
     * @return This application's id.
     */
    public int getId() {
        return m_id;
    }

    /**
     * Returns this application's elapsed time in milliseconds.
     *
     * @return This application's elapsed time in milliseconds.
     */
    public long getElapsedTime() {
        return System.currentTimeMillis() - m_startTime;
    }

    /**
     * Shuts down the underlying application.
     */
    void kill() {
        m_application.signalShutdown();
    }

    /**
     * Waits for this application to die.
     *
     * @throws InterruptedException
     */
    void join() throws InterruptedException {
        m_application.join();
    }
}
