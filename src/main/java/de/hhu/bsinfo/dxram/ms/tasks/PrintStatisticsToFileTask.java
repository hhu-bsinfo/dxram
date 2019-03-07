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

package de.hhu.bsinfo.dxram.ms.tasks;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import com.google.gson.annotations.Expose;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.ms.Task;
import de.hhu.bsinfo.dxram.ms.Signal;
import de.hhu.bsinfo.dxram.ms.TaskContext;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.stats.StatisticsManager;

/**
 * Print the statistics to a file.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 22.04.2016
 */
public class PrintStatisticsToFileTask implements Task {

    private static final Logger LOGGER = LogManager.getFormatterLogger(PrintStatisticsToFileTask.class);

    @Expose
    private String m_path = "";

    /**
     * Constructor
     */
    public PrintStatisticsToFileTask() {

    }

    /**
     * Constructor
     *
     * @param p_path
     *         Filepath of the file to print to.
     */
    public PrintStatisticsToFileTask(final String p_path) {
        super();
        m_path = p_path;
    }

    @Override
    public int execute(final TaskContext p_ctx) {
        if (m_path == null) {
            return -1;
        }

        File file = new File(m_path);
        if (file.exists()) {
            if (!file.delete()) {

                LOGGER.error("Deleting file %s failed", file);

                return -2;
            }
            try {
                if (!file.createNewFile()) {

                    LOGGER.error("Creating output file %s for statistics failed", m_path);

                    return -3;
                }
            } catch (final IOException e) {

                LOGGER.error("Creating output file %s for statistics failed: %s", m_path, e);

                return -4;
            }
        }

        PrintStream out;
        try {
            out = new PrintStream(file);
        } catch (final FileNotFoundException e) {

            LOGGER.error("Creating print stream for statistics failed", e);

            return -5;
        }

        StatisticsManager.get().printStatistics(out);

        out.close();

        return 0;
    }

    @Override
    public void handleSignal(final Signal p_signal) {
        // ignore signals
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeString(m_path);
    }

    @Override
    public void importObject(final Importer p_importer) {
        m_path = p_importer.readString(m_path);
    }

    @Override
    public int sizeofObject() {
        return Integer.BYTES + m_path.getBytes(StandardCharsets.US_ASCII).length;
    }
}
