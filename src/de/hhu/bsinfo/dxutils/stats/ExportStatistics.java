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

package de.hhu.bsinfo.dxutils.stats;

import java.io.File;
import java.util.Collection;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxnet.DXNetMain;

/**
 * Helper class to export statistics.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 29.11.2017
 */
public final class ExportStatistics {
    private static final Logger LOGGER = LogManager.getFormatterLogger(DXNetMain.class.getSimpleName());

    /**
     * Static class
     */
    private ExportStatistics() {

    }

    /**
     * Print the statistics to files.
     *
     * @param p_path
     *         The folder to write into.
     */
    public static void writeStatisticsToFile(final String p_path) {
        // #if LOGGER >= INFO
        LOGGER.info("Writing statistics to files...");
        // #endif /* LOGGER >= INFO */

        File folder = new File(p_path);
        if (folder.exists()) {
            File[] contents = folder.listFiles();
            if (contents != null) {
                for (File file : contents) {
                    if (!file.delete()) {
                        // #if LOGGER >= ERROR
                        LOGGER.error("Could not delete file (%s).", file.getName());
                        // #endif /* LOGGER >= ERROR */
                        return;
                    }
                }
            }
        } else {
            if (!new File(p_path).mkdirs()) {
                // #if LOGGER >= ERROR
                LOGGER.error("Could not create folder (%s) to write statistics to.", p_path);
                // #endif /* LOGGER >= ERROR */

                return;
            }
        }

        Collection<StatisticsRecorder> recorders = Statistics.getRecorders();
        for (StatisticsRecorder recorder : recorders) {
            recorder.writeStatisticsToFile(p_path);
        }
    }
}
