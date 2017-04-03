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

package de.hhu.bsinfo.dxram.stats;

import java.util.Collection;

/**
 * Recording usage statistics
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 23.03.2016
 */
public class Statistics {

    /**
     * Constructor
     */
    public Statistics() {

    }

    /**
     * Get all available statistic recorders
     *
     * @return Get recorders
     */
    public static Collection<StatisticsRecorder> getRecorders() {
        return StatisticsRecorderManager.getRecorders();
    }

    /**
     * Print the statistics of all created recorders to the console.
     */
    public static void printStatistics() {
        StatisticsRecorderManager.getRecorders().forEach(System.out::println);
    }

    /**
     * Reset all statistics
     */
    public static void resetStatistics() {
        for (StatisticsRecorder recorder : StatisticsRecorderManager.getRecorders()) {
            recorder.reset();
        }
    }

    /**
     * Print the statistics of a specific recorder to the console.
     *
     * @param p_recorderName
     *     Name of the recorder.
     */
    public static void printStatistics(final String p_recorderName) {
        StatisticsRecorder recorder = StatisticsRecorderManager.getRecorder(p_recorderName);
        if (recorder != null) {
            System.out.println(recorder);
        }
    }
}
