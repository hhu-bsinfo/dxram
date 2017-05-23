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
public final class Statistics {

    /**
     * Constructor
     */
    private Statistics() {

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
     * Get a statistics recorder by name
     *
     * @param p_recorderName
     *         Name of the recorder.
     * @return Statistics recorder or null if invalid.
     */
    public static StatisticsRecorder getRecorder(final String p_recorderName) {
        return StatisticsRecorderManager.getRecorder(p_recorderName);
    }

    /**
     * Reset all statistics
     */
    public static void resetStatistics() {
        for (StatisticsRecorder recorder : StatisticsRecorderManager.getRecorders()) {
            recorder.reset();
        }
    }
}
