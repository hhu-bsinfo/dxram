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

package de.hhu.bsinfo.utils.stats;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Statistics Recorder Manager
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 27.10.2016
 */
public final class StatisticsRecorderManager {

    private static ReentrantLock ms_mapLock = new ReentrantLock(false);
    private static Map<String, StatisticsRecorder> ms_recorders = new HashMap<>();

    /**
     * Static class
     */
    private StatisticsRecorderManager() {

    }

    /**
     * Returns the recorders
     *
     * @return the recorders
     */
    public static Collection<StatisticsRecorder> getRecorders() {
        return ms_recorders.values();
    }

    /**
     * Returns the operation
     *
     * @param p_class
     *         the class
     * @param p_operationName
     *         the operation name
     * @return the operation
     */
    public static StatisticsOperation getOperation(final Class<?> p_class, final String p_operationName) {
        return getRecorder(p_class).getOperation(p_operationName);
    }

    /**
     * Returns the recorder
     *
     * @param p_name
     *         Get the recorder by its name
     * @return the recorder
     */
    public static StatisticsRecorder getRecorder(final String p_name) {
        StatisticsRecorder recorder = ms_recorders.get(p_name);
        if (recorder == null) {
            ms_mapLock.lock();
            recorder = ms_recorders.computeIfAbsent(p_name, mapper -> new StatisticsRecorder(p_name));
            ms_mapLock.unlock();
        }

        return recorder;
    }

    /**
     * Returns the recorder
     *
     * @param p_class
     *         the class
     * @return the recorder
     */
    private static StatisticsRecorder getRecorder(final Class<?> p_class) {
        return getRecorder(p_class.getSimpleName());
    }
}
