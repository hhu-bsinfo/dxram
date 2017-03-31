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
     * Print the statistics of a specific recorder to the console.
     *
     * @param p_className
     *     Fully qualified name of the class including package location (or relative to de.hhu.bsinfo)
     */
    public static void printStatistics(final String p_className) {
        Class<?> clss;
        try {
            clss = Class.forName(p_className);
        } catch (final ClassNotFoundException e) {
            // check again with longest common prefix of package names
            try {
                clss = Class.forName("de.hhu.bsinfo." + p_className);
            } catch (final ClassNotFoundException ignored) {
                return;
            }
        }

        printStatistics(clss);
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
     * @param p_class
     *     Class this recorder was created for.
     */
    private static void printStatistics(final Class<?> p_class) {
        StatisticsRecorder recorder = StatisticsRecorderManager.getRecorder(p_class);
        if (recorder != null) {
            System.out.println(recorder);
        }
    }
}
