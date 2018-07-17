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

package de.hhu.bsinfo.dxram.monitoring.metric;

import de.hhu.bsinfo.dxram.monitoring.MonitoringDataStructure;

import java.util.ArrayList;

/**
 * Metric Helper class which will load multiple collected DataStructures in a 2D grid
 *
 * @author Burak Akguel, burak.akguel@hhu.de, 14.07.2018
 */
public class Metric {
    /**
     * Helper method to put multiple data structure float variables in a 2D Grid.
     *
     * @param p_datas list of data structures
     * @return 2D grid
     */
    public static float[][] createFloatTable(final ArrayList<MonitoringDataStructure> p_datas) {
        int numData = p_datas.size();
        /******** Create Table *********/
        float[][] floatTable = new float[15][numData];

        for (int i = 0; i < numData; i++) {
            MonitoringDataStructure data = p_datas.get(i);
            // add cpu usage
            floatTable[0][i] = data.getCpuUsage();
            // add cpu loads
            float[] tmp = data.getCpuLoads();
            floatTable[1][i] = tmp[0];
            floatTable[2][i] = tmp[1];
            floatTable[3][i] = tmp[2];
            // add memory usage
            floatTable[4][i] = data.getMemoryUsage();
            // add network stats
            tmp = data.getNetworkStats();
            floatTable[5][i] = tmp[0];
            floatTable[6][i] = tmp[1];
            floatTable[7][i] = tmp[2];
            floatTable[8][i] = tmp[3];
            // add disk stats
            tmp = data.getDiskStats();
            floatTable[9][i] = tmp[0];
            floatTable[10][i] = tmp[1];
            // add jvm mem stats
            tmp = data.getJvmMemStats();
            floatTable[11][i] = tmp[0];
            floatTable[12][i] = tmp[1];
            floatTable[13][i] = tmp[2];
            floatTable[14][i] = tmp[3];
        }

        return floatTable;
    }

    /**
     * Helper method to store multiple data structure long variables in a 2D Grid.
     */
    public static long[][] createLongTable(final ArrayList<MonitoringDataStructure> p_datas) {
        int numData = p_datas.size();
        /******** Create Table *********/
        long[][] longTable = new long[4][numData];

        for (int i = 0; i < numData; i++) {
            MonitoringDataStructure data = p_datas.get(i);
            // add JVMThreadsState Values
            long[] tmp2 = data.getJvmThreadStats();

            for (int j = 0; j < 4; j++) {
                longTable[j][i] = tmp2[j];
            }
        }

        return longTable;
    }
}
