package de.hhu.bsinfo.dxram.monitoring.metric;

import de.hhu.bsinfo.dxram.monitoring.MonitoringDataStructure;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * This helper class provides methods to calculate a single AbstractChunk from multiple ones using the percentile.
 *
 * @author Burak Akguel, burak.akguel@hhu.de, 14.07.2018
 */
public class PercentileMetric extends Metric {
    /**
     * Helper method which calculates a single data structure from multiple ones using the percentile.
     *
     * @param p_k kth-Percentile - use numbers from 0 to 1 -> example 25 Perctile <=> 0.25
     */
    public static MonitoringDataStructure calculate(final ArrayList<MonitoringDataStructure> p_datas,
                                                    final float p_k) {
        if (p_k <= 0.0f || p_k >= 1.0f) {
            throw new IllegalArgumentException("Percentile argument 'p_kth' must be in (0.0, 1.0)!");
        }

        float[][] floatTable = createFloatTable(p_datas);
        long[][] longTable = createLongTable(p_datas);

        int numData = p_datas.size();
        int index = (int) Math.ceil(numData * p_k) - 1;

        for (int i = 0; i < floatTable.length; i++) {
            Arrays.sort(floatTable[i]);
            floatTable[i][0] = floatTable[i][index];
        }

        for (int i = 0; i < longTable.length; i++) {
            Arrays.sort(longTable[i]);
            longTable[i][0] = longTable[i][index];
        }

        MonitoringDataStructure dataStructure = new MonitoringDataStructure(p_datas.get(0).getNid(), System.nanoTime());
        dataStructure.setCpuUsage(floatTable[0][0]);
        dataStructure.setCpuLoads(new float[]{floatTable[1][0], floatTable[2][0], floatTable[3][0]});
        dataStructure.setMemoryUsage(floatTable[4][0]);
        dataStructure.setNetworsStats(
                new float[]{floatTable[5][0], floatTable[6][0], floatTable[7][0], floatTable[8][0]});
        dataStructure.setDiskStats(new float[]{floatTable[9][0], floatTable[10][0]});
        dataStructure.setJvmMemStats(
                new float[]{floatTable[11][0], floatTable[12][0], floatTable[13][0], floatTable[14][0]});
        dataStructure.setJvmThreadsStats(
                new long[]{longTable[0][0], longTable[1][0], longTable[2][0], longTable[3][0]});

        return dataStructure;
    }
}
