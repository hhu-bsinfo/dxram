package de.hhu.bsinfo.dxram.monitoring.metric;

import java.util.ArrayList;
import java.util.Arrays;

import de.hhu.bsinfo.dxram.monitoring.MonitoringDataStructure;

/**
 * This helper class provides methods to calculate a single DataStructure from multiple ones using the median.
 *
 * @author Burak Akguel, burak.akguel@hhu.de, 08.06.2018
 */
public class MedianMetric extends Metric {

    public static MonitoringDataStructure calculate(ArrayList<MonitoringDataStructure> p_datas) {
        int numData = p_datas.size();
        float[][] floatTable = createFloatTable(p_datas);
        long[][] longTable = createLongTable(p_datas);

        for (int i = 0; i < floatTable.length; i++) {
            Arrays.sort(floatTable[i]);
            if (numData % 2 == 0) {
                floatTable[i][0] = 0.5f * (floatTable[i][numData / 2 - 1] + floatTable[i][numData / 2]);
            } else {
                floatTable[i][0] = floatTable[i][numData / 2];
            }
        }
        for (int i = 0; i < longTable.length; i++) {
            Arrays.sort(longTable[i]);
            if (numData % 2 == 0) {
                longTable[i][0] = (longTable[i][numData / 2 - 1] + longTable[i][numData / 2]) / 2;
            } else {
                longTable[i][0] = longTable[i][numData / 2];
            }
        }

        MonitoringDataStructure dataStructure = new MonitoringDataStructure(p_datas.get(0).getNid(), System.nanoTime());
        dataStructure.setCpuUsage(floatTable[0][0]);
        dataStructure.setCpuLoads(new float[] {floatTable[1][0], floatTable[2][0], floatTable[3][0]});
        dataStructure.setMemoryUsage(floatTable[4][0]);
        dataStructure.setNetworsStats(
                new float[] {floatTable[5][0], floatTable[6][0], floatTable[7][0], floatTable[8][0]});
        dataStructure.setDiskStats(new float[] {floatTable[9][0], floatTable[10][0]});
        dataStructure.setJvmMemStats(
                new float[] {floatTable[11][0], floatTable[12][0], floatTable[13][0], floatTable[14][0]});
        dataStructure.setJvmThreadsStats(
                new long[] {longTable[0][0], longTable[1][0], longTable[2][0], longTable[3][0]});

        return dataStructure;
    }

}
