package de.hhu.bsinfo.dxram.monitoring.metric;

import java.util.ArrayList;

import de.hhu.bsinfo.dxram.monitoring.MonitoringDataStructure;

/**
 * This helper class provides methods to calculate a single DataStructure from multiple ones using the average.
 *
 * @author Burak Akguel, burak.akguel@hhu.de, 08.06.2018
 */
public class AverageMetric extends Metric {

    public static MonitoringDataStructure calculate(final short p_ownNid, ArrayList<MonitoringDataStructure> p_datas) {
        int numData = p_datas.size();
        float[][] floatValues = createFloatTable(p_datas);
        long[][] longValues = createLongTable(p_datas);
        for (int i = 0; i < floatValues.length; i++) {
            for (int j = 1; j < numData; j++) {
                floatValues[i][0] += floatValues[i][j];
            }
            floatValues[i][0] /= numData;
        }
        for (int i = 0; i < longValues.length; i++) {
            for (int j = 1; j < numData; j++) {
                longValues[i][0] += longValues[i][j];
            }
            longValues[i][0] /= numData;
        }

        MonitoringDataStructure dataStructure = new MonitoringDataStructure(p_ownNid, System.nanoTime());
        dataStructure.setCpuUsage(floatValues[0][0]);
        dataStructure.setCpuLoads(new float[] {floatValues[1][0], floatValues[2][0], floatValues[3][0]});
        dataStructure.setMemoryUsage(floatValues[4][0]);
        dataStructure.setNetworsStats(
                new float[] {floatValues[5][0], floatValues[6][0], floatValues[7][0], floatValues[8][0]});
        dataStructure.setDiskStats(new float[] {floatValues[9][0], floatValues[10][0]});
        dataStructure.setJvmMemStats(
                new float[] {floatValues[11][0], floatValues[12][0], floatValues[13][0], floatValues[14][0]});
        dataStructure.setJvmThreadsStats(
                new long[] {longValues[0][0], longValues[1][0], longValues[2][0], longValues[3][0]});

        return dataStructure;
    }

}
