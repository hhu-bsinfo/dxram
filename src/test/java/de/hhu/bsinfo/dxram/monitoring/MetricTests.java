package de.hhu.bsinfo.dxram.monitoring;

import de.hhu.bsinfo.dxram.monitoring.metric.*;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.*;

public class MetricTests {

    private ArrayList<MonitoringDataStructure> dataStructures;

    @Before
    public void createDataSet() {
        dataStructures = new ArrayList<>();

        for (int i = 0; i < 10; i++) { // 10 data structures
            long[] longs = new long[4];
            float[] floats = new float[15];

            for (int j = 0; j < 15; j++) { // 15 floats
                floats[j] = j + i;
            }

            for (int j = 0; j < 4; j++) { // 4 longs
                longs[j] = 10 + i;
            }

            dataStructures.add(new MonitoringDataStructure((short)0, floats, longs, i));
        }
    }

    @Test
    public void average() {
        MonitoringDataStructure dataStructure = AverageMetric.calculate(dataStructures);

        assertEquals(4.5f, dataStructure.getCpuUsage(), 0);
        assertArrayEquals(new float[]{5.5f, 6.5f, 7.5f}, dataStructure.getCpuLoads(), 0);
        assertEquals(8.5f, dataStructure.getMemoryUsage(), 0);
        assertArrayEquals(new float[]{9.5f, 10.5f, 11.5f, 12.5f}, dataStructure.getNetworkStats(), 0);
        assertArrayEquals(new float[]{13.5f, 14.5f}, dataStructure.getDiskStats(), 0);
        assertArrayEquals(new float[]{15.5f, 16.5f, 17.5f, 18.5f}, dataStructure.getJvmMemStats(), 0);

        assertArrayEquals(new long[]{14, 14, 14, 14}, dataStructure.getJvmThreadStats()); // floor solutions
    }

    @Test
    public void maximum() {
        MonitoringDataStructure dataStructure = MaxMetric.calculate(dataStructures);

        assertEquals(9, dataStructure.getCpuUsage(), 0);
        assertArrayEquals(new float[]{10, 11, 12}, dataStructure.getCpuLoads(), 0);
        assertEquals(13, dataStructure.getMemoryUsage(), 0);
        assertArrayEquals(new float[]{14, 15, 16, 17}, dataStructure.getNetworkStats(), 0);
        assertArrayEquals(new float[]{18, 19}, dataStructure.getDiskStats(), 0);
        assertArrayEquals(new float[]{20, 21, 22, 23}, dataStructure.getJvmMemStats(), 0);

        assertArrayEquals(new long[]{19, 19, 19, 19}, dataStructure.getJvmThreadStats()); // floor solutions
    }

    @Test
    public void minimum() {
        MonitoringDataStructure dataStructure = MinMetric.calculate(dataStructures);

        assertEquals(0, dataStructure.getCpuUsage(), 0);
        assertArrayEquals(new float[]{1, 2, 3}, dataStructure.getCpuLoads(), 0);
        assertEquals(4, dataStructure.getMemoryUsage(), 0);
        assertArrayEquals(new float[]{5, 6, 7, 8}, dataStructure.getNetworkStats(), 0);
        assertArrayEquals(new float[]{9, 10}, dataStructure.getDiskStats(), 0);
        assertArrayEquals(new float[]{11, 12, 13, 14}, dataStructure.getJvmMemStats(), 0);

        assertArrayEquals(new long[]{10, 10, 10, 10}, dataStructure.getJvmThreadStats()); // floor solutions
    }

    @Test
    public void median1() {
        MonitoringDataStructure dataStructure = MedianMetric.calculate(dataStructures);

        assertEquals(4.5f, dataStructure.getCpuUsage(), 0);
        assertArrayEquals(new float[]{5.5f, 6.5f, 7.5f}, dataStructure.getCpuLoads(), 0);
        assertEquals(8.5f, dataStructure.getMemoryUsage(), 0);
        assertArrayEquals(new float[]{9.5f, 10.5f, 11.5f, 12.5f}, dataStructure.getNetworkStats(), 0);
        assertArrayEquals(new float[]{13.5f, 14.5f}, dataStructure.getDiskStats(), 0);
        assertArrayEquals(new float[]{15.5f, 16.5f, 17.5f, 18.5f}, dataStructure.getJvmMemStats(), 0);

        assertArrayEquals(new long[]{14, 14, 14, 14}, dataStructure.getJvmThreadStats()); // floor solutions
    }

    @Test
    public void median2() {
        dataStructures.remove(0);
        MonitoringDataStructure dataStructure = MedianMetric.calculate(dataStructures);

        assertEquals(5, dataStructure.getCpuUsage(), 0);
        assertArrayEquals(new float[]{6,7,8}, dataStructure.getCpuLoads(), 0);
        assertEquals(9, dataStructure.getMemoryUsage(), 0);
        assertArrayEquals(new float[]{10, 11, 12, 13}, dataStructure.getNetworkStats(), 0);
        assertArrayEquals(new float[]{14, 15}, dataStructure.getDiskStats(), 0);
        assertArrayEquals(new float[]{16, 17, 18, 19}, dataStructure.getJvmMemStats(), 0);

        assertArrayEquals(new long[]{15, 15, 15, 15}, dataStructure.getJvmThreadStats()); // floor solutions
    }

    @Test
    public void percentile() {
        MonitoringDataStructure dataStructure = PercentileMetric.calculate(dataStructures, 0.9f);

        int idx = (int) (0.9*dataStructures.size()) - 1;
        assertEquals(dataStructures.get(idx).getCpuUsage(), dataStructure.getCpuUsage(), 0);
        assertArrayEquals(dataStructures.get(idx).getCpuLoads(), dataStructure.getCpuLoads(), 0);
        assertEquals(dataStructures.get(idx).getMemoryUsage(), dataStructure.getMemoryUsage(), 0);
        assertArrayEquals(dataStructures.get(idx).getNetworkStats(), dataStructure.getNetworkStats(), 0);
        assertArrayEquals(dataStructures.get(idx).getDiskStats(), dataStructure.getDiskStats(), 0);
        assertArrayEquals(dataStructures.get(idx).getJvmMemStats(), dataStructure.getJvmMemStats(), 0);

        assertArrayEquals(dataStructures.get(idx).getJvmThreadStats(), dataStructure.getJvmThreadStats()); // floor solutions
    }
}
