package de.hhu.bsinfo.dxram.monitoring;

import java.lang.management.MemoryPoolMXBean;
import java.util.Arrays;

import de.hhu.bsinfo.dxmonitor.monitor.CpuMonitor;
import de.hhu.bsinfo.dxmonitor.monitor.DiskMonitor;
import de.hhu.bsinfo.dxmonitor.monitor.JVMMemMonitor;
import de.hhu.bsinfo.dxmonitor.monitor.JVMThreadsMonitor;
import de.hhu.bsinfo.dxmonitor.monitor.MemMonitor;
import de.hhu.bsinfo.dxmonitor.monitor.Monitor;
import de.hhu.bsinfo.dxmonitor.monitor.NetworkMonitor;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

/**
 * Monitoring Data structure class
 *
 * @author Burak Akguel, burak.akguel@hhu.de, 08.06.2018
 */
public class MonitoringDataStructure extends DataStructure {
    private long m_timestamp;

    private short m_nid;
    // CPU Data
    private float m_cpuUsage;
    private float[] m_loads;
    // Memory Data
    private float m_memoryUsage;
    // Network Data
    private float m_rxThroughput;
    private float m_rxError;
    private float m_txThroughput;
    private float m_txError;
    // Disk Data
    private float m_readPercent;
    private float m_writePercent;
    // JVM Mem Data
    private float m_jvmHeapUsage;
    private float m_jvmEdenUsage;
    private float m_jvmSurvivorUsage;
    private float m_jvmOldUsage;
    // JVM Threads Data
    private long m_jvmDaemonThreadCnt;
    private long m_jvmNonDaemonThreadCnt;
    private long m_jvmThreadCount;
    private long m_jvmPeakThreadCnt;

    /****** CONSTRUCTOR ******/
    public MonitoringDataStructure(short p_nid, long p_timestamp) {
        m_nid = p_nid;

        m_timestamp = p_timestamp;

        // default
        m_cpuUsage = 0;
        m_loads = new float[3];
        for (int i = 0; i < 3; i++) {
            m_loads[i] = 0;
        }

        m_memoryUsage = 0;

        m_rxThroughput = 0;
        m_rxError = 0;
        m_txThroughput = 0;
        m_txError = 0;

        m_readPercent = 0;
        m_writePercent = 0;

        m_jvmHeapUsage = 0;
        m_jvmEdenUsage = 0;
        m_jvmSurvivorUsage = 0;
        m_jvmOldUsage = 0;

        m_jvmDaemonThreadCnt = 0;
        m_jvmNonDaemonThreadCnt = 0;
        m_jvmThreadCount = 0;
        m_jvmPeakThreadCnt = 0;
    }

    public MonitoringDataStructure(short p_nid, float[] p_data, long[] p_data2, long p_timestamp) {
        m_nid = p_nid;

        m_timestamp = p_timestamp;

        // CPU
        m_cpuUsage = p_data[0];
        m_loads = new float[3];
        m_loads[0] = p_data[1];
        m_loads[1] = p_data[2];
        m_loads[2] = p_data[3];
        // MEMORY
        m_memoryUsage = p_data[4];
        // NETWORK
        m_rxThroughput = p_data[5];
        m_rxError = p_data[6];
        m_txThroughput = p_data[7];
        m_txError = p_data[8];
        // DISK
        m_readPercent = p_data[9];
        m_writePercent = p_data[10];
        // JVM Mem
        m_jvmHeapUsage = p_data[11];
        m_jvmEdenUsage = p_data[12];
        m_jvmSurvivorUsage = p_data[13];
        m_jvmOldUsage = p_data[14];
        // jvm Thread
        m_jvmDaemonThreadCnt = p_data2[0];
        m_jvmNonDaemonThreadCnt = p_data2[1];
        m_jvmThreadCount = p_data2[2];
        m_jvmPeakThreadCnt = p_data2[3];

    }

    /****** DATASTRUCTURE FUNCTIONS ******/

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeLong(m_timestamp);
        // CPU
        p_exporter.writeFloat(m_cpuUsage);
        p_exporter.writeInt(m_loads.length);
        for (int i = 0; i < m_loads.length; i++) {
            p_exporter.writeFloat(m_loads[i]);
        }
        // MEMORY
        p_exporter.writeFloat(m_memoryUsage);
        // NETWORK
        p_exporter.writeFloat(m_rxThroughput);
        p_exporter.writeFloat(m_rxError);
        p_exporter.writeFloat(m_txThroughput);
        p_exporter.writeFloat(m_txError);
        // DISK
        p_exporter.writeFloat(m_readPercent);
        p_exporter.writeFloat(m_writePercent);
        // JVM
        p_exporter.writeFloat(m_jvmHeapUsage);
        p_exporter.writeFloat(m_jvmEdenUsage);
        p_exporter.writeFloat(m_jvmSurvivorUsage);
        p_exporter.writeFloat(m_jvmOldUsage);

        p_exporter.writeLong(m_jvmDaemonThreadCnt);
        p_exporter.writeLong(m_jvmNonDaemonThreadCnt);
        p_exporter.writeLong(m_jvmThreadCount);
        p_exporter.writeLong(m_jvmPeakThreadCnt);
    }

    @Override
    public void importObject(Importer p_importer) {
        m_timestamp = p_importer.readLong(m_timestamp);
        // CPU
        m_cpuUsage = p_importer.readFloat(m_cpuUsage);
        int length = p_importer.readInt(3);
        m_loads = new float[length];
        for (int i = 0; i < length; i++) {
            m_loads[i] = p_importer.readFloat(m_loads[i]);
        }
        // MEMORY
        m_memoryUsage = p_importer.readFloat(m_memoryUsage);
        // NETWORK
        m_rxThroughput = p_importer.readFloat(m_rxThroughput);
        m_rxError = p_importer.readFloat(m_rxError);
        m_txThroughput = p_importer.readFloat(m_txThroughput);
        m_txError = p_importer.readFloat(m_txError);
        // DISK
        m_readPercent = p_importer.readFloat(m_readPercent);
        m_writePercent = p_importer.readFloat(m_writePercent);
        // JVM
        m_jvmHeapUsage = p_importer.readFloat(m_jvmHeapUsage);
        m_jvmEdenUsage = p_importer.readFloat(m_jvmEdenUsage);
        m_jvmSurvivorUsage = p_importer.readFloat(m_jvmSurvivorUsage);
        m_jvmOldUsage = p_importer.readFloat(m_jvmOldUsage);

        m_jvmDaemonThreadCnt = p_importer.readLong(m_jvmDaemonThreadCnt);
        m_jvmNonDaemonThreadCnt = p_importer.readLong(m_jvmNonDaemonThreadCnt);
        m_jvmThreadCount = p_importer.readLong(m_jvmThreadCount);
        m_jvmPeakThreadCnt = p_importer.readLong(m_jvmPeakThreadCnt);

    }

    @Override
    public int sizeofObject() {
        return Long.BYTES + 15 * Float.BYTES + Integer.BYTES + 4 * Long.BYTES;
    }

    @Override
    public String toString() {
        return String.format("==MonitoringData Node: %s==\n" + "Timestamp : %s\n" + "Cpu: Usage: %f, Loads: %s\n" +
                        "Memory: Usage: %f\n" +
                        "Network: rThroughput: %f, tThroughput: %f\n" + "Disk: Reads: %f, Writes: %f\n" +
                        "JVM: Heap: %f, Eden: %f, Survivor: %f, Old: %f\n\n",
                NodeID.toHexString(m_nid), String.valueOf(m_timestamp), m_cpuUsage, Arrays.toString(m_loads),
                m_memoryUsage, m_rxThroughput, m_txThroughput,
                m_readPercent, m_writePercent, m_jvmHeapUsage, m_jvmEdenUsage, m_jvmSurvivorUsage,
                m_jvmOldUsage); // todo add jvm thread stats
    }

    /**
     * Get needed information from certain components.
     *
     * @param p_monitor
     */
    void fillWithData(Monitor p_monitor) {
        if (p_monitor instanceof CpuMonitor) {
            m_cpuUsage = ((CpuMonitor) p_monitor).getProgress().getCpuUsage();
            m_loads = ((CpuMonitor) p_monitor)
                    .getLoads(); // todo think of a better way (creating a new float array every getLoads call is expensive)
        } else if (p_monitor instanceof MemMonitor) {
            m_memoryUsage = ((MemMonitor) p_monitor).getState().getUsedPercent();
        } else if (p_monitor instanceof DiskMonitor) {
            m_readPercent = ((DiskMonitor) p_monitor).getProgress().getReadUsagePercentage();
            m_writePercent = ((DiskMonitor) p_monitor).getProgress().getWriteUsagePercentage();
        } else if (p_monitor instanceof NetworkMonitor) {
            m_rxThroughput = ((NetworkMonitor) p_monitor).getProgress().getReceiveThroughput();
            m_rxError = ((NetworkMonitor) p_monitor).getProgress().getReceivePacketErrorCount();
            m_txThroughput = ((NetworkMonitor) p_monitor).getProgress().getTransmitThroughput();
            m_txError = ((NetworkMonitor) p_monitor).getProgress().getTransmitPacketErrorCount();
        } else if (p_monitor instanceof JVMMemMonitor) {
            JVMMemMonitor monitor = (JVMMemMonitor) p_monitor;
            m_jvmHeapUsage = monitor.getState().getHeapUsage().getUsed();
            for (MemoryPoolMXBean p : monitor.getState().getListMemoryPoolMXBean()) {
                if (p.getName().contains("Eden")) {
                    m_jvmEdenUsage = p.getUsage().getUsed();
                } else if (p.getName().contains("Old")) {
                    m_jvmOldUsage = p.getUsage().getUsed();
                } else if (p.getName().contains("Survivor")) {
                    m_jvmSurvivorUsage = p.getUsage().getUsed();
                }
            }
        } else if (p_monitor instanceof JVMThreadsMonitor) {
            JVMThreadsMonitor monitor = (JVMThreadsMonitor) p_monitor;
            m_jvmDaemonThreadCnt = monitor.getState().getDaemonThreadCount();
            m_jvmNonDaemonThreadCnt = monitor.getState().getNonDaemonThreadCount();
            m_jvmThreadCount = monitor.getState().getThreadCount();
            m_jvmPeakThreadCnt = monitor.getState().getPeakThreadCount();
        }

    }

    /****** GETTERS ******/
    // will be used by the PeerMonitoringHandler class to fill the Monitoring Data Message and calculate the average
    public float getCpuUsage() {
        return m_cpuUsage;
    }

    public float[] getCpuLoads() {
        return m_loads;
    }

    public float getMemoryUsage() {
        return m_memoryUsage;
    }

    public float[] getNetworkStats() {
        return new float[] {m_rxThroughput, m_rxError, m_txThroughput, m_txError};
    }

    public float[] getDiskStats() {
        return new float[] {m_readPercent, m_writePercent};
    }

    public float[] getJvmMemStats() {
        return new float[] {m_jvmHeapUsage, m_jvmEdenUsage, m_jvmSurvivorUsage, m_jvmOldUsage};
    }

    public long[] getJvmThreadStats() {
        return new long[] {m_jvmDaemonThreadCnt, m_jvmNonDaemonThreadCnt, m_jvmThreadCount, m_jvmPeakThreadCnt};
    }

    public long getTimestamp() {
        return m_timestamp;
    }

    public short getNid() {
        return m_nid;
    }

    /****** SETTERS ******/
    public void setCpuUsage(float p_usage) {
        m_cpuUsage = p_usage;
    }

    public void setCpuLoads(float[] p_loads) {
        m_loads = p_loads;
    }

    public void setMemoryUsage(float p_usage) {
        m_memoryUsage = p_usage;
    }

    public void setNetworsStats(float[] p_stats) {
        m_rxThroughput = p_stats[0];
        m_rxError = p_stats[1];
        m_txThroughput = p_stats[2];
        m_txError = p_stats[3];
    }

    public void setDiskStats(float[] p_stats) {
        m_readPercent = p_stats[0];
        m_writePercent = p_stats[1];
    }

    public void setJvmMemStats(float[] p_stats) {
        m_jvmHeapUsage = p_stats[0];
        m_jvmEdenUsage = p_stats[1];
        m_jvmSurvivorUsage = p_stats[2];
        m_jvmOldUsage = p_stats[3];
    }

    public void setJvmThreadsStats(long[] p_stats) {
        m_jvmDaemonThreadCnt = p_stats[0];
        m_jvmNonDaemonThreadCnt = p_stats[1];
        m_jvmThreadCount = p_stats[2];
        m_jvmPeakThreadCnt = p_stats[3];
    }

}
