/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

package de.hhu.bsinfo.dxmonitor.component;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.List;

import de.hhu.bsinfo.dxmonitor.state.JVMState;

/**
 * Checks if the JVM is not in an invalid state by checking if the thresholds have not been exceeded. This class
 * can also be used to get an overview of the component.
 *
 * @author Burak Akguel, burak.akguel@hhu.de, 23.11.17
 */
public class JVMComponent extends AbstractComponent {

    private JVMState m_currentState;

    private int m_pid;

    //mem variables
    private float m_heapThreshold;
    private float m_nonheapThreshold;
    private float m_edenThreshold;
    private float m_survivorThreshold;
    private float m_oldThreshold;
    private float m_codecacheThreshold;
    private float m_metaspaceThreshold;
    private float m_compressedClassThreshold;

    private float m_heapUsed;
    private float m_heapCommited;
    private float m_nonHeapUsed;
    private float m_nonHeapCommited;

    private float m_edenUsed;
    private float m_edenCommitted;
    private float m_survivorUsed;
    private float m_survivorCommitted;
    private float m_oldUsed;
    private float m_oldCommitted;
    private float m_codeCacheUsed;
    private float m_codeCacheCommited;
    private float m_metaspaceUsed;
    private float m_metaspaceCommited;
    private float m_compressedCommited;
    private float m_compressedUsed;

    //thread variables
    private int m_liveThreadCount;
    private int m_daemonThreadCount;
    private long[] m_deadlockedThreads;
    private boolean m_isDeadlock = false;

    //gc variables
    private long[] m_gcCounts;
    private long[] m_gcTime;
    private String[] m_gcNames;

    /**
     * This constructor should be used in callback mode. All threshold values should be entered in percent (e.g. 0.25
     * 25 %)
     *
     * @param p_pid
     *     process id of the jvm
     * @param p_heap
     *     heap usage threshold
     * @param p_nonheap
     *     nonheap usage threshold
     * @param p_eden
     *     eden space usage threshold
     * @param p_survivor
     *     survivor space usage threshold
     * @param p_old
     *     old space usage threshold
     * @param p_codecache
     *     codecache space usage threshold
     * @param p_metaspace
     *     metaspace space usage threshold
     * @param p_compressed
     *     compressed space usage threshold
     */
    public JVMComponent(final int p_pid, final float p_heap, final float p_nonheap, final float p_eden, final float p_survivor, final float p_old,
        final float p_codecache, final float p_metaspace, final float p_compressed) {

        m_callback = true;
        m_pid = p_pid;
        m_heapThreshold = p_heap;
        m_nonheapThreshold = p_nonheap;
        m_edenThreshold = p_eden;
        m_survivorThreshold = p_survivor;
        m_oldThreshold = p_old;
        m_codecacheThreshold = p_codecache;
        m_metaspaceThreshold = p_metaspace;
        m_compressedClassThreshold = p_compressed;

        m_currentState = new JVMState(m_pid);
    }

    /**
     * This constructor should be used in overview mode.
     *
     * @param p_pid
     *     process id of the jvm
     */
    public JVMComponent(final int p_pid, final float p_secondDelay) {
        m_callback = false;
        m_pid = p_pid;

        m_secondDelay = p_secondDelay;

        m_currentState = new JVMState(m_pid);
    }

    /**
     * @see AbstractComponent
     */
    @Override
    public void analyse() {
        StringBuilder callBack = new StringBuilder("JVM pid " + m_pid + ":\n");
        if (m_heapThreshold - m_heapUsed / m_heapCommited < 0) {
            callBack.append("Heap Usage Callback\n");
        }
        if (m_nonheapThreshold - m_nonHeapUsed / m_nonHeapCommited < 0) {
            callBack.append("Non Heap Usage Callback\n");
        }
        if (m_edenThreshold - m_edenUsed / m_edenCommitted < 0) {
            callBack.append("Eden Space Usage Callback\n");
        }
        if (m_survivorThreshold - m_survivorUsed / m_survivorCommitted < 0) {
            callBack.append("Survivor Space Usage Callback\n");
        }
        if (m_oldThreshold - m_oldUsed / m_oldCommitted < 0) {
            callBack.append("Old Space Usage Callback\n");
        }
        if (m_metaspaceThreshold - m_metaspaceUsed / m_metaspaceCommited < 0) {
            callBack.append("Metaspace Usage Callback\n");
        }
        if (m_compressedClassThreshold - m_compressedUsed / m_compressedCommited < 0) {
            callBack.append("Compressed Class Usage Callback\n");
        }
        if (m_codecacheThreshold - m_codeCacheUsed / m_codeCacheCommited < 0) {
            callBack.append("Codecache Class Usage Callback\n");
        }
        if (m_deadlockedThreads != null) {
            callBack.append("Deadlocked Threads ").append(Arrays.toString(m_deadlockedThreads)).append(" Callback\n");
        }

        String tmp = callBack.toString();
        //if(tmp.equals("JVM pid "+pid+":\n")) output = tmp+"no callbacks!\n";
        //else output = tmp;
    }

    /**
     * @see AbstractComponent
     */
    @Override
    public void updateData() {
        m_currentState.updateStats();
    }

    /**
     * @see AbstractComponent
     */
    @Override
    public void calculate() {

        //calc heap + nonheap stats
        MemoryUsage heap = m_currentState.getHeapMemoryUsage();
        m_heapUsed = heap.getUsed();
        m_heapCommited = heap.getCommitted();
        MemoryUsage nonheap = m_currentState.getNonHeapMemoryUsage();
        m_nonHeapUsed = nonheap.getUsed();
        m_nonHeapCommited = nonheap.getCommitted();

        // calc Memory Region stats
        List<MemoryPoolMXBean> pools = m_currentState.getMemoryPools();
        for (MemoryPoolMXBean p : pools) {
            if (p.getName().contains("Eden")) {
                m_edenUsed = p.getUsage().getUsed();
                m_edenCommitted = p.getUsage().getCommitted();
            } else if (p.getName().contains("Old")) {
                m_oldUsed = p.getUsage().getUsed();
                m_oldCommitted = p.getUsage().getCommitted();
            } else if (p.getName().contains("Survivor")) {
                m_survivorUsed = p.getUsage().getUsed();
                m_survivorCommitted = p.getUsage().getCommitted();
            } else if (p.getName().contains("Cache")) {
                m_codeCacheUsed = p.getUsage().getUsed();
                m_codeCacheCommited = p.getUsage().getCommitted();
            } else if (p.getName().contains("Meta")) {
                m_metaspaceUsed = p.getUsage().getUsed();
                m_metaspaceCommited = p.getUsage().getUsed();
            } else if (p.getName().contains("Compres")) {
                m_compressedCommited = p.getUsage().getCommitted();
                m_compressedUsed = p.getUsage().getUsed();
            }
        }

        // calculate threadstats
        ThreadMXBean threadMXBean = m_currentState.getThreads();
        m_liveThreadCount = threadMXBean.getThreadCount();
        m_daemonThreadCount = threadMXBean.getDaemonThreadCount();
        m_deadlockedThreads = threadMXBean.findDeadlockedThreads();
        m_isDeadlock = m_deadlockedThreads != null;

        List<GarbageCollectorMXBean> gcs = m_currentState.getGarbageCollectors();
        int numgc = gcs.size();
        m_gcCounts = new long[numgc];
        m_gcNames = new String[numgc];
        m_gcTime = new long[numgc];
        for (int i = 0; i < numgc; i++) {
            GarbageCollectorMXBean g = gcs.get(i);
            m_gcCounts[i] = g.getCollectionCount();
            m_gcNames[i] = g.getName();
            m_gcTime[i] = g.getCollectionTime();

        }

    }

    /**
     * @see AbstractComponent
     */
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("GC Stats - \n\t");
        for (int i = 0; i < m_gcCounts.length; i++) {
            builder.append("type: ").append(m_gcNames[i]).append("\t time spent: ").append(m_gcTime[i] / 1000.0).append("s\t").append(m_gcCounts[i])
                .append(" collections\n\t");
        }

        return String.format("======== JVM ========\n" + "Thread Stats - \n\tCount: %d\tDaemon Count: %d\t Deadlock: %s\n" +
                "Heap Stats - \n\tCommitedSize: %d Bytes\t Use: %d Bytes\tUsage: %1.2f\n" +
                "Non Heap Stats - \n\tCommitedSize: %d Bytes\t Use: %d Bytes\tUsage: %1.2f\n" +
                "Metaspace - \n\tCommitedSize: %d Bytes\t Use: %d Bytes\tUsage: %1.2f\n" +
                "Compressed Class Space - \n\tCommitedSize: %d Bytes\t Use: %d Bytes\tUsage: %1.2f\n" +
                "Code Cache - \n\tCommitedSize: %d Bytes\t Use: %d Bytes\tUsage: %1.2f\n" +
                "Par Eden Space - \n\tCommitedSize: %d Bytes\t Use: %d Bytes\tUsage: %1.2f\n" +
                "Par Survivor Space - \n\tCommitedSize: %d Bytes\t Use: %d Bytes\tUsage: %1.2f\n" +
                "CMS Old Gen - \n\tCommitedSize: %d Bytes\t Use: %d Bytes\tUsage: %1.2f\n" + "%s", m_liveThreadCount, m_daemonThreadCount,
            m_isDeadlock ? "Yes\t" + Arrays.toString(m_deadlockedThreads) : "No", (long) m_heapCommited, (long) m_heapUsed, m_heapUsed / m_heapCommited,
            (long) m_nonHeapCommited, (long) m_nonHeapUsed, m_nonHeapUsed / m_nonHeapCommited, (long) m_metaspaceCommited, (long) m_metaspaceUsed,
            m_metaspaceUsed / m_metaspaceCommited, (long) m_compressedCommited, (long) m_compressedUsed, m_compressedUsed / m_compressedCommited,
            (long) m_codeCacheUsed, (long) m_codeCacheCommited, m_codeCacheUsed / m_codeCacheCommited, (long) m_edenCommitted, (long) m_edenUsed,
            m_edenUsed / m_edenCommitted, (long) m_survivorCommitted, (long) m_survivorUsed, m_survivorUsed / m_survivorCommitted, (long) m_oldCommitted,
            (long) m_oldUsed, m_oldUsed / m_oldCommitted, builder.toString());
    }

    public float getHeapUsage() {
        return m_heapUsed;
    }

    public float getEdenUsage() {
        return m_edenUsed;
    }

    public float getSurvivorUsage() {
        return m_survivorUsed;
    }

    public float getOldUsage() {
        return m_oldUsed;
    }

}
