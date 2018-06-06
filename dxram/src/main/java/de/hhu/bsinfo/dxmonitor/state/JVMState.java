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

package de.hhu.bsinfo.dxmonitor.state;

import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import com.sun.tools.attach.VirtualMachine;

/**
 * @author Burak Akguel, burak.akguel@hhu.de, 23.11.17
 */

public class JVMState extends AbstractState {

    private JVM m_jvm;
    private MBeanServerConnection m_mbs;

    /**
     * Creates an jvm state for the process id.
     *
     * @param p_pid
     *     Process ID of the running JVM
     */
    public JVMState(final int p_pid) {
        try {
            VirtualMachine vm = VirtualMachine.attach(String.valueOf(p_pid));
            m_mbs = getMBSrvConn(vm);
            vm.detach();
        } catch (Exception e) {

            e.printStackTrace();
        }
        updateStats();
    }

    /**
     * Updates the JVM state.
     */
    @Override
    public void updateStats() {
        try {
            if (m_jvm == null) {
                m_jvm = new JVM(m_mbs);
            } else {
                m_jvm.updateStats();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns an MemoyUsage object. This object has an overview of the current heap memory usage.
     *
     * @return heap memory usage
     */
    public MemoryUsage getHeapMemoryUsage() {
        return m_jvm.m_memoryMXBean.getHeapMemoryUsage();
    }

    /**
     * Returns an MemoyUsage object. This object has an overview of the current non-heap memory usage.
     *
     * @return non-heap memory usage
     */
    public MemoryUsage getNonHeapMemoryUsage() {
        return m_jvm.m_memoryMXBean.getNonHeapMemoryUsage();
    }

    /**
     * Returns an list of garbage collectors.
     *
     * @return list of garbage collectors
     */
    public List<GarbageCollectorMXBean> getGarbageCollectors() {
        return m_jvm.m_garbageCollectorMXBeans;
    }

    /**
     * Returns an list with MemoryPoolMXBean object for each memory region in the jvm.
     *
     * @return List of memory usage overview for all memory pools
     */
    public List<MemoryPoolMXBean> getMemoryPools() {
        return m_jvm.m_memoryPoolMXBean;
    }

    /**
     * Returns a ThreadMXBean object. This object has informations about all running daemon and non-daemon threads.
     *
     * @return thread informations
     */
    public ThreadMXBean getThreads() {
        return m_jvm.m_threadMXBean;
    }

    /**
     * Connects via JMX and RMI to the (remote) java virtual machine.
     *
     * @param p_vm
     *     attached Virtuale Machine object
     * @return MBeanServerConnection to fetch the needed MXBean objects
     * @throws IOException
     */
    private MBeanServerConnection getMBSrvConn(final VirtualMachine p_vm) throws IOException {
        String connectorAddress = p_vm.getAgentProperties().getProperty("com.sun.management.jmxremote.localConnectorAddress");
        if (connectorAddress == null) {
            p_vm.startLocalManagementAgent();
            connectorAddress = p_vm.getAgentProperties().getProperty("com.sun.management.jmxremote.localConnectorAddress");
        }

        JMXServiceURL url = new JMXServiceURL(connectorAddress);
        JMXConnector connector = JMXConnectorFactory.connect(url);
        return connector.getMBeanServerConnection();
    }

    /**
     * Private jvm class. Represents a jvm state.
     */
    private static final class JVM {

        private MBeanServerConnection m_mbs;

        private MemoryMXBean m_memoryMXBean;
        private ThreadMXBean m_threadMXBean;
        private List<MemoryPoolMXBean> m_memoryPoolMXBean;
        private List<GarbageCollectorMXBean> m_garbageCollectorMXBeans;

        /**
         * Calls the updateStats() method to initialize the needed variables.
         *
         * @param p_mbs
         *     MBeanServerConnection
         * @throws IOException
         */
        private JVM(final MBeanServerConnection p_mbs) throws IOException {
            m_mbs = p_mbs;
            updateStats();
        }

        /**
         * Updates the MXBean objects of the jvm state by using the MBeansServerConnection to load the needed classes.
         */
        void updateStats() throws IOException {
            m_memoryMXBean = ManagementFactory.newPlatformMXBeanProxy(m_mbs, ManagementFactory.MEMORY_MXBEAN_NAME, MemoryMXBean.class);
            m_threadMXBean = ManagementFactory.newPlatformMXBeanProxy(m_mbs, ManagementFactory.THREAD_MXBEAN_NAME, ThreadMXBean.class);
            try {
                m_garbageCollectorMXBeans = getGarbageCollectorMXBeansFromRemote(m_mbs);
                m_memoryPoolMXBean = getMemoryPoolMXBeansFromRemote(m_mbs);
            } catch (MalformedObjectNameException e) {
                e.printStackTrace();
            }
        }

        /**
         * Returns a list of information about the memory pools.
         *
         * @param p_mbs
         * @return list of memory pools
         */
        private List<MemoryPoolMXBean> getMemoryPoolMXBeansFromRemote(final MBeanServerConnection p_mbs) throws MalformedObjectNameException, IOException {
            Set<ObjectName> gcnames = p_mbs.queryNames(new ObjectName(ManagementFactory.MEMORY_POOL_MXBEAN_DOMAIN_TYPE + ",name=*"), null);
            List<MemoryPoolMXBean> mBeans = new ArrayList<>(gcnames.size());
            for (ObjectName on : gcnames) {
                mBeans.add(ManagementFactory.newPlatformMXBeanProxy(p_mbs, on.toString(), MemoryPoolMXBean.class));
            }
            return mBeans;
        }

        /**
         * Returns a list of garbage collector informations.
         *
         * @param p_mBeanServerConn
         * @return list of garbage collectors
         */
        private List<GarbageCollectorMXBean> getGarbageCollectorMXBeansFromRemote(final MBeanServerConnection p_mBeanServerConn)
            throws MalformedObjectNameException, IOException {
            Set<ObjectName> gcnames = m_mbs.queryNames(new ObjectName(ManagementFactory.GARBAGE_COLLECTOR_MXBEAN_DOMAIN_TYPE + ",name=*"), null);
            List<GarbageCollectorMXBean> gcBeans = new ArrayList<>(gcnames.size());
            for (ObjectName on : gcnames) {
                gcBeans.add(ManagementFactory.newPlatformMXBeanProxy(m_mbs, on.toString(), GarbageCollectorMXBean.class));
            }
            return gcBeans;
        }
    }
}
