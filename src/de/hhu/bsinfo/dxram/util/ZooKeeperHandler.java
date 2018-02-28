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

package de.hhu.bsinfo.dxram.util;

import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * Class for accessing ZooKeeper
 *
 * @author Florian Klein, florian.klein@hhu.de, 02.12.2013
 */
public final class ZooKeeperHandler {

    private static final Logger LOGGER = LogManager.getFormatterLogger(ZooKeeperHandler.class.getSimpleName());

    // Attributes
    private ZooKeeper m_zookeeper;
    private String m_path;
    private String m_connection;
    private int m_timeout;

    // Constructors

    /**
     * Creates an instance of ZooKeeperHandler
     *
     * @param p_path
     *     Path in zookeeper to handle.
     * @param p_connection
     *     Connection string
     * @param p_timeout
     *     Timeout in ms
     */
    public ZooKeeperHandler(final String p_path, final String p_connection, final int p_timeout) {
        m_path = p_path;
        m_connection = p_connection;
        m_timeout = p_timeout;
    }

    // Methods

    /**
     * Returns the path
     *
     * @return the path
     */
    public String getPath() {
        return m_path;
    }

    /**
     * Connects to ZooKeeper
     *
     * @throws ZooKeeperException
     *     if ZooKeeper could not accessed
     */
    public synchronized void connect() throws ZooKeeperException {
        // #if LOGGER == TRACE
        LOGGER.trace("Entering connect");
        // #endif /* LOGGER == TRACE */

        if (m_zookeeper == null) {
            try {
                m_zookeeper = new ZooKeeper(m_connection, m_timeout, new ZooKeeperWatcher());

                if (!exists("")) {
                    m_zookeeper.create(m_path, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
            } catch (final IOException e) {
                throw new ZooKeeperException("Could not initialize ZooKeeper", e);
            } catch (final KeeperException e) {
                if (e.code() != KeeperException.Code.NODEEXISTS) {
                    throw new ZooKeeperException("Could not access ZooKeeper", e);
                }
            } catch (final InterruptedException e) {
                throw new ZooKeeperException("Could not access ZooKeeper", e);
            }
        }
        // #if LOGGER == TRACE
        LOGGER.trace(getClass().getName(), "Exiting connect");
        // #endif /* LOGGER == TRACE */
    }

    /**
     * Disconnects from ZooKeeper
     *
     * @throws ZooKeeperException
     *     if ZooKeeper could not accessed
     */
    public void close() throws ZooKeeperException {
        close(false);
    }

    /**
     * Disconnects from ZooKeeper
     *
     * @param p_delete
     *     if true alle ZooKeeper nodes are deleted
     * @throws ZooKeeperException
     *     if ZooKeeper could not accessed
     */
    public synchronized void close(final boolean p_delete) throws ZooKeeperException {
        if (m_zookeeper != null) {
            try {
                if (p_delete) {
                    ZKUtil.deleteRecursive(m_zookeeper, m_path);
                }
                m_zookeeper.close();
            } catch (final InterruptedException | KeeperException e) {
                throw new ZooKeeperException("Could not access ZooKeeper", e);
            }
            m_zookeeper = null;
        }
    }

    /**
     * Checks if a node exists
     *
     * @param p_path
     *     the node path
     * @return true if the node exists, false otherwise
     * @throws ZooKeeperException
     *     if ZooKeeper could not accessed
     */
    public boolean exists(final String p_path) throws ZooKeeperException {
        return getStatus(p_path, null) != null;
    }

    /**
     * Checks if a node exists
     *
     * @param p_path
     *     the node path
     * @param p_watcher
     *     the watcher
     * @return true if the node exists, fals eotherwise
     * @throws ZooKeeperException
     *     if ZooKeeper could not accessed
     */
    public boolean exists(final String p_path, final Watcher p_watcher) throws ZooKeeperException {
        return getStatus(p_path, p_watcher) != null;
    }

    /**
     * Gets the status of a node in ZooKeeper
     *
     * @param p_path
     *     the node path
     * @return true if the node exists, fals eotherwise
     * @throws ZooKeeperException
     *     if ZooKeeper could not accessed
     */
    public Stat getStatus(final String p_path) throws ZooKeeperException {
        return getStatus(p_path, null);
    }

    /**
     * Gets the status of a node in ZooKeeper
     *
     * @param p_path
     *     the node path
     * @param p_watcher
     *     the watcher
     * @return true if the node exists, fals eotherwise
     * @throws ZooKeeperException
     *     if ZooKeeper could not accessed
     */
    public Stat getStatus(final String p_path, final Watcher p_watcher) throws ZooKeeperException {
        Stat ret;

        assert p_path != null;

        try {
            if (m_zookeeper == null) {
                connect();
            }

            if (!p_path.isEmpty()) {
                ret = m_zookeeper.exists(m_path + '/' + p_path, p_watcher);
            } else {
                ret = m_zookeeper.exists(m_path, p_watcher);
            }
        } catch (final KeeperException | InterruptedException e) {
            throw new ZooKeeperException("Could not access ZooKeeper", e);
        }

        return ret;
    }

    /**
     * Creates a node in ZooKeeper
     *
     * @param p_path
     *     the node path
     * @throws ZooKeeperException
     *     if ZooKeeper could not accessed
     * @throws InterruptedException
     *     if connection to ZooKeeper is interrupted
     * @throws KeeperException
     *     if node already exists in ZooKeeper
     */
    public void create(final String p_path) throws ZooKeeperException, KeeperException, InterruptedException {
        create(p_path, new byte[0], CreateMode.PERSISTENT);
    }

    /**
     * Creates a node in ZooKeeper
     *
     * @param p_path
     *     the node path
     * @param p_data
     *     the node data
     * @throws ZooKeeperException
     *     if ZooKeeper could not accessed
     * @throws InterruptedException
     *     if connection to ZooKeeper is interrupted
     * @throws KeeperException
     *     if node already exists in ZooKeeper
     */
    public void create(final String p_path, final byte[] p_data) throws ZooKeeperException, KeeperException, InterruptedException {
        create(p_path, p_data, CreateMode.PERSISTENT);
    }

    /**
     * Deletes a node in ZooKeeper
     *
     * @param p_path
     *     the node path
     * @throws ZooKeeperException
     *     if ZooKeeper could not accessed
     */
    public void delete(final String p_path) throws ZooKeeperException {
        delete(p_path, -1);
    }

    /**
     * Deletes a node in ZooKeeper
     *
     * @param p_path
     *     the node path
     * @param p_version
     *     the node version (-1 for every version)
     * @throws ZooKeeperException
     *     if ZooKeeper could not accessed
     */
    public void delete(final String p_path, final int p_version) throws ZooKeeperException {
        assert p_path != null;

        try {
            if (m_zookeeper == null) {
                connect();
            }
            m_zookeeper.delete(m_path + '/' + p_path, p_version);
        } catch (final KeeperException | InterruptedException e) {
            throw new ZooKeeperException("Could not access ZooKeeper", e);
        }
    }

    /**
     * Creates a barrier node ein ZooKeeper
     *
     * @param p_path
     *     the barrier node path
     * @throws ZooKeeperException
     *     if ZooKeeper could not accessed
     * @throws InterruptedException
     *     if connection to ZooKeeper is interrupted
     * @throws KeeperException
     *     if node already exists in ZooKeeper
     */
    public void createBarrier(final String p_path) throws ZooKeeperException, KeeperException, InterruptedException {
        create(p_path);
    }

    /**
     * Deletes a barrier node ein ZooKeeper
     *
     * @param p_path
     *     the barrier node path
     * @throws ZooKeeperException
     *     if ZooKeeper could not accessed
     */
    public void deleteBarrier(final String p_path) throws ZooKeeperException {
        delete(p_path);
    }

    /**
     * Waits for the deletion of a barrier node ein ZooKeeper
     *
     * @param p_path
     *     the barrier node path
     * @param p_watcher
     *     the watcher
     * @throws ZooKeeperException
     *     if ZooKeeper could not accessed
     */
    public void waitForBarrier(final String p_path, final Watcher p_watcher) throws ZooKeeperException {
        while (exists(p_path, p_watcher)) {
            try {
                Thread.sleep(1000);
            } catch (final InterruptedException ignored) {
            }
        }
    }

    /**
     * Gets the child nodes of a node in ZooKeeper
     *
     * @param p_path
     *     the node path
     * @return the child nodes
     * @throws ZooKeeperException
     *     if ZooKeeper could not accessed
     */
    public List<String> getChildren(final String p_path) throws ZooKeeperException {
        return getChildren(p_path, null);
    }

    /**
     * Gets the child nodes of a node in ZooKeeper
     *
     * @param p_path
     *     the node path
     * @param p_watcher
     *     the watcher
     * @return the child nodes
     * @throws ZooKeeperException
     *     if ZooKeeper could not accessed
     */
    public List<String> getChildren(final String p_path, final Watcher p_watcher) throws ZooKeeperException {
        List<String> ret;

        try {
            if (m_zookeeper == null) {
                connect();
            }

            ret = m_zookeeper.getChildren(m_path + '/' + p_path, p_watcher);
        } catch (final KeeperException | InterruptedException e) {
            throw new ZooKeeperException("Could not access ZooKeeper", e);
        }

        return ret;
    }

    /**
     * Gets the data of a node in ZooKeeper
     *
     * @param p_path
     *     the node path
     * @return the node data
     * @throws ZooKeeperException
     *     if ZooKeeper could not accessed
     */
    public byte[] getData(final String p_path) throws ZooKeeperException {
        return getData(p_path, null, null);
    }

    /**
     * Gets the data of a node in ZooKeeper
     *
     * @param p_path
     *     the node path
     * @param p_status
     *     the node status
     * @return the node data
     * @throws ZooKeeperException
     *     if ZooKeeper could not accessed
     */
    public byte[] getData(final String p_path, final Stat p_status) throws ZooKeeperException {
        return getData(p_path, null, p_status);
    }

    /**
     * Gets the data of a node in ZooKeeper
     *
     * @param p_path
     *     the node path
     * @param p_watcher
     *     the watcher
     * @return the node data
     * @throws ZooKeeperException
     *     if ZooKeeper could not accessed
     */
    public byte[] getData(final String p_path, final Watcher p_watcher) throws ZooKeeperException {
        return getData(p_path, p_watcher, null);
    }

    /**
     * Gets the data of a node in ZooKeeper
     *
     * @param p_path
     *     the node path
     * @param p_watcher
     *     the watcher
     * @param p_status
     *     the node status
     * @return the node data
     * @throws ZooKeeperException
     *     if ZooKeeper could not accessed
     */
    public byte[] getData(final String p_path, final Watcher p_watcher, final Stat p_status) throws ZooKeeperException {
        byte[] ret;

        try {
            if (m_zookeeper == null) {
                connect();
            }

            ret = m_zookeeper.getData(m_path + '/' + p_path, p_watcher, p_status);
        } catch (final KeeperException | InterruptedException e) {
            throw new ZooKeeperException("Could not access ZooKeeper", e);
        }

        return ret;
    }

    /**
     * Sets the data of a node in ZooKeeper
     *
     * @param p_path
     *     the node path
     * @param p_data
     *     the node data
     * @throws ZooKeeperException
     *     if ZooKeeper could not accessed
     */
    public void setData(final String p_path, final byte[] p_data) throws ZooKeeperException {
        setData(p_path, p_data, -1);
    }

    /**
     * Sets the data of a node in ZooKeeper
     *
     * @param p_path
     *     the node path
     * @param p_data
     *     the node data
     * @param p_version
     *     the node version (-1 for every version)
     * @throws ZooKeeperException
     *     if ZooKeeper could not accessed
     */
    public void setData(final String p_path, final byte[] p_data, final int p_version) throws ZooKeeperException {
        assert p_path != null;
        assert p_data != null;

        try {
            if (m_zookeeper == null) {
                connect();
            }

            m_zookeeper.setData(m_path + '/' + p_path, p_data, p_version);
        } catch (final KeeperException | InterruptedException e) {
            throw new ZooKeeperException("Could not access ZooKeeper", e);
        }
    }

    /**
     * Sets an exists watcher
     *
     * @param p_path
     *     the node path
     * @param p_watcher
     *     the watcher
     * @throws ZooKeeperException
     *     if ZooKeeper could not accessed
     */
    public void setExistsWatch(final String p_path, final Watcher p_watcher) throws ZooKeeperException {
        try {
            if (m_zookeeper == null) {
                connect();
            }

            m_zookeeper.exists(p_path, p_watcher);
        } catch (final KeeperException | InterruptedException e) {
            throw new ZooKeeperException("Could not access ZooKeeper", e);
        }
    }

    /**
     * Sets a data watcher
     *
     * @param p_path
     *     the node path
     * @param p_watcher
     *     the watcher
     * @throws ZooKeeperException
     *     if ZooKeeper could not accessed
     */
    public void setDataWatch(final String p_path, final Watcher p_watcher) throws ZooKeeperException {
        try {
            if (m_zookeeper == null) {
                connect();
            }

            m_zookeeper.getData(p_path, p_watcher, null);
        } catch (final KeeperException | InterruptedException e) {
            throw new ZooKeeperException("Could not access ZooKeeper", e);
        }
    }

    /**
     * Sets a children watcher
     *
     * @param p_path
     *     the node path
     * @param p_watcher
     *     the watcher
     * @throws ZooKeeperException
     *     if ZooKeeper could not accessed
     */
    public void setChildrenWatch(final String p_path, final Watcher p_watcher) throws ZooKeeperException {
        try {
            if (m_zookeeper == null) {
                connect();
            }

            m_zookeeper.getChildren(m_path + '/' + p_path, p_watcher);
        } catch (final KeeperException | InterruptedException e) {
            throw new ZooKeeperException("Could not access ZooKeeper", e);
        }
    }

    /**
     * Executes multiple operations
     *
     * @param p_operations
     *     the operations to be executed
     * @throws ZooKeeperException
     *     if ZooKeeper could not accessed
     */
    public void executeOperations(final Iterable<Op> p_operations) throws ZooKeeperException {
        try {
            if (m_zookeeper == null) {
                connect();
            }

            m_zookeeper.multi(p_operations);
        } catch (final InterruptedException | KeeperException e) {
            throw new ZooKeeperException("Could not access ZooKeeper", e);
        }
    }

    /**
     * Creates a node in ZooKeeper
     *
     * @param p_path
     *     the node path
     * @param p_data
     *     the node data
     * @param p_mode
     *     the creation mode
     * @throws ZooKeeperException
     *     if ZooKeeper could not accessed
     * @throws InterruptedException
     *     if connection to ZooKeeper is interrupted
     * @throws KeeperException
     *     if node already exists in ZooKeeper
     */
    private void create(final String p_path, final byte[] p_data, final CreateMode p_mode) throws ZooKeeperException, KeeperException, InterruptedException {
        assert p_path != null;
        assert p_data != null;
        assert p_mode != null;

        if (m_zookeeper == null) {
            connect();
        }

        m_zookeeper.create(m_path + '/' + p_path, p_data, Ids.OPEN_ACL_UNSAFE, p_mode);
    }

    // Classes

    /**
     * Watcher class for ZooKeeper
     *
     * @author Florian Klein 06.12.2013
     */
    private static final class ZooKeeperWatcher implements Watcher {

        // Constructors

        /**
         * Creates an instance of ZooKeeperWatcher
         */
        private ZooKeeperWatcher() {
        }

        // Methods
        @Override
        public void process(final WatchedEvent p_event) {
            // #if LOGGER >= ERROR
            if (p_event.getType() == Event.EventType.None && p_event.getState() == KeeperState.Expired) {
                LOGGER.error(getClass().getName(), "ZooKeeper state expired");
            }
            // #endif /* LOGGER >= ERROR */
        }

    }

    /**
     * Exception for failed zookeeper accesses
     *
     * @author Florian Klein 09.03.2012
     */
    public static class ZooKeeperException extends Exception {

        // Constants
        private static final long serialVersionUID = 5917319024322071829L;

        // Constructors

        /**
         * Creates an instance of ZooKeeperException
         *
         * @param p_message
         *     the message
         */
        public ZooKeeperException(final String p_message) {
            super(p_message);
        }

        /**
         * Creates an instance of ZooKeeperException
         *
         * @param p_message
         *     the message
         * @param p_cause
         *     the cause
         */
        public ZooKeeperException(final String p_message, final Throwable p_cause) {
            super(p_message, p_cause);
        }

    }

}
