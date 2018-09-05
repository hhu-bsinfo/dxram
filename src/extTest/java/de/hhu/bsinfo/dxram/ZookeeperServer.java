package de.hhu.bsinfo.dxram;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import de.hhu.bsinfo.dxutils.FileSystemUtils;

/**
 * Wrapper class to start a zookeeper server before running DXRAM (for tests)
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
class ZookeeperServer {
    private static final String ZOOKEEPER_SERVER = "bin/zkServer.sh";

    private final String m_path;

    private final String m_dataDir;
    private final int m_port;

    private Process m_process;

    /**
     * Constructor
     *
     * @param p_path
     *         Path to root dir of zookeeper installation (includes folders like bin, conf)
     */
    ZookeeperServer(final String p_path) {
        m_path = p_path;

        if (!new File(m_path).exists()) {
            throw new RuntimeException("Zookeeper root path does not exist: " + p_path);
        }

        Properties prop = getConfigParameter();

        m_dataDir = prop.getProperty("dataDir");
        m_port = Integer.parseInt(prop.getProperty("clientPort"));
    }

    /**
     * Start the zookeeper server
     *
     * @return True if successful, false otherwise
     */
    public boolean start() {
        deleteZookeeperDataDir();

        try {
            m_process = new ProcessBuilder().inheritIO().command(
                    m_path + '/' + ZOOKEEPER_SERVER, "start").start();
        } catch (final IOException ignored) {
            return false;
        }

        cleanupEntries();

        return true;
    }

    /**
     * Shutdown the running zookeeper server and cleanup resources created by it
     */
    public void shutdown() {
        m_process.destroy();

        deleteZookeeperDataDir();
        new File("zookeeper.out").delete();
    }

    /**
     * Delete the zookeeper data directory (contains zookeeper log)
     */
    private void deleteZookeeperDataDir() {
        File file = new File(m_dataDir);

        if (file.exists()) {
            try {
                FileSystemUtils.deleteRecursively(file);
            } catch (IOException e) {
                throw new RuntimeException(
                        "Deleting zookeeper data dir '" + file.getAbsolutePath() + "' failed: " + e.getMessage());
            }
        }
    }

    /**
     * Cleanup all dxram entries (folder /dxram) in a running zookeeper instance
     */
    private void cleanupEntries() {
        String zooKeeperAddress = String.format("127.0.0.1:%d", m_port);

        CuratorFramework curatorClient = CuratorFrameworkFactory.newClient(zooKeeperAddress,
                new ExponentialBackoffRetry(1000, 3));
        curatorClient.start();

        try {
            curatorClient.delete().deletingChildrenIfNeeded().forPath("/dxram");
        } catch (final Exception ignored) {
            // ignore, happens if entry does not exist
        }

        curatorClient.close();
    }

    /**
     * Get the config parameters from the zookeeper configuration file
     *
     * @return Properties object with zookeeper config parameters
     */
    private Properties getConfigParameter() {
        Properties prop = new Properties();

        InputStream inputStream;

        try {
            inputStream = new FileInputStream(m_path + "/conf/zoo.cfg");
        } catch (final FileNotFoundException ignored) {
            throw new RuntimeException("Could not find zookeeper config in folder " + m_path + "/conf");
        }

        try {
            prop.load(inputStream);
            inputStream.close();
        } catch (final IOException e) {
            throw new RuntimeException("Loading zookeeper config file failed: " + e.getMessage());
        }

        return prop;
    }
}
