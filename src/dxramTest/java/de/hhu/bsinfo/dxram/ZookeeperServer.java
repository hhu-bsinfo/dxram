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

public class ZookeeperServer {
    private static final String ZOOKEEPER_SERVER = "bin/zkServer.sh";

    private final String m_path;

    private final String m_dataDir;
    private final int m_port;

    private Process m_process;

    public ZookeeperServer(final String p_path) {
        m_path = p_path;

        if (!new File(m_path).exists()) {
            throw new RuntimeException("Zookeeper root path does not exist: " + p_path);
        }

        Properties prop = getConfigParameter();

        m_dataDir = prop.getProperty("dataDir");
        m_port = Integer.parseInt(prop.getProperty("clientPort"));
    }

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

    public void shutdown() {
        m_process.destroy();

        deleteZookeeperDataDir();
    }

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
