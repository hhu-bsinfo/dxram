package de.hhu.bsinfo.dxram.app;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.FileSystems;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import de.hhu.bsinfo.dxutils.FileSystemUtils;

/**
 * Loader scanning application folder for jar files and loading classes
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 05.09.18
 */
class ApplicationLoader {
    private static final Logger LOGGER = LogManager.getFormatterLogger(ApplicationLoader.class.getSimpleName());

    private final HashMap<String, Class<? extends AbstractApplication>> m_applicationClasses = new HashMap<>();

    private ApplicationWatcher m_watcher;

    private final File m_applicationDirectory;

    /**
     * Constructor
     *
     * @param p_path
     *         Path to folder to scan for dxapp jars
     */
    ApplicationLoader(final String p_path) {
        LOGGER.info("Scanning for applications in %s", p_path);

        m_applicationDirectory = new File(p_path);

        if (m_applicationDirectory.exists() && m_applicationDirectory.isDirectory()) {
            File[] files = m_applicationDirectory.listFiles();

            if (files != null) {
                Arrays.stream(files)
                        .flatMap(file -> getApplicationClasses(file).stream())
                        .forEach(clazz -> m_applicationClasses.put(clazz.getName(), clazz));
            }

            // don't use closure because the pre-processor removes the log call on performance build type
            for (String clazz : m_applicationClasses.keySet()) {
                LOGGER.info("Found application %s", clazz);
            }

            if (FileSystemUtils.isNetworkMount(m_applicationDirectory)) {
                LOGGER.warn("Hot Reloading is not supported on NFS");
            } else {
                // Try to activate hot reloading applications
                try {
                    m_watcher = new ApplicationWatcher(p_path, this::onApplicationChanged);
                    m_watcher.start();
                    LOGGER.info("Hot Reloading is active");
                } catch (IOException p_e) {
                    LOGGER.warn("Couldn't initialize application watcher (Hot Reloading disabled)");
                }
            }

        } else {
            LOGGER.warn("Can't scan for applications from %s, no such directory", p_path);
        }
    }

    /**
     * Register an application class at the loader (external). Allows registering in-code
     * application classes without having to load jars (e.g. for testing).
     *
     * @param p_class
     *         Application class to register
     */
    public void registerApplicationClass(final Class<? extends AbstractApplication> p_class) {
        m_applicationClasses.put(p_class.getName(), p_class);
    }

    /**
     * Get a loaded application class by its fully qualified class name
     *
     * @param p_class
     *         Fully qualified class name of application
     * @return Loaded application class or null if not found
     */
    public Class<? extends AbstractApplication> getApplicationClass(final String p_class) {
        return m_applicationClasses.get(p_class);
    }

    /**
     * Get a list of loaded application classes
     *
     * @return List of loaded application classes
     */
    public List<String> getLoadedApplicationClasses() {
        return new ArrayList<>(m_applicationClasses.keySet());
    }

    /**
     * Get all classes which extend the AbstractApplication class from a far file
     *
     * @param p_jar
     *         Jar file to search for classes to load
     * @return List of loaded classes
     */
    private List<Class<? extends AbstractApplication>> getApplicationClasses(final File p_jar) {
        List<Class<? extends AbstractApplication>> classes = new ArrayList<>();

        ClassLoader classLoader = getClass().getClassLoader();
        URLClassLoader ucl;

        try {
            ucl = new URLClassLoader(new URL[] {p_jar.toURI().toURL()}, classLoader);
        } catch (final MalformedURLException e) {
            LOGGER.error(e);

            return classes;
        }

        JarInputStream jarFile;

        try {
            jarFile = new JarInputStream(new FileInputStream(p_jar));
        } catch (final IOException e) {
            LOGGER.error("Opening jar %s failed: %s", p_jar.getAbsolutePath(), e.getMessage());

            return classes;
        }

        while (true) {
            String classname = getNextClass(jarFile, p_jar);

            if (classname == null) {
                break;
            }

            if (classname.isEmpty()) {
                continue;
            }

            try {
                Class<?> clazz = Class.forName(classname, true, ucl);

                if (AbstractApplication.class.equals(clazz.getSuperclass())) {
                    LOGGER.info("Found application %s", clazz.getName());

                    // check for default constructor
                    clazz.getConstructor();

                    classes.add((Class<? extends AbstractApplication>) clazz);
                }
            } catch (final ClassNotFoundException ignored) {
                LOGGER.error("Could not find class %s in jar %s", classname, p_jar.getAbsolutePath());
            } catch (final NoSuchMethodException ignored) {
                LOGGER.error("Could not load class %s in jar %s, missing default constructor", classname,
                        p_jar.getAbsolutePath());
            }
        }

        return classes;
    }

    /**
     * Get the next class file from the jar package
     *
     * @param p_jarFile
     *         Jar input stream
     * @param p_jar
     *         Original jar file (path)
     * @return Classname
     */
    private static String getNextClass(final JarInputStream p_jarFile, final File p_jar) {
        JarEntry jarEntry = null;

        try {
            jarEntry = p_jarFile.getNextJarEntry();
        } catch (final IOException e) {
            LOGGER.error("Getting next jar entry from %s failed: %s", p_jar.getAbsolutePath(), e.getMessage());
        }

        if (jarEntry == null) {
            return null;
        }

        if (jarEntry.getName().endsWith(".class")) {
            String classname = jarEntry.getName().replaceAll("/", "\\.");
            classname = classname.substring(0, classname.length() - 6);
            return classname;
        }

        return "";
    }

    private void onApplicationChanged(@NotNull String p_filename) {
        File applicationJar = new File(m_applicationDirectory, p_filename);

        for (Class<? extends AbstractApplication> appClass : getApplicationClasses(applicationJar)) {
            m_applicationClasses.put(appClass.getName(), appClass);
            LOGGER.info("Reloaded application %s", appClass.getName());
        }
    }

    public static class ApplicationWatcher extends Thread {

        private static final Logger LOGGER = LogManager.getFormatterLogger(ApplicationWatcher.class.getSimpleName());

        private final WatchService m_watchService;
        private final ApplicationChangeListener m_changeListener;

        private static final String JAR_SUFFIX = ".jar";
        private static final String THREAD_NAME = "ApplicationWatcher";

        public interface ApplicationChangeListener {
            void onApplicationChanged(@NotNull String p_filename);
        }

        public ApplicationWatcher(final @NotNull String p_path,
                final @NotNull ApplicationChangeListener p_changeListener) throws IOException {
            super(THREAD_NAME);
            m_watchService = FileSystems.getDefault().newWatchService();
            m_changeListener = p_changeListener;

            Paths.get(p_path).register(m_watchService, StandardWatchEventKinds.ENTRY_CREATE,
                    StandardWatchEventKinds.ENTRY_MODIFY);
        }

        @Override
        public void run() {

            WatchKey key = null;
            String fileName;
            for (; ; ) {

                try {
                    key = m_watchService.take();
                } catch (InterruptedException p_e) {
                    LOGGER.warn("Interrupted while waiting for events");
                }

                if (key == null) {
                    LOGGER.warn("Service returned null key");
                    continue;
                }

                for (WatchEvent<?> event : key.pollEvents()) {
                    fileName = event.context().toString();

                    if (fileName.endsWith(JAR_SUFFIX)) {
                        m_changeListener.onApplicationChanged(fileName);
                    }
                }

                key.reset();
            }
        }
    }
}
