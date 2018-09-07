package de.hhu.bsinfo.dxram.app;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Loader scanning application folder for jar files and loading classes
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 05.09.18
 */
class ApplicationLoader {
    private static final Logger LOGGER = LogManager.getFormatterLogger(ApplicationLoader.class.getSimpleName());

    private final HashMap<String, Class<? extends AbstractApplication>> m_applicationClasses = new HashMap<>();

    /**
     * Constructor
     *
     * @param p_path
     *         Path to folder to scan for dxapp jars
     */
    ApplicationLoader(final String p_path) {
        LOGGER.info("Scanning for applications in %s", p_path);

        File dir = new File(p_path);

        if (dir.exists() && dir.isDirectory()) {
            File[] files = dir.listFiles();

            if (files != null) {
                Arrays.stream(files)
                        .flatMap(file -> getApplicationClasses(file).stream())
                        .forEach(clazz -> m_applicationClasses.put(clazz.getName(), clazz));
            }

            m_applicationClasses.keySet().forEach(clazz -> LOGGER.info("Found application %s", clazz));
        } else {
            LOGGER.warn("Can't scan for applications from %s, no such directory", p_path);
        }
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
}
