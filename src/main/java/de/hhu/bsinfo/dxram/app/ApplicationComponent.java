package de.hhu.bsinfo.dxram.app;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

import de.hhu.bsinfo.dxram.DXRAMComponentOrder;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;

/**
 * Component to run applications locally on the DXRAM instance with access to all exposed services
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 17.05.17
 */
public class ApplicationComponent extends AbstractDXRAMComponent<ApplicationComponentConfig> {
    private List<Class<? extends AbstractApplication>> m_applicationClasses = new ArrayList<>();

    /**
     * Constructor
     */
    public ApplicationComponent() {
        super(DXRAMComponentOrder.Init.APPLICATION, DXRAMComponentOrder.Shutdown.APPLICATION,
                ApplicationComponentConfig.class);
    }

    /**
     * Get the path where all application jars are located
     *
     * @return Path with application jars
     */
    String getApplicationPath() {
        return getConfig().getApplicationPath();
    }

    /**
     * Get the loaded application classes
     *
     * @return Loaded classes implementing the application interface
     */
    List<Class<? extends AbstractApplication>> getApplicationClasses() {
        return m_applicationClasses;
    }

    @Override
    protected boolean supportsSuperpeer() {
        return false;
    }

    @Override
    protected boolean supportsPeer() {
        return true;
    }

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {

    }

    @Override
    protected boolean initComponent(final DXRAMContext.Config p_config) {
        LOGGER.info("Loading application %s", getConfig().getApplicationPath());

        File dir = new File(getConfig().getApplicationPath());

        if (dir.exists() && dir.isDirectory()) {
            File[] files = dir.listFiles();

            if (files != null) {
                for (File file : files) {
                    m_applicationClasses.addAll(getApplicationClasses(file));
                }
            }
        } else {
            LOGGER.warn("Can't load applications from %s, no such directory", getConfig().getApplicationPath());
        }

        return true;
    }

    @Override
    protected boolean shutdownComponent() {
        m_applicationClasses.clear();

        return false;
    }

    /**
     * Load any external dependencies (jar packages) required by the application
     *
     * @param p_application
     *         Application to load external dependencies for
     */
    public void loadExternalDependencies(final AbstractApplication p_application) {
        for (String dep : p_application.getExternalDependencies()) {
            try {
                URLClassLoader loader = (URLClassLoader) ClassLoader.getSystemClassLoader();
                URL url = new File(dep).toURI().toURL();

                // Disallow if already loaded
                for (java.net.URL it : java.util.Arrays.asList(loader.getURLs())) {
                    if (it.equals(url)) {
                        return;
                    }
                }

                Method method = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
                method.setAccessible(true);
                method.invoke(loader, url);

                LOGGER.info("[%s] Loaded external dependency %s", p_application.getApplicationName(), dep);
            } catch (final NoSuchMethodException | IllegalAccessException | MalformedURLException |
                    InvocationTargetException e) {
                LOGGER.error("Could not load dependency %s for application %s", dep,
                        p_application.getApplicationName(), e);

            }
        }
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
    private String getNextClass(final JarInputStream p_jarFile, final File p_jar) {
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
