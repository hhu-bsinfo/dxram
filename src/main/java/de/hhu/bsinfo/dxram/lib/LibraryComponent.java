package de.hhu.bsinfo.dxram.lib;

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
 * Component to run libraries locally on the DXRAM instance with access to all exposed services
 *
 * @author Kai Neyenhuys, kai.neyenhuys@hhu.de, 17.07.18
 */
public class LibraryComponent extends AbstractDXRAMComponent<LibraryComponentConfig> {
    private List<Class<? extends AbstractLibrary>> m_libraryClasses = new ArrayList<>();

    /**
     * Constructor
     */
    public LibraryComponent() {
        super(DXRAMComponentOrder.Init.LIBRARY, DXRAMComponentOrder.Shutdown.LIBRARY,
                LibraryComponentConfig.class);
    }

    /**
     * Get the path where all lib jars are located
     *
     * @return Path with lib jars
     */
    String getLibraryPath() {
        return getConfig().getLibraryPath();
    }

    /**
     * Get the loaded library classes
     *
     * @return Loaded classes implementing the library interface
     */
    List<Class<? extends AbstractLibrary>> getLibraryClasses() {
        return m_libraryClasses;
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
        LOGGER.info("Loading libraries %s", getConfig().getLibraryPath());

        File dir = new File(getConfig().getLibraryPath());

        if (dir.exists() && dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File file : files) {
                    m_libraryClasses.addAll(getLibraryClasses(file));
                }
            }
        } else {
            LOGGER.warn("Can't load libraries from %s, no such directory", getConfig().getLibraryPath());
        }

        return true;
    }

    @Override
    protected boolean shutdownComponent() {
        m_libraryClasses.clear();

        return false;
    }

    /**
     * Get all classes which extend the AbstractLibrary class from a jar file
     *
     * @param p_jar
     *         Jar file to search for classes to load
     * @return List of loaded classes
     */
    private List<Class<? extends AbstractLibrary>> getLibraryClasses(final File p_jar) {
        List<Class<? extends AbstractLibrary>> classes = new ArrayList<>();
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
                if (AbstractLibraryDependency.class.equals(clazz.getSuperclass())) {
                    LOGGER.info("Found Dependency Class: %s", clazz.getName());

                    String[] dependencies = ((AbstractLibraryDependency) clazz.newInstance()).getDependency();
                    loadDeps(dependencies);
                }
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                e.printStackTrace();
            } catch (NoClassDefFoundError ignored) {

            }
        }

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
                if (AbstractLibrary.class.equals(clazz.getSuperclass())) {
                    LOGGER.info("Found library %s", clazz.getName());
                    // check for default constructor
                    clazz.getConstructor();
                    classes.add((Class<? extends AbstractLibrary>) clazz);
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

    private synchronized void loadDeps(String[] p_dependencies) {
        for (String dep : p_dependencies) {
            try {
                URLClassLoader loader = (URLClassLoader) ClassLoader.getSystemClassLoader();
                URL url = new File(dep).toURI().toURL();

                //Disallow if already loaded
                for (URL it : java.util.Arrays.asList(loader.getURLs())) {
                    if (it.equals(url)) {
                        return;
                    }
                }

                Method method = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
                method.setAccessible(true);
                method.invoke(loader, url);

                LOGGER.info("Load dependency %s", dep);
            } catch (final NoSuchMethodException | IllegalAccessException | MalformedURLException |
                    InvocationTargetException e) {
                LOGGER.error("Could not load dependency %s", dep, e);

            }
        }
    }

    private String getNextClass(JarInputStream p_jarFile, File p_jar) {
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
