package de.hhu.bsinfo.dxram.loader;

import java.net.URLStreamHandlerFactory;
import java.nio.file.Path;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxutils.loader.JarClassLoader;

public class DistributedLoader extends JarClassLoader {
    private static final Logger LOGGER = LogManager.getFormatterLogger(DistributedLoader.class);
    private final Path m_baseDir;
    private LoaderComponent m_loader;

    public DistributedLoader(Path p_baseDir, ClassLoader p_parent) {
        super(p_baseDir, p_parent);
        m_baseDir = p_baseDir;
    }

    public DistributedLoader(Path p_baseDir) {
        super(p_baseDir);
        m_baseDir = p_baseDir;
    }

    public DistributedLoader(Path p_baseDir, ClassLoader p_parent, URLStreamHandlerFactory p_factory) {
        super(p_baseDir, p_parent, p_factory);
        m_baseDir = p_baseDir;
    }

    @Override
    protected Class<?> findClass(String p_name) throws ClassNotFoundException {
        Class<?> result;
        try {
            LOGGER.info(String.format("Try to find class %s local", p_name));
            result = super.findClass(p_name);
        } catch (ClassNotFoundException e) {
            LOGGER.info(String.format("Class %s not found local, try to find remote...", p_name));
            getJar(p_name);
            result = super.findClass(p_name);
        }
        return result;
    }

    public void registerLoaderComponent(LoaderComponent p_loader) {
        m_loader = p_loader;
    }

    private void getJar(String p_name) throws ClassNotFoundException {
        String myPackage = p_name.substring(0, p_name.lastIndexOf('.'));
        LOGGER.info(String.format("Ask LoaderComponent for %s", myPackage));
        try {
            add(m_loader.getJar(myPackage));
        } catch (NullPointerException e) {
            LOGGER.error("LoaderComponent not registered.");
        }

        LOGGER.info(String.format("Added %s to ClassLoader", myPackage));
    }

}

