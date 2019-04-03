package de.hhu.bsinfo.dxram.loader;

import de.hhu.bsinfo.dxutils.loader.JarClassLoader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URLStreamHandlerFactory;
import java.nio.file.Path;

public class DistributedLoader extends JarClassLoader {
    private static final Logger LOGGER = LogManager.getFormatterLogger(DistributedLoader.class);
    private final Path m_baseDir;
    private LoaderComponent m_loader;


    public DistributedLoader(Path p_baseDir, ClassLoader p_parent, LoaderComponent p_loader) {
        super(p_baseDir, p_parent);
        m_baseDir = p_baseDir;
        m_loader = p_loader;
    }

    public DistributedLoader(Path p_baseDir, LoaderComponent p_loader) {
        super(p_baseDir);
        m_baseDir = p_baseDir;
        m_loader = p_loader;
    }

    public DistributedLoader(Path p_baseDir, ClassLoader p_parent, URLStreamHandlerFactory p_factory, LoaderComponent p_loader) {
        super(p_baseDir, p_parent, p_factory);
        m_baseDir = p_baseDir;
        m_loader = p_loader;
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

    private void getJar(String p_name) throws ClassNotFoundException {
        String myPackage = p_name.substring(0, p_name.lastIndexOf('.'));
        LOGGER.info(String.format("Ask LoaderComponent for %s", myPackage));
        add(m_loader.getJar(myPackage));
        LOGGER.info(String.format("Added %s to ClassLoader", myPackage));
    }

}

