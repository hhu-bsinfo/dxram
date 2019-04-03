package de.hhu.bsinfo.dxram.loader;

import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxram.chunk.ChunkComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.chunk.operation.Get;
import de.hhu.bsinfo.dxram.nameservice.NameserviceComponent;
import de.hhu.bsinfo.dxutils.dependency.Dependency;
import de.hhu.bsinfo.dxutils.loader.JarClassLoader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.net.URLStreamHandlerFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

public class DistributedLoader extends JarClassLoader {
    private static final Logger LOGGER = LogManager.getFormatterLogger(DistributedLoader.class);
    private ClassTable m_classTable;
    private final Path m_baseDir;
    private ChunkComponent m_chunk;
    private NameserviceComponent m_name;


    public DistributedLoader(Path p_baseDir, ClassLoader p_parent, ChunkComponent p_chunk, NameserviceComponent p_name) {
        super(p_baseDir, p_parent);
        m_baseDir = p_baseDir;
        m_chunk = p_chunk;
        m_name = p_name;
    }

    public DistributedLoader(Path p_baseDir, ChunkComponent p_chunk, NameserviceComponent p_name) {
        super(p_baseDir);
        m_baseDir = p_baseDir;
        m_chunk = p_chunk;
        m_name = p_name;
    }

    public DistributedLoader(Path p_baseDir, ClassLoader p_parent, URLStreamHandlerFactory p_factory, ChunkComponent p_chunk, NameserviceComponent p_name) {
        super(p_baseDir, p_parent, p_factory);
        m_baseDir = p_baseDir;
        m_chunk = p_chunk;
        m_name = p_name;
    }

    @Override
    protected Class<?> findClass(String p_name) throws ClassNotFoundException {
        Class<?> result;
        try {
            LOGGER.info(String.format("Try to find class %s", p_name));
            result = super.findClass(p_name);
        } catch (ClassNotFoundException e) {
            LOGGER.info(String.format("Class %s not found, DXClassloader tries to download jar...", p_name));
            //String jarName = m_classTable.getJarName(p_name);
            String jarName = "dxrest.jar";
            if (jarName != null) {
                LOGGER.info(String.format("Class found in: %s, start downloading.", jarName));
                getJar(jarName);
                result = super.findClass(p_name);
            } else {
                throw new ClassNotFoundException(p_name);
            }
        }
        return result;
    }

    private void getClassTable() {

    }

    private void getJar(String p_name) {
        long jarChunkId = m_name.getChunkID("c1", 10);
        LOGGER.info(String.format("CID of FileChunk is %s", ChunkID.toHexString(jarChunkId)));
        FileChunk jarChunk = new FileChunk();
        jarChunk.setID(jarChunkId);

        boolean isRdy = m_chunk.getMemory().get().get(jarChunk);

        System.out.println("isRdy = " + isRdy);
        Path jar = jarChunk.getFile(Paths.get(m_baseDir + File.separator + p_name));

        add(jar);
        LOGGER.info(String.format("Added %s to ClassLoader", p_name));
    }


    public void cleanBaseDir() {
        try {
            Files.walk(m_baseDir)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        } catch (IOException e) {
            LOGGER.error(e);
        }


        LOGGER.info("Base dir cleanup finished.");
    }
}

