package de.hhu.bsinfo.dxram.loader;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.HashMap;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

public class ClassTable extends AbstractChunk {
    private HashMap<String, String> m_jarMap;
    private static final Logger LOGGER = LogManager.getFormatterLogger(ClassTable.class);

    public ClassTable() {
        m_jarMap = new HashMap<>();
    }

    public ClassTable(HashMap<String, String> p_jarMap) {
        m_jarMap = p_jarMap;
    }

    private void registerClass(String p_name, String p_jarName) {
        if (!m_jarMap.containsKey(p_name)) {
            m_jarMap.put(p_name, p_jarName);
            LOGGER.info(String.format("added %s from %s", p_name, p_jarName));
        }
    }

    public byte[] serializeMap() {
        Object o = m_jarMap;

        byte[] yourBytes = null;

        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            ObjectOutput out = new ObjectOutputStream(bos);
            out.writeObject(o);
            out.flush();
            yourBytes = bos.toByteArray();
        } catch (IOException e) {
            LOGGER.error(e);
        }

        return yourBytes;
    }

    public String getJarName(String p_className) {
        String myPackage = p_className.substring(0, p_className.lastIndexOf('.'));
        return m_jarMap.get(myPackage);
    }

    public void registerJar(String p_name) {
        try {
            JarInputStream jarFile = new JarInputStream(new FileInputStream(p_name));
            JarEntry entry;

            while (true) {
                entry = jarFile.getNextJarEntry();
                if (entry == null) {
                    break;
                }
                if (entry.getName().endsWith(".class")) {
                    String className = entry.getName().replaceAll("/", "\\.");
                    String myClass = className.substring(0, className.lastIndexOf('.'));
                    String myPackage = myClass.substring(0, myClass.lastIndexOf('.'));
                    try {
                        registerClass(myPackage, p_name);
                    } catch (Throwable e) {
                        LOGGER.warn("WARNING: failed to instantiate ");
                        e.printStackTrace();
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error(String.format("Oops.. Encounter an issue while parsing jar: %s", e));
        }
        LOGGER.info(String.format("ClassTable size: %s", m_jarMap.size()));
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeByteArray(serializeMap());
    }

    @Override
    public void importObject(Importer p_importer) {

    }

    @Override
    public int sizeofObject() {
        int hashMapSize = 32 * m_jarMap.size() + 4 * 16;

        return hashMapSize;
    }
}
