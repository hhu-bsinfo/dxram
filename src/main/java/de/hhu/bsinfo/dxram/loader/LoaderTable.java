package de.hhu.bsinfo.dxram.loader;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

public class LoaderTable extends AbstractChunk {
    private HashMap<String, String> m_packageJarMap;
    private HashMap<String, byte[]> m_jarByteArrays;
    private static final Logger LOGGER = LogManager.getFormatterLogger(LoaderTable.class);

    public LoaderTable() {
        m_packageJarMap = new HashMap<>();
        m_jarByteArrays = new HashMap<>();
    }

    private void registerClass(String p_name, String p_jarName) {
        if (!m_packageJarMap.containsKey(p_name)) {
            m_packageJarMap.put(p_name, p_jarName);
            LOGGER.info(String.format("added %s from %s", p_name, p_jarName));
        }
    }

    public byte[] serializeMap() {
        Object o = m_packageJarMap;

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

    public String getJarName(String p_name) throws NotInClusterException {
        String myPackage;
        if (p_name.lastIndexOf('.') != -1){
            myPackage = p_name.substring(0, p_name.lastIndexOf('.'));
        }else {
            myPackage = p_name;
        }

        if (m_packageJarMap.containsKey(myPackage)) {
            return m_packageJarMap.get(myPackage);
        }else{
            throw new NotInClusterException();
        }

    }

    public boolean containsJar(String name) {
        return m_packageJarMap.containsKey(name);
    }

    public byte[] getJarByte(String p_jarName) {
        return m_jarByteArrays.get(p_jarName);
    }

    public void registerJarBytes(String p_name, byte[] p_jarBytes) {
        try {
            m_jarByteArrays.put(p_name, p_jarBytes);

            JarInputStream jarFile = new JarInputStream(new ByteArrayInputStream(p_jarBytes));
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
        LOGGER.info(String.format("LoaderTable size: %s", m_packageJarMap.size()));
    }

    public int jarMapSize(){
        return m_packageJarMap.size();
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
        int hashMapSize = 32 * m_packageJarMap.size() + 4 * 16;

        return hashMapSize;
    }
}
