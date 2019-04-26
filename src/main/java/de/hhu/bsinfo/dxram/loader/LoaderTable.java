/*
 * Copyright (C) 2019 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.loader;

import lombok.Getter;

import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author Julien Bernhart, julien.bernhart@hhu.de, 2019-04-17
 */
public class LoaderTable {
    private HashMap<String, String> m_packageJarMap;
    @Getter
    private HashMap<String, byte[]> m_jarByteArrays;
    private static final Logger LOGGER = LogManager.getFormatterLogger(LoaderTable.class);

    public LoaderTable() {
        m_packageJarMap = new HashMap<>();
        m_jarByteArrays = new HashMap<>();
    }

    public void registerJarMap(HashMap<String, byte[]> p_jarByteArrays) {
        for (Map.Entry<String, byte[]> entry : p_jarByteArrays.entrySet()) {
            registerJarBytes(entry.getKey(), entry.getValue());
        }
        LOGGER.info("map registered");
    }

    /**
     * Register package with in HashMap with name of jar, it is possible to register more than one package
     * with the same jar, the jar is only stored one time
     *
     * @param p_name
     *         package name
     * @param p_jarName
     *         jar name
     */
    private void registerClass(String p_name, String p_jarName) {
        if (!m_packageJarMap.containsKey(p_name)) {
            m_packageJarMap.put(p_name, p_jarName);
            LOGGER.info(String.format("added %s from %s", p_name, p_jarName));
        }
    }

    public HashSet<String> getLoadedJars() {
        return new HashSet<>(m_jarByteArrays.keySet());
    }

    /**
     * Returns the name of the jar with the class p_name and throws a Exception if the class is not found in cluster
     *
     * @param p_name
     *         class name
     * @return jar name
     * @throws NotInClusterException
     *         class is not found in cluster
     */
    public String getJarName(String p_name) throws NotInClusterException {
        String myPackage;
        if (p_name.lastIndexOf('.') != -1) {
            myPackage = p_name.substring(0, p_name.lastIndexOf('.'));
        } else {
            myPackage = p_name;
        }

        if (m_packageJarMap.containsKey(myPackage)) {
            return m_packageJarMap.get(myPackage);
        } else {
            throw new NotInClusterException();
        }

    }

    /**
     * Check if jar is already registered
     *
     * @param p_name
     *         jar name
     * @return true if jar is in table
     */
    public boolean containsJar(String p_name) {
        return m_jarByteArrays.containsKey(p_name);
    }

    public byte[] getJarByte(String p_jarName) {
        return m_jarByteArrays.get(p_jarName);
    }

    /**
     * Register byte array of jar with filename
     *
     * @param p_name
     *         jar name
     * @param p_jarBytes
     *         jar byte array
     */
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
        LOGGER.info(String.format("%s registered, new LoaderTable size: %s, loaded jars: %s",
                p_name, m_packageJarMap.size(), m_jarByteArrays.size()));
    }

    /**
     * Only for testing. Flushes all maps.
     */
    public void flushMaps() {
        m_packageJarMap = new HashMap<>();
        m_jarByteArrays = new HashMap<>();
    }

    /**
     * @return number of registered packages
     */
    public int jarMapSize() {
        return m_packageJarMap.size();
    }
}
