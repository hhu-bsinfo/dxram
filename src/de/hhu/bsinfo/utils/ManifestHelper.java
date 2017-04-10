/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.utils;

import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

/**
 * Helper class to get information from the manifest file in a jar archive.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 30.03.2016
 */
public final class ManifestHelper {
    /**
     * Utils class
     */
    private ManifestHelper() {
    }

    /**
     * Get the value of a property within the manifest file.
     *
     * @param p_class
     *     Target class within the jar package.
     * @param p_key
     *     Key for the value to get.
     * @return If value is found it is returned as a string, null otherwise.
     */
    public static String getProperty(final Class<?> p_class, final String p_key) {
        String value = null;
        try {
            Enumeration<URL> resources = p_class.getClassLoader().getResources("META-INF/MANIFEST.MF");
            while (resources.hasMoreElements()) {
                Manifest manifest = new Manifest(resources.nextElement().openStream());
                // check that this is your manifest and do what you need or get the next one

                Attributes attr = manifest.getMainAttributes();
                value = attr.getValue(p_key);

                if (value != null) {
                    break;
                }
            }
        } catch (final IOException e) {
            e.printStackTrace();
        }
        return value;
    }

}
