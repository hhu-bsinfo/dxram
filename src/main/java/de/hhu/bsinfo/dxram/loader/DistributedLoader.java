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

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLStreamHandlerFactory;
import java.nio.file.Files;
import java.nio.file.Path;

public class DistributedLoader extends URLClassLoader {
    private LoaderComponent m_loader;
    private static final String JAR_SUFFIX = ".jar";

    public DistributedLoader(ClassLoader p_parent) {
        super(new URL[0], p_parent);
    }

    public DistributedLoader(ClassLoader p_parent, URLStreamHandlerFactory p_factory) {
        super(new URL[0], p_parent, p_factory);
    }

    public DistributedLoader() {
        super(new URL[0]);
    }

    @Override
    protected Class<?> findClass(String p_name) throws ClassNotFoundException {
        Class<?> result;
        try {
            result = super.findClass(p_name);
        } catch (ClassNotFoundException e) {
            getJar(p_name);
            result = super.findClass(p_name);
        }
        return result;
    }

    public void registerLoaderComponent(LoaderComponent p_loader) {
        m_loader = p_loader;
    }

    /**
     * Gets the jar with the class from the LoaderComponent and adds it to the ClassLoader
     *
     * @param p_name
     * @throws ClassNotFoundException
     */
    private void getJar(String p_name) throws ClassNotFoundException {
        String myPackage;
        if (p_name.lastIndexOf('.') != -1) {
            myPackage = p_name.substring(0, p_name.lastIndexOf('.'));
        } else {
            myPackage = p_name;
        }

        try {
            add(m_loader.getJar(myPackage));
        } catch (NullPointerException e) {
            // this is fine, just in case classloader is used before LoaderComponent is registerd
        }
    }

    /**
     * Initializes this classloader instance.
     */
    public void initPlugins(Path p_baseDir) {
        try {
            Files.list(p_baseDir).forEach(this::add);
        } catch (IOException e) {
        }
    }

    /**
     * Adds a new jar file to the class loader.
     *
     * @param p_path
     *         The jar file's path.
     */
    public void add(final Path p_path) {
        if (!p_path.toString().endsWith(JAR_SUFFIX)) {
            return;
        }

        try {
            addURL(p_path.toUri().toURL());
        } catch (MalformedURLException e) {
        }
    }

}

