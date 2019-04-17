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

import java.nio.file.Path;

import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.engine.Module;
import de.hhu.bsinfo.dxram.engine.ModuleConfig;
import de.hhu.bsinfo.dxram.engine.Service;
import de.hhu.bsinfo.dxutils.dependency.Dependency;

/**
 * @author Julien Bernhart, julien.bernhart@hhu.de, 2019-04-17
 */
@Module.Attributes(supportsSuperpeer = true, supportsPeer = true)
public class LoaderService extends Service<ModuleConfig> {
    @Dependency
    private LoaderComponent m_loader;

    public DistributedLoader getClassLoader() {
        return m_loader.getM_loader();
    }

    public void cleanLoaderDir() {
        m_loader.cleanLoaderDir();
    }

    public void addJar(Path p_jarPath) {
        m_loader.addJarToLoader(p_jarPath);
    }

    public int numberLoadedEntries() {
        return m_loader.numberLoadedEntries();
    }

    @Override
    protected boolean startService(DXRAMConfig p_config) {
        return true;
    }

    @Override
    protected boolean shutdownService() {
        return true;
    }
}
