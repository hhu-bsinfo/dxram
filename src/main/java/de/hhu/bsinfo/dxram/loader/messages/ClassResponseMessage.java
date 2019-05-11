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

package de.hhu.bsinfo.dxram.loader.messages;

import lombok.Getter;

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Response;
import de.hhu.bsinfo.dxram.loader.LoaderJar;

/**
 * @author Julien Bernhart, julien.bernhart@hhu.de, 2019-04-17
 */
public class ClassResponseMessage extends Response {
    @Getter
    private LoaderJar m_loaderJar;

    public ClassResponseMessage() {
        super();
        m_loaderJar = new LoaderJar();
    }

    public ClassResponseMessage(final ClassRequestMessage p_request, final LoaderJar p_loaderJar) {
        super(p_request, LoaderMessages.SUBTYPE_CLASS_RESPONSE);
        m_loaderJar = p_loaderJar;
    }

    @Override
    protected final int getPayloadLength() {
        return m_loaderJar.sizeofObject();
    }

    @Override
    protected void writePayload(AbstractMessageExporter p_exporter) {
        m_loaderJar.exportObject(p_exporter);
    }

    @Override
    protected void readPayload(AbstractMessageImporter p_importer) {
        m_loaderJar.importObject(p_importer);
    }
}
