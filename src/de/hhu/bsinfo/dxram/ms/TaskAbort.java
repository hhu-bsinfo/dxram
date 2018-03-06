/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
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

package de.hhu.bsinfo.dxram.ms;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

/**
 * Task script node to completely abort the current task script
 * (on errors for example)
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 15.01.2017
 */
final class TaskAbort implements TaskScriptNode {

    @Expose
    private String m_abortMsg = "";

    /**
     * Default constructor
     */
    public TaskAbort() {

    }

    /**
     * Get the abort message
     *
     * @return Abort message
     */
    String getAbortMsg() {
        return m_abortMsg;
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeString(m_abortMsg);
    }

    @Override
    public void importObject(final Importer p_importer) {
        m_abortMsg = p_importer.readString(m_abortMsg);
    }

    @Override
    public int sizeofObject() {
        return ObjectSizeUtil.sizeofString(m_abortMsg);
    }
}
