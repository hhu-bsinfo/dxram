/*
 * Copyright (C) 2016 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

package de.hhu.bsinfo.dxgraph.conv;

import de.hhu.bsinfo.utils.main.AbstractMain;

/**
 * Multi threaded converter, expecting edge list in binary form:
 * 8 bytes source nodeId and 8 bytes destination node id and outputting a binary ordered edge list.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.02.2016
 */
public final class ConverterBinaryEdgeListToBoel extends AbstractBinaryEdgeListTo {
    /**
     * Constructor
     */
    private ConverterBinaryEdgeListToBoel() {
        super("Convert a binary edge list to an ordered edge list (binary file)");
    }

    /**
     * Main entry point.
     * @param p_args
     *            Console arguments.
     */
    public static void main(final String[] p_args) {
        AbstractMain main = new ConverterBinaryEdgeListToBoel();
        main.run(p_args);
    }

    @Override
    protected VertexStorage createVertexStorageInstance() {
        return new VertexStorageBinaryUnsafe();
    }

    @Override
    protected AbstractFileWriterThread createWriterInstance(final String p_outputPath, final int p_id, final long p_idRangeStartIncl,
            final long p_idRangeEndExcl, final VertexStorage p_storage) {
        return new FileWriterBinaryThread(p_outputPath, p_id, p_idRangeStartIncl, p_idRangeEndExcl, p_storage);
    }
}
