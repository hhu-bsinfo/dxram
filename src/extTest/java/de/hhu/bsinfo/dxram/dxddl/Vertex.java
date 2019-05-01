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

package de.hhu.bsinfo.dxram.dxddl;

import de.hhu.bsinfo.dxutils.serialization.Importable;
import de.hhu.bsinfo.dxutils.serialization.Exportable;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.Exporter;

import static de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil.*;

public class Vertex implements Importable, Exportable {



    private int depth;
    private Vertex[] outNeighbors;
    private Vertex[] inNeighbors;

    public Vertex() {
        this.outNeighbors = new Vertex[0];
        for (int i0 = 0; i0 < 0; i0++) {
            Vertex obj1 = new Vertex();
            this.outNeighbors[i0] = obj1;
        }
        this.inNeighbors = new Vertex[0];
        for (int i0 = 0; i0 < 0; i0++) {
            Vertex obj2 = new Vertex();
            this.inNeighbors[i0] = obj2;
        }
    }

    public int getDepth() {
        return this.depth;
    }

    public void setDepth(int depth) {
        this.depth = depth;
    }

    public Vertex[] getOutNeighbors() {
        return this.outNeighbors;
    }

    public Vertex[] getInNeighbors() {
        return this.inNeighbors;
    }



    @Override
    public void importObject(final Importer p_importer) {
        this.depth = p_importer.readInt(this.depth);
        for (int i0 = 0; i0 < 0; i0++) {
            p_importer.importObject(this.outNeighbors[i0]);
        }
        
        for (int i0 = 0; i0 < 0; i0++) {
            p_importer.importObject(this.inNeighbors[i0]);
        }
        
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeInt(this.depth);
        for (int i0 = 0; i0 < 0; i0++)
            p_exporter.exportObject(this.outNeighbors[i0]);
        
        for (int i0 = 0; i0 < 0; i0++)
            p_exporter.exportObject(this.inNeighbors[i0]);
        
    }

    @Override
    public int sizeofObject() {
        int size = 0;

        // size of basic types
        size += 4;

        // size of complex types
        for (int i0 = 0; i0 < 0; i0++)
            size += this.outNeighbors[i0].sizeofObject();
        for (int i0 = 0; i0 < 0; i0++)
            size += this.inNeighbors[i0].sizeofObject();
        
        return size;
    }
}