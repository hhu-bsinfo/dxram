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

public class Edge implements Importable, Exportable {



    private double weight;
    private Vertex src;
    private Vertex dst;

    public Edge() {
        this.src = new Vertex();
        this.dst = new Vertex();
    }

    public double getWeight() {
        return this.weight;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }

    public Vertex getSrc() {
        return this.src;
    }

    public void setSrc(Vertex src) {
        if (src == null)
            throw new NullPointerException("Parameter src must not be null");
        this.src = src;
    }

    public Vertex getDst() {
        return this.dst;
    }

    public void setDst(Vertex dst) {
        if (dst == null)
            throw new NullPointerException("Parameter dst must not be null");
        this.dst = dst;
    }



    @Override
    public void importObject(final Importer p_importer) {
        this.weight = p_importer.readDouble(this.weight);
        p_importer.importObject(this.src);
        
        p_importer.importObject(this.dst);
        
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeDouble(this.weight);
        p_exporter.exportObject(this.src);
        p_exporter.exportObject(this.dst);
    }

    @Override
    public int sizeofObject() {
        int size = 0;

        // size of basic types
        size += 8;

        // size of complex types
        size += this.src.sizeofObject();
        size += this.dst.sizeofObject();
        
        return size;
    }
}