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

public class SimpleChunk implements Importable, Exportable {



    private long id;
    private String name;
    private int[] numbers;
    private SimpleChunk parent;
    private SimpleChunk[] children;

    public SimpleChunk() {
        this.name = new String();
        this.numbers = new int[0];
        this.parent = new SimpleChunk();
        this.children = new SimpleChunk[0];
        for (int i0 = 0; i0 < 0; i0++) {
            SimpleChunk obj4 = new SimpleChunk();
            this.children[i0] = obj4;
        }
    }

    public long getId() {
        return this.id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        if (name == null)
            throw new NullPointerException("Parameter name must not be null");
        this.name = name;
    }

    public int[] getNumbers() {
        return this.numbers;
    }

    public SimpleChunk getParent() {
        return this.parent;
    }

    public void setParent(SimpleChunk parent) {
        if (parent == null)
            throw new NullPointerException("Parameter parent must not be null");
        this.parent = parent;
    }

    public SimpleChunk[] getChildren() {
        return this.children;
    }



    @Override
    public void importObject(final Importer p_importer) {
        this.id = p_importer.readLong(this.id);
        this.name = p_importer.readString(this.name);
        for (int i0 = 0; i0 < 0; i0++)
            this.numbers[i0] = p_importer.readInt(this.numbers[i0]);
        
        p_importer.importObject(this.parent);
        
        for (int i0 = 0; i0 < 0; i0++) {
            p_importer.importObject(this.children[i0]);
        }
        
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeLong(this.id);
        p_exporter.writeString(this.name);
        for (int i0 = 0; i0 < 0; i0++)
            p_exporter.writeInt(this.numbers[i0]);
        
        p_exporter.exportObject(this.parent);
        for (int i0 = 0; i0 < 0; i0++)
            p_exporter.exportObject(this.children[i0]);
        
    }

    @Override
    public int sizeofObject() {
        int size = 0;

        // size of basic types
        size += 8;

        // size of complex types
        size += sizeofString(this.name);
        size += this.parent.sizeofObject();
        for (int i0 = 0; i0 < 0; i0++)
            size += this.children[i0].sizeofObject();
        
        return size;
    }
}