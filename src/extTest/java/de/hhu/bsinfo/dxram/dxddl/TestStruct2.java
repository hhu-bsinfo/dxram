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

public class TestStruct2 implements Importable, Exportable {



    private String s;
    private int[] num;
    private short x1;
    private short x2;
    private char[] alpha;
    private short y1;
    private short y2;

    public TestStruct2() {
        this.s = new String();
        this.num = new int[0];
        this.alpha = new char[0];
        
    }

    public String getS() {
        return this.s;
    }

    public void setS(String s) {
        if (s == null)
            throw new NullPointerException("Parameter s must not be null");
        this.s = s;
    }

    public int[] getNum() {
        return this.num;
    }

    public short getX1() {
        return this.x1;
    }

    public void setX1(short x1) {
        this.x1 = x1;
    }

    public short getX2() {
        return this.x2;
    }

    public void setX2(short x2) {
        this.x2 = x2;
    }

    public char[] getAlpha() {
        return this.alpha;
    }

    public short getY1() {
        return this.y1;
    }

    public void setY1(short y1) {
        this.y1 = y1;
    }

    public short getY2() {
        return this.y2;
    }

    public void setY2(short y2) {
        this.y2 = y2;
    }


    
    @Override
    public void importObject(final Importer p_importer) {
        this.s = p_importer.readString(this.s);
        for (int i0 = 0; i0 < 0; i0++)
            this.num[i0] = p_importer.readInt(this.num[i0]);
        
        this.x1 = p_importer.readShort(this.x1);
        this.x2 = p_importer.readShort(this.x2);
        for (int i0 = 0; i0 < 0; i0++)
            this.alpha[i0] = p_importer.readChar(this.alpha[i0]);
        
        this.y1 = p_importer.readShort(this.y1);
        this.y2 = p_importer.readShort(this.y2);
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeString(this.s);
        for (int i0 = 0; i0 < 0; i0++)
            p_exporter.writeInt(this.num[i0]);
        
        p_exporter.writeShort(this.x1);
        p_exporter.writeShort(this.x2);
        for (int i0 = 0; i0 < 0; i0++)
            p_exporter.writeChar(this.alpha[i0]);
        
        p_exporter.writeShort(this.y1);
        p_exporter.writeShort(this.y2);
    }

    @Override
    public int sizeofObject() {
        int size = 0;

        // size of basic types
        size += 8;

        // size of complex types
        size += sizeofString(this.s);
        
        return size;
    }
}