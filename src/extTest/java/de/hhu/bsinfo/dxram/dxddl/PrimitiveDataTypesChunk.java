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

public class PrimitiveDataTypesChunk implements Importable, Exportable {



    private boolean b;
    private byte b2;
    private char c;
    private double d;
    private float f;
    private int num;
    private long bignum;
    private short s;

    public PrimitiveDataTypesChunk() {
        
    }

    public boolean getB() {
        return this.b;
    }

    public void setB(boolean b) {
        this.b = b;
    }

    public byte getB2() {
        return this.b2;
    }

    public void setB2(byte b2) {
        this.b2 = b2;
    }

    public char getC() {
        return this.c;
    }

    public void setC(char c) {
        this.c = c;
    }

    public double getD() {
        return this.d;
    }

    public void setD(double d) {
        this.d = d;
    }

    public float getF() {
        return this.f;
    }

    public void setF(float f) {
        this.f = f;
    }

    public int getNum() {
        return this.num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    public long getBignum() {
        return this.bignum;
    }

    public void setBignum(long bignum) {
        this.bignum = bignum;
    }

    public short getS() {
        return this.s;
    }

    public void setS(short s) {
        this.s = s;
    }



    @Override
    public void importObject(final Importer p_importer) {
        this.b = p_importer.readBoolean(this.b);
        this.b2 = p_importer.readByte(this.b2);
        this.c = p_importer.readChar(this.c);
        this.d = p_importer.readDouble(this.d);
        this.f = p_importer.readFloat(this.f);
        this.num = p_importer.readInt(this.num);
        this.bignum = p_importer.readLong(this.bignum);
        this.s = p_importer.readShort(this.s);
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeBoolean(this.b);
        p_exporter.writeByte(this.b2);
        p_exporter.writeChar(this.c);
        p_exporter.writeDouble(this.d);
        p_exporter.writeFloat(this.f);
        p_exporter.writeInt(this.num);
        p_exporter.writeLong(this.bignum);
        p_exporter.writeShort(this.s);
    }

    @Override
    public int sizeofObject() {
        int size = 0;

        // size of basic types
        size += 30;
        return size;
    }
}