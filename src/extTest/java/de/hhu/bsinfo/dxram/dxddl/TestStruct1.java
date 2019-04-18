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

public class TestStruct1 implements Importable, Exportable {



    private boolean b;
    private char[] c;
    private String s;
    private int i;
    private double[] d;

    public TestStruct1() {
        this.c = new char[0];
        this.s = new String();
        this.d = new double[0];
    }

    public boolean getB() {
        return this.b;
    }

    public void setB(boolean b) {
        this.b = b;
    }

    public char[] getC() {
        return this.c;
    }

    public String getS() {
        return this.s;
    }

    public void setS(String s) {
        if (s == null)
            throw new NullPointerException("Parameter s must not be null");
        this.s = s;
    }

    public int getI() {
        return this.i;
    }

    public void setI(int i) {
        this.i = i;
    }

    public double[] getD() {
        return this.d;
    }


    
    @Override
    public void importObject(final Importer p_importer) {
        this.b = p_importer.readBoolean(this.b);
        for (int i0 = 0; i0 < 0; i0++)
            this.c[i0] = p_importer.readChar(this.c[i0]);
        
        this.s = p_importer.readString(this.s);
        this.i = p_importer.readInt(this.i);
        for (int i0 = 0; i0 < 0; i0++)
            this.d[i0] = p_importer.readDouble(this.d[i0]);
        
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeBoolean(this.b);
        for (int i0 = 0; i0 < 0; i0++)
            p_exporter.writeChar(this.c[i0]);
        
        p_exporter.writeString(this.s);
        p_exporter.writeInt(this.i);
        for (int i0 = 0; i0 < 0; i0++)
            p_exporter.writeDouble(this.d[i0]);
        
    }

    @Override
    public int sizeofObject() {
        int size = 0;

        // size of basic types
        size += 5;

        // size of complex types
        size += sizeofString(this.s);
        
        return size;
    }
}