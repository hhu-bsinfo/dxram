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

public class StringsStruct implements Importable, Exportable {



    private String s1;
    private String s2;

    public StringsStruct() {
        this.s1 = new String();
        this.s2 = new String();
    }

    public String getS1() {
        return this.s1;
    }

    public void setS1(String s1) {
        if (s1 == null)
            throw new NullPointerException("Parameter s1 must not be null");
        this.s1 = s1;
    }

    public String getS2() {
        return this.s2;
    }

    public void setS2(String s2) {
        if (s2 == null)
            throw new NullPointerException("Parameter s2 must not be null");
        this.s2 = s2;
    }


    
    @Override
    public void importObject(final Importer p_importer) {
        this.s1 = p_importer.readString(this.s1);
        this.s2 = p_importer.readString(this.s2);
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeString(this.s1);
        p_exporter.writeString(this.s2);
    }

    @Override
    public int sizeofObject() {
        int size = 0;

        // size of complex types
        size += sizeofString(this.s1);
        size += sizeofString(this.s2);
        
        return size;
    }
}