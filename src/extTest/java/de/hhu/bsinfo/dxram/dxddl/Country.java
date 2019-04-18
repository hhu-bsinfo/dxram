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

public class Country implements Importable, Exportable {



    private String name;
    private long population;
    private int area;
    private City capital;
    private City[] cities;

    public Country() {
        this.name = new String();
        this.capital = new City();
        this.cities = new City[0];
        for (int i0 = 0; i0 < 0; i0++) {
            City obj4 = new City();
            this.cities[i0] = obj4;
        }
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        if (name == null)
            throw new NullPointerException("Parameter name must not be null");
        this.name = name;
    }

    public long getPopulation() {
        return this.population;
    }

    public void setPopulation(long population) {
        this.population = population;
    }

    public int getArea() {
        return this.area;
    }

    public void setArea(int area) {
        this.area = area;
    }

    public City getCapital() {
        return this.capital;
    }

    public void setCapital(City capital) {
        if (capital == null)
            throw new NullPointerException("Parameter capital must not be null");
        this.capital = capital;
    }

    public City[] getCities() {
        return this.cities;
    }



    @Override
    public void importObject(final Importer p_importer) {
        this.name = p_importer.readString(this.name);
        this.population = p_importer.readLong(this.population);
        this.area = p_importer.readInt(this.area);
        p_importer.importObject(this.capital);
        
        for (int i0 = 0; i0 < 0; i0++) {
            p_importer.importObject(this.cities[i0]);
        }
        
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeString(this.name);
        p_exporter.writeLong(this.population);
        p_exporter.writeInt(this.area);
        p_exporter.exportObject(this.capital);
        for (int i0 = 0; i0 < 0; i0++)
            p_exporter.exportObject(this.cities[i0]);
        
    }

    @Override
    public int sizeofObject() {
        int size = 0;

        // size of basic types
        size += 12;

        // size of complex types
        size += sizeofString(this.name);
        size += this.capital.sizeofObject();
        for (int i0 = 0; i0 < 0; i0++)
            size += this.cities[i0].sizeofObject();
        
        return size;
    }
}