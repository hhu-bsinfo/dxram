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

public class City implements Importable, Exportable {



    private String name;
    private Country country;
    private int population;
    private int area;

    public City() {
        this.name = new String();
        this.country = new Country();
        
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        if (name == null)
            throw new NullPointerException("Parameter name must not be null");
        this.name = name;
    }

    public Country getCountry() {
        return this.country;
    }

    public void setCountry(Country country) {
        if (country == null)
            throw new NullPointerException("Parameter country must not be null");
        this.country = country;
    }

    public int getPopulation() {
        return this.population;
    }

    public void setPopulation(int population) {
        this.population = population;
    }

    public int getArea() {
        return this.area;
    }

    public void setArea(int area) {
        this.area = area;
    }



    @Override
    public void importObject(final Importer p_importer) {
        this.name = p_importer.readString(this.name);
        p_importer.importObject(this.country);
        
        this.population = p_importer.readInt(this.population);
        this.area = p_importer.readInt(this.area);
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeString(this.name);
        p_exporter.exportObject(this.country);
        p_exporter.writeInt(this.population);
        p_exporter.writeInt(this.area);
    }

    @Override
    public int sizeofObject() {
        int size = 0;

        // size of basic types
        size += 8;

        // size of complex types
        size += sizeofString(this.name);
        size += this.country.sizeofObject();
        
        return size;
    }
}