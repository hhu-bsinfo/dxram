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

public class Person implements Importable, Exportable {



    private String name;
    private short age;
    private long dateOfBirth;
    private City placeOfBirth;
    private String homeAddress;
    private String email;
    private int[] favoriteNumbers;
    private Person[] friends;
    private Person[] family;

    public Person() {
        this.name = new String();
        this.placeOfBirth = new City();
        this.homeAddress = new String();
        this.email = new String();
        this.favoriteNumbers = new int[0];
        this.friends = new Person[0];
        for (int i0 = 0; i0 < 0; i0++) {
            Person obj7 = new Person();
            this.friends[i0] = obj7;
        }
        this.family = new Person[0];
        for (int i0 = 0; i0 < 0; i0++) {
            Person obj8 = new Person();
            this.family[i0] = obj8;
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

    public short getAge() {
        return this.age;
    }

    public void setAge(short age) {
        this.age = age;
    }

    public long getDateOfBirth() {
        return this.dateOfBirth;
    }

    public void setDateOfBirth(long dateOfBirth) {
        this.dateOfBirth = dateOfBirth;
    }

    public City getPlaceOfBirth() {
        return this.placeOfBirth;
    }

    public void setPlaceOfBirth(City placeOfBirth) {
        if (placeOfBirth == null)
            throw new NullPointerException("Parameter placeOfBirth must not be null");
        this.placeOfBirth = placeOfBirth;
    }

    public String getHomeAddress() {
        return this.homeAddress;
    }

    public void setHomeAddress(String homeAddress) {
        if (homeAddress == null)
            throw new NullPointerException("Parameter homeAddress must not be null");
        this.homeAddress = homeAddress;
    }

    public String getEmail() {
        return this.email;
    }

    public void setEmail(String email) {
        if (email == null)
            throw new NullPointerException("Parameter email must not be null");
        this.email = email;
    }

    public int[] getFavoriteNumbers() {
        return this.favoriteNumbers;
    }

    public Person[] getFriends() {
        return this.friends;
    }

    public Person[] getFamily() {
        return this.family;
    }



    @Override
    public void importObject(final Importer p_importer) {
        this.name = p_importer.readString(this.name);
        this.age = p_importer.readShort(this.age);
        this.dateOfBirth = p_importer.readLong(this.dateOfBirth);
        p_importer.importObject(this.placeOfBirth);
        
        this.homeAddress = p_importer.readString(this.homeAddress);
        this.email = p_importer.readString(this.email);
        for (int i0 = 0; i0 < 0; i0++)
            this.favoriteNumbers[i0] = p_importer.readInt(this.favoriteNumbers[i0]);
        
        for (int i0 = 0; i0 < 0; i0++) {
            p_importer.importObject(this.friends[i0]);
        }
        
        for (int i0 = 0; i0 < 0; i0++) {
            p_importer.importObject(this.family[i0]);
        }
        
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeString(this.name);
        p_exporter.writeShort(this.age);
        p_exporter.writeLong(this.dateOfBirth);
        p_exporter.exportObject(this.placeOfBirth);
        p_exporter.writeString(this.homeAddress);
        p_exporter.writeString(this.email);
        for (int i0 = 0; i0 < 0; i0++)
            p_exporter.writeInt(this.favoriteNumbers[i0]);
        
        for (int i0 = 0; i0 < 0; i0++)
            p_exporter.exportObject(this.friends[i0]);
        
        for (int i0 = 0; i0 < 0; i0++)
            p_exporter.exportObject(this.family[i0]);
        
    }

    @Override
    public int sizeofObject() {
        int size = 0;

        // size of basic types
        size += 10;

        // size of complex types
        size += sizeofString(this.name);
        size += this.placeOfBirth.sizeofObject();
        size += sizeofString(this.homeAddress);
        size += sizeofString(this.email);
        for (int i0 = 0; i0 < 0; i0++)
            size += this.friends[i0].sizeofObject();
        for (int i0 = 0; i0 < 0; i0++)
            size += this.family[i0].sizeofObject();
        
        return size;
    }
}