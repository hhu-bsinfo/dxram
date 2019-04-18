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

public class ComplexChunk implements Importable, Exportable {



    private SimpleChunk[] misc;
    private int num;
    private ComplexChunk parent;
    private double temp;
    private SimpleChunk another;
    private short[] refs;
    private TestStruct1 test1;
    private ComplexChunk[] children;
    private String name;
    private Person person;
    private Weekday day;
    private TestStruct2 test2;
    private Country country;
    private Month month;
    private City city;
    private PrimitiveDataTypesStruct primitiveTypes;
    private PrimitiveDataTypesChunk primitiveTypes2;
    private OS myOS;
    private HotDrink favoriteDrink;
    private StringsChunk moreStrings;
    private ProgrammingLanguage favoriteLang;
    private StringsStruct moreStrings2;

    public ComplexChunk() {
        this.misc = new SimpleChunk[0];
        for (int i0 = 0; i0 < 0; i0++) {
            SimpleChunk obj0 = new SimpleChunk();
            this.misc[i0] = obj0;
        }
        this.parent = new ComplexChunk();
        this.another = new SimpleChunk();
        this.refs = new short[0];
        this.test1 = new TestStruct1();
        this.children = new ComplexChunk[0];
        for (int i0 = 0; i0 < 0; i0++) {
            ComplexChunk obj7 = new ComplexChunk();
            this.children[i0] = obj7;
        }
        this.name = new String();
        this.person = new Person();
        this.test2 = new TestStruct2();
        this.country = new Country();
        this.city = new City();
        this.primitiveTypes = new PrimitiveDataTypesStruct();
        this.primitiveTypes2 = new PrimitiveDataTypesChunk();
        this.moreStrings = new StringsChunk();
        this.moreStrings2 = new StringsStruct();
    }

    public SimpleChunk[] getMisc() {
        return this.misc;
    }

    public int getNum() {
        return this.num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    public ComplexChunk getParent() {
        return this.parent;
    }

    public void setParent(ComplexChunk parent) {
        if (parent == null)
            throw new NullPointerException("Parameter parent must not be null");
        this.parent = parent;
    }

    public double getTemp() {
        return this.temp;
    }

    public void setTemp(double temp) {
        this.temp = temp;
    }

    public SimpleChunk getAnother() {
        return this.another;
    }

    public void setAnother(SimpleChunk another) {
        if (another == null)
            throw new NullPointerException("Parameter another must not be null");
        this.another = another;
    }

    public short[] getRefs() {
        return this.refs;
    }

    public TestStruct1 getTest1() {
        return this.test1;
    }

    public void setTest1(TestStruct1 test1) {
        if (test1 == null)
            throw new NullPointerException("Parameter test1 must not be null");
        this.test1 = test1;
    }

    public ComplexChunk[] getChildren() {
        return this.children;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        if (name == null)
            throw new NullPointerException("Parameter name must not be null");
        this.name = name;
    }

    public Person getPerson() {
        return this.person;
    }

    public void setPerson(Person person) {
        if (person == null)
            throw new NullPointerException("Parameter person must not be null");
        this.person = person;
    }

    public Weekday getDay() {
        return this.day;
    }

    public void setDay(Weekday day) {
        if (day == null)
            throw new NullPointerException("Parameter day must not be null");
        this.day = day;
    }

    public TestStruct2 getTest2() {
        return this.test2;
    }

    public void setTest2(TestStruct2 test2) {
        if (test2 == null)
            throw new NullPointerException("Parameter test2 must not be null");
        this.test2 = test2;
    }

    public Country getCountry() {
        return this.country;
    }

    public void setCountry(Country country) {
        if (country == null)
            throw new NullPointerException("Parameter country must not be null");
        this.country = country;
    }

    public Month getMonth() {
        return this.month;
    }

    public void setMonth(Month month) {
        if (month == null)
            throw new NullPointerException("Parameter month must not be null");
        this.month = month;
    }

    public City getCity() {
        return this.city;
    }

    public void setCity(City city) {
        if (city == null)
            throw new NullPointerException("Parameter city must not be null");
        this.city = city;
    }

    public PrimitiveDataTypesStruct getPrimitiveTypes() {
        return this.primitiveTypes;
    }

    public void setPrimitiveTypes(PrimitiveDataTypesStruct primitiveTypes) {
        if (primitiveTypes == null)
            throw new NullPointerException("Parameter primitiveTypes must not be null");
        this.primitiveTypes = primitiveTypes;
    }

    public PrimitiveDataTypesChunk getPrimitiveTypes2() {
        return this.primitiveTypes2;
    }

    public void setPrimitiveTypes2(PrimitiveDataTypesChunk primitiveTypes2) {
        if (primitiveTypes2 == null)
            throw new NullPointerException("Parameter primitiveTypes2 must not be null");
        this.primitiveTypes2 = primitiveTypes2;
    }

    public OS getMyOS() {
        return this.myOS;
    }

    public void setMyOS(OS myOS) {
        if (myOS == null)
            throw new NullPointerException("Parameter myOS must not be null");
        this.myOS = myOS;
    }

    public HotDrink getFavoriteDrink() {
        return this.favoriteDrink;
    }

    public void setFavoriteDrink(HotDrink favoriteDrink) {
        if (favoriteDrink == null)
            throw new NullPointerException("Parameter favoriteDrink must not be null");
        this.favoriteDrink = favoriteDrink;
    }

    public StringsChunk getMoreStrings() {
        return this.moreStrings;
    }

    public void setMoreStrings(StringsChunk moreStrings) {
        if (moreStrings == null)
            throw new NullPointerException("Parameter moreStrings must not be null");
        this.moreStrings = moreStrings;
    }

    public ProgrammingLanguage getFavoriteLang() {
        return this.favoriteLang;
    }

    public void setFavoriteLang(ProgrammingLanguage favoriteLang) {
        if (favoriteLang == null)
            throw new NullPointerException("Parameter favoriteLang must not be null");
        this.favoriteLang = favoriteLang;
    }

    public StringsStruct getMoreStrings2() {
        return this.moreStrings2;
    }

    public void setMoreStrings2(StringsStruct moreStrings2) {
        if (moreStrings2 == null)
            throw new NullPointerException("Parameter moreStrings2 must not be null");
        this.moreStrings2 = moreStrings2;
    }



    @Override
    public void importObject(final Importer p_importer) {
        for (int i0 = 0; i0 < 0; i0++) {
            p_importer.importObject(this.misc[i0]);
        }
        
        this.num = p_importer.readInt(this.num);
        p_importer.importObject(this.parent);
        
        this.temp = p_importer.readDouble(this.temp);
        p_importer.importObject(this.another);
        
        for (int i0 = 0; i0 < 0; i0++)
            this.refs[i0] = p_importer.readShort(this.refs[i0]);
        
        p_importer.importObject(this.test1);
        
        for (int i0 = 0; i0 < 0; i0++) {
            p_importer.importObject(this.children[i0]);
        }
        
        this.name = p_importer.readString(this.name);
        p_importer.importObject(this.person);
        
        this.day = Weekday.values()[p_importer.readInt(this.day.ordinal())];
        p_importer.importObject(this.test2);
        
        p_importer.importObject(this.country);
        
        this.month = Month.values()[p_importer.readInt(this.month.ordinal())];
        p_importer.importObject(this.city);
        
        p_importer.importObject(this.primitiveTypes);
        
        p_importer.importObject(this.primitiveTypes2);
        
        this.myOS = OS.values()[p_importer.readInt(this.myOS.ordinal())];
        this.favoriteDrink = HotDrink.values()[p_importer.readInt(this.favoriteDrink.ordinal())];
        p_importer.importObject(this.moreStrings);
        
        this.favoriteLang = ProgrammingLanguage.values()[p_importer.readInt(this.favoriteLang.ordinal())];
        p_importer.importObject(this.moreStrings2);
        
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        for (int i0 = 0; i0 < 0; i0++)
            p_exporter.exportObject(this.misc[i0]);
        
        p_exporter.writeInt(this.num);
        p_exporter.exportObject(this.parent);
        p_exporter.writeDouble(this.temp);
        p_exporter.exportObject(this.another);
        for (int i0 = 0; i0 < 0; i0++)
            p_exporter.writeShort(this.refs[i0]);
        
        p_exporter.exportObject(this.test1);
        for (int i0 = 0; i0 < 0; i0++)
            p_exporter.exportObject(this.children[i0]);
        
        p_exporter.writeString(this.name);
        p_exporter.exportObject(this.person);
        p_exporter.writeInt(this.day.ordinal());
        p_exporter.exportObject(this.test2);
        p_exporter.exportObject(this.country);
        p_exporter.writeInt(this.month.ordinal());
        p_exporter.exportObject(this.city);
        p_exporter.exportObject(this.primitiveTypes);
        p_exporter.exportObject(this.primitiveTypes2);
        p_exporter.writeInt(this.myOS.ordinal());
        p_exporter.writeInt(this.favoriteDrink.ordinal());
        p_exporter.exportObject(this.moreStrings);
        p_exporter.writeInt(this.favoriteLang.ordinal());
        p_exporter.exportObject(this.moreStrings2);
    }

    @Override
    public int sizeofObject() {
        int size = 0;

        // size of basic types
        size += 12;

        // size of complex types
        for (int i0 = 0; i0 < 0; i0++)
            size += this.misc[i0].sizeofObject();
        size += this.parent.sizeofObject();
        size += this.another.sizeofObject();
        size += this.test1.sizeofObject();
        for (int i0 = 0; i0 < 0; i0++)
            size += this.children[i0].sizeofObject();
        size += sizeofString(this.name);
        size += this.person.sizeofObject();
        size += this.day.sizeofObject();
        size += this.test2.sizeofObject();
        size += this.country.sizeofObject();
        size += this.month.sizeofObject();
        size += this.city.sizeofObject();
        size += this.primitiveTypes.sizeofObject();
        size += this.primitiveTypes2.sizeofObject();
        size += this.myOS.sizeofObject();
        size += this.favoriteDrink.sizeofObject();
        size += this.moreStrings.sizeofObject();
        size += this.favoriteLang.sizeofObject();
        size += this.moreStrings2.sizeofObject();
        
        return size;
    }
}