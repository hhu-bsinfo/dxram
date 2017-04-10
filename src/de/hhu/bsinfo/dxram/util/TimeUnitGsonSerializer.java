/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.util;

import java.lang.reflect.Type;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import de.hhu.bsinfo.utils.unit.TimeUnit;

/**
 * Special Gson serializer/deserializer for a TimeUnit
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.10.2016
 */
public class TimeUnitGsonSerializer implements JsonDeserializer<TimeUnit>, JsonSerializer<TimeUnit> {
    @Override
    public TimeUnit deserialize(final JsonElement p_jsonElement, final Type p_type, final JsonDeserializationContext p_jsonDeserializationContext) {

        JsonObject jsonObj = p_jsonElement.getAsJsonObject();
        long value = jsonObj.get("m_value").getAsLong();

        JsonElement unitElem = jsonObj.get("m_unit");
        String unit;
        if (unitElem == null) {
            unit = TimeUnit.MS;
        } else {
            unit = unitElem.getAsString();
        }

        return new TimeUnit(value, unit);
    }

    @Override
    public JsonElement serialize(final TimeUnit p_timeUnit, final Type p_type, final JsonSerializationContext p_jsonSerializationContext) {

        JsonObject jsonObj = new JsonObject();

        // DXRAM's default time units are at least ms
        jsonObj.addProperty("m_value", p_timeUnit.getMs());
        jsonObj.addProperty("m_unit", TimeUnit.MS);

        return jsonObj;
    }
}
