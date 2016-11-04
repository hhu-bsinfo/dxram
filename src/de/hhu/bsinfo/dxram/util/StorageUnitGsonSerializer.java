package de.hhu.bsinfo.dxram.util;

import java.lang.reflect.Type;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import de.hhu.bsinfo.utils.unit.StorageUnit;

/**
 * Special Gson serializer/deserializer for a StorageUnit
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.10.2016
 */
public class StorageUnitGsonSerializer implements JsonDeserializer<StorageUnit>, JsonSerializer<StorageUnit> {
    /**
     * @param p_jsonElement
     * @param p_type
     * @param p_jsonDeserializationContext
     * @return
     * @throws JsonParseException
     */
    @Override
    public StorageUnit deserialize(final JsonElement p_jsonElement, final Type p_type, final JsonDeserializationContext p_jsonDeserializationContext) {

        JsonObject jsonObj = p_jsonElement.getAsJsonObject();
        long value = jsonObj.get("m_value").getAsLong();

        JsonElement unitElem = jsonObj.get("m_unit");
        String unit;
        if (unitElem == null) {
            unit = StorageUnit.BYTE;
        } else {
            unit = unitElem.getAsString();
        }

        return new StorageUnit(value, unit);
    }

    @Override
    public JsonElement serialize(final StorageUnit p_storageUnit, final Type p_type, final JsonSerializationContext p_jsonSerializationContext) {

        JsonObject jsonObj = new JsonObject();

        jsonObj.addProperty("m_value", p_storageUnit.getBytes());
        jsonObj.addProperty("m_unit", StorageUnit.BYTE);

        return jsonObj;
    }
}
