package de.hhu.bsinfo.dxram.util;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import de.hhu.bsinfo.utils.StorageUnit;

import java.lang.reflect.Type;

/**
 * Special Gson serializer/deserializer for a StorageUnit
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 24.10.16
 */
public class StorageUnitGsonSerializer implements JsonDeserializer<StorageUnit>, JsonSerializer<StorageUnit> {
    @Override
    public StorageUnit deserialize(final JsonElement p_jsonElement, final Type p_type,
           final JsonDeserializationContext p_jsonDeserializationContext) throws JsonParseException {

        JsonObject jsonObj = p_jsonElement.getAsJsonObject();
        long value = jsonObj.get("m_value").getAsLong();

        JsonElement unitElem = jsonObj.get("m_unit");
        String unit;
        if (unitElem == null) {
            unit = StorageUnit.UNIT_BYTE;
        } else {
            unit = unitElem.getAsString();
        }

        return new StorageUnit(value, unit);
    }

    @Override
    public JsonElement serialize(final StorageUnit p_storageUnit, final Type p_type,
            final JsonSerializationContext p_jsonSerializationContext) {

        JsonObject jsonObj = new JsonObject();

        jsonObj.addProperty("m_value", p_storageUnit.getBytes());
        jsonObj.addProperty("m_unit", StorageUnit.UNIT_BYTE);

        return jsonObj;
    }
}
