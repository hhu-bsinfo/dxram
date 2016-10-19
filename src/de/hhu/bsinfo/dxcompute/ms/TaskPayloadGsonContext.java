package de.hhu.bsinfo.dxcompute.ms;

import java.lang.reflect.Type;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;

/**
 * Helper class for using gson and deserializer to create the correct TaskPayload instances.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 19.10.16
 */
public class TaskPayloadGsonContext implements JsonDeserializer<TaskPayload> {

	public static Gson createGsonInstance() {
		return new GsonBuilder()
				.setPrettyPrinting()
				.excludeFieldsWithoutExposeAnnotation()
				.registerTypeAdapter(TaskPayload.class,
						new TaskPayloadGsonContext())
				.create();
	}

	@Override
	public TaskPayload deserialize(final JsonElement p_jsonElement, final Type p_type,
			final JsonDeserializationContext p_jsonDeserializationContext) throws JsonParseException {

		JsonObject jsonObj = p_jsonElement.getAsJsonObject();
		short taskType = jsonObj.get("m_typeId").getAsShort();
		short taskSubtype = jsonObj.get("m_subtypeId").getAsShort();

		Class<? extends TaskPayload> clazz = TaskPayloadManager.getRegisteredClass(taskType, taskSubtype);

		if (clazz == null) {
			return null;
		} else {
			return p_jsonDeserializationContext.deserialize(p_jsonElement, clazz);
		}
	}
}
