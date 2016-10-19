package de.hhu.bsinfo.dxcompute.ms.tcmd;

import java.lang.reflect.Type;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import de.hhu.bsinfo.dxcompute.ms.TaskPayload;
import de.hhu.bsinfo.dxcompute.ms.TaskPayloadManager;
import de.hhu.bsinfo.dxcompute.ms.tasks.NullTaskPayload;

/**
 * Created by nothaas on 10/19/16.
 */
public class TestJson {

	static class BagOfPrimitives {
		private int value1 = 1;
		private String value2 = "abc";
		private int value3 = 3;

		BagOfPrimitives() {
			// no-args constructor
		}

		@Override
		public String toString() {

			return value1 + ", " + value2 + ", " + value3;
		}
	}

	static class TaskDeserializer implements JsonDeserializer<TaskPayload> {

		@Override
		public TaskPayload deserialize(JsonElement jsonElement, Type type,
				JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {

			System.out.println("asd");
			JsonObject jsonObj = jsonElement.getAsJsonObject();
			short taskType = jsonObj.get("m_typeId").getAsShort();
			short taskSubtype = jsonObj.get("m_subtypeId").getAsShort();

			TaskPayload payload = TaskPayloadManager.createInstance(taskType, taskSubtype);
			return jsonDeserializationContext.deserialize(jsonElement, payload.getClass());
		}
	}

	public static void main(String[] args) {

		TaskPayloadManager.registerTaskPayloadClass((short) 0, (short) 0, NullTaskPayload.class);

		NullTaskPayload nullTask = new NullTaskPayload();
		Gson gson = new GsonBuilder().setPrettyPrinting().excludeFieldsWithoutExposeAnnotation()
				.registerTypeAdapter(TaskPayload.class,
						new TaskDeserializer()).create();
		String json = gson.toJson(nullTask);

		System.out.println(json);

		TaskPayload payload = gson.fromJson(json, TaskPayload.class);
		System.out.println(payload);
	}
}
