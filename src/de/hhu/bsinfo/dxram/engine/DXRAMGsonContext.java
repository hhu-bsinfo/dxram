package de.hhu.bsinfo.dxram.engine;

import java.lang.reflect.Type;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import de.hhu.bsinfo.dxram.util.StorageUnitGsonSerializer;
import de.hhu.bsinfo.dxram.util.TimeUnitGsonSerializer;
import de.hhu.bsinfo.utils.unit.StorageUnit;
import de.hhu.bsinfo.utils.unit.TimeUnit;

/**
 * Gson context for DXRAM handling serialization and deserialization of components and services
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 20.10.16
 */
class DXRAMGsonContext {

	static Gson createGsonInstance() {
		return new GsonBuilder()
				.setPrettyPrinting()
				.excludeFieldsWithoutExposeAnnotation()
				.registerTypeAdapter(AbstractDXRAMComponent.class,
						new ComponentSerializer())
				.registerTypeAdapter(AbstractDXRAMService.class,
						new ServiceSerializer())
				.registerTypeAdapter(StorageUnit.class,
						new StorageUnitGsonSerializer())
				.registerTypeAdapter(TimeUnit.class,
						new TimeUnitGsonSerializer())
				.create();
	}

	/**
	 * Gson serializer and deserizlier for components
	 */
	private static class ComponentSerializer
			implements JsonDeserializer<AbstractDXRAMComponent>, JsonSerializer<AbstractDXRAMComponent> {
		@Override
		public AbstractDXRAMComponent deserialize(final JsonElement p_jsonElement, final Type p_type,
				final JsonDeserializationContext p_jsonDeserializationContext) throws JsonParseException {

			JsonObject jsonObj = p_jsonElement.getAsJsonObject();
			String className = jsonObj.get("m_class").getAsString();
			boolean enabled = jsonObj.get("m_enabled").getAsBoolean();

			// don't create instance if disabled
			if (!enabled) {
				return null;
			}

			Class<?> clazz;
			try {
				clazz = Class.forName(className);
			} catch (final ClassNotFoundException e) {
				return null;
			}

			if (!clazz.getSuperclass().equals(AbstractDXRAMComponent.class)) {
				// check if there is an "interface"/abstract class between DXRAMComponent and the instance to
				// create
				if (!clazz.getSuperclass().getSuperclass().equals(AbstractDXRAMComponent.class)) {
					return null;
				}
			}

			return p_jsonDeserializationContext.deserialize(p_jsonElement, clazz);
		}

		@Override
		public JsonElement serialize(final AbstractDXRAMComponent p_abstractDXRAMComponent, final Type p_type,
				final JsonSerializationContext p_jsonSerializationContext) {

			Class<?> clazz;
			try {
				clazz = Class.forName(p_abstractDXRAMComponent.getClass().getName());
			} catch (final ClassNotFoundException e) {
				return null;
			}

			return p_jsonSerializationContext.serialize(p_abstractDXRAMComponent, clazz);
		}
	}

	/**
	 * Gson serializer and deserializer for services
	 */
	private static class ServiceSerializer
			implements JsonDeserializer<AbstractDXRAMService>, JsonSerializer<AbstractDXRAMService> {
		@Override
		public AbstractDXRAMService deserialize(final JsonElement p_jsonElement, final Type p_type,
				final JsonDeserializationContext p_jsonDeserializationContext) throws JsonParseException {

			JsonObject jsonObj = p_jsonElement.getAsJsonObject();
			String className = jsonObj.get("m_class").getAsString();
			boolean enabled = jsonObj.get("m_enabled").getAsBoolean();

			// don't create instance if disabled
			if (!enabled) {
				return null;
			}

			Class<?> clazz;
			try {
				clazz = Class.forName(className);
			} catch (final ClassNotFoundException e) {
				return null;
			}

			if (!clazz.getSuperclass().equals(AbstractDXRAMService.class)) {
				// check if there is an "interface"/abstract class between DXRAMService and the instance to
				// create
				if (!clazz.getSuperclass().getSuperclass().equals(AbstractDXRAMService.class)) {
					return null;
				}
			}

			return p_jsonDeserializationContext.deserialize(p_jsonElement, clazz);
		}

		@Override
		public JsonElement serialize(final AbstractDXRAMService p_abstractDXRAMService, final Type p_type,
				final JsonSerializationContext p_jsonSerializationContext) {

			Class<?> clazz;
			try {
				clazz = Class.forName(p_abstractDXRAMService.getClass().getName());
			} catch (final ClassNotFoundException e) {
				return null;
			}

			return p_jsonSerializationContext.serialize(p_abstractDXRAMService, clazz);
		}
	}
}
