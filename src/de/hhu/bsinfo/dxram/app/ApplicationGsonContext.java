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

package de.hhu.bsinfo.dxram.app;

import java.lang.reflect.Type;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxutils.StorageUnitGsonSerializer;
import de.hhu.bsinfo.dxutils.TimeUnitGsonSerializer;
import de.hhu.bsinfo.dxutils.unit.StorageUnit;
import de.hhu.bsinfo.dxutils.unit.TimeUnit;

/**
 * Gson context for applications handling serialization and deserialization of configuration values
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 17.05.2017
 */
final class ApplicationGsonContext {
    private static final Logger LOGGER = LogManager.getFormatterLogger(ApplicationGsonContext.class.getSimpleName());

    /**
     * Hidden constructor
     */
    private ApplicationGsonContext() {
    }

    /**
     * Create a Gson instance with all adapters attached for serialization/deserialization
     *
     * @return Gson context
     */
    static Gson createGsonInstance(final Class<? extends AbstractApplication> p_appClass) {
        return new GsonBuilder().setPrettyPrinting().excludeFieldsWithoutExposeAnnotation()
                .registerTypeAdapter(AbstractApplication.class, new ApplicationSerializer(p_appClass))
                .registerTypeAdapter(StorageUnit.class, new StorageUnitGsonSerializer()).registerTypeAdapter(TimeUnit.class, new TimeUnitGsonSerializer())
                .create();
    }

    /**
     * Gson serializer and deserializer for DXRAM application classes
     */
    private static class ApplicationSerializer implements JsonDeserializer<AbstractApplication>, JsonSerializer<AbstractApplication> {
        private Class<? extends AbstractApplication> m_appClass;

        public ApplicationSerializer(final Class<? extends AbstractApplication> p_appClass) {
            m_appClass = p_appClass;
        }

        @Override
        public AbstractApplication deserialize(final JsonElement p_jsonElement, final Type p_type,
                final JsonDeserializationContext p_jsonDeserializationContext) {

            JsonObject jsonObj = p_jsonElement.getAsJsonObject();
            String className = jsonObj.get("m_class").getAsString();
            boolean enabled = jsonObj.get("m_enabled").getAsBoolean();

            // don't create instance if disabled
            if (!enabled) {
                return null;
            }

            if (!m_appClass.getSuperclass().equals(AbstractApplication.class)) {
                // check if there is an "interface"/abstract class between DXRAMComponent and the instance to
                // create
                if (!m_appClass.getSuperclass().getSuperclass().equals(AbstractApplication.class)) {
                    LOGGER.fatal("Could class '%s' is not a subclass of AbstractApplication, check your config file", className);
                    return null;
                }
            }

            return p_jsonDeserializationContext.deserialize(p_jsonElement, m_appClass);
        }

        @Override
        public JsonElement serialize(final AbstractApplication p_abstractApplication, final Type p_type,
                final JsonSerializationContext p_jsonSerializationContext) {

            return p_jsonSerializationContext.serialize(p_abstractApplication, m_appClass);
        }
    }
}
