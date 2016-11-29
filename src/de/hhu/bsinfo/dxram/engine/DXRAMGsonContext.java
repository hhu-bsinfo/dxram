/*
 * Copyright (C) 2016 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

package de.hhu.bsinfo.dxram.engine;

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

import de.hhu.bsinfo.dxram.util.StorageUnitGsonSerializer;
import de.hhu.bsinfo.dxram.util.TimeUnitGsonSerializer;
import de.hhu.bsinfo.utils.unit.StorageUnit;
import de.hhu.bsinfo.utils.unit.TimeUnit;

/**
 * Gson context for DXRAM handling serialization and deserialization of components and services
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 20.10.2016
 */
final class DXRAMGsonContext {

    private static final Logger LOGGER = LogManager.getFormatterLogger(DXRAMGsonContext.class.getSimpleName());

    /**
     * Hidden constructor
     */
    private DXRAMGsonContext() {
    }

    static Gson createGsonInstance() {
        return new GsonBuilder().setPrettyPrinting().excludeFieldsWithoutExposeAnnotation()
            .registerTypeAdapter(AbstractDXRAMComponent.class, new ComponentSerializer())
            .registerTypeAdapter(AbstractDXRAMService.class, new ServiceSerializer()).registerTypeAdapter(StorageUnit.class, new StorageUnitGsonSerializer())
            .registerTypeAdapter(TimeUnit.class, new TimeUnitGsonSerializer()).create();
    }

    /**
     * Gson serializer and deserializer for components
     */
    private static class ComponentSerializer implements JsonDeserializer<AbstractDXRAMComponent>, JsonSerializer<AbstractDXRAMComponent> {
        @Override
        public AbstractDXRAMComponent deserialize(final JsonElement p_jsonElement, final Type p_type,
            final JsonDeserializationContext p_jsonDeserializationContext) {

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
            } catch (final ClassNotFoundException ignore) {
                LOGGER.fatal("Could not find component for class name '%s', check your config file", className);
                return null;
            }

            if (!clazz.getSuperclass().equals(AbstractDXRAMComponent.class)) {
                // check if there is an "interface"/abstract class between DXRAMComponent and the instance to
                // create
                if (!clazz.getSuperclass().getSuperclass().equals(AbstractDXRAMComponent.class)) {
                    LOGGER.fatal("Could class '%s' is not a subclass of AbstractDXRAMComponent, check your config file", className);
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
            } catch (final ClassNotFoundException ignore) {
                return null;
            }

            return p_jsonSerializationContext.serialize(p_abstractDXRAMComponent, clazz);
        }
    }

    /**
     * Gson serializer and deserializer for services
     */
    private static class ServiceSerializer implements JsonDeserializer<AbstractDXRAMService>, JsonSerializer<AbstractDXRAMService> {
        @Override
        public AbstractDXRAMService deserialize(final JsonElement p_jsonElement, final Type p_type,
            final JsonDeserializationContext p_jsonDeserializationContext) {

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
            } catch (final ClassNotFoundException ignore) {
                LOGGER.fatal("Could not find service for class name '%s', check your config file", className);
                return null;
            }

            if (!clazz.getSuperclass().equals(AbstractDXRAMService.class)) {
                // check if there is an "interface"/abstract class between DXRAMService and the instance to
                // create
                if (!clazz.getSuperclass().getSuperclass().equals(AbstractDXRAMService.class)) {
                    LOGGER.fatal("Could class '%s' is not a subclass of AbstractDXRAMService, check your config file", className);
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
            } catch (final ClassNotFoundException ignore) {
                return null;
            }

            return p_jsonSerializationContext.serialize(p_abstractDXRAMService, clazz);
        }
    }
}
