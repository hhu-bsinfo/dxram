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

import de.hhu.bsinfo.dxutils.StorageUnitGsonSerializer;
import de.hhu.bsinfo.dxutils.TimeUnitGsonSerializer;
import de.hhu.bsinfo.dxutils.unit.StorageUnit;
import de.hhu.bsinfo.dxutils.unit.TimeUnit;

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

    /**
     * Create a Gson instance with all adapters attached for serialization/deserialization
     *
     * @return Gson context
     */
    static Gson createGsonInstance() {
        return new GsonBuilder().setPrettyPrinting().excludeFieldsWithoutExposeAnnotation()
                .registerTypeAdapter(DXRAMComponentConfig.class, new ComponentConfigSerializer())
                .registerTypeAdapter(DXRAMServiceConfig.class, new ServiceConfigSerializer())
                .registerTypeAdapter(StorageUnit.class, new StorageUnitGsonSerializer()).registerTypeAdapter(
                        TimeUnit.class, new TimeUnitGsonSerializer())
                .create();
    }

    /**
     * Gson serializer and deserializer for component configs
     */
    private static class ComponentConfigSerializer
            implements JsonDeserializer<DXRAMComponentConfig>, JsonSerializer<DXRAMComponentConfig> {
        @Override
        public DXRAMComponentConfig deserialize(final JsonElement p_jsonElement, final Type p_type,
                final JsonDeserializationContext p_jsonDeserializationContext) {

            JsonObject jsonObj = p_jsonElement.getAsJsonObject();
            String className = jsonObj.get("m_class").getAsString();

            Class<?> clazz;
            try {
                clazz = Class.forName(className);
            } catch (final ClassNotFoundException ignore) {
                LOGGER.fatal("Could not find component config for class name '%s', check your config file", className);
                return null;
            }

            if (!clazz.getSuperclass().equals(DXRAMComponentConfig.class)) {
                // check if there is an "interface"/abstract class between DXRAMComponent and the instance to
                // create
                if (!clazz.getSuperclass().getSuperclass().equals(DXRAMComponentConfig.class)) {
                    LOGGER.fatal("Class '%s' is not a subclass of DXRAMComponentConfig, check your config file",
                            className);
                    return null;
                }
            }

            return p_jsonDeserializationContext.deserialize(p_jsonElement, clazz);
        }

        @Override
        public JsonElement serialize(final DXRAMComponentConfig p_abstractDXRAMComponentConfig,
                final Type p_type,
                final JsonSerializationContext p_jsonSerializationContext) {

            Class<?> clazz;
            try {
                clazz = Class.forName(p_abstractDXRAMComponentConfig.getClass().getName());
            } catch (final ClassNotFoundException ignore) {
                return null;
            }

            return p_jsonSerializationContext.serialize(p_abstractDXRAMComponentConfig, clazz);
        }
    }

    /**
     * Gson serializer and deserializer for service configs
     */
    private static class ServiceConfigSerializer
            implements JsonDeserializer<DXRAMServiceConfig>, JsonSerializer<DXRAMServiceConfig> {
        @Override
        public DXRAMServiceConfig deserialize(final JsonElement p_jsonElement, final Type p_type,
                final JsonDeserializationContext p_jsonDeserializationContext) {

            JsonObject jsonObj = p_jsonElement.getAsJsonObject();
            String className = jsonObj.get("m_class").getAsString();

            Class<?> clazz;
            try {
                clazz = Class.forName(className);
            } catch (final ClassNotFoundException ignore) {
                LOGGER.fatal("Could not find service config for class name '%s', check your config file", className);
                return null;
            }

            if (!clazz.getSuperclass().equals(DXRAMServiceConfig.class)) {
                // check if there is an "interface"/abstract class between DXRAMService and the instance to
                // create
                if (!clazz.getSuperclass().getSuperclass().equals(DXRAMServiceConfig.class)) {
                    LOGGER.fatal("Class '%s' is not a subclass of DXRAMServiceConfig, check your config file",
                            className);
                    return null;
                }
            }

            return p_jsonDeserializationContext.deserialize(p_jsonElement, clazz);
        }

        @Override
        public JsonElement serialize(final DXRAMServiceConfig p_abstractDXRAMService, final Type p_type,
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
