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

package de.hhu.bsinfo.dxram.ms;

import java.lang.reflect.Type;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import de.hhu.bsinfo.dxutils.StorageUnitGsonSerializer;
import de.hhu.bsinfo.dxutils.TimeUnitGsonSerializer;
import de.hhu.bsinfo.dxutils.unit.StorageUnit;
import de.hhu.bsinfo.dxutils.unit.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Gson context for handling serialization and deserialization of task scripts
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 15.01.2017
 */
final class TaskScriptGsonContext {

    private static final Logger LOGGER = LogManager.getFormatterLogger(TaskScriptGsonContext.class.getSimpleName());

    /**
     * Static class
     */
    private TaskScriptGsonContext() {

    }

    /**
     * Create a Gson instance with all adapters attached for serialization/deserialization
     * @return Gson context
     */
    static Gson createGsonInstance() {
        return new GsonBuilder().setPrettyPrinting().excludeFieldsWithoutExposeAnnotation()
            .registerTypeAdapter(TaskScriptNode.class, new TaskScriptNodeSerializer())
                .registerTypeAdapter(StorageUnit.class, new StorageUnitGsonSerializer())
                .registerTypeAdapter(TimeUnit.class, new TimeUnitGsonSerializer()).create();
    }

    /**
     * Gson deserializer for TaskScriptNodes
     */
    private static class TaskScriptNodeSerializer implements JsonDeserializer<TaskScriptNode> {
        @Override
        public TaskScriptNode deserialize(final JsonElement p_jsonElement, final Type p_type, final JsonDeserializationContext p_jsonDeserializationContext) {

            JsonObject jsonObj = p_jsonElement.getAsJsonObject();

            if (jsonObj.has("m_task")) {
                String className = jsonObj.get("m_task").getAsString();

                Class<?> clazz;
                try {
                    clazz = Class.forName(className);
                } catch (final ClassNotFoundException ignore) {
                    LOGGER.fatal("Could not find task for class name '%s', check your task script", className);
                    return null;
                }

                // check if class implements the Task interface
                boolean impl = false;
                for (Class<?> iface : clazz.getInterfaces()) {
                    if (iface.equals(Task.class)) {
                        impl = true;
                        break;
                    }
                }

                if (!impl) {
                    LOGGER.fatal("Class '%s' does not implement the interface Task, check your task script", className);
                    return null;
                }

                return p_jsonDeserializationContext.deserialize(p_jsonElement, clazz);
            }
            if (jsonObj.has("m_cond")) {
                return p_jsonDeserializationContext.deserialize(p_jsonElement, TaskResultCondition.class);
            }
            if (jsonObj.has("m_switchCases")) {
                return p_jsonDeserializationContext.deserialize(p_jsonElement, TaskResultSwitch.class);
            }
            if (jsonObj.has("m_abortMsg")) {
                return p_jsonDeserializationContext.deserialize(p_jsonElement, TaskAbort.class);
            }

            return null;
        }
    }
}
