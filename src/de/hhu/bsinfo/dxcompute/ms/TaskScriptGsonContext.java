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

package de.hhu.bsinfo.dxcompute.ms;

import java.lang.reflect.Type;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

final class TaskScriptGsonContext {

    private static final Logger LOGGER = LogManager.getFormatterLogger(TaskScriptGsonContext.class.getSimpleName());

    private TaskScriptGsonContext() {

    }

    static Gson createGsonInstance() {
        return new GsonBuilder().setPrettyPrinting().excludeFieldsWithoutExposeAnnotation()
            .registerTypeAdapter(TaskScriptNode.class, new AbstractTaskScriptNodeSerializer()).create();
    }

    private static class AbstractTaskScriptNodeSerializer implements JsonDeserializer<TaskScriptNode> {

        @Override
        public TaskScriptNode deserialize(final JsonElement p_jsonElement, final Type p_type, final JsonDeserializationContext p_jsonDeserializationContext) {

            JsonObject jsonObj = p_jsonElement.getAsJsonObject();

            if (jsonObj.has("m_task")) {
                return deserializeTaskPayload(p_jsonElement, p_type, p_jsonDeserializationContext);
            }
            if (jsonObj.has("m_cond")) {
                return deserializeTaskResultCondition(p_jsonElement, p_type, p_jsonDeserializationContext);
            }

            return null;
        }

        private TaskScriptNode deserializeTaskPayload(final JsonElement p_jsonElement, final Type p_type,
            final JsonDeserializationContext p_jsonDeserializationContext) {
            JsonObject jsonObj = p_jsonElement.getAsJsonObject();

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

        private TaskScriptNode deserializeTaskResultCondition(final JsonElement p_jsonElement, final Type p_type,
            final JsonDeserializationContext p_jsonDeserializationContext) {

            return p_jsonDeserializationContext.deserialize(p_jsonElement, TaskResultCondition.class);
        }
    }
}
