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

package de.hhu.bsinfo.dxram.remote;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.boot.BootService;
import org.junit.Test;
import org.junit.runner.Description;
import org.junit.runner.Runner;
import org.junit.runner.notification.RunNotifier;

import java.lang.reflect.Field;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DXRAMJunitRunner extends Runner {

    private DXRAM m_instance;

    private Class m_testClass;

    public DXRAMJunitRunner(Class testClass) {
        super();
        m_testClass = testClass;
    }

    @Override
    public Description getDescription() {
        return Description
                .createTestDescription(m_testClass, "My runner description");
    }

    @Override
    public void run(RunNotifier notifier) {

        System.setProperty("dxram.config", "/home/krakowski/dxram/config/dxram.json");
        System.setProperty("dxram.m_config.m_engineConfig.m_address.m_port", "22223");

        m_instance = new DXRAM();

        if (!m_instance.initialize(true)) {
            System.exit(-1);
        }

        BootService bootService = m_instance.getService(BootService.class);

        List<Short> onlineNodes = bootService.getOnlineNodeIDs();
        while (onlineNodes.size() != 3) {

            // Wait until DXRam finds other nodes
            try {
                Thread.sleep(1000);
            } catch (InterruptedException p_e) {
                p_e.printStackTrace();
            }

            onlineNodes = bootService.getOnlineNodeIDs();
        }

        Set<Field> annotatedFields = findFields(m_testClass, DXRAMInstance.class);

        if (annotatedFields.size() != 1) {
            throw new IllegalStateException("Detected more than one DXRAMInstance annotation");
        }

        Field instanceField = annotatedFields.iterator().next();

        try {
            Object testObject = m_testClass.newInstance();

            try {
                instanceField.setAccessible(true);
                instanceField.set(testObject, m_instance);
            } catch (IllegalAccessException p_e) {
                p_e.printStackTrace();
            }

            for (Method method : m_testClass.getMethods()) {
                if (method.isAnnotationPresent(Test.class)) {
                    notifier.fireTestStarted(Description
                            .createTestDescription(m_testClass, method.getName()));
                    method.invoke(testObject);
                    notifier.fireTestFinished(Description
                            .createTestDescription(m_testClass, method.getName()));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            m_instance.shutdown();
        }
    }

    /**
     * @return null safe set
     */
    private static Set<Field> findFields(Class<?> classs, Class<? extends Annotation> ann) {
        Set<Field> set = new HashSet<>();
        Class<?> c = classs;
        while (c != null) {
            for (Field field : c.getDeclaredFields()) {
                if (field.isAnnotationPresent(ann)) {
                    set.add(field);
                }
            }
            c = c.getSuperclass();
        }
        return set;
    }
}
