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

package de.hhu.bsinfo.dxram;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Test;
import org.junit.runner.Description;
import org.junit.runner.Runner;
import org.junit.runner.notification.RunNotifier;

import de.hhu.bsinfo.dxram.engine.DXRAMContextCreatorDefault;
import de.hhu.bsinfo.dxram.util.NodeRole;

public class DXRAMJunitRunner extends Runner {
    private static String ZOOKEEPER_SERVER = "bin/zkServer.sh";

    private Process m_zookeeperServerProcess;
    private DXRAM[] m_instances;

    private Class m_testClass;

    public DXRAMJunitRunner(final Class p_testClass) {
        super();
        m_testClass = p_testClass;
    }

    @Override
    public Description getDescription() {
        return Description
                .createTestDescription(m_testClass, "My runner description");
    }

    @Override
    public void run(final RunNotifier p_notifier) {
        Configurator.setRootLevel(Level.DEBUG);

        DXRAMRunnerConfiguration config = (DXRAMRunnerConfiguration) m_testClass.getAnnotation(
                DXRAMRunnerConfiguration.class);

        if (config == null) {
            throw new RuntimeException(
                    String.format("DXRAMRunnerConfiguration annotation not found (%s)", m_testClass.getSimpleName()));
        }

        startZookeeper(config.zookeeperPath());

        m_instances = new DXRAM[config.nodes().length];

        System.out.println("Creating " + m_instances.length + " DXRAM instances");

        for (int i = 0; i < m_instances.length; i++) {
            m_instances[i] = createNodeInstance(config.nodes()[i]);
        }

        DXRAM testInstance = getInstanceForTest(m_instances, config);

        runTestOnInstance(testInstance, p_notifier);

        cleanupNodeInstances(m_instances);
        shutdownZookeeper();
    }

    private DXRAM createNodeInstance(final DXRAMRunnerConfiguration.Node p_config) {
        // TODO requires refactoring of configuration stuff in engine -> builder library?
        // TODO set configuration values for zookeeper
        // TODO set configuration values for engine etc

        System.setProperty("dxram.config", "/home/krakowski/dxram/config/dxram.json");
        System.setProperty("dxram.m_config.m_engineConfig.m_address.m_port", "22223");

        DXRAM instance = new DXRAM();

        if (!instance.initialize(new DXRAMContextCreatorDefault(), true)) {
            System.out.println("Creating instance failed");
            System.exit(-1);
        }

        return instance;
    }

    private void cleanupNodeInstances(final DXRAM[] p_instances) {
        System.out.println("Cleanup DXRAM instances");

        for (DXRAM instance : p_instances) {
            instance.shutdown();
        }
    }

    private static DXRAM getInstanceForTest(final DXRAM[] p_instances, final DXRAMRunnerConfiguration p_config) {
        // sort by node type
        ArrayList<DXRAM> superpeers = new ArrayList<>();
        ArrayList<DXRAM> peers = new ArrayList<>();

        for (int i = 0; i < p_config.nodes().length; i++) {
            if (p_config.nodes()[i].nodeRole() == NodeRole.SUPERPEER) {
                superpeers.add(p_instances[i]);
            } else if (p_config.nodes()[i].nodeRole() == NodeRole.PEER) {
                peers.add(p_instances[i]);
            } else {
                throw new IllegalStateException();
            }
        }

        if (p_config.runTestOnNodeRole() == NodeRole.SUPERPEER) {
            return superpeers.get(p_config.runTestOnNodeIdx());
        } else if (p_config.runTestOnNodeRole() == NodeRole.PEER) {
            return peers.get(p_config.runTestOnNodeIdx());
        } else {
            throw new IllegalStateException();
        }
    }

    private void runTestOnInstance(final DXRAM p_instance, final RunNotifier p_notifier) {
        Set<Field> annotatedFields = findFields(m_testClass, ClientInstance.class);

        if (annotatedFields.size() != 1) {
            throw new IllegalStateException("Detected more than one ClientInstance annotation");
        }

        Field instanceField = annotatedFields.iterator().next();

        try {
            Object testObject = m_testClass.newInstance();

            try {
                instanceField.setAccessible(true);
                instanceField.set(testObject, p_instance);
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }

            System.out.println("Running tests");

            for (Method method : m_testClass.getMethods()) {
                if (method.isAnnotationPresent(Test.class)) {
                    p_notifier.fireTestStarted(Description
                            .createTestDescription(m_testClass, method.getName()));
                    method.invoke(testObject);
                    p_notifier.fireTestFinished(Description
                            .createTestDescription(m_testClass, method.getName()));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Searches for annotated fields within the specified class.
     *
     * @return A Set containing all fields annotated by the specified annotation.
     */
    private static Set<Field> findFields(Class<?> p_class, Class<? extends Annotation> p_annotation) {
        Set<Field> set = new HashSet<>();
        Class<?> clazz = p_class;

        while (clazz != null) {
            for (Field field : clazz.getDeclaredFields()) {
                if (field.isAnnotationPresent(p_annotation)) {
                    set.add(field);
                }
            }

            clazz = clazz.getSuperclass();
        }

        return set;
    }

    private void startZookeeper(final String p_path) {
        System.out.println("Starting zookeeper");

        try {
            m_zookeeperServerProcess = new ProcessBuilder().inheritIO().command(
                    p_path + '/' + ZOOKEEPER_SERVER, "start").start();
        } catch (final IOException e) {
            System.out.println("Failed to start zookeeper server: ");
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private void shutdownZookeeper() {
        System.out.println("Shutting down zookeeper");
        m_zookeeperServerProcess.destroy();
    }
}
