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
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.Description;
import org.junit.runner.Runner;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunNotifier;

import de.hhu.bsinfo.dxutils.unit.IPV4Unit;

/**
 * JUnit runner to run local DXRAM instances (in the same JVM!!!) and connect them over localhost ethernet. This allows
 * quick testing and verification of essential features. This requires zookeeper to be installed on your local machine
 * as well as setting the correct configuration values in the config.properties file (e.g. zookeeper path)
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
public class DXRAMJunitRunner extends Runner {
    private ZookeeperServer m_zookeeper;
    private DXRAM[] m_instances;

    private Class m_testClass;

    /**
     * Constructor
     *
     * @param p_testClass
     *         Test class to run
     */
    public DXRAMJunitRunner(final Class p_testClass) {
        super();
        m_testClass = p_testClass;
    }

    @Override
    public Description getDescription() {
        return Description
                .createTestDescription(m_testClass, "DXRAM instance test runner");
    }

    @Override
    public void run(final RunNotifier p_notifier) {
        configureLogger();

        DXRAMTestConfiguration config = (DXRAMTestConfiguration) m_testClass.getAnnotation(
                DXRAMTestConfiguration.class);

        if (config == null) {
            throw new RuntimeException(
                    String.format("DXRAMTestConfiguration annotation not found (%s)", m_testClass.getSimpleName()));
        }

        Properties props = getConfigProperties();

        m_zookeeper = new ZookeeperServer(props.getProperty("zookeeper_path"));

        System.out.println("Starting zookeeper");

        if (!m_zookeeper.start()) {
            throw new RuntimeException("Starting zookeeper failed");
        }

        IPV4Unit zookeeperConnection = new IPV4Unit(props.getProperty("zookeeper_ip"),
                Integer.parseInt(props.getProperty("zookeeper_port")));

        m_instances = new DXRAM[config.nodes().length];

        System.out.println("Creating " + m_instances.length + " DXRAM instances");

        for (int i = 0; i < m_instances.length; i++) {
            m_instances[i] = createNodeInstance(zookeeperConnection, config, i, 22221 + i);
        }

        DXRAM testInstance = getInstanceForTest(m_instances, config);

        runTestOnInstance(testInstance, p_notifier);

        cleanupNodeInstances(m_instances);
        m_zookeeper.shutdown();
    }

    /**
     * Configure the logger (log4j) for the test to run
     */
    private void configureLogger() {
        LoggerContext lc = (LoggerContext) LogManager.getContext(false);
        Appender appender = LogAppenderAsssert.createAppender("unitTest", PatternLayout.createDefaultLayout(), null);
        appender.start();
        lc.getConfiguration().addAppender(appender);
        lc.getRootLogger().addAppender(lc.getConfiguration().getAppender(appender.getName()));
        lc.updateLoggers();

        Configurator.setRootLevel(Level.DEBUG);
    }

    /**
     * Create a DXRAM instance
     *
     * @param p_zookeeperConnection
     *         Address to zookeeper server
     * @param p_config
     *         Test configuration to use
     * @param p_nodeIdx
     *         Index of node to start
     * @param p_nodePort
     *         Port to assign to node
     * @return
     */
    private DXRAM createNodeInstance(final IPV4Unit p_zookeeperConnection, final DXRAMTestConfiguration p_config,
            final int p_nodeIdx, final int p_nodePort) {
        DXRAM instance = new DXRAM();

        if (!instance.initialize(new DXRAMTestContextCreator(p_zookeeperConnection, p_config, p_nodeIdx, p_nodePort),
                true)) {
            System.out.println("Creating instance failed");
            System.exit(-1);
        }

        return instance;
    }

    /**
     * Cleanup all instances
     *
     * @param p_instances
     *         Instances to cleanup
     */
    private void cleanupNodeInstances(final DXRAM[] p_instances) {
        System.out.println("Cleanup DXRAM instances");

        for (DXRAM instance : p_instances) {
            instance.shutdown();
        }
    }

    /**
     * Get the instance to be used for the test to run
     *
     * @param p_instances
     *         List of instances running
     * @param p_config
     *         Config of test
     * @return Instance to assign to the test
     */
    private static DXRAM getInstanceForTest(final DXRAM[] p_instances, final DXRAMTestConfiguration p_config) {
        return p_instances[p_config.runTestOnNodeIdx()];
    }

    /**
     * Run the test on an instance
     *
     * @param p_instance
     *         Instance to run test on
     * @param p_notifier
     *         Notifier
     */
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

            for (Method method : m_testClass.getMethods()) {
                if (method.isAnnotationPresent(BeforeClass.class)) {
                    method.invoke(testObject);
                }
            }

            System.out.println("Running tests");

            for (Method method : m_testClass.getMethods()) {
                if (method.isAnnotationPresent(Test.class)) {
                    Description testDescription = Description.createTestDescription(m_testClass, method.getName());

                    p_notifier.fireTestStarted(testDescription);

                    try {
                        method.invoke(testObject);
                    } catch (final Throwable e) {
                        p_notifier.fireTestFailure(new Failure(testDescription, e));
                    }

                    p_notifier.fireTestFinished(testDescription);
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

    /**
     * Get the configuration values for the runner of the config.properties file
     *
     * @return Properties object with config values
     */
    private Properties getConfigProperties() {
        Properties prop = new Properties();

        InputStream inputStream = getClass().getResourceAsStream("config.properties");

        if (inputStream == null) {
            throw new RuntimeException("Could not find property file in resources");
        }

        try {
            prop.load(inputStream);
            inputStream.close();
        } catch (final IOException e) {
            throw new RuntimeException("Loading property file failed: " + e.getMessage());
        }

        return prop;
    }
}
