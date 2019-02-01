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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.junit.runner.Description;
import org.junit.runner.Runner;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunNotifier;

import de.hhu.bsinfo.dxram.engine.DXRAMConfig;

/**
 * JUnit runner to run local DXRAM instances (in the same JVM!!!) and connect them over localhost ethernet. This allows
 * quick testing and verification of essential features. This requires zookeeper to be installed on your local machine
 * as well as setting the correct configuration values in the config.properties file (e.g. zookeeper path)
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
public class DXRAMJunitRunner extends Runner {
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

        m_instances = new DXRAM[config.nodes().length];

        System.out.println("Creating " + m_instances.length + " DXRAM instances");

        for (int i = 0; i < m_instances.length; i++) {
            m_instances[i] = createNodeInstance(props.getProperty("dxram_build_dist_out"), config, i, 22221 + i);
        }

        runTestOnInstances(m_instances, p_notifier);

        cleanupNodeInstances(m_instances);
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
     * @param p_dxramBuildDistDir
     *         Path to directory containing the build output for distribution (required to test with applications,
     *         backup/logging etc)
     * @param p_config
     *         Test configuration to use
     * @param p_nodeIdx
     *         Index of node to start
     * @param p_nodePort
     *         Port to assign to node
     * @return New DXRAM instance
     */
    private DXRAM createNodeInstance(final String p_dxramBuildDistDir, final DXRAMTestConfiguration p_config,
            final int p_nodeIdx, final int p_nodePort) {
        if (!new File(p_dxramBuildDistDir).exists()) {
            throw new RuntimeException("Directory '" + p_dxramBuildDistDir +
                    "' with DXRAM build distribution output does not exist, required for testing applications, " +
                    "backup/logging etc");
        }

        DXRAM instance = new DXRAM();

        DXRAMConfig config = new DXRAMConfigBuilderTest(p_dxramBuildDistDir, p_config, p_nodeIdx,
                p_nodePort).build(instance.createDefaultConfigInstance());

        if (!instance.initialize(config, true)) {
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
        System.out.println("Cleanup DXRAM instances: " + p_instances.length);

        // cleanup in reverse order: superpeers have to soft shutdown last
        for (int i = p_instances.length - 1; i >= 0; i--) {
            p_instances[i].shutdown();
        }
    }

    /**
     * Run the test on one or multiple instances using threads
     *
     * @param p_instances
     *         Instances to run test on
     * @param p_notifier
     *         Notifier
     */
    private void runTestOnInstances(final DXRAM[] p_instances, final RunNotifier p_notifier) {
        try {
            Object testObject = m_testClass.newInstance();

            for (Method method : m_testClass.getMethods()) {
                if (method.isAnnotationPresent(BeforeTestInstance.class)) {
                    for (Annotation anno : method.getAnnotations()) {
                        if (anno instanceof BeforeTestInstance) {
                            BeforeTestInstance testInstanceAnno = (BeforeTestInstance) anno;

                            method.invoke(testObject, p_instances[testInstanceAnno.runOnNodeIdx()]);
                            break;
                        }
                    }
                }
            }

            System.out.println("Running tests");

            TestRunnerThread[] threads = new TestRunnerThread[p_instances.length];

            for (Method method : m_testClass.getMethods()) {
                if (method.isAnnotationPresent(TestInstance.class)) {
                    for (Annotation anno : method.getAnnotations()) {
                        if (anno instanceof TestInstance) {
                            TestInstance testInstanceAnno = (TestInstance) anno;

                            for (int nodeIdx : testInstanceAnno.runOnNodeIdx()) {
                                if (threads[nodeIdx] == null) {
                                    threads[nodeIdx] = new TestRunnerThread(nodeIdx, p_instances[nodeIdx], testObject,
                                            p_notifier);

                                }

                                threads[nodeIdx].pushTestMethod(method);
                            }
                        }
                    }
                }
            }

            for (TestRunnerThread t : threads) {
                if (t != null) {
                    t.start();
                }
            }

            for (TestRunnerThread t : threads) {
                if (t != null) {
                    t.join();
                }
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Class running the test method in a separate thread
     */
    private static final class TestRunnerThread extends Thread {
        private final int m_instanceIdx;
        private final DXRAM m_instance;
        private final Object m_testObject;
        private final ArrayList<Method> m_testMethods;
        private final RunNotifier m_notifier;

        /**
         * Constructor
         *
         * @param p_instanceIdx
         *         Index of the instance assigned to this thread
         * @param p_instance
         *         DXRAM instance to pass to test method
         * @param p_testObject
         *         Test object to run the test on
         * @param p_notifier
         *         Test notifier for error signaling
         */
        public TestRunnerThread(final int p_instanceIdx, final DXRAM p_instance, final Object p_testObject,
                final RunNotifier p_notifier) {
            m_instanceIdx = p_instanceIdx;
            m_instance = p_instance;
            m_testObject = p_testObject;
            m_testMethods = new ArrayList<Method>();
            m_notifier = p_notifier;
        }

        /**
         * Add a test method to run by this thread
         *
         * @param p_testMethod
         *         Method this thread has to run
         */
        public void pushTestMethod(final Method p_testMethod) {
            m_testMethods.add(p_testMethod);
        }

        @Override
        public void run() {
            for (Method method : m_testMethods) {
                Description testDescription = Description.createTestDescription(m_testObject.getClass(),
                        m_instanceIdx + " | " + method.getName());

                m_notifier.fireTestStarted(testDescription);

                try {
                    method.invoke(m_testObject, m_instance);
                } catch (final Throwable e) {
                    m_notifier.fireTestFailure(new Failure(testDescription, e));
                }

                m_notifier.fireTestFinished(testDescription);
            }
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
