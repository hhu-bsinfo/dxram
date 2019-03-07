package de.hhu.bsinfo.dxram;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.List;

import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMConfigBuilderException;
import de.hhu.bsinfo.dxram.engine.DXRAMConfigBuilderJVMArgs;
import de.hhu.bsinfo.dxram.engine.DXRAMConfigBuilderJsonFile2;

/**
 * Main entry point for running a standalone DXRAM instance.
 *
 *
 */
public final class DXRAMMain {

    private static final String STARTUP_DONE_STR = "!---ooo---!";

    /**
     * Main entry point
     *
     * @param p_args
     *         Program arguments.
     */
    public static void main(final String[] p_args) {
        printJVMArgs();
        printCmdArgs(p_args);
        System.out.println();

        DXRAM dxram = new DXRAM();

        System.out.println("Starting DXRAM, version " + dxram.getVersion());

        DXRAMConfig config = bootstrapConfig(dxram);

        if (!dxram.initialize(config, true)) {
            System.out.println("Initializing DXRAM failed.");
            System.exit(-1);
        }

        // used for deploy script
        System.out.println(STARTUP_DONE_STR);

        dxram.run();

        System.exit(0);
    }

    /**
     * Bootstrap configuration loading/creation
     *
     * @param p_dxram
     *          DXRAM instance
     * @return Configuration instance to use to initialize DXRAM
     */
    private static DXRAMConfig bootstrapConfig(final DXRAM p_dxram) {
        DXRAMConfig config = p_dxram.createDefaultConfigInstance();

        DXRAMConfigBuilderJsonFile2 configBuilderFile = new DXRAMConfigBuilderJsonFile2();
        DXRAMConfigBuilderJVMArgs configBuilderJvmArgs = new DXRAMConfigBuilderJVMArgs();

        // JVM args override any default and/or config values loaded from file
        try {
            config = configBuilderJvmArgs.build(configBuilderFile.build(config));
        } catch (final DXRAMConfigBuilderException e) {
            System.out.println("Bootstrapping configuration failed: " + e.getMessage());
            System.exit(-1);
        }

        return config;
    }

    /**
     * Print all cmd args specified on startup
     *
     * @param p_args
     *         Main arguments
     */
    private static void printCmdArgs(final String[] p_args) {
        StringBuilder builder = new StringBuilder();
        builder.append("Cmd arguments: ");

        for (String arg : p_args) {
            builder.append(arg);
            builder.append(' ');
        }

        System.out.println(builder);
        System.out.println();
    }

    /**
     * Print all JVM args specified on startup
     */
    private static void printJVMArgs() {
        RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
        List<String> args = runtimeMxBean.getInputArguments();

        StringBuilder builder = new StringBuilder();
        builder.append("JVM arguments: ");

        for (String arg : args) {
            builder.append(arg);
            builder.append(' ');
        }

        System.out.println(builder);
        System.out.println();
    }
}
