package de.hhu.bsinfo.dxram.commands;

import picocli.CommandLine;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.boot.ZookeeperBootComponent;
import de.hhu.bsinfo.dxram.boot.ZookeeperBootComponentConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMConfigBuilderException;
import de.hhu.bsinfo.dxram.engine.DXRAMConfigBuilderJVMArgs;
import de.hhu.bsinfo.dxram.engine.DXRAMConfigBuilderJsonFile2;
import de.hhu.bsinfo.dxram.generated.BuildConfig;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.unit.IPV4Unit;

@CommandLine.Command(name = "start", description = "Starts a new DXRAM instance.%n")
public class Start implements Runnable {

    @CommandLine.Option(names = "--bootstrap", description = "Runs this instance in bootstrapping mode.")
    private boolean m_isBootstrap = false;

    @CommandLine.Option(names = "--superpeer", description = "Runs this instance as an superpeer.")
    private boolean m_isSuperpeer = false;

    @CommandLine.Option(names = "--join", description = "The bootstrapper's connection information")
    private String m_bootstrapAddress = "127.0.0.1:2181";

    @CommandLine.Option(names = "--level", description = "The log level to use")
    private String m_logLevel = "INFO";

    @Override
    public void run() {

        Configurator.setLevel("de.hhu.bsinfo", Level.toLevel(m_logLevel, Level.INFO));

        DXRAM.printBanner();

        if (m_isBootstrap) {
            m_isSuperpeer = true;
        }

        DXRAM dxram = new DXRAM();

        DXRAMConfig config = overrideConfig(dxram.createDefaultConfigInstance());

        boolean success = dxram.initialize(config, true);
        if (!success) {
            System.out.println("Initializing DXRAM failed.");
            System.exit(-1);
        }

        dxram.run();
        System.exit(0);
    }

    private DXRAMConfig overrideConfig(final DXRAMConfig p_config) {

        DXRAMConfigBuilderJsonFile2 configBuilderFile = new DXRAMConfigBuilderJsonFile2();
        DXRAMConfigBuilderJVMArgs configBuilderJvmArgs = new DXRAMConfigBuilderJVMArgs();

        DXRAMConfig overridenConfig = null;

        // JVM args override any default and/or config values loaded from file
        try {
            overridenConfig = configBuilderJvmArgs.build(configBuilderFile.build(p_config));
        } catch (final DXRAMConfigBuilderException e) {
            System.out.println("Bootstrapping configuration failed: " + e.getMessage());
            System.exit(-1);
        }

        // Set specified node role
        overridenConfig.getEngineConfig().setRole(m_isSuperpeer ? NodeRole.SUPERPEER_STR : NodeRole.PEER_STR);

        // Set bootstrap flag
        ZookeeperBootComponentConfig bootConfig = overridenConfig.getComponentConfig(ZookeeperBootComponent.class);
        bootConfig.setBootstrap(m_isBootstrap);

        // Set bootstrap address
        String[] connection = m_bootstrapAddress.split(":");
        bootConfig.setConnection(new IPV4Unit(connection[0], Integer.parseInt(connection[1])));

        return overridenConfig;
    }
}
