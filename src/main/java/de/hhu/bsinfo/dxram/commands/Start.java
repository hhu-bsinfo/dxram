package de.hhu.bsinfo.dxram.commands;

import picocli.CommandLine;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.boot.ZookeeperBootComponent;
import de.hhu.bsinfo.dxram.boot.ZookeeperBootComponentConfig;
import de.hhu.bsinfo.dxram.chunk.ChunkComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkComponentConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMConfigBuilderException;
import de.hhu.bsinfo.dxram.engine.DXRAMConfigBuilderJVMArgs;
import de.hhu.bsinfo.dxram.engine.DXRAMConfigBuilderJsonFile2;
import de.hhu.bsinfo.dxram.ms.ComputeRole;
import de.hhu.bsinfo.dxram.ms.MasterSlaveComputeService;
import de.hhu.bsinfo.dxram.ms.MasterSlaveComputeServiceConfig;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponentConfig;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.unit.IPV4Unit;
import de.hhu.bsinfo.dxutils.unit.StorageUnit;

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

    @CommandLine.Option(
            names = { "--storage", "--kvsize", "--kv" },
            description = "Amount of main memory to use for the key value store in MB.")
    private int m_storage = 128;

    @CommandLine.Option(
            names = "--handler",
            description = "Number of threads to spawn for handling incoming and assembled network messages.")
    private int m_handler = 2;

    @CommandLine.Option(
            names = { "--master-slave-role", "--msrole" },
            description = "Compute role to assign to the current instance (${COMPLETION-CANDIDATES}).")
    private ComputeRole m_msrole = ComputeRole.NONE;

    @CommandLine.Option(names = { "--compute-group", "--cg" }, description = "Compute group id to assign to the current instance.")
    private short m_computeGroup = 0;
    
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

        // Set key-value storage size
        ChunkComponentConfig chunkConfig = overridenConfig.getComponentConfig(ChunkComponent.class);
        chunkConfig.setKeyValueStoreSize(new StorageUnit(m_storage, StorageUnit.MB));

        // Set Number of threads to spawn for handling incoming and assembled network messages
        NetworkComponentConfig netConfig = overridenConfig.getComponentConfig(NetworkComponent.class);
        netConfig.getCoreConfig().setNumMessageHandlerThreads(m_handler);

        // Set the compute role and compute group id to assign to the current instance (master, slave or none)
        MasterSlaveComputeServiceConfig msConfig = overridenConfig.getServiceConfig(MasterSlaveComputeService.class);
        msConfig.setRole(m_msrole.toString());            
        msConfig.setComputeGroupId(m_computeGroup);

        return overridenConfig;
    }
}
