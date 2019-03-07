package de.hhu.bsinfo.dxram.management.endpoints;

import io.javalin.Context;
import io.javalin.UploadedFile;
import io.javalin.apibuilder.EndpointGroup;
import io.javalin.core.util.FileUtil;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxram.app.ApplicationComponent;
import de.hhu.bsinfo.dxram.app.messages.ApplicationSubmitMessage;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.boot.NodeRegistry;
import de.hhu.bsinfo.dxram.boot.ZookeeperBootComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.plugin.PluginComponent;
import de.hhu.bsinfo.dxram.util.NodeCapabilities;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.NodeID;

import static io.javalin.apibuilder.ApiBuilder.path;
import static io.javalin.apibuilder.ApiBuilder.post;

public class Submit implements EndpointGroup {

    private static final Logger LOGGER = LogManager.getFormatterLogger(Submit.class);

    private final ZookeeperBootComponent m_boot;
    private final ApplicationComponent m_app;
    private final NetworkComponent m_network;

    private static final String QUERY_NODE = "node";

    private static final String QUERY_ARGS = "args";

    public Submit(final DXRAMComponentAccessor p_accessor) {
        m_boot = (ZookeeperBootComponent) p_accessor.getComponent(AbstractBootComponent.class);
        m_app = p_accessor.getComponent(ApplicationComponent.class);
        m_network = p_accessor.getComponent(NetworkComponent.class);
    }

    @Override
    public void addEndpoints() {
        path("submit", () -> {
            post(this::handleSubmit);
        });
    }

    private void handleSubmit(final Context p_context) {
        UploadedFile file = p_context.uploadedFile("archive");

        if (file == null) {
            LOGGER.debug("Received empty submit request");
            p_context.status(400);
            return;
        }

        List<Short> targets = new ArrayList<>();

        // Set specified node id as target if it exists.
        // Use all COMPUTE nodes if no node id was specified.
        String nodeParam = p_context.queryParam(QUERY_NODE);
        if (nodeParam != null && !nodeParam.isEmpty()) {
            targets.add(NodeID.parse(nodeParam));
        } else {
            targets.addAll(m_boot.getOnlineNodes().stream()
                    .filter(details -> details.getRole() == NodeRole.PEER)
                    .map(NodeRegistry.NodeDetails::getId)
                    .collect(Collectors.toList()));
        }

        // Read in parameters for application
        String argsParam = p_context.queryParam(QUERY_ARGS);
        String[] args;
        if (argsParam != null && !argsParam.isEmpty()) {
            args = argsParam.split(" ");
        } else {
            args = new String[]{};
        }

        // Save java archive to hard disk
        FileUtil.streamToFile(file.getContent(),
                "plugin" + File.separator + file.getName());

        // Instruct all target nodes to start the submitted application
        for (Short target : targets) {
            ApplicationSubmitMessage message = new ApplicationSubmitMessage(target, file.getName(), args);

            try {
                m_network.sendMessage(message);
            } catch (NetworkException e) {
                LOGGER.warn("Couldn't submit application to node %s", NodeID.toHexStringShort(target));
            }
        }

        p_context.status(200);
    }
}
