package de.hhu.bsinfo.dxram.management.endpoints;

import io.javalin.Context;
import io.javalin.Handler;
import io.javalin.apibuilder.EndpointGroup;

import java.util.List;
import java.util.stream.Collectors;

import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.boot.NodeRegistry;
import de.hhu.bsinfo.dxram.boot.ZookeeperBootComponent;
import de.hhu.bsinfo.dxutils.NodeID;

import static io.javalin.apibuilder.ApiBuilder.*;

public class Members implements EndpointGroup {

    private final MembersInfo m_info = new MembersInfo();

    private ZookeeperBootComponent m_bootComponent;

    public Members(final AbstractBootComponent p_bootComponent) {
        m_bootComponent = (ZookeeperBootComponent) p_bootComponent;
    }

    @Override
    public void addEndpoints() {
        path("members", () -> {
            get(this::handleMemberGet);
            path(":id", () -> {
                get(this::handleMemberGetId);
            });
        });
    }

    private void handleMemberGet(Context p_context) {
        m_info.setNodes(m_bootComponent.getOnlineNodeIds().stream()
                .map(NodeID::toHexStringShort)
                .collect(Collectors.toList()));

        p_context.json(m_info);
    }

    private void handleMemberGetId(Context p_context) {

        short nodeId;

        try {
            nodeId = NodeID.parse(p_context.pathParam("id"));
        } catch (NumberFormatException e) {
            p_context.status(400);
            return;
        }

        NodeRegistry.NodeDetails result = m_bootComponent.getOnlineNodes().stream()
                .filter(details -> details.getId() == nodeId)
                .findFirst().orElse(null);

        if (result == null) {
            p_context.status(400);
            return;
        }

        p_context.json(result);
    }

    public static final class MembersInfo {
        private List<String> nodes;

        public List<String> getNodes() {
            return nodes;
        }

        public void setNodes(List<String> p_nodes) {
            nodes = p_nodes;
        }
    }

}
