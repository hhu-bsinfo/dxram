package de.hhu.bsinfo.dxterm.cmd;

import java.util.ArrayList;
import java.util.List;

import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxterm.TerminalServiceAccessor;
import de.hhu.bsinfo.utils.NodeID;

/**
 * Created by nothaas on 5/26/17.
 */
public class TcmdUtils {

    private TcmdUtils() {

    }

    public static List<String> getAllOnlineSuperpeerNodeIDsCompSuggestions(final TerminalServiceAccessor p_services) {
        BootService boot = p_services.getService(BootService.class);

        List<String> list = new ArrayList<String>();
        for (Short nodeId : boot.getOnlineNodeIDs()) {
            if (boot.getNodeRole(nodeId) == NodeRole.SUPERPEER) {
                list.add(NodeID.toHexString(nodeId));
            }
        }

        return list;
    }

    public static List<String> getAllOnlinePeerNodeIDsCompSuggestions(final TerminalServiceAccessor p_services) {
        BootService boot = p_services.getService(BootService.class);

        List<String> list = new ArrayList<String>();
        for (Short nodeId : boot.getOnlineNodeIDs()) {
            if (boot.getNodeRole(nodeId) == NodeRole.PEER) {
                list.add(NodeID.toHexString(nodeId));
            }
        }

        return list;
    }

    public static List<String> getAllOnlineNodeIDsCompSuggestions(final TerminalServiceAccessor p_services) {
        BootService boot = p_services.getService(BootService.class);

        List<String> list = new ArrayList<String>();
        for (Short nodeId : boot.getOnlineNodeIDs()) {
            list.add(NodeID.toHexString(nodeId));
        }

        return list;
    }

    public static List<String> getBooleanCompSuggestions() {
        List<String> list = new ArrayList<String>();

        list.add("false");
        list.add("true");

        return list;
    }
}
