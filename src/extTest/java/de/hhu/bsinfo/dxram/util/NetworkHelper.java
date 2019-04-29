package de.hhu.bsinfo.dxram.util;

import java.util.Optional;

import de.hhu.bsinfo.dxram.boot.BootService;

public class NetworkHelper {

    public static short findStorageNode(BootService p_bootService) {
        Optional<Short> targetOptional = p_bootService.getSupportingNodes(NodeCapabilities.STORAGE).stream()
                .filter(id -> id != p_bootService.getNodeID())
                .findFirst();

        while (!targetOptional.isPresent()) {
            targetOptional = p_bootService.getSupportingNodes(NodeCapabilities.STORAGE).stream()
                    .filter(id -> id != p_bootService.getNodeID())
                    .findFirst();
        }

        return targetOptional.get();
    }

    public static short findPeer(BootService p_bootService) {
        Optional<Short> targetOptional = p_bootService.getOnlinePeerNodeIDs().stream()
                .filter(id -> id != p_bootService.getNodeID())
                .findFirst();

        while (!targetOptional.isPresent()) {
            targetOptional = p_bootService.getOnlinePeerNodeIDs().stream()
                    .filter(id -> id != p_bootService.getNodeID())
                    .findFirst();
        }

        return targetOptional.get();
    }
}
