package de.hhu.bsinfo.dxterm.cmd;

import java.util.Collections;
import java.util.List;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.migration.MigrationService;
import de.hhu.bsinfo.dxterm.AbstractTerminalCommand;
import de.hhu.bsinfo.dxterm.TerminalCommandString;
import de.hhu.bsinfo.dxterm.TerminalServerStdin;
import de.hhu.bsinfo.dxterm.TerminalServerStdout;
import de.hhu.bsinfo.dxterm.TerminalServiceAccessor;
import de.hhu.bsinfo.utils.NodeID;

/**
 * Migrate an existing chunk from one peer to another one
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 29.05.2017
 */
public class TcmdChunkMigrate extends AbstractTerminalCommand {
    public TcmdChunkMigrate() {
        super("chunkmigrate");
    }

    @Override
    public String getHelp() {
        return "Migrate a chunk from a source peer to a target peer\n" + "Usage: chunkmigrate <lid> <source nid> <target nid>\n" +
                "  lid: LID of the chunk located on the source to migrate\n" + "  source nid: NID of the source chunk\n" +
                "  target nid: NID of the target chunk";
    }

    @Override
    public void exec(final TerminalCommandString p_cmd, final TerminalServerStdout p_stdout, final TerminalServerStdin p_stdin,
            final TerminalServiceAccessor p_services) {
        long lid = p_cmd.getArgChunkId(0, ChunkID.INVALID_ID);
        short sourceNid = p_cmd.getArgNodeId(1, NodeID.INVALID_ID);
        short targetNid = p_cmd.getArgNodeId(2, NodeID.INVALID_ID);

        if (lid == ChunkID.INVALID_ID) {
            p_stdout.printlnErr("No lid specified");
            return;
        }

        if (sourceNid == NodeID.INVALID_ID) {
            p_stdout.printlnErr("No source nid specified");
            return;
        }

        if (targetNid == NodeID.INVALID_ID) {
            p_stdout.printlnErr("No target nid specified");
            return;
        }

        MigrationService migrate = p_services.getService(MigrationService.class);

        if (!migrate.migrate(ChunkID.getChunkID(sourceNid, lid), targetNid)) {
            p_stdout.printflnErr("Migrating 0x%X to target node 0x%X failed", ChunkID.getChunkID(sourceNid, lid), targetNid);
        }
    }

    @Override
    public List<String> getArgumentCompletionSuggestions(final int p_argumentPos, final TerminalCommandString p_cmdStr,
            final TerminalServiceAccessor p_services) {
        switch (p_argumentPos) {
            case 1:
            case 2:
                return TcmdUtils.getAllOnlinePeerNodeIDsCompSuggestions(p_services);
            default:
                return Collections.emptyList();
        }
    }
}
