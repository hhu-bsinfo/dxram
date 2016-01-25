package de.hhu.bsinfo.dxramtodo.cmd;

import java.util.ArrayList;
import java.util.Arrays;

import de.uniduesseldorf.dxram.commands.CmdUtils;
import de.uniduesseldorf.dxram.core.chunk.BackupRange;
import de.uniduesseldorf.dxram.core.chunk.ChunkCommandMessage;
import de.uniduesseldorf.dxram.core.chunk.ChunkCommandRequest;
import de.uniduesseldorf.dxram.core.chunk.ChunkCommandResponse;
import de.uniduesseldorf.dxram.core.chunk.DataMessage;
import de.uniduesseldorf.dxram.core.chunk.DataRequest;
import de.uniduesseldorf.dxram.core.chunk.GetRequest;
import de.uniduesseldorf.dxram.core.chunk.LockRequest;
import de.uniduesseldorf.dxram.core.chunk.MultiGetRequest;
import de.uniduesseldorf.dxram.core.chunk.PutRequest;
import de.uniduesseldorf.dxram.core.chunk.RemoveRequest;
import de.uniduesseldorf.dxram.core.chunk.UnlockMessage;
import de.uniduesseldorf.dxram.core.chunk.ChunkStatistic.Operation;
import de.uniduesseldorf.dxram.core.chunk.messages.ChunkMessages;
import de.uniduesseldorf.dxram.core.dxram.Core;
import de.uniduesseldorf.dxram.core.engine.DXRAMException;

import de.uniduesseldorf.menet.AbstractMessage;
import de.uniduesseldorf.menet.NetworkException;
import de.uniduesseldorf.utils.StatisticsManager;

public class CommandService {
	m_network.registerMessageType(ChunkMessages.TYPE, ChunkMessages.SUBTYPE_CHUNK_COMMAND_MESSAGE, ChunkMessages.ChunkCommandMessage.class);
	m_network.registerMessageType(ChunkMessages.TYPE, ChunkMessages.SUBTYPE_CHUNK_COMMAND_REQUEST, ChunkMessages.ChunkCommandRequest.class);
	m_network.registerMessageType(ChunkMessages.TYPE, ChunkMessages.SUBTYPE_CHUNK_COMMAND_RESPONSE, ChunkMessages.ChunkCommandResponse.class);

	
	@Override
	public void onIncomingMessage(final AbstractMessage p_message) {
		LOGGER.trace("Entering incomingMessage with: p_message=" + p_message);

		if (p_message != null) {
			if (p_message.getType() == ChunkMessages.TYPE) {
				switch (p_message.getSubtype()) {
				case ChunkMessages.SUBTYPE_GET_REQUEST:
					incomingGetRequest((GetRequest) p_message);
					break;
				case ChunkMessages.SUBTYPE_PUT_REQUEST:
					incomingPutRequest((PutRequest) p_message);
					break;
				case ChunkMessages.SUBTYPE_REMOVE_REQUEST:
					incomingRemoveRequest((RemoveRequest) p_message);
					break;
				case ChunkMessages.SUBTYPE_LOCK_REQUEST:
					incomingLockRequest((LockRequest) p_message);
					break;
				case ChunkMessages.SUBTYPE_UNLOCK_MESSAGE:
					incomingUnlockMessage((UnlockMessage) p_message);
					break;
				case ChunkMessages.SUBTYPE_DATA_REQUEST:
					incomingDataRequest((DataRequest) p_message);
					break;
				case ChunkMessages.SUBTYPE_DATA_MESSAGE:
					incomingDataMessage((DataMessage) p_message);
					break;
				case ChunkMessages.SUBTYPE_MULTIGET_REQUEST:
					incomingMultiGetRequest((MultiGetRequest) p_message);
					break;
				case ChunkMessages.SUBTYPE_CHUNK_COMMAND_MESSAGE:
					incomingCommandMessage((ChunkCommandMessage) p_message);
					break;
				case ChunkMessages.SUBTYPE_CHUNK_COMMAND_REQUEST:
					incomingCommandRequest((ChunkCommandRequest) p_message);
					break;
				default:
					break;
				}
			}
		}

		LOGGER.trace("Exiting incomingMessage");
	}
	
	/**
	 * Handles an incoming CommandMessage
	 * @param p_message
	 *            the CommandMessage
	 */
	private void incomingCommandMessage(final ChunkCommandMessage p_message) {
		String cmd;

		Operation.INCOMING_COMMAND.enter();

		cmd = p_message.getCommand();

		if (Core.getCommandListener() != null) {
			Core.getCommandListener().processCmd(cmd, false);
		} else {
			System.out.println("error: command message received but no command listener registered");
		}

		Operation.INCOMING_COMMAND.leave();
	}

	/**
	 * Handles 'chunkinfo' command. Called by incomingCommandRequest
	 * @param p_command
	 *            the CommandMessage
	 * @return the result string
	 */
	private String cmdReqChunkinfo(final String p_command) {
		short nodeID;
		short primaryPeer;
		short[] backupPeers;
		long localID;
		long chunkID;
		String ret = null;
		String[] arguments;

		arguments = p_command.split(" ");

		nodeID = CmdUtils.getNIDfromTuple(arguments[1]);
		localID = CmdUtils.getLIDfromTuple(arguments[1]);
		chunkID = CmdUtils.getCIDfromTuple(arguments[1]);
		System.out.println("   cmdReqChunkinfo for " + nodeID + "," + localID);

		try {

			if (m_memoryManager.isResponsible(chunkID)) {
				backupPeers = getBackupPeersForLocalChunks(chunkID);
				ret = "  Stored on peer=" + m_nodeID + ", backup_peers=" + Arrays.toString(backupPeers);
			} else {
				primaryPeer = m_lookup.get(chunkID).getPrimaryPeer();
				ret = "  Chunk not stored on this peer. Contact peer " + primaryPeer + " or a superpeer";
			}
		} catch (final DXRAMException de) {
			ret = "error: " + de.toString();
		}

		return ret;
	}

	/**
	 * Handles 'chunkinfo' command. Called by incomingCommandRequest
	 * @param p_command
	 *            the CommandMessage
	 * @return the result string
	 */
	private String cmdReqCIDT(final String p_command) {
		String ret;
		ArrayList<Long> chunkIDLocalArray;
		ArrayList<Long> chunkIDMigratedArray;

		try {
			// de.uniduesseldorf.dxram.core.chunk.storage.CIDTable.printDebugInfos();
			// System.out.println("cmdReqCIDT 0");
			// TODO
			chunkIDLocalArray = null;
			// chunkIDLocalArray = m_memoryManager.getCIDrangesOfAllLocalChunks();
			// System.out.println("cmdReqCIDT 1");
			ret = "  Local (ranges): ";
			if (chunkIDLocalArray == null) {
				ret = ret + "empty.\n";
			} else if (chunkIDLocalArray.size() == 0) {
				ret = ret + "empty.\n";
			} else {
				boolean first = true;
				for (long l : chunkIDLocalArray) {
					if (first) {
						first = false;
					} else {
						ret = ret + ",";
					}
					ret += CmdUtils.getLIDfromCID(l);
				}
				ret = ret + "\n";
			}
			// System.out.println("cmdReqCIDT 2");

			// TODO
			chunkIDMigratedArray = null;
			// chunkIDMigratedArray = m_memoryManager.getCIDOfAllMigratedChunks();
			// System.out.println("cmdReqCIDT 3");

			ret = ret + "  Migrated (NodeID,LocalID): ";
			if (chunkIDMigratedArray == null) {
				ret = ret + "empty.";
			} else if (chunkIDMigratedArray.size() == 0) {
				ret = ret + "empty.";
			} else {
				boolean first = true;
				for (long l : chunkIDMigratedArray) {
					if (first) {
						first = false;
					} else {
						ret = ret + "; ";
					}
					ret += CmdUtils.getTupleFromCID(l);
				}
			}
		} finally {}
		// TODO take back in when done fixing stuff above
		// } catch (final MemoryException me) {
		// System.out.println("cmdReqCIDT: MemoryException");
		// ret = "error: internal error";
		// }
		return ret;
	}

	/**
	 * Handles 'backups' command. Called by incomingCommandRequest
	 * @param p_command
	 *            the CommandMessage
	 * @return the result string
	 */
	private String cmdReqBackups(final String p_command) {
		String ret = "";

		// System.out.println("ChunkHandler.cmdReqBackups");
		ret = ret + "  Backup ranges for locally created chunks\n";
		if (m_ownBackupRanges != null) {
			// System.out.println("   m_ownBackupRanges");
			for (int i = 0; i < m_ownBackupRanges.size(); i++) {
				final BackupRange br = m_ownBackupRanges.get(i);
				ret = ret + "    BR" + Integer.toString(i) + ": ";

				if (br != null) {
					// System.out.println("   BackupRange: "+i+", m_firstChunkIDORRangeID="+br.m_firstChunkIDORRangeID);
					ret = ret + Long.toString(br.m_firstChunkIDORRangeID) + " (";

					if (m_ownBackupRanges.size() == 0) {
						ret = ret + "    None.";
					} else {
						for (int j = 0; j < br.m_backupPeers.length; j++) {
							// System.out.println("      backup peer: "+j+": "+br.m_backupPeers[j]);
							ret = ret + Short.toString(br.m_backupPeers[j]);
							if (j < br.m_backupPeers.length - 1) {
								ret = ret + ",";
							}
						}
					}
					ret = ret + ")\n";
				}
			}
			if (m_ownBackupRanges.size() == 0) {
				ret = ret + "    None.\n";
			}
		} else {
			ret = ret + "    None.\n";
		}

		ret = ret + "  Backup ranges for migrated chunks\n";
		if (m_migrationBackupRanges != null) {
			// System.out.println("   m_migrationBackupRanges");
			for (int i = 0; i < m_migrationBackupRanges.size(); i++) {
				final BackupRange br = m_migrationBackupRanges.get(i);
				ret = ret + "    BR" + Integer.toString(i) + ": ";

				if (br != null) {
					// System.out.println("   BackupRange: "+i+", m_firstChunkIDORRangeID="+br.m_firstChunkIDORRangeID);
					ret = ret + Long.toString(br.m_firstChunkIDORRangeID) + " (";

					for (int j = 0; j < br.m_backupPeers.length; j++) {
						// System.out.println("      backup peer: "+j+": "+br.m_backupPeers[j]);
						ret = ret + Short.toString(br.m_backupPeers[j]);
						if (j < br.m_backupPeers.length - 1) {
							ret = ret + ",";
						}
					}
					ret = ret + ")\n";
				}
				if (m_migrationBackupRanges.size() == 0) {
					ret = ret + "    None.\n";
				}
			}
			if (m_migrationBackupRanges.size() == 0) {
				ret = ret + "    None.\n";
			}
		} else {
			ret = ret + "    None.\n";
		}

		return ret;
	}

	/**
	 * Handles an incoming CommandRequest
	 * @param p_request
	 *            the CommandRequest
	 */
	private void incomingCommandRequest(final ChunkCommandRequest p_request) {
		String cmd;
		String res = null;

		Operation.INCOMING_COMMAND.enter();

		cmd = p_request.getArgument();

		if (cmd.indexOf("chunkinfo") >= 0) {
			res = cmdReqChunkinfo(cmd);
		} else if (cmd.indexOf("backups") >= 0) {
			res = cmdReqBackups(cmd);
		} else if (cmd.indexOf("cidt") >= 0) {
			res = cmdReqCIDT(cmd);
		} else if (cmd.indexOf("stats") >= 0) {
			// stats command?
			res = StatisticsManager.getStatistics();
		} else {
			// command handled in callback?
			if (Core.getCommandListener() != null) {
				res = Core.getCommandListener().processCmd(cmd, true);
			} else {
				res = "error: no command listener registered";
				System.out.println("error: command request received but no command listener registered");
			}
		}

		// send back result
		try {
			new ChunkCommandResponse(p_request, res).send(m_network);
		} catch (final NetworkException e) {
			e.printStackTrace();
		}
		Operation.INCOMING_COMMAND.leave();
	}
}
