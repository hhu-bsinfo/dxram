/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.lookup.messages;

/**
 * Type and list of subtypes for all lookup messages
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 30.03.2016
 */
public final class LookupMessages {

    // Constants
    public static final byte SUBTYPE_JOIN_REQUEST = 1;
    public static final byte SUBTYPE_JOIN_RESPONSE = 2;
    public static final byte SUBTYPE_GET_LOOKUP_RANGE_REQUEST = 3;
    public static final byte SUBTYPE_GET_LOOKUP_RANGE_RESPONSE = 4;
    public static final byte SUBTYPE_REMOVE_CHUNKIDS_REQUEST = 5;
    public static final byte SUBTYPE_REMOVE_CHUNKIDS_RESPONSE = 6;
    public static final byte SUBTYPE_INSERT_NAMESERVICE_ENTRIES_REQUEST = 7;
    public static final byte SUBTYPE_INSERT_NAMESERVICE_ENTRIES_RESPONSE = 8;
    public static final byte SUBTYPE_GET_CHUNKID_FOR_NAMESERVICE_ENTRY_REQUEST = 9;
    public static final byte SUBTYPE_GET_CHUNKID_FOR_NAMESERVICE_ENTRY_RESPONSE = 10;
    public static final byte SUBTYPE_GET_NAMESERVICE_ENTRY_COUNT_REQUEST = 11;
    public static final byte SUBTYPE_GET_NAMESERVICE_ENTRY_COUNT_RESPONSE = 12;
    public static final byte SUBTYPE_GET_NAMESERVICE_ENTRIES_REQUEST = 13;
    public static final byte SUBTYPE_GET_NAMESERVICE_ENTRIES_RESPONSE = 14;
    public static final byte SUBTYPE_NAMESERVICE_UPDATE_PEER_CACHES_MESSAGE = 15;
    public static final byte SUBTYPE_MIGRATE_REQUEST = 16;
    public static final byte SUBTYPE_MIGRATE_RESPONSE = 17;
    public static final byte SUBTYPE_MIGRATE_RANGE_REQUEST = 18;
    public static final byte SUBTYPE_MIGRATE_RANGE_RESPONSE = 19;
    public static final byte SUBTYPE_INIT_RANGE_REQUEST = 20;
    public static final byte SUBTYPE_INIT_RANGE_RESPONSE = 21;
    public static final byte SUBTYPE_GET_ALL_BACKUP_RANGES_REQUEST = 22;
    public static final byte SUBTYPE_GET_ALL_BACKUP_RANGES_RESPONSE = 23;
    public static final byte SUBTYPE_UPDATE_METADATA_AFTER_RECOVERY_MESSAGE = 24;
    public static final byte SUBTYPE_PING_SUPERPEER_MESSAGE = 25;

    public static final byte SUBTYPE_SEND_BACKUPS_MESSAGE = 26;
    public static final byte SUBTYPE_SEND_SUPERPEERS_MESSAGE = 27;
    public static final byte SUBTYPE_ASK_ABOUT_BACKUPS_REQUEST = 28;
    public static final byte SUBTYPE_ASK_ABOUT_BACKUPS_RESPONSE = 29;
    public static final byte SUBTYPE_ASK_ABOUT_SUCCESSOR_REQUEST = 30;
    public static final byte SUBTYPE_ASK_ABOUT_SUCCESSOR_RESPONSE = 31;
    public static final byte SUBTYPE_NOTIFY_ABOUT_NEW_PREDECESSOR_MESSAGE = 32;
    public static final byte SUBTYPE_NOTIFY_ABOUT_NEW_SUCCESSOR_MESSAGE = 33;

    public static final byte SUBTYPE_START_RECOVERY_MESSAGE = 34;
    public static final byte SUBTYPE_REPLACE_BACKUP_PEER_REQUEST = 35;
    public static final byte SUBTYPE_REPLACE_BACKUP_PEER_RESPONSE = 36;

    public static final byte SUBTYPE_BARRIER_ALLOC_REQUEST = 37;
    public static final byte SUBTYPE_BARRIER_ALLOC_RESPONSE = 38;
    public static final byte SUBTYPE_BARRIER_FREE_REQUEST = 39;
    public static final byte SUBTYPE_BARRIER_FREE_RESPONSE = 40;
    public static final byte SUBTYPE_BARRIER_SIGN_ON_REQUEST = 41;
    public static final byte SUBTYPE_BARRIER_SIGN_ON_RESPONSE = 42;
    public static final byte SUBTYPE_BARRIER_RELEASE_MESSAGE = 43;
    public static final byte SUBTYPE_BARRIER_STATUS_REQUEST = 44;
    public static final byte SUBTYPE_BARRIER_STATUS_RESPONSE = 45;
    public static final byte SUBTYPE_BARRIER_CHANGE_SIZE_REQUEST = 46;
    public static final byte SUBTYPE_BARRIER_CHANGE_SIZE_RESPONSE = 47;

    public static final byte SUBTYPE_SUPERPEER_STORAGE_CREATE_REQUEST = 48;
    public static final byte SUBTYPE_SUPERPEER_STORAGE_CREATE_RESPONSE = 49;
    public static final byte SUBTYPE_SUPERPEER_STORAGE_GET_REQUEST = 50;
    public static final byte SUBTYPE_SUPERPEER_STORAGE_GET_RESPONSE = 51;
    public static final byte SUBTYPE_SUPERPEER_STORAGE_GET_ANON_REQUEST = 52;
    public static final byte SUBTYPE_SUPERPEER_STORAGE_GET_ANON_RESPONSE = 53;
    public static final byte SUBTYPE_SUPERPEER_STORAGE_PUT_REQUEST = 54;
    public static final byte SUBTYPE_SUPERPEER_STORAGE_PUT_RESPONSE = 55;
    public static final byte SUBTYPE_SUPERPEER_STORAGE_PUT_ANON_REQUEST = 56;
    public static final byte SUBTYPE_SUPERPEER_STORAGE_PUT_ANON_RESPONSE = 57;
    public static final byte SUBTYPE_SUPERPEER_STORAGE_REMOVE_MESSAGE = 58;
    public static final byte SUBTYPE_SUPERPEER_STORAGE_STATUS_REQUEST = 59;
    public static final byte SUBTYPE_SUPERPEER_STORAGE_STATUS_RESPONSE = 60;

    public static final byte SUBTYPE_GET_LOOKUP_TREE_REQUEST = 61;
    public static final byte SUBTYPE_GET_LOOKUP_TREE_RESPONSE = 62;
    public static final byte SUBTYPE_GET_METADATA_SUMMARY_REQUEST = 63;
    public static final byte SUBTYPE_GET_METADATA_SUMMARY_RESPONSE = 64;

    /**
     * Hidden constructor
     */
    private LookupMessages() {
    }
}
