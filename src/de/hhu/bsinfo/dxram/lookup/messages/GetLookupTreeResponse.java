/*
 * Copyright (C) 2016 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.data.MessagesDataStructureImExporter;
import de.hhu.bsinfo.dxram.lookup.overlay.storage.LookupTree;
import de.hhu.bsinfo.dxram.lookup.overlay.storage.MetadataHandler;
import de.hhu.bsinfo.ethnet.AbstractResponse;

/**
 * Response to a GetLookupTreeRequest
 *
 * @author Michael Birkhoff, michael.birkhoff@hhu.de, 06.09.2016
 */
public class GetLookupTreeResponse extends AbstractResponse {

    // Attributes
    private LookupTree m_tree;

    // Constructors

    /**
     * Creates an instance of GetLookupTreeResponse
     */
    public GetLookupTreeResponse() {
        super();

        m_tree = null;
    }

    /**
     * Creates an instance of GetLookupTreeResponse
     *
     * @param p_request
     *     the GetLookupTreeRequest
     * @param p_trees
     *     the CIDTrees
     */
    public GetLookupTreeResponse(final GetLookupTreeRequest p_request, final LookupTree p_trees) {
        super(p_request, LookupMessages.SUBTYPE_GET_LOOKUP_TREE_RESPONSE);

        m_tree = p_trees;
    }

    // Getters

    /**
     * Get CIDTrees
     *
     * @return the CIDTrees
     */
    public final LookupTree getCIDTree() {
        return m_tree;
    }

    @Override
    protected final int getPayloadLength() {
        return m_tree.sizeofObject();
    }

    // Methods
    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        final MessagesDataStructureImExporter exporter = new MessagesDataStructureImExporter(p_buffer);

        exporter.exportObject(m_tree);
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        final MessagesDataStructureImExporter importer = new MessagesDataStructureImExporter(p_buffer);

        m_tree = new LookupTree();
        importer.importObject(m_tree);
    }

}
