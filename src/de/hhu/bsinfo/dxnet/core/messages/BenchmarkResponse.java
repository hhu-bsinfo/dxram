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

package de.hhu.bsinfo.dxnet.core.messages;

import de.hhu.bsinfo.dxnet.core.Response;

/**
 * This is a benchmark response which is used in DXNetMain.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 11.10.2017
 */
public class BenchmarkResponse extends Response {

    /**
     * Creates an instance of BenchmarkResponse.
     */
    public BenchmarkResponse() {
        super();
    }

    /**
     * Creates an instance of BenchmarkResponse
     *
     * @param p_request
     *         the BenchmarkRequest
     */
    public BenchmarkResponse(final BenchmarkRequest p_request) {
        super(p_request, Messages.SUBTYPE_BENCHMARK_RESPONSE);
    }
}
