package de.hhu.bsinfo.dxram.dxddl;

import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.operation.CreateLocal;
import de.hhu.bsinfo.dxram.chunk.operation.CreateReservedLocal;
import de.hhu.bsinfo.dxram.chunk.operation.ReserveLocal;
import de.hhu.bsinfo.dxram.chunk.operation.Remove;
import de.hhu.bsinfo.dxram.chunk.operation.PinningLocal;
import de.hhu.bsinfo.dxram.chunk.operation.RawReadLocal;
import de.hhu.bsinfo.dxram.chunk.operation.RawWriteLocal;

public final class DirectAccessSecurityManager {

    private static boolean INITIALIZED = false;
    static long NID;
    private static short NEXT;

    public static void init(
            final BootService boot, 
            final CreateLocal create, 
            final CreateReservedLocal create_reserved, 
            final ReserveLocal reserve, 
            final Remove remove, 
            final PinningLocal pinning, 
            final RawReadLocal rawread, 
            final RawWriteLocal rawwrite) {
        if (!INITIALIZED) {
            INITIALIZED = true;

            DirectVertex.init(create, create_reserved, reserve, remove, pinning, rawread, rawwrite);
            DirectPrimitiveDataTypesChunk.init(create, create_reserved, reserve, remove, pinning, rawread, rawwrite);
            DirectGraph.init(create, create_reserved, reserve, remove, pinning, rawread, rawwrite);
            DirectComplexChunk.init(create, create_reserved, reserve, remove, pinning, rawread, rawwrite);
            DirectCountry.init(create, create_reserved, reserve, remove, pinning, rawread, rawwrite);
            DirectStringsChunk.init(create, create_reserved, reserve, remove, pinning, rawread, rawwrite);
            DirectCity.init(create, create_reserved, reserve, remove, pinning, rawread, rawwrite);
            DirectSimpleChunk.init(create, create_reserved, reserve, remove, pinning, rawread, rawwrite);
            DirectEdge.init(create, create_reserved, reserve, remove, pinning, rawread, rawwrite);
            DirectPerson.init(create, create_reserved, reserve, remove, pinning, rawread, rawwrite);

            NID = (long) boot.getNodeID() << 48;
            NEXT = 0;

            DirectVertex.setTYPE(nextTypeID());
            DirectPrimitiveDataTypesChunk.setTYPE(nextTypeID());
            DirectGraph.setTYPE(nextTypeID());
            DirectComplexChunk.setTYPE(nextTypeID());
            DirectCountry.setTYPE(nextTypeID());
            DirectStringsChunk.setTYPE(nextTypeID());
            DirectCity.setTYPE(nextTypeID());
            DirectSimpleChunk.setTYPE(nextTypeID());
            DirectEdge.setTYPE(nextTypeID());
            DirectPerson.setTYPE(nextTypeID());
        } else {
            throw new RuntimeException("Initialization already completed. Multiple calls not allowed.");
        }
    }

    private static short nextTypeID() {
        return NEXT++;
    }
}
