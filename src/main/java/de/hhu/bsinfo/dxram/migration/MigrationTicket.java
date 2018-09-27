package de.hhu.bsinfo.dxram.migration;

import de.hhu.bsinfo.dxram.migration.data.MigrationIdentifier;

import java.util.concurrent.CompletableFuture;

public class MigrationTicket {

    private final CompletableFuture<MigrationStatus> m_future;

    private final MigrationIdentifier m_identifier;

    public MigrationTicket(CompletableFuture<MigrationStatus> p_future, MigrationIdentifier p_identifier) {
        m_future = p_future;
        m_identifier = p_identifier;
    }

    CompletableFuture<MigrationStatus> getFuture() {
        return m_future;
    }

    public MigrationIdentifier getIdentifier() {
        return m_identifier;
    }
}
