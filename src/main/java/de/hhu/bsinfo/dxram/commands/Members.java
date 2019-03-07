package de.hhu.bsinfo.dxram.commands;

import okhttp3.ResponseBody;
import picocli.CommandLine;
import retrofit2.Call;
import retrofit2.Response;

import java.io.IOException;

import de.hhu.bsinfo.dxram.commands.endpoints.MembersEndpoint;
import de.hhu.bsinfo.dxutils.NodeID;

@CommandLine.Command(
        name = "members",
        description = "Lists all members within the DXRAM network.%n")
public class Members extends RemoteCommand {

    @CommandLine.Option(names = "--id", description = "The node id to query")
    private String m_nodeId;

    @Override
    public void run() {
        MembersEndpoint endpoint = m_retrofit.create(MembersEndpoint.class);

        Call<ResponseBody> call = m_nodeId != null ? endpoint.getDetails(m_nodeId) : endpoint.getMembers();

        try {
            String response = call.execute().body().string();
            prettyPrint(response);
        } catch (IOException e) {
            // ignored
        }
    }
}
