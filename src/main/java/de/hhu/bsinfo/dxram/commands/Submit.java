package de.hhu.bsinfo.dxram.commands;

import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.RequestBody;
import okhttp3.ResponseBody;
import picocli.CommandLine;
import retrofit2.Call;

import java.io.File;
import java.io.IOException;

import de.hhu.bsinfo.dxram.commands.endpoints.MembersEndpoint;
import de.hhu.bsinfo.dxram.commands.endpoints.SubmitEndpoint;

@CommandLine.Command(
        name = "submit",
        description = "Submits an application to a specific or all nodes.%n")
public class Submit extends RemoteCommand {

    @CommandLine.Parameters(index = "0", description = "The application's java archive file")
    private File m_archiveFile;

    @CommandLine.Parameters(index = "1..*", arity = "0..*", description = "The application arguments", defaultValue = "")
    private String[] m_args;

    @CommandLine.Option(names = "--id", description = "The node id to submit the application to", defaultValue = "")
    private String m_nodeId;

    @Override
    public void run() {
        SubmitEndpoint submitEndpoint = m_retrofit.create(SubmitEndpoint.class);

        RequestBody requestFile = RequestBody.create(MediaType.parse("application/json"), m_archiveFile);

        MultipartBody.Part body = MultipartBody.Part.createFormData("archive", m_archiveFile.getName(), requestFile);

        Call<Void> call = submitEndpoint.submit(body, m_nodeId, String.join(" ", m_args));

        try {
            call.execute();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
