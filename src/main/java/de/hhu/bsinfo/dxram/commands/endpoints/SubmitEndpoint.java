package de.hhu.bsinfo.dxram.commands.endpoints;

import okhttp3.MultipartBody;
import okhttp3.ResponseBody;
import retrofit2.Call;
import retrofit2.http.GET;
import retrofit2.http.Multipart;
import retrofit2.http.POST;
import retrofit2.http.Part;
import retrofit2.http.Path;
import retrofit2.http.Query;

public interface SubmitEndpoint {

    @Multipart
    @POST("submit")
    Call<Void> submit(@Part MultipartBody.Part p_archive, @Query("node") String p_nodeId, @Query("args") String p_args);
}
