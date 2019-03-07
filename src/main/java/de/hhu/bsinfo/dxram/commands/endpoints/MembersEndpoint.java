package de.hhu.bsinfo.dxram.commands.endpoints;

import okhttp3.ResponseBody;
import retrofit2.Call;
import retrofit2.http.GET;
import retrofit2.http.Path;

public interface MembersEndpoint {

    @GET("members")
    Call<ResponseBody> getMembers();

    @GET("members/{id}")
    Call<ResponseBody> getDetails(@Path("id") String p_nodeId);

}
