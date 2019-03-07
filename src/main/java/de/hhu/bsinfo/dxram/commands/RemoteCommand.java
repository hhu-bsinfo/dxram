package de.hhu.bsinfo.dxram.commands;

import retrofit2.Retrofit;
import retrofit2.converter.gson.GsonConverterFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public abstract class RemoteCommand implements Runnable {

    protected Retrofit m_retrofit = new Retrofit.Builder()
            .baseUrl("http://localhost:7000")
            .build();

    protected Gson m_gson = new GsonBuilder().setPrettyPrinting().create();

    protected JsonParser m_parser = new JsonParser();

    protected final void prettyPrint(final String p_json) {
        JsonElement element = m_parser.parse(p_json);
        String output = m_gson.toJson(element);
        System.out.println(output);
    }

}
