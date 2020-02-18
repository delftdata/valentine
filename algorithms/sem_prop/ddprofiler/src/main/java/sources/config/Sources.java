package sources.config;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Sources {

    @JsonProperty
    private int api_version;

    @JsonProperty
    private List<GenericSource> sources;

    public int getApi_version() {
	return api_version;
    }

    public List<GenericSource> getSources() {
	return sources;
    }

}
