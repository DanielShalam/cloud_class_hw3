package temp;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;

import java.io.Serializable;

@JsonRootName("person")
public class CreatedEvent implements Serializable {
    private String page_id;
    private String domain;
    private String timestamp;
    private String user_name;
    private String user_is_bot;

    public CreatedEvent() {

    }

    @JsonCreator
    public CreatedEvent(@JsonProperty("page_id") String page_id, @JsonProperty("domain") String domain,
                        @JsonProperty("timestamp") String timestamp, @JsonProperty("user_name") String user_name,
                        @JsonProperty("user_is_bot") String user_is_bot) {
        this.page_id = page_id;
        this.domain = domain;
        this.timestamp = timestamp;
        this.user_name = user_name;
        this.user_is_bot = user_is_bot;
    }

    //getters and setters stripped

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getPage_id() {
        return page_id;
    }

    public void setPage_id(String page_id) {
        this.page_id = page_id;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getUser_name() {
        return user_name;
    }

    public void setUser_name(String user_name) {
        this.user_name = user_name;
    }

    public String getUser_is_bot() {
        return user_is_bot;
    }

    public void setUser_is_bot(String user_is_bot) {
        this.user_is_bot = user_is_bot;
    }

    @Override
    public String toString(){
        return this.getDomain();
    }

}
