public class User {
    private Integer APP_ID;
    private String CLIENT_SECRET;
    private String code;
    private String ACCESS_TOKEN;

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public Integer getAPP_ID() {
        return APP_ID;
    }

    public void setAPP_ID(Integer APP_ID) {
        this.APP_ID = APP_ID;
    }

    public String getCLIENT_SECRET() {
        return CLIENT_SECRET;
    }

    public void setCLIENT_SECRET(String CLIENT_SECRET) {
        this.CLIENT_SECRET = CLIENT_SECRET;
    }

    public String getACCESS_TOKEN() {
        return ACCESS_TOKEN;
    }

    public void setACCESS_TOKEN(String ACCESS_TOKEN) {
        this.ACCESS_TOKEN = ACCESS_TOKEN;
    }
}
