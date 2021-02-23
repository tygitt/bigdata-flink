package cn.brc.trackingEvent.bean;

import java.io.Serializable;

public class ExpandData implements Serializable {

    private static final long serialVersionUID = 1L;

    private String page_id;

    private String project_id;

    private String role;

    private String lbcx;

    private String vcr_id;

    private String vr_id;

    public ExpandData() {
    }

    public String getPage_id() {
        return page_id;
    }

    public void setPage_id(String page_id) {
        this.page_id = page_id;
    }

    @Override
    public String toString() {
        return "ExpandData{" +
                "page_id='" + page_id + '\'' +
                ", project_id='" + project_id + '\'' +
                ", role='" + role + '\'' +
                ", lbcx='" + lbcx + '\'' +
                ", vcr_id='" + vcr_id + '\'' +
                ", vr_id='" + vr_id + '\'' +
                '}';
    }

    public String getProject_id() {
        return project_id;
    }

    public ExpandData(String page_id, String project_id, String role, String lbcx, String vcr_id, String vr_id) {
        this.page_id = page_id;
        this.project_id = project_id;
        this.role = role;
        this.lbcx = lbcx;
        this.vcr_id = vcr_id;
        this.vr_id = vr_id;
    }

    public void setProject_id(String project_id) {
        this.project_id = project_id;
    }

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public String getLbcx() {
        return lbcx;
    }

    public void setLbcx(String lbcx) {
        this.lbcx = lbcx;
    }

    public String getVcr_id() {
        return vcr_id;
    }

    public void setVcr_id(String vcr_id) {
        this.vcr_id = vcr_id;
    }

    public String getVr_id() {
        return vr_id;
    }

    public void setVr_id(String vr_id) {
        this.vr_id = vr_id;
    }
}
