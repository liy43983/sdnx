package cn.trustfar.db.po;

public class ViewData {

    private Integer systemId;
    private Integer indexId;
    private String statisticsTime;
    private String statisticalKey;
    private String statisticalValue;

    public ViewData() {
    }

    public ViewData(Integer systemId, Integer indexId, String statisticsTime, String statisticalKey, String statisticalValue) {
        this.systemId = systemId;
        this.indexId = indexId;
        this.statisticsTime = statisticsTime;
        this.statisticalKey = statisticalKey;
        this.statisticalValue = statisticalValue;
    }

    public Integer getSystemId() {
        return systemId;
    }

    public void setSystemId(Integer systemId) {
        this.systemId = systemId;
    }

    public Integer getIndexId() {
        return indexId;
    }

    public void setIndexId(Integer indexId) {
        this.indexId = indexId;
    }

    public String getStatisticsTime() {
        return statisticsTime;
    }

    public void setStatisticsTime(String statisticsTime) {
        this.statisticsTime = statisticsTime;
    }

    public String getStatisticalKey() {
        return statisticalKey;
    }

    public void setStatisticalKey(String statisticalKey) {
        this.statisticalKey = statisticalKey;
    }

    public String getStatisticalValue() {
        return statisticalValue;
    }

    public void setStatisticalValue(String statisticalValue) {
        this.statisticalValue = statisticalValue;
    }

    @Override
    public String toString() {
        return "ViewData{" +
                "systemId=" + systemId +
                ", indexId=" + indexId +
                ", statisticsTime='" + statisticsTime + '\'' +
                ", statisticalKey='" + statisticalKey + '\'' +
                ", statisticalValue='" + statisticalValue + '\'' +
                '}';
    }
}
