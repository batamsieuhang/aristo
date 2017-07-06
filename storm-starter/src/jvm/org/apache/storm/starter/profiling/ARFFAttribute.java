package org.apache.storm.starter.profiling;


/**
 * Created by vgol on 24/9/2016.
 */
public class ARFFAttribute {

    private String attributeName;
    private String attributeType;

    public ARFFAttribute(String attributeName, String attributeType) {
        this.attributeName = attributeName;
        this.attributeType = attributeType;
    }

    public String getAttributeName() {
        return attributeName;
    }

    public void setAttributeName(String attributeName) {
        this.attributeName = attributeName;
    }

    public String getAttributeType() {
        return attributeType;
    }

    public void setAttributeType(String attributeType) {
        this.attributeType = attributeType;
    }
}
