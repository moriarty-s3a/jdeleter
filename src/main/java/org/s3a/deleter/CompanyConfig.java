package org.s3a.deleter;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class CompanyConfig {
    @JsonProperty("companyId")
    private String id;
    @JsonProperty("companyName")
    private String name;
    @JsonProperty("retentionDays")
    private Integer retention;
}
