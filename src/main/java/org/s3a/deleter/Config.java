package org.s3a.deleter;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.Map;
import java.util.Set;

@Data
public class Config {
    @JsonProperty("default")
    private CompanyConfig defaultConfig;
    private Set<CompanyConfig> companies;
}
