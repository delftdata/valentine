package sources.config;

import com.fasterxml.jackson.annotation.JsonProperty;

import sources.SourceType;

public class CSVSourceConfig implements SourceConfig {

    private String sourceName;

    private String relationName;

    @JsonProperty
    private String path;

    @JsonProperty
    private String separator;

    @Override
    public String getPath() {
	return path;
    }

    public String getSeparator() {
	return separator;
    }

    @Deprecated
    public void setPath(String path) {
	this.path = path;
    }

    public void setSeparator(String separator) {
	this.separator = separator;
    }

    @Override
    public String getSourceName() {
	return sourceName;
    }

    @Override
    public void setSourceName(String sourceName) {
	this.sourceName = sourceName;
    }

    @Override
    public SourceType getSourceType() {
	return SourceType.csv;
    }

    public String getRelationName() {
	return relationName;
    }

    @Deprecated
    public void setRelationName(String relationName) {
	this.relationName = relationName;
    }

    @Override
    public SourceConfig selfCopy() {
	CSVSourceConfig copy = new CSVSourceConfig();
	copy.sourceName = this.sourceName;
	copy.relationName = this.relationName;
	copy.path = this.path;
	copy.separator = this.separator;
	return copy;
    }

}
