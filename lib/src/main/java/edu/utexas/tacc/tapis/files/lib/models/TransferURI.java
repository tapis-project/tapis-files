package edu.utexas.tacc.tapis.files.lib.models;



import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;

import javax.validation.ValidationException;
import java.beans.JavaBean;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A Transfer urI must look like tapis://{systemId}/{path}
 */
public class TransferURI {

    private String systemId;
    private String path;
    private String protocol;
    private static final Pattern pattern = Pattern.compile("(http:\\/\\/|https:\\/\\/|tapis:\\/\\/)([\\w -\\.]+)\\/?(.*)");


    public TransferURI(String stringURI) throws ValidationException {

        Matcher matcher = pattern.matcher(stringURI);
        if (!matcher.find()) {
            throw new ValidationException("Invalid transfer URI");
        }
        systemId = matcher.group(2);
        var tmp = Optional.ofNullable(matcher.group(3)).orElse("/");
        path = FilenameUtils.normalize(tmp);
        protocol = matcher.group(1);
    }

    public TransferURI(String protocol, String systemId, String path) {
        this.path = FilenameUtils.normalize(path);
        this.systemId = systemId;
        this.protocol = protocol;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public String getSystemId() {
        return systemId;
    }

    public void setSystemId(String systemId) {
        this.systemId = systemId;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    @JsonValue
    public String toString() {
        String tmpPath = StringUtils.removeStart(path, "/");
        return String.format("%s%s/%s", protocol, systemId, tmpPath);
    }
}
