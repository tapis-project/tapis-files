package edu.utexas.tacc.tapis.files.lib.models;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class AclEntry {

    /**
     * true if the acl entry represents a default entry (starts with "default:").
     */
    private boolean defaultAcl;

    /**
     * type of acl entry.  Typically this would be file, group, or mask.
     */
    private String type;

    /**
     * principal for the acl entry.  Typically the groupid or userid.
     */
    private String principal;

    /**
     * the actual permissions granted.  For example, rwx or rw-.
     */
    String permissions;

    public AclEntry() {

    }

    public AclEntry(boolean defaultAcl, String type, String principal, String permissions) {
        this.defaultAcl = defaultAcl;
        this.type = type;
        this.principal = principal;
        this.permissions = permissions;
    }

    public boolean isDefaultAcl() {
        return defaultAcl;
    }

    public void setDefaultAcl(boolean defaultAcl) {
        this.defaultAcl = defaultAcl;
    }

    public String getPermissions() {
        return permissions;
    }

    public void setPermissions(String permissions) {
        this.permissions = permissions;
    }

    public String getPrincipal() {
        return principal;
    }

    public void setPrincipal(String principal) {
        this.principal = principal;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    /**
     * Parses output from getfacl command.
     *
     * @param getFaclStdOut stdout from getfacl command.
     * @return List containing AclEntry objects representing the file acls.
     * @throws Exception
     */
    public static List<AclEntry> parseAclEntries(String getFaclStdOut) throws TapisException, IOException {
        List<AclEntry> aclEntries = new ArrayList<AclEntry>();
        try {
            try (BufferedReader bReader = new BufferedReader(new StringReader(getFaclStdOut))) {
                aclEntries = bReader.lines().map(line -> {
                    return parseAclEntryString(line);
                }).filter(entry -> {
                    return (entry != null);
                }).collect(Collectors.toList());
            }
        } catch (RuntimeException ex) {
            throw new TapisException("Unable to parse results", ex);
        }

        return aclEntries;
    }

    /**
     * Takes a string containing a single line of output from getfacl, and converts
     * it into an AclEntry
     * @param aclEntryString single acl entry line from stdout of getfacl command
     * @return AclEntry containing the acl information
     */
    public static AclEntry parseAclEntryString(String aclEntryString) {
        AclEntry aclEntry = new AclEntry();
        if((aclEntryString == null) || (aclEntryString.isBlank())) {
            return null;
        }
        aclEntryString = aclEntryString.trim();
        String[] parts = aclEntryString.split(":");
        int index = 0;
        if (parts.length == 4) {
            if (!"default".equals(parts[index])) {
                throw new RuntimeException("Unknown format for ACLEntry - expecting 'default':" + aclEntryString);
            }
            index++;
            aclEntry.setDefaultAcl(true);
        }

        if (parts.length > index) {
            String type = parts[index++];
            if ((type != null) && (!type.isBlank())) {
                aclEntry.setType(type);
            }
        }

        if (parts.length > index) {
            String principal = parts[index++];
            if ((principal != null) && (!principal.isBlank())) {
                aclEntry.setPrincipal(principal);
            }
        }

        if (parts.length > index) {
            String permissions = parts[index++];
            if ((permissions != null) && (!permissions.isBlank())) {
                aclEntry.setPermissions(permissions);
            }
        }

        return aclEntry;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AclEntry aclEntry = (AclEntry) o;
        return defaultAcl == aclEntry.isDefaultAcl() &&
                Objects.equals(type, aclEntry.type) &&
                Objects.equals(principal, aclEntry.principal) &&
                Objects.equals(permissions, aclEntry.permissions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(isDefaultAcl(), type, principal, permissions);
    }
}
