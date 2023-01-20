package edu.utexas.tacc.tapis.files.api.resources;

class AclEntry {
    boolean isDefault;
    String type;
    String principal;
    String permissions;

    public AclEntry(String aclEntryString) throws Exception {
        aclEntryString = aclEntryString.trim();
        String[] parts = aclEntryString.split(":");
        int index = 0;
        if (parts.length == 4) {
            if (parts[index] != "default") {
                throw new Exception("Unknown format for ACLEntry - expecting 'default':" + aclEntryString);
            }
            index++;
            isDefault = true;
        }
        if (parts.length > index) {
            type = parts[index++];
        }

        if (parts.length > index) {
            principal = parts[index++];
        }

        if (parts.length > index) {
            permissions = parts[index];
        }
    }

    public boolean isDefault() {
        return isDefault;
    }

    public String getPermissions() {
        return permissions;
    }

    public String getPrincipal() {
        return principal;
    }

    public String getType() {
        return type;
    }
}
