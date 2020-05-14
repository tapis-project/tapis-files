package edu.utexas.tacc.tapis.files.lib.cache;

import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;

import java.util.Objects;

public class SSHConnectionCacheKey {

    private final TSystem system;
    private final String username;

    public SSHConnectionCacheKey(TSystem sys, String uname) {
        system = sys;
        username = uname;
    }

    public TSystem getSystem() {
        return system;
    }

    public String getUsername() {
        return username;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SSHConnectionCacheKey that = (SSHConnectionCacheKey) o;
        return Objects.equals(system.getId(), that.system.getId()) &&
                Objects.equals(username, that.username);
    }

    @Override
    public int hashCode() {
        return Objects.hash(system.getId(), username);
    }
}
