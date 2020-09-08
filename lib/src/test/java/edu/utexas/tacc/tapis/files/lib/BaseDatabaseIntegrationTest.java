package edu.utexas.tacc.tapis.files.lib;

import org.flywaydb.core.Flyway;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

@Test(groups={"integration"})
public abstract class BaseDatabaseIntegrationTest {

    @BeforeMethod
    public void doMigrations() {
        Flyway flyway = Flyway.configure()
                .dataSource("jdbc:postgresql://localhost:5432/test", "test", "test")
                .load();
        flyway.clean();
        flyway.migrate();
    }


}
