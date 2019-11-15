package edu.utexas.tacc.tapis.files.lib;

import org.flywaydb.core.Flyway;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

@Test(groups={"integration"})
public abstract class BaseDatabaseIntegrationTest {

    @BeforeTest
    public void setUp() {
        Flyway flyway = Flyway.configure()
                .dataSource("jdbc:postgresql://localhost:5432/test", "test", "test")
                .load();
        flyway.clean();
        flyway.migrate();
    }


}
