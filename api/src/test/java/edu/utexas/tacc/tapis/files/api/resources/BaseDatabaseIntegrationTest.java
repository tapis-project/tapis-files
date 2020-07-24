package edu.utexas.tacc.tapis.files.api.resources;

import org.flywaydb.core.Flyway;
import org.glassfish.jersey.test.JerseyTestNg;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups={"integration"})
public abstract class BaseDatabaseIntegrationTest extends JerseyTestNg.ContainerPerClassTest {

    @BeforeMethod
    public void doBeforeTest() {
        Flyway flyway = Flyway.configure()
                .dataSource("jdbc:postgresql://localhost:5432/test", "test", "test")
                .load();
        flyway.clean();
        flyway.migrate();
    }


}
