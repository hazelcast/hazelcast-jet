package com.hazelcast.jet.sql;

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.sql.impl.connector.kafka.KafkaSqlConnector;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.hazelcast.jet.sql.impl.schema.JetSchema.OPTION_CLASS_NAME;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SqlDdlTest extends SimpleTestInClusterSupport {

    private static final String CONNECTOR_NAME = "kafka";
    private static final String SERVER_NAME = "kafka_server";
    private static final String TABLE_NAME = "kafka_topic";

    private static JetSqlService sqlService;

    @BeforeClass
    public static void beforeClass() {
        initialize(1, null);
        sqlService = new JetSqlService(instance());

        sqlService.execute(format("CREATE FOREIGN DATA WRAPPER %s OPTIONS (%s '%s')",
                CONNECTOR_NAME, OPTION_CLASS_NAME, KafkaSqlConnector.class.getName()));
        sqlService.execute(format("CREATE SERVER %s FOREIGN DATA WRAPPER %s",
                SERVER_NAME, CONNECTOR_NAME));
        sqlService.execute(format("CREATE FOREIGN TABLE %s (__key INT, this INT) SERVER %s",
                TABLE_NAME, SERVER_NAME));
    }

    @Test
    public void when_createsDuplicateConnectorWithoutReplace_then_throws() {
        assertThatThrownBy(() -> sqlService.execute(format("CREATE FOREIGN DATA WRAPPER %s OPTIONS (%s '%s')",
                CONNECTOR_NAME, OPTION_CLASS_NAME, KafkaSqlConnector.class.getName()))
        ).isInstanceOf(JetException.class);
    }

    @Test
    public void when_createsDuplicateConnectorWithReplace_then_succeeds() {
        sqlService.execute(format("CREATE OR REPLACE FOREIGN DATA WRAPPER %s OPTIONS (%s '%s')",
                CONNECTOR_NAME, OPTION_CLASS_NAME, KafkaSqlConnector.class.getName()));
    }

    @Test
    public void when_createsDuplicateServerWithoutReplace_then_throws() {
        assertThatThrownBy(() -> sqlService.execute(format("CREATE SERVER %s FOREIGN DATA WRAPPER %s",
                SERVER_NAME, CONNECTOR_NAME))
        ).isInstanceOf(JetException.class);
    }

    @Test
    public void when_createsDuplicateServerWithReplace_then_succeeds() {
        sqlService.execute(format("CREATE OR REPLACE SERVER %s FOREIGN DATA WRAPPER %s",
                SERVER_NAME, CONNECTOR_NAME));
    }

    @Test
    public void when_createsDuplicateTableWithoutReplace_then_throws() {
        assertThatThrownBy(() -> sqlService.execute(format("CREATE FOREIGN TABLE %s (__key INT, this INT) SERVER %s",
                TABLE_NAME, SERVER_NAME))
        ).isInstanceOf(JetException.class);
    }

    @Test
    public void when_createsDuplicateTableWithReplace_then_succeeds() {
        sqlService.execute(format("CREATE OR REPLACE FOREIGN TABLE %s (__key INT, this INT) SERVER %s",
                TABLE_NAME, SERVER_NAME));
    }
}
