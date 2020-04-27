package com.hazelcast.jet.sql;

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.sql.impl.connector.kafka.KafkaSqlConnector;
import org.apache.calcite.runtime.CalciteException;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.hazelcast.jet.sql.impl.schema.JetSchema.OPTION_CLASS_NAME;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SqlDdlTest extends SimpleTestInClusterSupport {

    private static final String CONNECTOR_NAME = "kafka";
    private static final String SERVER_NAME = "kafka_server";
    private static final String TABLE_NAME = "kafka_topic";

    private static final String CONNECTOR_TO_REMOVE_NAME = "kafka_to_remove";
    private static final String SERVER_TO_REMOVE_NAME = "kafka_server_to_remove";
    private static final String TABLE_TO_REMOVE_NAME = "kafka_topic_to_remove";

    private static final String CONNECTOR_TO_REMOVE_ONE_BY_ONE_NAME = "kafka_to_remove_one_by_one";
    private static final String SERVER_TO_REMOVE_ONE_BY_ONE_NAME = "kafka_server_to_remove_one_by_one";
    private static final String TABLE_TO_REMOVE_ONE_BY_ONE_NAME = "kafka_topic_to_remove_one_by_one";

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

        sqlService.execute(format("CREATE FOREIGN DATA WRAPPER %s OPTIONS (%s '%s')",
                CONNECTOR_TO_REMOVE_NAME, OPTION_CLASS_NAME, KafkaSqlConnector.class.getName()));
        sqlService.execute(format("CREATE SERVER %s FOREIGN DATA WRAPPER %s",
                SERVER_TO_REMOVE_NAME, CONNECTOR_TO_REMOVE_NAME));
        sqlService.execute(format("CREATE FOREIGN TABLE %s (__key INT, this INT) SERVER %s",
                TABLE_TO_REMOVE_NAME, SERVER_TO_REMOVE_NAME));

        sqlService.execute(format("CREATE FOREIGN DATA WRAPPER %s OPTIONS (%s '%s')",
                CONNECTOR_TO_REMOVE_ONE_BY_ONE_NAME, OPTION_CLASS_NAME, KafkaSqlConnector.class.getName()));
        sqlService.execute(format("CREATE SERVER %s FOREIGN DATA WRAPPER %s",
                SERVER_TO_REMOVE_ONE_BY_ONE_NAME, CONNECTOR_TO_REMOVE_ONE_BY_ONE_NAME));
        sqlService.execute(format("CREATE FOREIGN TABLE %s (__key INT, this INT) SERVER %s",
                TABLE_TO_REMOVE_ONE_BY_ONE_NAME, SERVER_TO_REMOVE_ONE_BY_ONE_NAME));
    }

    @Test
    public void when_createsDuplicateConnector_then_throws() {
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
    public void when_createsDuplicateConnectorWithIfNotExists_then_succeeds() {
        sqlService.execute(format("CREATE FOREIGN DATA WRAPPER IF NOT EXISTS %s OPTIONS (%s '%s')",
                CONNECTOR_NAME, OPTION_CLASS_NAME, KafkaSqlConnector.class.getName()));
    }

    @Test
    public void when_createsDuplicateServer_then_throws() {
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
    public void when_createsDuplicateServerWithIfNotExists_then_succeeds() {
        sqlService.execute(format("CREATE SERVER IF NOT EXISTS %s FOREIGN DATA WRAPPER %s",
                SERVER_NAME, CONNECTOR_NAME));
    }

    @Test
    public void when_createsDuplicateTable_then_throws() {
        assertThatThrownBy(() -> sqlService.execute(format("CREATE FOREIGN TABLE %s (__key INT, this INT) SERVER %s",
                TABLE_NAME, SERVER_NAME))
        ).isInstanceOf(JetException.class);
    }

    @Test
    public void when_createsDuplicateTableWithReplace_then_succeeds() {
        sqlService.execute(format("CREATE OR REPLACE FOREIGN TABLE %s (__key INT, this INT) SERVER %s",
                TABLE_NAME, SERVER_NAME));
    }

    @Test
    public void when_createsDuplicateTableWithIfNotExists_then_succeeds() {
        sqlService.execute(format("CREATE FOREIGN TABLE IF NOT EXISTS %s (__key INT, this INT) SERVER %s",
                TABLE_NAME, SERVER_NAME));
    }

    @Test
    public void when_dropsUnknownConnector_then_throws() {
        assertThatThrownBy(() -> sqlService.execute("DROP FOREIGN DATA WRAPPER unknown_connector"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void when_dropsUnknownConnectorWithIfExists_then_succeeds() {
        sqlService.execute("DROP FOREIGN DATA WRAPPER IF EXISTS unknown_connector");
    }

    @Test
    public void when_dropsUnknownServer_then_throws() {
        assertThatThrownBy(() -> sqlService.execute("DROP SERVER unknown_server"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void when_dropsUnknownServerWithIfExists_then_succeeds() {
        sqlService.execute("DROP SERVER IF EXISTS unknown_server");
    }

    @Test
    public void when_dropsUnknownTable_then_throws() {
        assertThatThrownBy(() -> sqlService.execute("DROP FOREIGN TABLE unknown_table"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void when_dropsUnknownTableWithIfExists_then_succeeds() {
        sqlService.execute("DROP FOREIGN TABLE IF EXISTS unknown_table");
    }

    @Test
    public void when_dropsConnectorWithDependantServers_then_throws() {
        assertThatThrownBy(() -> sqlService.execute(format("DROP FOREIGN DATA WRAPPER %s", CONNECTOR_NAME)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void when_dropsServerWithDependantTables_then_throws() {
        assertThatThrownBy(() -> sqlService.execute(format("DROP SERVER %s RESTRICT", SERVER_NAME)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void when_dropsCascadeConnector_then_dropsServersAndTables() {
        sqlService.execute(format("DROP FOREIGN DATA WRAPPER %s CASCADE", CONNECTOR_TO_REMOVE_NAME));

        assertThatThrownBy(() -> sqlService.execute(
                format("CREATE SERVER %s FOREIGN DATA WRAPPER %s", SERVER_TO_REMOVE_NAME, CONNECTOR_TO_REMOVE_NAME))
        ).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> sqlService.execute(
                format("SELECT * FROM %s", TABLE_TO_REMOVE_NAME))
        ).isInstanceOf(CalciteException.class);
    }

    @Test
    public void when_dropsObject_then_itsNoLongerAvailable() {
        sqlService.execute(format("DROP FOREIGN TABLE %s", TABLE_TO_REMOVE_ONE_BY_ONE_NAME));
        assertThatThrownBy(() -> sqlService.execute(
                format("SELECT * FROM %s", TABLE_TO_REMOVE_ONE_BY_ONE_NAME))
        ).isInstanceOf(CalciteException.class);

        sqlService.execute(format("DROP SERVER %s", SERVER_TO_REMOVE_ONE_BY_ONE_NAME));
        assertThatThrownBy(() -> sqlService.execute(
                format("CREATE FOREIGN TABLE %s (__key INT, this INT) SERVER %s",
                        TABLE_TO_REMOVE_ONE_BY_ONE_NAME, SERVER_TO_REMOVE_ONE_BY_ONE_NAME))
        ).isInstanceOf(IllegalArgumentException.class);

        sqlService.execute(format("DROP FOREIGN DATA WRAPPER %s", CONNECTOR_TO_REMOVE_ONE_BY_ONE_NAME));
        assertThatThrownBy(() -> sqlService.execute(
                format("CREATE SERVER %s FOREIGN DATA WRAPPER %s",
                        SERVER_TO_REMOVE_ONE_BY_ONE_NAME, CONNECTOR_TO_REMOVE_ONE_BY_ONE_NAME))
        ).isInstanceOf(IllegalArgumentException.class);
    }
}
