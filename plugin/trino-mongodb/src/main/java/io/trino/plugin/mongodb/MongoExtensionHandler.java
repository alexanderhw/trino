package io.trino.plugin.mongodb;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import org.bson.Document;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;

public class MongoExtensionHandler {

    private static final String EXTENSION_DB_URL = "trino.extension.db.url";
    private static final String EXTENSION_DB_USER = "trino.extension.db.user";
    private static final String EXTENSION_DB_PASSWORD = "trino.extension.db.password";
    private static final String EXTENSION_DB_ACTIVE = "trino.extension.db.active";
    private static final String EXTENSION_DB_DRIVER = "trino.extension.db.driver";
    private static final String EXTENSION_CONFIG_FILE_PATH = "etc/extension.properties";

    private Map<String, String> properties = new HashMap<>();
    private final Map<String, Document> storage = new ConcurrentHashMap<>();

    private static volatile MongoExtensionHandler instance;
    private static final Logger log = Logger.get(MongoExtensionHandler.class);
    private static final ObjectMapper jsonMapper = new ObjectMapper();
    private static boolean active = false;

    private static final String CREATE_DATABASE_SQL = """
            create database if not exists `trino_metadata`;
            """;

    private static final String CREATE_TABLE_SQL = """
            create table if not exists `trino_metadata`.`trino_table_metadata` (
                `app_id` varchar(255) not null,
                `database_name` varchar(255) not null,
                `table_name` varchar(255) not null,
                `database_type` varchar(255) not null,
                `metadata` longtext not null,
                primary key (`app_id`, `database_name`, `table_name`)
            ) engine=InnoDB default charset=utf8mb4;
            """;

    private static final String MAINTAIN_SQL = """
            insert into `trino_metadata`.`trino_table_metadata` (
                app_id, database_name, table_name, database_type, metadata
            ) values (
                '%s', '%s', '%s', '%s', '%s'
            ) on duplicate key update metadata = '%s';
            """;

    private static final String QUERY_SQL = """
            select
                metadata
            from
                `trino_metadata`.`trino_table_metadata`
            where
                app_id = '%s' and `database_name` = '%s' and `table_name` = '%s';
            """;

    private MongoExtensionHandler() {
        File file = new File(EXTENSION_CONFIG_FILE_PATH);
        try {
            this.properties = loadPropertiesFrom(file.getPath());
        } catch (IOException e) {
            log.error("Failed to load properties from " + EXTENSION_CONFIG_FILE_PATH, e);
            throw new RuntimeException(e);
        }

        active = Boolean.parseBoolean(properties.getOrDefault(EXTENSION_DB_ACTIVE, "false"));
        if (!active) {
            log.warn("Database management not activated");
            return;
        }

        Objects.requireNonNull(properties.getOrDefault(EXTENSION_DB_URL, "extension db url cannot be null"));
        Objects.requireNonNull(properties.getOrDefault(EXTENSION_DB_USER, "extension db user cannot be null"));
        Objects.requireNonNull(properties.getOrDefault(EXTENSION_DB_PASSWORD, "extension db password cannot be null"));
        Objects.requireNonNull(properties.getOrDefault(EXTENSION_DB_DRIVER, "extension db driver cannot be null"));

        try {
            Class.forName(properties.get(EXTENSION_DB_DRIVER));
        } catch (ClassNotFoundException e) {
            log.error("Failed to load extension db driver", e);
            throw new RuntimeException(e);
        }
    }

    public static MongoExtensionHandler getInstance() {
        if (instance == null) {
            synchronized (MongoExtensionHandler.class) {
                if (instance == null) {
                    instance = new MongoExtensionHandler();
                }
            }
        }
        return instance;
    }

    public void maintainTableMetadata(String appID, String database, String table, String databaseType, Document document) {
        if (!active) {
            // no need to maintain in database
            storage.put(buildKey(appID, database, table), document);
            return;
        }

        String md = null;
        try {
            md = jsonMapper.writeValueAsString(document);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize document", e);
            return;
        }

        Connection connection = null;
        try {
            String url = properties.get(EXTENSION_DB_URL);
            String user = properties.get(EXTENSION_DB_USER);
            String password = properties.get(EXTENSION_DB_PASSWORD);
            connection = DriverManager.getConnection(url, user, password);
            connection.setAutoCommit(false);
            Statement statement = connection.createStatement();
            statement.executeUpdate(CREATE_DATABASE_SQL);
            statement.executeUpdate(CREATE_TABLE_SQL);
            statement.executeUpdate(MAINTAIN_SQL.formatted(appID, database, table, databaseType, md, md));
            connection.commit();
        } catch (Exception e) {
            log.warn("Failed to maintain table metadata", e);
            if (connection != null) {
                try {
                    connection.rollback();
                } catch (Exception var1) {
                    log.error("Failed to rollback transaction", var1);
                }
            }
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (Exception var2) {
                    log.error("Failed to close connection", var2);
                }
            }
        }
    }

    public Document queryTableMetadata(String appID, String database, String table, String databaseType) {
        Optional<Document> cached = Optional.ofNullable(storage.get(buildKey(appID, database, table)));
        if (cached.isPresent() || active) {
            return cached.get();
        }

        String sql = QUERY_SQL.formatted(appID, database, table);
        Connection connection = null;
        try {
            String url = properties.get(EXTENSION_DB_URL);
            String user = properties.get(EXTENSION_DB_USER);
            String password = properties.get(EXTENSION_DB_PASSWORD);
            connection = DriverManager.getConnection(url, user, password);
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            if (resultSet.next()) {
                String md = resultSet.getString("metadata");
                TableMetadata tableMetadata = jsonMapper.readValue(md, TableMetadata.class);
                Document document = new Document("table", tableMetadata.getTable());
                ImmutableList.Builder<Document> builder = ImmutableList.builder();
                for (Field field : tableMetadata.getFields()) {
                    Document fieldDocument = new Document();
                    fieldDocument.append("name", field.getName());
                    fieldDocument.append("type", field.getType().replaceAll("@", "\""));
                    fieldDocument.append("hidden", field.isHidden());
                    builder.add(fieldDocument);
                }
                document.append("fields", builder.build());
                cacheMetadata(appID, database, table, document);
                return document;
            }
        } catch (Exception e) {
            log.error("Failed to query table metadata: " + e.getLocalizedMessage());
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (Exception var2) {
                    log.error("Failed to close connection", var2);
                }
            }
        }
        return null;
    }

    public String buildKey(String appID, String database, String table) {
        return appID + "-" + database + "-" + table;
    }

    public void cacheMetadata(String appID, String database, String table, Document document) {
        storage.put(buildKey(appID, database, table), document);
    }

    public ObjectMapper getJsonMapper() {
        return jsonMapper;
    }

    static class TableMetadata {

        private String table;
        private List<Field> fields;

        public TableMetadata(String table, List<Field> fields) {
            this.table = table;
            this.fields = fields;
        }

        public String getTable() {
            return table;
        }

        public List<Field> getFields() {
            return fields;
        }

        public void setFields(List<Field> fields) {
            this.fields = fields;
        }

        public void setTable(String table) {
            this.table = table;
        }
    }

    static class Field {
        private String name;
        private String type;
        private boolean hidden;

        public Field(String name, String type, boolean hidden) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public String getType() {
            return type;
        }

        public boolean isHidden() {
            return hidden;
        }

        public void setHidden(boolean hidden) {
            this.hidden = hidden;
        }

        public void setType(String type) {
            this.type = type;
        }

        public void setName(String name) {
            this.name = name;
        }
    }
}
