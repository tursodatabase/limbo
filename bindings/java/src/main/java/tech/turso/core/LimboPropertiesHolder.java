package tech.turso.core;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import tech.turso.jdbc4.JDBC4DatabaseMetaData;
import tech.turso.utils.Logger;
import tech.turso.utils.LoggerFactory;

public class LimboPropertiesHolder {
  private static final Logger logger = LoggerFactory.getLogger(LimboPropertiesHolder.class);

  private static String driverName = "";
  private static String driverVersion = "";

  static {
    try (InputStream limboJdbcPropStream =
        JDBC4DatabaseMetaData.class.getClassLoader().getResourceAsStream("limbo-jdbc.properties")) {
      if (limboJdbcPropStream == null) {
        throw new IOException("Cannot load limbo-jdbc.properties from jar");
      }

      final Properties properties = new Properties();
      properties.load(limboJdbcPropStream);
      driverName = properties.getProperty("driverName");
      driverVersion = properties.getProperty("driverVersion");
    } catch (IOException e) {
      logger.error("Failed to load driverName and driverVersion");
    }
  }

  public static String getDriverName() {
    return driverName;
  }

  public static String getDriverVersion() {
    return driverVersion;
  }
}
