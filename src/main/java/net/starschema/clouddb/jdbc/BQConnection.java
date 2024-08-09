/**
 * Copyright (c) 2015, STARSCHEMA LTD. All rights reserved.
 *
 * <p>Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met:
 *
 * <p>1. Redistributions of source code must retain the above copyright notice, this list of
 * conditions and the following disclaimer. 2. Redistributions in binary form must reproduce the
 * above copyright notice, this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * <p>THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
 * WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package net.starschema.clouddb.jdbc;

import static net.starschema.clouddb.jdbc.ConnectionUtils.parseArrayQueryParam;
import static net.starschema.clouddb.jdbc.ConnectionUtils.parseBooleanQueryParam;
import static net.starschema.clouddb.jdbc.ConnectionUtils.parseIntQueryParam;
import static net.starschema.clouddb.jdbc.ConnectionUtils.tryParseLabels;

import com.google.api.client.http.HttpTransport;
import com.google.api.services.bigquery.Bigquery;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.security.GeneralSecurityException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The connection class which builds the connection between BigQuery and the Driver
 *
 * @author Gunics Balázs, Horváth Attila
 */
public class BQConnection implements Connection {
  /** Variable to store auto commit mode */
  private boolean autoCommitEnabled = false;

  /** Instance log4j.Logger */
  Logger logger;

  /** The bigquery client to access the service. */
  private final Bigquery bigquery;

  private final String dataset;

  /** The ProjectId for the connection */
  private final String projectId;

  /** Boolean to determine if the Connection is closed */
  private boolean isclosed = false;

  private final Long maxBillingBytes;

  private final Integer timeoutMs;

  private final Map<String, String> labels;

  private final boolean useQueryCache;

  private final Set<BQStatementRoot> runningStatements =
      Collections.synchronizedSet(new HashSet<BQStatementRoot>());

  /** Boolean to determine whether or not to use legacy sql (default: false) * */
  private final boolean useLegacySql;

  /**
   * Enum that describes whether to create a job in projects that support stateless queries. Copied
   * from <a
   * href="https://github.com/googleapis/java-bigquery/blob/v2.34.0/google-cloud-bigquery/src/main/java/com/google/cloud/bigquery/QueryJobConfiguration.java#L98-L111">google-cloud-bigquery
   * 2.34.0</a>
   */
  public enum JobCreationMode {
    /** If unspecified JOB_CREATION_REQUIRED is the default. */
    JOB_CREATION_MODE_UNSPECIFIED,
    /** Default. Job creation is always required. */
    JOB_CREATION_REQUIRED,

    /**
     * Job creation is optional. Returning immediate results is prioritized. BigQuery will
     * automatically determine if a Job needs to be created. The conditions under which BigQuery can
     * decide to not create a Job are subject to change. If Job creation is required,
     * JOB_CREATION_REQUIRED mode should be used, which is the default.
     *
     * <p>Note that no job ID will be created if the results were returned immediately.
     */
    JOB_CREATION_OPTIONAL;

    JobCreationMode() {}
  }

  /** The job creation mode - */
  private final JobCreationMode jobCreationMode;

  /** List to contain sql warnings in */
  private final List<SQLWarning> SQLWarningList = new ArrayList<>();

  /** getter for useLegacySql */
  public boolean getUseLegacySql() {
    return useLegacySql;
  }

  /** String to contain the url except the url prefix */
  private String URLPART = null;

  /**
   * Extracts the JDBC URL then makes a connection to the Bigquery.
   *
   * @param url the JDBC connection URL
   * @param loginProp
   * @throws GeneralSecurityException
   * @throws IOException
   * @throws SQLException
   */
  public BQConnection(String url, Properties loginProp) throws SQLException {
    this(url, loginProp, Oauth2Bigquery.HTTP_TRANSPORT);
  }

  /**
   * Like {@link BQConnection(String,Properties)} but allows setting the {@link HttpTransport} for
   * testing.
   */
  public BQConnection(String url, Properties loginProp, HttpTransport httpTransport)
      throws SQLException {
    this.logger = LoggerFactory.getLogger(this.getClass());
    this.URLPART = url;
    this.isclosed = false;

    try {
      Pattern pathParamsMatcher =
          Pattern.compile("^jdbc:BQDriver::?([^?]*)", Pattern.CASE_INSENSITIVE);
      Matcher pathParamsMatchData = pathParamsMatcher.matcher(URLDecoder.decode(url, "UTF-8"));
      String pathParams;
      if (pathParamsMatchData.find()) {
        pathParams = pathParamsMatchData.group(1);
      } else {
        pathParams =
            URLDecoder.decode(url.substring(url.lastIndexOf(":") + 1, url.indexOf('?')), "UTF-8");
      }

      Pattern projectAndDatasetMatcher = Pattern.compile("^([^/$]+)(?:/([^$]*))?$");

      Matcher matchData = projectAndDatasetMatcher.matcher(pathParams);

      if (matchData.find()) {
        this.projectId = CatalogName.toProjectId(matchData.group(1));
        this.dataset = matchData.group(2);
      } else {
        this.projectId = CatalogName.toProjectId(pathParams);
      }
    } catch (UnsupportedEncodingException e1) {
      throw new BQSQLException(e1);
    }

    Properties caseInsensitiveLoginProps = new Properties();

    if (loginProp != null) {
      for (Object p : loginProp.keySet()) {
        String prop = (String) p;
        caseInsensitiveLoginProps.setProperty(prop.toLowerCase(), loginProp.getProperty(prop));
      }
    }

    Properties caseInsensitiveProps;

    try {
      // parse the connection string and override anything passed via loginProps.
      caseInsensitiveProps = BQSupportFuncts.getUrlQueryComponents(url, caseInsensitiveLoginProps);
    } catch (UnsupportedEncodingException e2) {
      throw new BQSQLException(e2);
    }

    String userId = caseInsensitiveProps.getProperty("user");
    String userKey = caseInsensitiveProps.getProperty("password");
    String userPath = caseInsensitiveProps.getProperty("path");

    // extract a list of "delegate" service accounts leading to a "target" service account to
    // use
    // for impersonation. if only a single account is provided, then it will be used as the
    // "target"
    List<String> targetServiceAccounts =
        parseArrayQueryParam(caseInsensitiveProps.getProperty("targetserviceaccount"), ',');

    // extract OAuth access token
    String oAuthAccessToken = caseInsensitiveProps.getProperty("oauthaccesstoken");

    // extract withServiceAccount property
    boolean serviceAccount =
        parseBooleanQueryParam(caseInsensitiveProps.getProperty("withserviceaccount"), false);

    // extract withApplicationDefaultCredentials
    boolean applicationDefaultCredentials =
        parseBooleanQueryParam(
            caseInsensitiveProps.getProperty("withapplicationdefaultcredentials"), false);

    // extract useLegacySql property
    this.useLegacySql =
        parseBooleanQueryParam(caseInsensitiveProps.getProperty("uselegacysql"), false);

    String jsonAuthContents = caseInsensitiveProps.getProperty("jsonauthcontents");

    // extract timeoutMs property
    this.timeoutMs = parseIntQueryParam("timeoutMs", caseInsensitiveProps.getProperty("timeoutms"));

    // extract readTimeout property
    Integer readTimeout =
        parseIntQueryParam("readTimeout", caseInsensitiveProps.getProperty("readtimeout"));

    // extract connectTimeout property
    Integer connectTimeout =
        parseIntQueryParam("connectTimeout", caseInsensitiveProps.getProperty("connecttimeout"));

    String maxBillingBytesParam = caseInsensitiveProps.getProperty("maxbillingbytes");
    if (maxBillingBytesParam != null) {
      try {
        this.maxBillingBytes = Long.parseLong(maxBillingBytesParam);
      } catch (NumberFormatException e) {
        throw new BQSQLException("Bad number for maxBillingBytes", e);
      }
    } else {
      this.maxBillingBytes = null;
    }

    // extract UA String
    String userAgent = caseInsensitiveProps.getProperty("useragent");

    // extract any labels
    this.labels = tryParseLabels(caseInsensitiveProps.getProperty("labels"));
    // extract custom endpoint for connections through restricted VPC
    String rootUrl = caseInsensitiveProps.getProperty("rooturl");
    // see if the user wants to use or bypass BigQuery's cache
    // by default, the cache is enabled
    this.useQueryCache =
        parseBooleanQueryParam(caseInsensitiveProps.getProperty("querycache"), true);

    final String jobCreationModeString = caseInsensitiveProps.getProperty("jobcreationmode");
    if (jobCreationModeString == null) {
      jobCreationMode = null;
    } else {
      try {
        jobCreationMode = JobCreationMode.valueOf(jobCreationModeString);
      } catch (IllegalArgumentException e) {
        throw new BQSQLException(
            "could not parse " + jobCreationModeString + " as job creation mode", e);
      }
    }

    // Create Connection to BigQuery
    if (serviceAccount) {
      try {
        // Support for old behavior, passing no actual password, but passing the path as
        // 'password'
        if (userPath == null) {
          userPath = userKey;
          userKey = null;
        }
        this.bigquery =
            Oauth2Bigquery.authorizeViaService(
                userId,
                userPath,
                userKey,
                userAgent,
                jsonAuthContents,
                readTimeout,
                connectTimeout,
                rootUrl,
                httpTransport,
                targetServiceAccounts,
                this.getProjectId());
        this.logger.info("Authorized with service account");
      } catch (GeneralSecurityException | IOException e) {
        throw new BQSQLException(e);
      }
    } else if (oAuthAccessToken != null) {
      try {
        this.bigquery =
            Oauth2Bigquery.authorizeViaToken(
                oAuthAccessToken,
                userAgent,
                connectTimeout,
                readTimeout,
                rootUrl,
                httpTransport,
                targetServiceAccounts,
                this.getProjectId());
        this.logger.info("Authorized with OAuth access token");
      } catch (SQLException e) {
        throw new BQSQLException(e);
      }
    } else if (applicationDefaultCredentials) {
      try {
        this.bigquery =
            Oauth2Bigquery.authorizeViaApplicationDefault(
                userAgent,
                connectTimeout,
                readTimeout,
                rootUrl,
                httpTransport,
                targetServiceAccounts,
                this.getProjectId());
      } catch (IOException e) {
        throw new BQSQLException(e);
      }
    } else {
      throw new IllegalArgumentException("Must provide a valid mechanism to authenticate.");
    }
    logger.debug("The project id for this connections is: " + projectId);
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Uses SQLWarningList.clear() to clear all warnings
   */
  @Override
  public void clearWarnings() throws SQLException {
    if (this.isclosed) {
      throw new BQSQLException("Connection is closed.");
    }
    this.SQLWarningList.clear();
  }

  /**
   * Returns a series of labels to add to every query. <a
   * href="https://cloud.google.com/bigquery/docs/adding-labels#job-label">...</a>
   *
   * <p>A label that has a key with an empty value is used as a tag. <a
   * href="https://cloud.google.com/bigquery/docs/adding-labels#adding_a_tag">...</a>
   */
  public Map<String, String> getLabels() {
    return this.labels;
  }

  /**
   * Return whether or not to use BigQuery's cache, as determined by the {@code cache} JDBC
   * parameter. Note that use of the cache is enabled by default.
   */
  public boolean getUseQueryCache() {
    return this.useQueryCache;
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Sets bigquery to null and isclosed to true if the connection is not already closed else no
   * operation is performed
   */
  @Override
  public void close() throws SQLException {
    if (!this.isclosed) {
      this.cancelRunningQueries();
      this.isclosed = true;
    }
  }

  public String getDataSet() {
    return this.dataset;
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws Exception
   *
   * @throws SQLException
   *     <p>There is no Commit in Google BigQuery + Connection Status
   */
  @Override
  public void commit() throws SQLException {
    if (this.isclosed) {
      throw new BQSQLException("There's no commit in Google BigQuery.\nConnection Status: Closed.");
    }
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not implemented yet.
   */
  @Override
  public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    throw new BQSQLException(
        "Not implemented." + "createArrayOf(String typeName, Object[] elements)");
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not implemented yet.
   *
   * @throws BQSQLException
   */
  @Override
  public Blob createBlob() throws SQLException {
    throw new BQSQLException("Not implemented." + "createBlob()");
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not implemented yet.
   *
   * @throws BQSQLException
   */
  @Override
  public Clob createClob() throws SQLException {
    throw new BQSQLException("Not implemented." + "createClob()");
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Creates a new BQStatement object with the projectid in this Connection
   *
   * @return a new BQStatement object with the projectid in this Connection
   * @throws SQLException if the Connection is closed
   */
  @Override
  public Statement createStatement() throws SQLException {
    if (this.isclosed) {
      throw new BQSQLException("Connection is closed.");
    }
    logger.debug(
        "Creating statement with resultsettype: forward only," + " concurrency: read only");
    return new BQStatement(projectId, this);
  }

  /** {@inheritDoc} */
  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency)
      throws SQLException {
    if (this.isClosed()) {
      throw new BQSQLException("The Connection is Closed");
    }
    logger.debug(
        "Creating statement with resultsettype: "
            + resultSetType
            + " concurrency: "
            + resultSetConcurrency);
    return new BQStatement(projectId, this, resultSetType, resultSetConcurrency);
  }

  /** {@inheritDoc} */
  @Override
  public Statement createStatement(
      int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    throw new BQSQLException("Not implemented." + "createStaement(int,int,int)");
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not implemented yet.
   *
   * @throws BQSQLException
   */
  @Override
  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    throw new BQSQLException("Not implemented." + "createStruct(string,object[])");
  }

  @Override
  public void setSchema(String schema) {
    logger.info("Ignoring schema setting: {}", schema);
  }

  @Override
  public String getSchema() {
    return getDataSet();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Getter for autoCommitEnabled
   *
   * @return auto commit state;
   */
  @Override
  public boolean getAutoCommit() throws SQLException {
    return this.autoCommitEnabled;
  }

  /** Getter method for the authorized bigquery client */
  public Bigquery getBigquery() {
    return this.bigquery;
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Return projectid
   *
   * @return projectid Contained in this Connection instance
   */
  @Override
  public String getCatalog() throws SQLException {
    logger.debug("function call getCatalog returning projectId: " + projectId);
    return projectId;
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not implemented yet.
   *
   * @throws BQSQLException
   */
  @Override
  public Properties getClientInfo() throws SQLException {
    throw new BQSQLException("Not implemented." + "getClientInfo()");
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not implemented yet.
   *
   * @throws BQSQLException
   */
  @Override
  public String getClientInfo(String name) throws SQLException {
    throw new BQSQLException("Not implemented." + "");
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * There's no commit.
   *
   * @return CLOSE_CURSORS_AT_COMMIT
   */
  @Override
  public int getHoldability() throws SQLException {
    return ResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Return a new BQDatabaseMetadata object constructed from this Connection instance
   *
   * @return a new BQDatabaseMetadata object constructed from this Connection instance
   */
  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    BQDatabaseMetadata metadata = new BQDatabaseMetadata(this);
    return metadata;
  }

  /** Getter method for projectId */
  public String getProjectId() {
    return projectId;
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Transactions are not supported.
   *
   * @return TRANSACTION_NONE
   */
  @Override
  public int getTransactionIsolation() {
    return java.sql.Connection.TRANSACTION_NONE;
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not implemented yet.
   *
   * @throws BQSQLException
   */
  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    throw new BQSQLException("Not implemented." + "getTypeMap()");
  }

  /**
   * @return The URL which is in the JDBC drivers connection URL
   */
  public String getURLPart() {
    return this.URLPART;
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * If SQLWarningList is empty returns null else it returns the first item Contained inside <br>
   * Subsequent warnings will be chained to this SQLWarning.
   *
   * @return SQLWarning (The First item Contained in SQLWarningList) + all others chained to it
   */
  @Override
  public SQLWarning getWarnings() throws SQLException {
    if (this.isclosed) {
      throw new BQSQLException("Connection is closed.");
    }
    if (this.SQLWarningList.isEmpty()) {
      return null;
    }

    SQLWarning forreturn = this.SQLWarningList.get(0);
    this.SQLWarningList.remove(0);
    if (!this.SQLWarningList.isEmpty()) {
      for (SQLWarning warning : this.SQLWarningList) {
        forreturn.setNextWarning(warning);
      }
    }
    return forreturn;
  }

  /** {@inheritDoc} */
  @Override
  public boolean isClosed() throws SQLException {
    return this.isclosed;
  }

  /** {@inheritDoc} */
  @Override
  public boolean isReadOnly() {
    return true;
  }

  /** {@inheritDoc} */
  @Override
  public boolean isValid(int timeout) throws SQLException {
    if (this.isclosed) {
      return false;
    }
    if (timeout < 0) {
      throw new BQSQLException(
          "Timeout value can't be negative. ie. it must be 0 or above; timeout value is: "
              + timeout);
    }
    try {
      this.bigquery.datasets().list(projectId).execute();
    } catch (IOException e) {
      return false;
    }
    return true;
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Returns false to everything
   *
   * @return false
   */
  @Override
  public boolean isWrapperFor(Class<?> arg0) throws SQLException {
    // TODO Implement
    return false;
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Creates and returns a PreparedStatement object
   *
   * @throws BQSQLException
   */
  @Override
  public PreparedStatement prepareStatement(String sql) {
    this.logger.debug("Creating Prepared Statement project id is: {} with parameters:", projectId);
    this.logger.debug(sql);
    return new BQPreparedStatement(sql, projectId, this);
  }

  /** {@inheritDoc} */
  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    this.logger.debug(
        "Creating Prepared Statement project id is: {}, resultSetType (int) is: {}, resultSetConcurrency (int) is: {} with parameters:",
        projectId,
        resultSetType,
        resultSetConcurrency);
    this.logger.debug(sql);
    return new BQPreparedStatement(sql, projectId, this, resultSetType, resultSetConcurrency);
  }

  public void addRunningStatement(BQStatementRoot stmt) {
    this.runningStatements.add(stmt);
  }

  public void removeRunningStatement(BQStatementRoot stmt) {
    this.runningStatements.remove(stmt);
  }

  public int getNumberRunningQueries() {
    return this.runningStatements.size();
  }

  public int cancelRunningQueries() {
    int numFailed = 0;
    synchronized (this.runningStatements) {
      for (BQStatementRoot stmt : this.runningStatements) {
        try {
          stmt.cancel();
        } catch (SQLException e) {
          numFailed++;
        }
      }
    }
    return numFailed;
  }

  public Long getMaxBillingBytes() {
    return maxBillingBytes;
  }

  public Integer getTimeoutMs() {
    return timeoutMs;
  }

  public JobCreationMode getJobCreationMode() {
    return jobCreationMode;
  }

  /** BiqQuery does not support this well if at all. */
  @Override
  public void rollback() {
    logger.debug("function call: rollback() not implemented ");
  }

  @Override
  public void setAutoCommit(boolean autoCommit) {
    this.autoCommitEnabled = autoCommit;
  }

  @Override
  public void setReadOnly(boolean readOnly) {
    throw new UnsupportedOperationException();
  }

  // =========================================================================
  // Not Implemented

  public void abort(Executor executor) throws SQLException {
    throw new UnsupportedOperationException("Not implemented.");
  }

  public void setNetworkTimeout(Executor executor, int milliseconds) {
    throw new UnsupportedOperationException("Not implemented.");
  }

  public int getNetworkTimeout() {
    throw new UnsupportedOperationException("Not implemented.");
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) {
    throw new UnsupportedOperationException("Not implemented." + "prepareStatement(string,int)");
  }

  @Override
  public PreparedStatement prepareStatement(
      String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) {
    throw new UnsupportedOperationException(
        "Not implemented." + "prepareStatement(String,int,int,int)");
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) {
    throw new UnsupportedOperationException("Not implemented." + "prepareStatement(String,int[])");
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) {
    throw new UnsupportedOperationException(
        "Not implemented." + "prepareStatement(String,String[])");
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) {
    throw new UnsupportedOperationException("Not implemented." + "releaseSavepoint(Savepoint)");
  }

  @Override
  public void rollback(Savepoint savepoint) {
    throw new UnsupportedOperationException("Not implemented." + "rollback(savepoint)");
  }

  @Override
  public void setCatalog(String catalog) {
    throw new UnsupportedOperationException("Not implemented." + "setCatalog(catalog)");
  }

  @Override
  public void setClientInfo(Properties properties) {
    throw new UnsupportedOperationException("Not implemented." + "setClientInfo(properties)");
  }

  @Override
  public void setClientInfo(String name, String value) {
    throw new UnsupportedOperationException("Not implemented. setClientInfo(properties)");
  }

  @Override
  public void setHoldability(int holdability) {
    throw new UnsupportedOperationException("Not implemented." + "setHoldability(int)");
  }

  @Override
  public Savepoint setSavepoint() {
    throw new UnsupportedOperationException("Not implemented." + "setSavepoint()");
  }

  @Override
  public Savepoint setSavepoint(String name) {
    throw new UnsupportedOperationException("Not implemented." + "setSavepoint(String)");
  }

  @Override
  public void setTransactionIsolation(int level) {
    throw new UnsupportedOperationException("Not implemented." + "setTransactionIsolation(int)");
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) {
    throw new UnsupportedOperationException(
        "Not implemented." + "setTypeMap(Map<String, Class<?>>");
  }

  @Override
  public <T> T unwrap(Class<T> arg0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String nativeSQL(String sql) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CallableStatement prepareCall(
      String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) {
    throw new UnsupportedOperationException("Not implemented." + "prepareCall(string,int,int,int)");
  }

  @Override
  public CallableStatement prepareCall(String sql) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) {
    throw new UnsupportedOperationException();
  }

  @Override
  public NClob createNClob() {
    throw new UnsupportedOperationException("Not implemented." + "createNClob()");
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    throw new UnsupportedOperationException("Not implemented." + "createSQLXML()");
  }
}
