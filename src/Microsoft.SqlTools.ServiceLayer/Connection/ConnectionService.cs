//
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.SqlTools.ServiceLayer.Connection.Contracts;
using Microsoft.SqlTools.ServiceLayer.Connection.ReliableConnection;
using Microsoft.SqlTools.ServiceLayer.Hosting.Protocol;
using Microsoft.SqlTools.ServiceLayer.LanguageServices.Contracts;
using Microsoft.SqlTools.ServiceLayer.SqlContext;
using Microsoft.SqlTools.ServiceLayer.Utility;
using Microsoft.SqlTools.ServiceLayer.Workspace;
using Microsoft.SqlServer.Management.Common;

namespace Microsoft.SqlTools.ServiceLayer.Connection
{
    /// <summary>
    /// Main class for the Connection Management services
    /// </summary>
    public class ConnectionService
    {
        #region Singleton Implementation

        /// <summary>
        /// Singleton service instance
        /// </summary>
        private static readonly Lazy<ConnectionService> InstanceLazy =
            new Lazy<ConnectionService>(() => new ConnectionService());

        /// <summary>
        /// Gets the singleton service instance
        /// </summary>
        public static ConnectionService Instance => InstanceLazy.Value;

        /// <summary>
        /// Default constructor should be private since it's a singleton class, but we need a constructor
        /// for use in unit test mocking.
        /// </summary>
        internal ConnectionService()
        {
        }

        /// <summary>
        /// Test constructor that injects dependency interfaces
        /// </summary>
        /// <param name="testFactory"></param>
        public ConnectionService(ISqlConnectionFactory testFactory)
        {
            connectionFactory = testFactory;
        }

        #endregion

        #region Member Variables

        /// <summary>
        /// The SQL connection factory object
        /// </summary>
        private ISqlConnectionFactory connectionFactory;

        #endregion

        #region Properties

        /// <summary>
        /// Map from script URIs to ConnectionInfo objects
        /// This is internal for testing access only
        /// </summary>
        internal Dictionary<string, ConnectionInfo> OwnerToConnectionMap { get; } = new Dictionary<string, ConnectionInfo>();

        /// <summary>
        /// Service host object for sending/receiving requests/events.
        /// Internal for testing purposes.
        /// </summary>
        internal IProtocolEndpoint ServiceHost
        {
            get;
            set;
        }

        /// <summary>
        /// Gets the SQL connection factory instance
        /// </summary>
        public ISqlConnectionFactory ConnectionFactory
        {
            get { return connectionFactory ?? (connectionFactory = new SqlConnectionFactory()); }
            internal set { connectionFactory = value; }
        }
        
        #endregion

        #region Events

        /// <summary>
        /// Callback for onconnection handler
        /// </summary>
        /// <param name="info">The connection that was made</param>
        public delegate Task OnConnectionHandler(ConnectionInfo info);

        /// <summary>
        /// Callback for ondisconnect handler
        /// </summary>
        public delegate Task OnDisconnectHandler(ConnectionSummary summary, string ownerUri);

        public event OnConnectionHandler OnConnection;

        public event OnDisconnectHandler OnDisconnect;

        #endregion

        public void InitializeService(IProtocolEndpoint serviceHost)
        {
            ServiceHost = serviceHost;

            // Register request and event handlers with the Service Host
            serviceHost.SetRequestHandler(ConnectionRequest.Type, HandleConnectRequest);
            serviceHost.SetRequestHandler(CancelConnectRequest.Type, HandleCancelConnectRequest);
            serviceHost.SetRequestHandler(DisconnectRequest.Type, HandleDisconnectRequest);
            serviceHost.SetRequestHandler(ListDatabasesRequest.Type, HandleListDatabasesRequest);

            // Register the configuration update handler
            WorkspaceService<SqlToolsSettings>.Instance.RegisterConfigChangeCallback((a, b, c) => Task.FromResult(0));
        }

        #region Request Handlers

        /// <summary>
        /// Handle new connection requests
        /// </summary>
        /// <param name="connectParams"></param>
        /// <param name="requestContext"></param>
        /// <returns></returns>
        protected async Task HandleConnectRequest(ConnectParams connectParams, RequestContext<bool> requestContext)
        {
            Logger.Write(LogLevel.Verbose, "HandleConnectRequest");

            try
            {
                RunConnectRequestHandlerTask(connectParams);
                await requestContext.SendResult(true);
            }
            catch
            {
                await requestContext.SendResult(false);
            }
        }

        /// <summary>
        /// Handle cancel connect requests
        /// </summary>
        protected async Task HandleCancelConnectRequest(CancelConnectParams cancelParams, RequestContext<bool> requestContext)
        {
            Logger.Write(LogLevel.Verbose, "HandleCancelConnectRequest");

            try
            {
                bool result = Instance.CancelConnect(cancelParams);
                await requestContext.SendResult(result);
            }
            catch (Exception ex)
            {
                await requestContext.SendError(ex.ToString());
            }
        }

        /// <summary>
        /// Handle disconnect requests
        /// </summary>
        protected async Task HandleDisconnectRequest(DisconnectParams disconnectParams, RequestContext<bool> requestContext)
        {
            Logger.Write(LogLevel.Verbose, "HandleDisconnectRequest");

            try
            {
                bool result = Instance.Disconnect(disconnectParams);
                await requestContext.SendResult(result);
            }
            catch (Exception ex)
            {
                await requestContext.SendError(ex.ToString());
            }

        }

        /// <summary>
        /// Handle requests to list databases on the current server
        /// </summary>
        protected async Task HandleListDatabasesRequest(ListDatabasesParams listDatabasesParams, RequestContext<ListDatabasesResponse> requestContext)
        {
            Logger.Write(LogLevel.Verbose, "ListDatabasesRequest");

            try
            {
                ListDatabasesResponse result = Instance.ListDatabases(listDatabasesParams);
                await requestContext.SendResult(result);
            }
            catch (Exception ex)
            {
                await requestContext.SendError(ex.ToString());
            }
        }

        #endregion

        #region Inter-Service API Methods

        // Attempts to link a URI to an actively used connection for this URI
        public virtual bool TryFindConnection(string ownerUri, out ConnectionInfo connectionInfo)
        {
            return OwnerToConnectionMap.TryGetValue(ownerUri, out connectionInfo);
        }

        /// <summary>
        /// Open a connection with the specified ConnectParams
        /// </summary>
        public async Task<ConnectionCompleteParams> Connect(ConnectParams connectionParams)
        {
            // Validate parameters
            ConnectionCompleteParams validationResults = ValidateConnectParams(connectionParams);
            if (validationResults != null)
            {
                return validationResults;
            }

            // If there is no ConnectionInfo in the map, create a new ConnectionInfo, 
            // but wait until later when we are connected to add it to the map.
            ConnectionInfo connectionInfo;
            if (!OwnerToConnectionMap.TryGetValue(connectionParams.OwnerUri, out connectionInfo))
            {
                connectionInfo = new ConnectionInfo(ConnectionFactory, connectionParams.OwnerUri, connectionParams.Connection);
            }

            // Resolve if it is an existing connection
            // Disconnect active connection if the URI is already connected for this connection type
            DbConnection existingConnection;
            if (connectionInfo.TryGetConnection(connectionParams.Type, out existingConnection))
            {
                var disconnectParams = new DisconnectParams
                {
                    OwnerUri = connectionParams.OwnerUri,
                    Type = connectionParams.Type
                };
                Disconnect(disconnectParams);
            }

            // Try to open a connection with the given ConnectParams
            ConnectionCompleteParams response = await TryOpenConnection(connectionInfo, connectionParams);
            if (response != null)
            {
                return response;
            }

            // If this is the first connection for this URI, add the ConnectionInfo to the map
            if (!OwnerToConnectionMap.ContainsKey(connectionParams.OwnerUri))
            {
                OwnerToConnectionMap[connectionParams.OwnerUri] = connectionInfo;
            }

            // Invoke callback notifications          
            if (OnConnection != null)
            {
                await OnConnection(connectionInfo);
            }

            // Return information about the connected SQL Server instance
            return GetConnectionCompleteParams(connectionParams.Type, connectionInfo);
        }

        /// <summary>
        /// Cancel a connection that is in the process of opening.
        /// </summary>
        public bool CancelConnect(CancelConnectParams cancelParams)
        {
            Validate.IsNotNullOrWhitespaceString(nameof(cancelParams.OwnerUri), cancelParams.OwnerUri);

            // Attempt to remove the connection from the connection info
            ConnectionInfo connectionInfo;
            if (!OwnerToConnectionMap.TryGetValue(cancelParams.OwnerUri, out connectionInfo))
            {
                return false;
            }
            
            connectionInfo.RemoveConnection(cancelParams.Type);
            return true;
        }

        /// <summary>
        /// Change the database context of a connection.
        /// </summary>
        /// <param name="ownerUri">URI of the owner of the connection</param>
        /// <param name="newDatabaseName">Name of the database to change the connection to</param>
        public void ChangeConnectionDatabaseContext(string ownerUri, string newDatabaseName)
        {
            ConnectionInfo info;
            if (TryFindConnection(ownerUri, out info))
            {
                try
                {
                    foreach (DbConnection connection in info.AllConnections)
                    {
                        if (connection.State == ConnectionState.Open)
                        {
                            connection.ChangeDatabase(newDatabaseName);
                        }
                    }

                    info.ConnectionDetails.DatabaseName = newDatabaseName;

                    // Fire a connection changed event
                    ConnectionChangedParams parameters = new ConnectionChangedParams();
                    ConnectionSummary summary = info.ConnectionDetails;
                    parameters.Connection = summary.Clone();
                    parameters.OwnerUri = ownerUri;
                    ServiceHost.SendEvent(ConnectionChangedNotification.Type, parameters);
                }
                catch (Exception e)
                {
                    Logger.Write(
                        LogLevel.Error,
                        string.Format(
                            "Exception caught while trying to change database context to [{0}] for OwnerUri [{1}]. Exception:{2}",
                            newDatabaseName,
                            ownerUri,
                            e.ToString())
                    );
                }
            }
        }

        /// <summary>
        /// List all databases on the server specified
        /// </summary>
        public ListDatabasesResponse ListDatabases(ListDatabasesParams listDatabasesParams)
        {
            // Verify parameters
            var owner = listDatabasesParams.OwnerUri;
            if (string.IsNullOrEmpty(owner))
            {
                throw new ArgumentException(SR.ConnectionServiceListDbErrorNullOwnerUri);
            }

            // Use the existing connection as a base for the search
            ConnectionInfo info;
            if (!TryFindConnection(owner, out info))
            {
                throw new Exception(SR.ConnectionServiceListDbErrorNotConnected(owner));
            }
            ConnectionDetails connectionDetails = info.ConnectionDetails.Clone();

            // Connect to master and query sys.databases
            connectionDetails.DatabaseName = "master";
            var connection = ConnectionFactory.CreateSqlConnection(connectionDetails.GetConnectionString());
            connection.Open();

            List<string> results = new List<string>();
            var systemDatabases = new[] { "master", "model", "msdb", "tempdb" };
            using (DbCommand command = connection.CreateCommand())
            {
                command.CommandText = "SELECT name FROM sys.databases ORDER BY name ASC";
                command.CommandTimeout = 15;
                command.CommandType = CommandType.Text;

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        results.Add(reader[0].ToString());
                    }
                }
            }

            // Put system databases at the top of the list
            results =
                results.Where(s => systemDatabases.Any(s.Equals)).Concat(
                results.Where(s => systemDatabases.All(x => !s.Equals(x)))).ToList();

            connection.Close();

            ListDatabasesResponse response = new ListDatabasesResponse {DatabaseNames = results.ToArray()};

            return response;
        }

        #endregion

        #region Private Helpers

        /// <summary>
        /// Validates the given ConnectParams object. 
        /// </summary>
        /// <param name="connectionParams">The params to validate</param>
        /// <returns>A ConnectionCompleteParams object upon validation error, 
        /// null upon validation success</returns>
        private static ConnectionCompleteParams ValidateConnectParams(ConnectParams connectionParams)
        {
            string paramValidationErrorMessage;
            if (connectionParams == null)
            {
                return new ConnectionCompleteParams
                {
                    Messages = SR.ConnectionServiceConnectErrorNullParams
                };
            }
            if (!connectionParams.IsValid(out paramValidationErrorMessage))
            {
                return new ConnectionCompleteParams
                {
                    OwnerUri = connectionParams.OwnerUri,
                    Messages = paramValidationErrorMessage
                };
            }

            // return null upon success
            return null;
        }

        /// <summary>
        /// Creates a ConnectionCompleteParams as a response to a successful connection. 
        /// Also sets the DatabaseName and IsAzure properties of ConnectionInfo.
        /// </summary>
        /// <returns>A ConnectionCompleteParams in response to the successful connection</returns>
        private static ConnectionCompleteParams GetConnectionCompleteParams(string connectionType, ConnectionInfo connectionInfo)
        {
            ConnectionCompleteParams response = new ConnectionCompleteParams { OwnerUri = connectionInfo.OwnerUri, Type = connectionType };

            try
            {
                DbConnection connection;
                connectionInfo.TryGetConnection(connectionType, out connection);

                // Update with the actual database name in connectionInfo and result
                // Doing this here as we know the connection is open - expect to do this only on connecting
                connectionInfo.ConnectionDetails.DatabaseName = connection.Database;
                response.ConnectionSummary = new ConnectionSummary
                {
                    ServerName = connectionInfo.ConnectionDetails.ServerName,
                    DatabaseName = connectionInfo.ConnectionDetails.DatabaseName,
                    UserName = connectionInfo.ConnectionDetails.UserName,
                };

                response.ConnectionId = connectionInfo.ConnectionId.ToString();

                var reliableConnection = connection as ReliableSqlConnection;
                DbConnection underlyingConnection = reliableConnection != null
                    ? reliableConnection.GetUnderlyingConnection()
                    : connection;

                ReliableConnectionHelper.ServerInfo serverInfo = ReliableConnectionHelper.GetServerVersion(underlyingConnection);
                response.ServerInfo = new ServerInfo
                {
                    ServerMajorVersion = serverInfo.ServerMajorVersion,
                    ServerMinorVersion = serverInfo.ServerMinorVersion,
                    ServerReleaseVersion = serverInfo.ServerReleaseVersion,
                    EngineEditionId = serverInfo.EngineEditionId,
                    ServerVersion = serverInfo.ServerVersion,
                    ServerLevel = serverInfo.ServerLevel,
                    ServerEdition = serverInfo.ServerEdition,
                    IsCloud = serverInfo.IsCloud,
                    AzureVersion = serverInfo.AzureVersion,
                    OsVersion = serverInfo.OsVersion
                };
                connectionInfo.IsAzure = serverInfo.IsCloud;
                connectionInfo.MajorVersion = serverInfo.ServerMajorVersion;
                connectionInfo.IsSqlDW = (serverInfo.EngineEditionId == (int)DatabaseEngineEdition.SqlDataWarehouse);
            }
            catch (Exception ex)
            {
                response.Messages = ex.ToString();
            }

            return response;
        }

        /// <summary>
        /// Tries to create and open a connection with the given ConnectParams.
        /// </summary>
        /// <returns>null upon success, a ConnectionCompleteParams detailing the error upon failure</returns>
        private static async Task<ConnectionCompleteParams> TryOpenConnection(ConnectionInfo connectionInfo, ConnectParams connectionParams)
        {
            ConnectionCompleteParams response = new ConnectionCompleteParams { OwnerUri = connectionInfo.OwnerUri, Type = connectionParams.Type };

            try
            {
                await connectionInfo.TryOpenConnection(connectionParams.Type);
            }
            catch (SqlException ex)
            {
                response.ErrorNumber = ex.Number;
                response.ErrorMessage = ex.Message;
                response.Messages = ex.ToString();
                return response;
            }
            catch (OperationCanceledException)
            {
                // OpenAsync was cancelled
                response.Messages = SR.ConnectionServiceConnectionCanceled;
                return response;
            }
            catch (Exception ex)
            {
                response.ErrorMessage = ex.Message;
                response.Messages = ex.ToString();
                return response;
            }

            // Return null upon success
            return null;
        }

        /// <summary>
        /// Close a connection with the specified connection details.
        /// </summary>
        public bool Disconnect(DisconnectParams disconnectParams)
        {
            // Validate parameters
            if (string.IsNullOrEmpty(disconnectParams?.OwnerUri))
            {
                return false;
            }

            // Lookup the ConnectionInfo owned by the URI
            ConnectionInfo info;
            if (!OwnerToConnectionMap.TryGetValue(disconnectParams.OwnerUri, out info))
            {
                return false;
            }

            // Call Close() on the connections we want to disconnect
            // If no connections were located, return false
            if (!CloseConnections(info, disconnectParams.Type))
            {
                return false;
            }

            // Remove the disconnected connections from the ConnectionInfo map
            if (disconnectParams.Type == null)
            {
                info.RemoveAllConnections();
            }
            else
            {
                info.RemoveConnection(disconnectParams.Type);
            }

            // If the ConnectionInfo has no more connections, remove the ConnectionInfo
            if (info.CountConnections == 0)
            {
                OwnerToConnectionMap.Remove(disconnectParams.OwnerUri);
            }

            // Handle Telemetry disconnect events if we are disconnecting the default connection
            if (disconnectParams.Type == null || disconnectParams.Type == ConnectionType.Default)
            {
                HandleDisconnectTelemetry(info);
                OnDisconnect?.Invoke(info.ConnectionDetails, info.OwnerUri);
            }

            // Return true upon success
            return true;
        }

        /// <summary>
        /// Closes DbConnections associated with the given ConnectionInfo. 
        /// If connectionType is not null, closes the DbConnection with the type given by connectionType.
        /// If connectionType is null, closes all DbConnections.
        /// </summary>
        /// <returns>true if connections were found and attempted to be closed,
        /// false if no connections were found</returns>
        private static bool CloseConnections(ConnectionInfo connectionInfo, string connectionType)
        {
            ICollection<DbConnection> connectionsToDisconnect = new List<DbConnection>();
            if (connectionType == null)
            {
                connectionsToDisconnect = connectionInfo.AllConnections;
            }
            else
            {
                // Make sure there is an existing connection of this type
                DbConnection connection;
                if (!connectionInfo.TryGetConnection(connectionType, out connection))
                {
                    return false;
                }
                connectionsToDisconnect.Add(connection);
            }

            if (connectionsToDisconnect.Count == 0)
            {
                return false;
            }

            foreach (DbConnection connection in connectionsToDisconnect)
            {
                try
                {
                    connection.Close();
                }
                catch (Exception)
                {
                    // Ignore
                }
            }

            return true;
        }

        private void RunConnectRequestHandlerTask(ConnectParams connectParams)
        {
            // create a task to connect asynchronously so that other requests are not blocked in the meantime
            Task.Run(async () => 
            {
                try
                {
                    // result is null if the ConnectParams was successfully validated 
                    ConnectionCompleteParams result = ValidateConnectParams(connectParams);
                    if (result != null)
                    {
                        await ServiceHost.SendEvent(ConnectionCompleteNotification.Type, result);
                        return;
                    }

                    // open connection based on request details
                    result = await Instance.Connect(connectParams);
                    await ServiceHost.SendEvent(ConnectionCompleteNotification.Type, result);
                }
                catch (Exception ex)
                {
                    ConnectionCompleteParams result = new ConnectionCompleteParams()
                    {
                        Messages = ex.ToString()
                    };
                    await ServiceHost.SendEvent(ConnectionCompleteNotification.Type, result);
                }
            });
        }

        /// <summary>
        /// Handles the Telemetry events that occur upon disconnect.
        /// </summary>
        /// <param name="connectionInfo">The connection that was disconnected</param>
        private void HandleDisconnectTelemetry(ConnectionInfo connectionInfo)
        {
            if (ServiceHost != null)
            {
                try
                {
                    // Send a telemetry notification for intellisense performance metrics
                    ServiceHost.SendEvent(TelemetryNotification.Type, new TelemetryParams()
                    {
                        Params = new TelemetryProperties
                        {
                            Properties = new Dictionary<string, string>
                            {
                                {"IsAzure", connectionInfo.IsAzure ? "1" : "0"}
                            },
                            EventName = TelemetryEventNames.IntellisenseQuantile,
                            Measures = connectionInfo.IntellisenseMetrics.Quantile
                        }
                    });
                }
                catch (Exception ex)
                {
                    Logger.Write(LogLevel.Verbose, "Could not send Connection telemetry event " + ex);
                }
            }
        }

        #endregion
    }
}
