//
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.SqlTools.ServiceLayer.Connection.Contracts;
using Microsoft.SqlTools.ServiceLayer.Utility;

namespace Microsoft.SqlTools.ServiceLayer.Connection
{
    /// <summary>
    /// Information pertaining to a unique connection instance.
    /// </summary>
    public class ConnectionInfo
    {
        #region Member Variables

        private readonly ConcurrentDictionary<string, CancellationTokenSource> connectionTypeToCancellationSourceMap =
                    new ConcurrentDictionary<string, CancellationTokenSource>();

        private readonly object cancellationTokenSourceLock = new object();

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        public ConnectionInfo(ISqlConnectionFactory factory, string ownerUri, ConnectionDetails details)
        {
            ConnectionTypeToConnectionMap = new ConcurrentDictionary<string, DbConnection>();
            Factory = factory;
            OwnerUri = ownerUri;
            ConnectionDetails = details;
            ConnectionId = Guid.NewGuid();
            IntellisenseMetrics = new InteractionMetrics<double>(new[] {50, 100, 200, 500, 1000, 2000});
        }

        #region Properties

        /// <summary>
        /// Unique Id, helpful to identify a connection info object
        /// </summary>
        public Guid ConnectionId { get; private set; }

        /// <summary>
        /// URI identifying the owner/user of the connection. Could be a file, service, resource, etc.
        /// </summary>
        public string OwnerUri { get; private set; }

        /// <summary>
        /// Factory used for creating the SQL connection associated with the connection info.
        /// </summary>
        public ISqlConnectionFactory Factory { get; }

        /// <summary>
        /// Properties used for creating/opening the SQL connection.
        /// </summary>
        public ConnectionDetails ConnectionDetails { get; }

        /// <summary>
        /// Intellisense Metrics
        /// </summary>
        public InteractionMetrics<double> IntellisenseMetrics { get; private set; }

        /// <summary>
        /// Returns true is the db connection is to a SQL db
        /// </summary>
        public bool IsAzure { get; set; }
      
        /// <summary>
        /// Returns true if the sql connection is to a DW instance
        /// </summary>
        public bool IsSqlDW { get; set; }

        /// <summary>
        /// Returns the major version number of the db we are connected to 
        /// </summary>
        public int MajorVersion { get; set; }

        /// <summary>
        /// All DbConnection instances held by this ConnectionInfo
        /// </summary>
        public ICollection<DbConnection> AllConnections => ConnectionTypeToConnectionMap.Values;

        /// <summary>
        /// All connection type strings held by this ConnectionInfo
        /// </summary>
        public ICollection<string> AllConnectionTypes => ConnectionTypeToConnectionMap.Keys;

        /// <summary>
        /// The count of DbConnectioninstances held by this ConnectionInfo 
        /// </summary>
        public int CountConnections => ConnectionTypeToConnectionMap.Count;

        /// <summary>
        /// A map containing all connections to the database that are associated with 
        /// this ConnectionInfo's OwnerUri.
        /// This is internal for testing access only
        /// </summary>
        internal ConcurrentDictionary<string, DbConnection> ConnectionTypeToConnectionMap { get; set; }

        #endregion

        #region Public Methods

        /// <summary>
        /// Try to get the DbConnection associated with the given connection type string. 
        /// </summary>
        /// <returns>true if a connection with type connectionType was located and out connection was set, 
        /// false otherwise </returns>
        /// <exception cref="ArgumentException">Thrown when connectionType is null or empty</exception>
        public bool TryGetConnection(string connectionType, out DbConnection connection)
        {
            Validate.IsNotNullOrEmptyString(nameof(connectionType), connectionType);
            return ConnectionTypeToConnectionMap.TryGetValue(connectionType, out connection);
        }

        /// <summary>
        /// Adds a DbConnection to this object and associates it with the given 
        /// connection type string. If a connection already exists with an identical 
        /// connection type string, it is not overwritten. Ignores calls where connectionType = null
        /// </summary>
        /// <exception cref="ArgumentException">Thrown when connectionType is null or empty</exception>
        public async Task<DbConnection> TryOpenConnection(string connectionType)
        {
            Validate.IsNotNullOrWhitespaceString(nameof(connectionType), connectionType);

            // create a sql connection instance
            DbConnection connection = Factory.CreateSqlConnection(ConnectionDetails.GetConnectionString());
            ConnectionTypeToConnectionMap.TryAdd(connectionType, connection);

            CancellationTokenSource source = null;
            try
            {
                using (source = new CancellationTokenSource())
                {

                    // Locking here to perform two operations as one atomic operation
                    lock (cancellationTokenSourceLock)
                    {
                        // If the URI is currently connecting from a different request, cancel it before we try to connect
                        CancellationTokenSource currentSource;
                        if (connectionTypeToCancellationSourceMap.TryGetValue(connectionType, out currentSource))
                        {
                            currentSource.Cancel();
                        }
                        connectionTypeToCancellationSourceMap[connectionType] = source;
                    }

                    // Create a task to handle cancellation requests
                    var cancellationTask = Task.Run(() =>
                    {
                        source.Token.WaitHandle.WaitOne();
                        try
                        {
                            source.Token.ThrowIfCancellationRequested();
                        }
                        catch (ObjectDisposedException)
                        {
                            // Ignore
                        }
                    });
                    var openTask = connection.OpenAsync(source.Token);

                    // Open the connection
                    await Task.WhenAny(openTask, cancellationTask).Unwrap();
                    source.Cancel();
                }
            }
            finally
            {
                // Remove our cancellation token from the map since we're no longer connecting
                // Using a lock here to perform two operations as one atomic operation
                lock (cancellationTokenSourceLock)
                {
                    // Only remove the token from the map if it is the same one created by this request
                    CancellationTokenSource sourceValue;
                    if (connectionTypeToCancellationSourceMap.TryGetValue(connectionType, out sourceValue) && sourceValue == source)
                    {
                        connectionTypeToCancellationSourceMap.TryRemove(connectionType, out sourceValue);
                    }
                }
            }

            return connection;
        }

        /// <summary>
        /// Removes the single DbConnection instance associated with string connectionType
        /// </summary>
        /// <exception cref="ArgumentException">Thrown when connectionType is null or empty</exception>
        public void RemoveConnection(string connectionType)
        {
            Validate.IsNotNullOrEmptyString(nameof(connectionType), connectionType);

            // Cancel any current connection attempts for this connection
            CancellationTokenSource source;
            if (connectionTypeToCancellationSourceMap.TryGetValue(connectionType, out source))
            {
                try
                {
                    source.Cancel();
                }
                catch
                {
                    // Ignore
                }
            }

            // Remove the connection from the map
            DbConnection connection;
            ConnectionTypeToConnectionMap.TryRemove(connectionType, out connection);
        }

        /// <summary>
        /// Removes all DbConnection instances held by this object
        /// </summary>
        public void RemoveAllConnections()
        {
            foreach (var type in AllConnectionTypes)
            {
                DbConnection connection;
                ConnectionTypeToConnectionMap.TryRemove(type, out connection);
            }
        }

        #endregion
    }
}
