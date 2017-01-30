//
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
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

        private readonly ConcurrentDictionary<string, ConnectionWithCancel> connectionMap =
            new ConcurrentDictionary<string, ConnectionWithCancel>();

        /// <summary>
        /// Factory used for creating the SQL connection associated with the connection info.
        /// </summary>
        private readonly ISqlConnectionFactory factory;

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        public ConnectionInfo(ISqlConnectionFactory connectionFactory, string ownerUri, ConnectionDetails details)
        {
            Validate.IsNotNull(nameof(factory), factory);
            Validate.IsNotNullOrWhitespaceString(nameof(ownerUri), ownerUri);
            Validate.IsNotNull(nameof(details), details);

            factory = connectionFactory;
            OwnerUri = ownerUri;
            ConnectionDetails = details;
            IntellisenseMetrics = new InteractionMetrics<double>(new[] {50, 100, 200, 500, 1000, 2000});
        }

        #region Properties

        public IDictionary<string, DbConnection> Connections
        {
            get { return connectionMap.ToImmutableDictionary(kvp => kvp.Key, kvp => kvp.Value.Connection); }
        }

        /// <summary>
        /// URI identifying the owner/user of the connection. Could be a file, service, resource, etc.
        /// </summary>
        public string OwnerUri { get; private set; }

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

        #endregion

        #region Public Methods

        //public bool CancelConnection(string connectionType)
        //{
        //    Validate.IsNotNullOrWhitespaceString(nameof(connectionType), connectionType);

        //    // Attempt to find the connection for the owner uri
        //    ConnectionWithCancel connection;
        //    if (!connectionMap.TryGetValue(connectionType, out connection))
        //    {
        //        throw new ArgumentOutOfRangeException(nameof(connectionType), "Requested connection type is not in progress of connecting");
        //    }
        //    return ;
        //}

        public Task<DbConnection> GetOrOpenConnection(string connectionType)
        {
            // Attempt to get the connection first
            ConnectionWithCancel connection;
            if (connectionMap.TryGetValue(connectionType, out connection))
            {
                return Task.FromResult(connection.Connection);
            }

            // If that fails, open a new connection
            return OpenConnection(connectionType);
        }

        /// <summary>
        /// Adds a DbConnection to this object and associates it with the given 
        /// connection type string. If a connection already exists with an identical 
        /// connection type string, it is not overwritten. Ignores calls where connectionType = null
        /// </summary>
        /// <exception cref="ArgumentException">Thrown when connectionType is null or empty</exception>
        public async Task<DbConnection> OpenConnection(string connectionType)
        {
            Validate.IsNotNullOrWhitespaceString(nameof(connectionType), connectionType);

            // Create a new connection or cancel an existing connect attempt
            ConnectionWithCancel connection = connectionMap.AddOrUpdate(connectionType,
                ct => new ConnectionWithCancel
                {
                    Connection = factory.CreateSqlConnection(ConnectionDetails.GetConnectionString())
                }, (ct, cancel) =>
                {
                    // If cancellation fails (b/c it has already been cancelled), throw
                    if (!cancel.CancelSource.IsCancellationRequested)
                    {
                        throw new InvalidOperationException("Connection type is already connected");
                    }
                    cancel.CancelSource.Cancel();
                    return cancel;
                });

            // Open the connection async
            await connection.Connection.OpenAsync(connection.CancelSource.Token);
            connection.CancelSource.Cancel();
            return connection.Connection;
        }

        /// <summary>
        /// Try to get the DbConnection associated with the given connection type string. 
        /// </summary>
        /// <returns>true if a connection with type connectionType was located and out connection was set, 
        /// false otherwise </returns>
        /// <exception cref="ArgumentException">Thrown when connectionType is null or empty</exception>
        public bool TryGetConnection(string connectionType, out DbConnection connection)
        {
            Validate.IsNotNullOrEmptyString(nameof(connectionType), connectionType);

            ConnectionWithCancel conn;
            if (connectionMap.TryGetValue(connectionType, out conn))
            {
                connection = conn.Connection;
                return true;
            }
            connection = null;
            return false;
        }

        /// <summary>
        /// Removes the single DbConnection instance associated with string connectionType
        /// </summary>
        /// <exception cref="ArgumentException">Thrown when connectionType is null or empty</exception>
        public void RemoveConnection(string connectionType)
        {
            Validate.IsNotNullOrEmptyString(nameof(connectionType), connectionType);

            ConnectionWithCancel conn;
            if (connectionMap.TryRemove(connectionType, out conn))
            {
                if (!conn.CancelSource.IsCancellationRequested)
                {
                    conn.CancelSource.Cancel();
                }
            }
        }

        /// <summary>
        /// Removes all DbConnection instances held by this object
        /// </summary>
        public void RemoveAllConnections()
        {
            foreach (string connType in connectionMap.Keys)
            {
                RemoveConnection(connType);
            }
        }

        #endregion

        private class ConnectionWithCancel : IDisposable
        {
            public DbConnection Connection { get; set; }
            public CancellationTokenSource CancelSource { get; }

            public ConnectionWithCancel()
            {
                CancelSource = new CancellationTokenSource();
            }

            public void Dispose()
            {
                if (!CancelSource.IsCancellationRequested)
                {
                    CancelSource.Cancel();
                }
                CancelSource.Dispose();
            }
        }
    }
}
