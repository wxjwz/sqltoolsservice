using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.SqlTools.ServiceLayer.Connection;
using Microsoft.SqlTools.ServiceLayer.Connection.Contracts;
using Moq;

namespace Microsoft.SqlTools.ServiceLayer.Test.Connection
{
    public class Common
    {

        public const string OwnerUri = "testFile";

        #region Properties

        public static ISqlConnectionFactory NoOpFactory
        {
            get
            {
                var mockFactory = new Mock<ISqlConnectionFactory>();
                mockFactory.Setup(factory => factory.CreateSqlConnection(It.IsAny<string>()));

                return mockFactory.Object;
            }
        }

        public static ConnectionDetails StandardConnectionDetails => new ConnectionDetails
        {
            DatabaseName = "123",
            Password = "456",
            ServerName = "789",
            UserName = "012"
        };

        #endregion

    }
}
