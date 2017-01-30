using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.SqlTools.ServiceLayer.Connection;
using Xunit;

namespace Microsoft.SqlTools.ServiceLayer.Test.Connection
{
    public class ConnectionInfoTests
    {
        [Fact]
        public void ConstructionTest()
        {
            // If: I create a new connection service with dummy properties
            ConnectionInfo ci = new ConnectionInfo(Common.NoOpFactory, Common.OwnerUri, Common.StandardConnectionDetails);
        }

    }
}
