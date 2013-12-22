using System;

namespace Hadoop.Client.Tests
{
    public static class Connect
    {
        public static ConnectionConfig WithTestUser(string to)
        {
            return new ConnectionConfig(
                server: new Uri(to),
                userName: "hue",
                password: "");
        } 
    }
}