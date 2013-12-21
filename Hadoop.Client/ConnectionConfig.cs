using System;

namespace Hadoop.Client
{    
    public class ConnectionConfig
    {
        public ConnectionConfig(Uri server, string userName, string password)
        {
            Server = server;
            UserName = userName;
            Password = password;
        }

        public Uri Server { get; private set; }
        
        public string UserName { get; private set; }
        
        public string Password { get; private set; }
    }
}
