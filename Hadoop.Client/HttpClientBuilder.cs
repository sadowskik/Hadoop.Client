using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;

namespace Hadoop.Client
{
    internal class HttpClientBuilder
    {
        private ConnectionConfig _connectionConfig;        
        private readonly bool _allowsAutoRedirect;

        private readonly IList<KeyValuePair<string, string>> _headers
            = new List<KeyValuePair<string, string>>();

        private HttpClientBuilder(bool allowsAutoRedirect)
        {
            _allowsAutoRedirect = allowsAutoRedirect;
        }

        public static HttpClientBuilder Create(bool allowsAutoRedirect = true)
        {
            return new HttpClientBuilder(allowsAutoRedirect);
        }

        public HttpClientBuilder WithBasicAuthenticationFrom(ConnectionConfig connectionConfig)
        {
            _connectionConfig = connectionConfig;
            return this;
        }

        public HttpClientBuilder AcceptJson()
        {
            var jsonAccept = new KeyValuePair<string, string>("accept", "application/json");
            _headers.Add(jsonAccept);

            return this;
        }

        public HttpClientBuilder AcceptOctetStream()
        {
            var jsonAccept = new KeyValuePair<string, string>("accept", "application/octet-stream");
            _headers.Add(jsonAccept);

            return this;            
        }

        public HttpClient Build()
        {
            var client = new HttpClient(new WebRequestHandler {AllowAutoRedirect = _allowsAutoRedirect});

            AddBasicAuthentication(client);
            AddHeaders(client);            

            return client;
        }

        private void AddBasicAuthentication(HttpClient client)
        {
            var userName = _connectionConfig.UserName;
            var password = _connectionConfig.Password;

            if (userName == null || password == null)
                return;

            var byteArray = Encoding.ASCII.GetBytes(userName + ":" + password);

            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue(
                "Basic", Convert.ToBase64String(byteArray));
        }

        private void AddHeaders(HttpClient client)
        {
            foreach (var header in _headers)
            {
                client.DefaultRequestHeaders.Add(header.Key, header.Value);
            }
        }
    }
}