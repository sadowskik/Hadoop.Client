using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

namespace Hadoop.Client.WebHdfs
{   
    public class WebHdfsHttpClient : IHdfsClient
    {
        private readonly BasicAuthCredential _authCredential;

        /// <summary>
        /// Gets or sets the handler to use when creating an HttpClient.
        /// </summary>
        internal WebRequestHandler RequestHandler { get; set; }

        /// <summary>
        /// Gets or Sets Uri for the WebHdfs endpoint.
        /// </summary>
        internal Uri WebHdfsUri { get; set; }

        public WebHdfsHttpClient(BasicAuthCredential authCredential)
        {
            _authCredential = authCredential;
            WebHdfsUri = new Uri(authCredential.Server, "/webhdfs/v1");
        }

        /// <summary>
        /// Method that creates an HttpClient to use when communicating with WebHdfs.
        /// </summary>
        /// <param name="allowsAutoRedirect">Allows the client to automatically redirect the user.</param>
        /// <returns>An Http Client for making requests.</returns>
        private HttpClient CreateHttpClient(bool allowsAutoRedirect = true)
        {
            var client = RequestHandler == null
                ? new HttpClient(new WebRequestHandler {AllowAutoRedirect = allowsAutoRedirect})
                : new HttpClient(RequestHandler);

            AddAuthentication(client);
            return client;
        }

        private void AddAuthentication(HttpClient client)
        {
            string userName = _authCredential.UserName;
            string password = _authCredential.Password;

            if (userName != null && password != null)
            {
                var byteArray = Encoding.ASCII.GetBytes(userName + ":" + password);

                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic",
                    Convert.ToBase64String(byteArray));
            }
            
            client.DefaultRequestHeaders.Add("accept", "application/octet-stream");
            client.DefaultRequestHeaders.Add("accept", "application/json");
        }

        internal Uri CreateRequestUri(WebHdfsOperation operation, string path, List<KeyValuePair<string, string>> parameters)
        {
            if (parameters == null)
                parameters = new List<KeyValuePair<string, string>>();

            parameters.Add(new KeyValuePair<string, string>(
                HadoopRemoteRestConstants.UserName,
                _authCredential.UserName.EscapeDataString()));

            string paramString = parameters.Aggregate("",
                (current, param) => current + string.Format("&{0}={1}", param.Key, param.Value));

            var queryString = string.Format("{0}?op={1}{2}", path, operation, paramString);
            
            var uri = new Uri(WebHdfsUri + queryString);
            return uri;
        }

        /// <inheritdocs/>
        public async Task<Stream> OpenFile(string path)
        {
            var client = CreateHttpClient();
            var resp = await client.GetAsync(CreateRequestUri(WebHdfsOperation.OPEN, path, null));
            resp.EnsureSuccessStatusCode();
            return await resp.Content.ReadAsStreamAsync();
        }

        /// <inheritdocs/>
        public async Task<string> CreateFile(string path, Stream content, bool overwrite)
        {
            var client = CreateHttpClient(false);

            var parameters = new List<KeyValuePair<string, string>> { new KeyValuePair<string, string>("overwrite", overwrite.ToString()) };
            var redir = await client.PutAsync(CreateRequestUri(WebHdfsOperation.CREATE, path, parameters), null);

            content.Position = 0;
            var fileContent = new StreamContent(content);
            var create = await client.PutAsync(redir.Headers.Location, fileContent);
            create.EnsureSuccessStatusCode();
            return create.Headers.Location.ToString();
        }

        /// <inheritdocs/>
        public async Task<bool> Delete(string path, bool recursive)
        {
            var client = CreateHttpClient();

            var parameters = new List<KeyValuePair<string, string>> { new KeyValuePair<string, string>("recursive", recursive.ToString()) };
            var drop = await client.DeleteAsync(CreateRequestUri(WebHdfsOperation.DELETE, path, parameters));
            drop.EnsureSuccessStatusCode();

            var content = await drop.Content.ReadAsAsync<JObject>();
            return content.Value<bool>("boolean");
        }

        /// <inheritdocs/>
        public async Task<DirectoryEntry> GetFileStatus(string path)
        {
            var client = CreateHttpClient();

            var status = await client.GetAsync(CreateRequestUri(WebHdfsOperation.GETFILESTATUS, path, null));
            status.EnsureSuccessStatusCode();

            var filesStatusTask = await status.Content.ReadAsAsync<JObject>();

            return new DirectoryEntry(filesStatusTask.Value<JObject>("FileStatus"));
        }

        public async Task<bool> CreateDirectory(string path)
        {
            var client = CreateHttpClient();

            var result = await client.PutAsync(CreateRequestUri(WebHdfsOperation.MKDIRS, path, null), null);

            return result.IsSuccessStatusCode;
        }
    }
}
