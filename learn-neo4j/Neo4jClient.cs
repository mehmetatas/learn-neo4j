using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace learn_neo4j
{
    public class Neo4jClient
    {
        private readonly HttpClient _client;
        private readonly string _endpoint;

        public Neo4jClient(string endpoint)
        {
            _client = new HttpClient();
            _endpoint = endpoint;
        }

        public void CreateIndex(string label, string property)
        {
            ExecuteCypher(String.Format("CREATE INDEX ON :{0}({1})", label, property));
        }

        public void DropIndex(string label, string property)
        {
            ExecuteCypher(String.Format("DROP INDEX ON :{0}({1})", label, property));
        }

        public void CreateUniqueKey(string label, string property)
        {
            ExecuteCypher(String.Format("CREATE CONSTRAINT ON (x:{0}) ASSERT x.{1} IS UNIQUE", label, property));
        }

        public void DropUniqueKey(string label, string property)
        {
            ExecuteCypher(String.Format("DROP CONSTRAINT ON (x:{0}) ASSERT x.{1} IS UNIQUE", label, property));
        }

        public void CreateNode(Neo4jNode node)
        {
            node["id"] = Guid.NewGuid();

            var cypher = new StringBuilder("CREATE (n");

            foreach (var label in node.Labels)
            {
                cypher.Append(":")
                    .Append(label);
            }

            if (node.PropertyNames.Any())
            {
                cypher.Append(" {");

                var comma = "";

                foreach (var name in node.PropertyNames)
                {
                    var propVal = node[name];

                    cypher.Append(comma)
                        .Append(name)
                        .Append(":")
                        .Append(JsonConvert.SerializeObject(propVal));

                    comma = ",";
                }

                cypher.Append("}");
            }

            cypher.Append(")");

            ExecuteCypher(cypher.ToString());
        }

        public Neo4jNode GetNode(Guid id)
        {
            var cypher = new StringBuilder("MATCH (n {id:'")
                .Append(id)
                .Append("'}) RETURN n, labels(n) ");

            var resp = ExecuteCypher(cypher.ToString());

            if (resp.Results[0].Data.Length == 0)
            {
                return null;
            }

            var rows = resp.Results[0].Data[0].Rows;

            var props = ((JObject)rows[0]).ToObject<Dictionary<string, object>>();
            var labels = ((JArray) rows[1]).ToObject<string[]>();

            return new Neo4jNode(labels, props);
        }

        public void UpdateNode(Neo4jNode node)
        {
            var cypher = new StringBuilder("MATCH (n {id:'")
                .Append(node.Id)
                .Append("'}) SET ");

            if (node.PropertyNames.Any())
            {
                var comma = "";

                foreach (var name in node.PropertyNames)
                {
                    if (name == "id")
                    {
                        continue;
                    }

                    var propVal = node[name];

                    cypher.Append(comma)
                        .Append("n.")
                        .Append(name)
                        .Append("=")
                        .Append(JsonConvert.SerializeObject(propVal));

                    comma = ",";
                }
            }

            ExecuteCypher(cypher.ToString());
        }

        public void DeleteNode(Neo4jNode node)
        {
            var cypher = new StringBuilder("MATCH (n {id:'")
                .Append(node.Id)
                .Append("'}) DELETE n ");

            ExecuteCypher(cypher.ToString());
        }

        public void CreateRelationship(Guid fromNode, Guid toNode, Neo4jRelationship relationship)
        {
            relationship["id"] = Guid.NewGuid();

            var cypher = new StringBuilder("MATCH (nFrom {id:'")
                .Append(fromNode)
                .Append("'}), (nTo {id: '")
                .Append(toNode)
                .Append("'}) CREATE (nFrom)-[:")
                .Append(relationship.Type);

            if (relationship.PropertyNames.Any())
            {
                cypher.Append(" {");

                var comma = "";

                foreach (var name in relationship.PropertyNames)
                {
                    var propVal = relationship[name];

                    cypher.Append(comma)
                        .Append(name)
                        .Append(":")
                        .Append(JsonConvert.SerializeObject(propVal));

                    comma = ",";
                }

                cypher.Append("}");
            }

            cypher.Append("]->(nTo)");

            ExecuteCypher(cypher.ToString());
        }

        public Neo4jRelationship GetRelationship(Guid relationshipId)
        {
            var cypher = new StringBuilder("MATCH ()-[r {id:'")
                .Append(relationshipId)
                .Append("'}]->() RETURN r, type(r)");

            var resp = ExecuteCypher(cypher.ToString());
            
            if (resp.Results[0].Data.Length == 0)
            {
                return null;
            }

            var rows = resp.Results[0].Data[0].Rows;

            var props = ((JObject)rows[0]).ToObject<Dictionary<string, object>>();
            var type = (string)rows[1];

            return new Neo4jRelationship(type, props);
        }

        public void UpdateRelationship(Neo4jRelationship relationship)
        {
            var cypher = new StringBuilder("MATCH ()-[r {id:'")
                .Append(relationship.Id)
                .Append("'}]->() SET ");

            if (relationship.PropertyNames.Any())
            {
                var comma = "";

                foreach (var name in relationship.PropertyNames)
                {
                    if (name == "id")
                    {
                        continue;
                    }

                    var propVal = relationship[name];

                    cypher.Append(comma)
                        .Append("r.")
                        .Append(name)
                        .Append("=")
                        .Append(JsonConvert.SerializeObject(propVal));

                    comma = ",";
                }
            }

            ExecuteCypher(cypher.ToString());
        }

        public void DeleteRelationship(Guid relationshipId)
        {
            var cypher = new StringBuilder("MATCH ()-[r {id:'")
                .Append(relationshipId)
                .Append("'}]->() DELETE r ");

            ExecuteCypher(cypher.ToString());
        }

        public Neo4jResponse ExecuteCypher(string cypher)
        {
            var httpReq = new HttpRequestMessage(HttpMethod.Post, _endpoint + "/transaction/commit");
            httpReq.Headers.Accept.Clear();
            httpReq.Headers.Remove("Accept");
            httpReq.Headers.Add("Accept", "application/json; charset=UTF-8");

            var req = new Neo4jRequest { Statements = new[] { new Neo4jStatement { Statement = cypher } } };

            var reqJson = JsonConvert.SerializeObject(req);

            httpReq.Content = new StringContent(reqJson, Encoding.UTF8, "application/json");

            var httpResp = _client.SendAsync(httpReq).Result;

            var respJson = httpResp.Content.ReadAsStringAsync().Result;

            var resp = JsonConvert.DeserializeObject<Neo4jResponse>(respJson);

            var exception = Neo4jException.FromErrors(resp.Errors);
            if (exception != null)
            {
                throw exception;
            }

            return resp;
        }
    }

    public class Neo4jRequest
    {
        [JsonProperty("statements")]
        public Neo4jStatement[] Statements { get; set; }
    }

    public class Neo4jStatement
    {
        [JsonProperty("statement")]
        public string Statement { get; set; }
    }

    public class Neo4jResponse
    {
        [JsonProperty("results")]
        public Neo4jResult[] Results { get; set; }

        [JsonProperty("errors")]
        public Neo4jError[] Errors { get; set; }
    }

    public class Neo4jError
    {
        [JsonProperty("code")]
        public string Code { get; set; }

        [JsonProperty("message")]
        public string Message { get; set; }
    }

    public class Neo4jException : Exception
    {
        private readonly Neo4jError _error;

        private Neo4jException(Neo4jError error, Neo4jException innerException)
            : base(error.Message, innerException)
        {
            _error = error;
        }

        public string ErrorCode
        {
            get { return _error.Code; }
        }

        public static Neo4jException FromErrors(Neo4jError[] errors)
        {
            Neo4jException tmp = null;

            if (errors != null)
            {
                for (var i = errors.Length - 1; i >= 0; i--)
                {
                    tmp = new Neo4jException(errors[i], tmp);
                }
            }

            return tmp;
        }
    }

    public class Neo4jResult
    {
        [JsonProperty("columns")]
        public string[] Columns { get; set; }

        [JsonProperty("data")]
        public Neo4jResultData[] Data { get; set; }
    }

    public class Neo4jResultData
    {
        [JsonProperty("row")]
        public object[] Rows { get; set; }
    }

    public class Neo4jNode
    {
        private readonly string[] _labels;
        private readonly IDictionary _properties;

        private Guid _id;

        public Neo4jNode(string[] labels, IDictionary properties = null)
        {
            _labels = new string[labels.Length];

            Array.Copy(labels, _labels, labels.Length);

            _properties = properties == null
                ? new Hashtable()
                : new Hashtable(properties);
        }

        public Guid Id
        {
            get
            {
                if (_id == Guid.Empty)
                {
                    var id = _properties["id"];

                    if (id is string)
                    {
                        _id = Guid.Parse((string) id);
                    }
                    else if (id is Guid)
                    {
                        _id = (Guid)id;
                    }
                } 
                return _id;
            }
        }

        public string[] Labels
        {
            get { return _labels; }
        }

        public IEnumerable<string> PropertyNames
        {
            get { return _properties.Keys.OfType<string>(); }
        }

        public object this[string propName]
        {
            get { return _properties[propName]; }
            set { _properties[propName] = value; }
        }
    }

    public class Neo4jRelationship
    {
        private readonly string _type;
        private readonly IDictionary _properties;

        private Guid _id;

        public Neo4jRelationship(string type, IDictionary properties = null)
        {
            _type = type;

            _properties = properties == null
                ? new Hashtable()
                : new Hashtable(properties);
        }

        public Guid Id
        {
            get
            {
                if (_id == Guid.Empty)
                {
                    var id = _properties["id"];

                    if (id is string)
                    {
                        _id = Guid.Parse((string) id);
                    }
                    else if (id is Guid)
                    {
                        _id = (Guid)id;
                    }
                } 
                return _id;
            }
        }

        public string Type
        {
            get { return _type; }
        }

        public IEnumerable<string> PropertyNames
        {
            get { return _properties.Keys.OfType<string>(); }
        }

        public object this[string propName]
        {
            get { return _properties[propName]; }
            set { _properties[propName] = value; }
        }
    }
}
