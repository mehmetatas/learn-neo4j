using System;
using System.Collections.Generic;

namespace learn_neo4j
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                CrudNode();
                CrudRelationship();

                Console.WriteLine("OK!");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
            Console.ReadLine();
        }

        private static void CrudNode()
        {
            var client = new Neo4jClient("http://localhost:7474/db/data");

            var node = new Neo4jNode(
                new[] { "Media" },
                new Dictionary<string, object>
                {
                    { "option", StringSplitOptions.RemoveEmptyEntries },
                    { "create_date", DateTime.Now },
                    { "type", 2 },
                    { "name", "The Big Bang Theory" }
                });

            Console.WriteLine("Create: " + node["name"]);

            client.CreateNode(node);

            var found = client.GetNode(node.Id);

            Console.WriteLine("Found: " + found["name"]);

            found["name"] += " 2";

            client.UpdateNode(found);

            Console.WriteLine("Updated: " + found["name"]);

            found = client.GetNode(node.Id);

            Console.WriteLine("Found2: " + found["name"]);

            client.DeleteNode(found);

            found = client.GetNode(node.Id);

            Console.WriteLine("Deleted: " + (found == null));
        }

        private static void CrudRelationship()
        {
            var client = new Neo4jClient("http://localhost:7474/db/data");

            var node1 = new Neo4jNode(
                new[] { "Music" },
                new Dictionary<string, object>
                {
                    { "type", 3 },
                    { "name", "Pretty Fly (For a white guy)" }
                });

            var node2 = new Neo4jNode(
                new[] { "Artist" },
                new Dictionary<string, object>
                {
                    { "type", 1 },
                    { "name", "The Offspring" }
                });

            client.CreateNode(node1);
            client.CreateNode(node2);

            var relationship = new Neo4jRelationship("SINGS", new Dictionary<string, object>
            {
                {"type", 1},
                {"description", "The Offpring sings Pretty Fly"}
            });

            client.CreateRelationship(node1.Id, node2.Id, relationship);

            var found = client.GetRelationship(relationship.Id);

            Console.WriteLine("Found: " + found["description"]);

            found["description"] += " Updated";

            client.UpdateRelationship(found);

            found = client.GetRelationship(relationship.Id);

            Console.WriteLine("Found2: " + found["description"]);

            client.DeleteRelationship(relationship.Id);

            found = client.GetRelationship(relationship.Id);

            Console.WriteLine("Deleted: " + (found == null));
        }
    }
}
