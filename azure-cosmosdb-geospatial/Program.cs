using Microsoft.Azure.Cosmos;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace azure_cosmosdb_geospatial
{
    class Program
    {
        private static string ConnectionString = ConfigurationManager.AppSettings["ConnectionString"];
        private static string DatabaseName = ConfigurationManager.AppSettings["DatabaseName"];
        private static string ContainerName = ConfigurationManager.AppSettings["ContainerName"];
        private const int ConcurrentWorkers = 100;
        private const int ConcurrentDocuments = 1;

        private static CosmosClient cosmosClient;
        private static Container container;

        static async Task Main(string[] args)
        {

            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                Program p = new Program();
                await p.Go();
            }
            catch (CosmosException ce)
            {
                Exception baseException = ce.GetBaseException();
                Console.WriteLine($"{ce.StatusCode} error occurred: {ce}");
            }
            catch (Exception e)
            {
                Console.WriteLine("Error: {0}", e);
            }
            finally
            {
                Console.WriteLine("End of demo, press any key to exit.");
                Console.ReadKey();
            }

        }

        public async Task Go()
        {
            cosmosClient = new CosmosClient(ConnectionString, new CosmosClientOptions()
            {
                AllowBulkExecution = true,
                ConnectionMode = ConnectionMode.Direct,
                MaxRequestsPerTcpConnection = -1,
                MaxTcpConnectionsPerEndpoint = -1,
                ConsistencyLevel = ConsistencyLevel.Eventual,
                MaxRetryAttemptsOnRateLimitedRequests = 999,
                MaxRetryWaitTimeOnRateLimitedRequests = TimeSpan.FromHours(1),
            });

            // Run query against Cosmos DB
            container = cosmosClient.GetDatabase(DatabaseName).GetContainer(ContainerName);

            while (true)
            {
                PrintPrompt();

                var c = Console.ReadKey(true);
                switch (c.Key)
                {
                    case ConsoleKey.D1:
                        await Proximity_Query();
                        break;
                    case ConsoleKey.D2:
                        await Polygon_Query();
                        break;
                    case ConsoleKey.D3:
                        await Intersect_Query();
                        break;
                /*    case ConsoleKey.D4:
                        await Validate_Query();
                        break;
                    case ConsoleKey.D5:
                        await ValidateDetailed_Query();
                        break;
                */
                    case ConsoleKey.Escape:
                        Console.WriteLine("Exiting...");
                        return;
                    default:
                        Console.WriteLine("Select choice");
                        break;
                }
            }
        }

        private void PrintPrompt()
        {
            Console.WriteLine("--------------------------------------------------------------------- ");
            Console.WriteLine("");
            Console.WriteLine("Press for demo scenario:\n");

            Console.WriteLine("1 - Scenario 1: Perform a proximity query against spatial data");
            Console.WriteLine("2 - Scenario 2: Perform a query to check if a point lies within a Polygon");
            Console.WriteLine("3 - Scenario 3: Perform a query to check if a there is an intersect");
        //    Console.WriteLine("4 - Scenario 4: Perform a query to check if a spatial object is valid");
        //    Console.WriteLine("5 - Scenario 5: Perform a query to validate a Polygon that is not closed");

            Console.WriteLine("--------------------------------------------------------------------- ");
            Console.WriteLine("");
            Console.WriteLine("Press space key to exit.\n");
        }


        static async Task Proximity_Query()
        {
            //Run query against container containing US building footprints
            var coordinates = "[-105.27695, 39.93242]";
            var sqlQueryText = "SELECT c.id FROM c " +
                               "WHERE ST_DISTANCE(c.geometry, {" +
                                            "'type': 'Point', " +
                                            "'coordinates':" + coordinates +
                               "}) < 50";


            await RunQuery(sqlQueryText);
        }

        static async Task Polygon_Query()
        {
            //Run query against container containing US building footprints
            var coordinates = "[[-105.272114783872183, 39.935004540501282], [-105.281085361423948, 39.934951144206337], [-105.281039593171144, 39.931175263349083], [-105.272114783872183, 39.931282055938986],  [-105.272114783872183, 39.935004540501282]]";
            var sqlQueryText = "SELECT c.id FROM c " +
                               "WHERE ST_WITHIN(c.geometry, {" +
                                            "'type':'Polygon', " +
                                            "'coordinates': [" + coordinates + "]" +
                               "})";

            await RunQuery(sqlQueryText);
        }

        static async Task Intersect_Query()
        {
            //Run query against container containing US building footprints
            var coordinates = "[[-105.272114783872183, 39.935004540501282], [-105.281085361423948, 39.934951144206337], [-105.281039593171144, 39.931175263349083], [-105.272114783872183, 39.931282055938986],  [-105.272114783872183, 39.935004540501282]]";
            var sqlQueryText = "SELECT c.id FROM c " +
                               "WHERE ST_INTERSECTS(c.geometry, {" +
                                            "'type':'Polygon', " +
                                            "'coordinates': [" + coordinates + "]" +
                               "})";

            await RunQuery(sqlQueryText);
        }

        static async Task Validate_Query()
        {
            //Run query against container containing US building footprints
            var coordinates = "[118.99, 32.94667]";
            var sqlQueryText = "SELECT ST_ISVALID({ " + 
                                            "'type': 'Point', " + 
                                            "'coordinates': " + coordinates +
                               "})";

            await RunQuery(sqlQueryText);
        }

        static async Task ValidateDetailed_Query()
        {
            //Run query against container containing US building footprints
            var coordinates = "[[118.99, 32.94667], [32, -5], [32, -4.7], [31.8, -4.7], [117, 32.94667]]";
            var sqlQueryText = "SELECT ST_ISVALIDDETAILED({ " + 
                                            "'type': 'Polygon', " +
                                            "'coordinates': [" + coordinates + "]" + 
                               "})";

            await RunQuery(sqlQueryText);
        }

        //Helper method to run query
        static async Task RunQuery(string sqlQueryText, int maxItemCountPerPage = 100, int maxConcurrency = -1)
        {
            Console.BackgroundColor = ConsoleColor.Blue;

            Console.WriteLine($"Running query: \"{sqlQueryText}\" against container {ContainerName}\n");
            Console.WriteLine("");

            Console.ResetColor();

            double totalRequestCharge = 0;
            QueryDefinition queryDefinition = new QueryDefinition(sqlQueryText);

            QueryRequestOptions requestOptions;
            requestOptions = new QueryRequestOptions()
            {
                MaxItemCount = maxItemCountPerPage,
                MaxConcurrency = maxConcurrency,
            };

            // Time the query
            Stopwatch stopWatch = new Stopwatch();
            stopWatch.Start();

            FeedIterator<dynamic> queryResultSetIterator = container.GetItemQueryIterator<dynamic>(queryDefinition, requestOptions: requestOptions);
            List<dynamic> reviews = new List<dynamic>();

            while (queryResultSetIterator.HasMoreResults)
            {
                FeedResponse<dynamic> currentResultSet = await queryResultSetIterator.ReadNextAsync();
                totalRequestCharge += currentResultSet.RequestCharge;

                foreach (var item in currentResultSet)
                {
                    reviews.Add(item);
                    //Console.WriteLine(item);
                }
                //if (useQueryOptions)
                //{
                //    Console.WriteLine($"Result count: {reviews.Count}");
                //}
            }
            Console.WriteLine($"Result count: {reviews.Count}");

            stopWatch.Stop();
            TimeSpan ts = stopWatch.Elapsed;

            //Print results
            string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:000}",
                ts.Hours, ts.Minutes, ts.Seconds,
                ts.Milliseconds);

            Console.ForegroundColor = ConsoleColor.Green;

            //Console.WriteLine($"\tQuery returned {reviews.Count} results");
            Console.WriteLine($"\tTotal time: {elapsedTime}");
            Console.WriteLine($"\tTotal Request Units consumed: {totalRequestCharge}\n");
            Console.WriteLine("\n\n\n");
            Console.ResetColor();

        }
    }
}