# cosmosdb-spatial-query-app

### Performance tests against Azure Cosmos DB

Geospatial performance tests can be done with the c# application. It can be connected to a Cosmos DB instance, by providing the Cosmos DB connection string, the key, and database and container name in the App.config file. It can run from a VM, but can also be used from a local machine. 

Within the app, three tests can be called, see the following example:
Note that three performance tests are implemented and are :
- described in: [blog post](https://towardsdatascience.com/geospatial-big-data-performance-tests-with-cosmos-db-and-data-enrichment-with-azure-synapse-257db7d29e41) 
- implemented at: [repository](https://github.com/delange/cosmosdb-spatial-query-perf.git)

<img src="./performance_app.jpg" width=1000px />
