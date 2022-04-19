#include <cstdlib>
#include <csignal>
#include <iostream>
#include <pthread.h>

#include <arrow/flight/client.h>
#include <arrow/flight/sql/client.h>
#include <arrow/table.h>

#include "sqlite/sqlite_server.h"

namespace flight = arrow::flight;
namespace flightsql = arrow::flight::sql;

int port = 31337;

struct createServerParams {
    long id;
    std::string path;
};

void print_results(
    flight::FlightInfo results, 
    flightsql::FlightSqlClient client,
    const flight::FlightCallOptions &call_options) {
        // Fetch each partition sequentially (though this can be done in parallel)
        for (const flight::FlightEndpoint& endpoint : results.endpoints()) {
            // Here we assume each partition is on the same server we originally queried, but this
            // isn't true in general: the server may split the query results between multiple
            // other servers, which we would have to connect to.

            // The "ticket" in the endpoint is opaque to the client. The server uses it to
            // identify which part of the query results to return.
            auto stream_result = client.DoGet(call_options, endpoint.ticket);
            std::unique_ptr<arrow::flight::FlightStreamReader> stream;
            if (stream_result.ok()) {
                stream = std::move(stream_result.ValueOrDie());
            }
            // Read all results into an Arrow Table, though we can iteratively process record
            // batches as they arrive as well
            auto table_result = stream->ToTable();
            auto table = std::move(table_result.ValueOrDie());

            std::cout << "Read one chunk:" << std::endl;
            std::cout << table->ToString() << std::endl;
        }
}

void* CreateServer(void *params) {
    // unpack the args
    struct createServerParams *csp = (struct createServerParams*) params;
    long id = csp->id;
    std::string path = csp->path;

    std::cout << "Creating server in thread: " << (long) id << std::endl;

    auto location_result = flight::Location::ForGrpcTcp("localhost", port);
    arrow::flight::Location location;
    if (location_result.ok()) {
        location = std::move(location_result).ValueOrDie();
    } else {
        std::cout << "Error occured when assigning location. Stopping." << std::endl;
        return (void*) 1;
    }

    arrow::flight::FlightServerOptions options(location);
    auto server_result = arrow::flight::sql::sqlite::SQLiteFlightSqlServer::Create(path);
    std::shared_ptr<arrow::flight::sql::sqlite::SQLiteFlightSqlServer> server;

    if (server_result.ok()) {
        server = std::move(server_result).ValueOrDie();
    } else {
        std::cout << "Error creating server. Stopping." << std::endl;
        return (void*) 1;
    }

    if (!server->Init(options).ok()) {
        std::cout << "Error initializing database. Stopping." << std::endl;
        return (void*) 1;
    }
    // // Exit with a clean error code (0) on SIGTERM
    if (!server->SetShutdownOnSignals({SIGTERM}).ok()) {
        std::cout << "Error assigning shutdown signal. Stopping." << std::endl;
        return (void*) 1;
    }

    std::cout << "Server listening on localhost:" << server->port() << std::endl;
    if (!server->Serve().ok()) {
        std::cout << "Error serving the data. Stopping." << std::endl;
        return (void*) 1;
    }

    // return arrow::Status::OK();
    return (void*) 0;
}

void* CreateClient(void *id) {
    std::cout << "Creating client in thread: " << (long) id << std::endl;

    auto location_result = flight::Location::ForGrpcTcp("localhost", port);
    arrow::flight::Location location;
    if (location_result.ok()) {
        location = std::move(location_result).ValueOrDie();
    } else {
        std::cout << "Error occured when assigning location. Stopping.";
        return (void*) 1;
    }
    std::cout << "Connecting to " << location.ToString() << std::endl;

    // Set up the Flight SQL client
    auto flight_client_result = flight::FlightClient::Connect(location);
    std::unique_ptr<flight::FlightClient> flight_client;
    
    if (flight_client_result.ok()) {
        flight_client = std::move(flight_client_result.ValueOrDie());
    }
    std::unique_ptr<flightsql::FlightSqlClient> client(
        new flightsql::FlightSqlClient(std::move(flight_client)));

    flight::FlightCallOptions call_options;
    auto tables_result = client->GetTables(call_options, NULL, NULL, NULL, NULL, NULL);
    std::unique_ptr<flight::FlightInfo> tables;

    if (tables_result.ok()) {
        // tables = std::move(tables_result.ValueOrDie());
        tables = std::move(tables_result.ValueOrDie());
        print_results(*tables, *client, call_options);
    } else {
        std::cout << "We got a problem" << std::endl;
        return (void*) 1;
    }

    // run query
    // const std::string kQuery = "SELECT L_ORDERKEY, O_ORDERDATE, SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS revenue FROM orders AS A INNER JOIN lineitem AS B ON A.O_ORDERKEY = B.L_ORDERKEY GROUP BY L_ORDERKEY, O_ORDERDATE LIMIT 10;";
    const std::string kQuery = "SELECT L_ORDERKEY, SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS revenue FROM lineitem GROUP BY L_ORDERKEY LIMIT 10;";

    std::cout << "Executing query: '" << kQuery << "'" << std::endl;
    auto flight_info_result = client->Execute(call_options, kQuery);
    std::unique_ptr<flight::FlightInfo> flight_info;

    std::cout << flight_info_result.status().ToString() << std::endl;
    std::cout << flight_info_result.ok() << std::endl;

    if (flight_info_result.ok()) {
        flight_info = std::move(flight_info_result.ValueOrDie());
        print_results(*flight_info, *client, call_options);
    } else {
        std::cout << "There was a problem executing this query..." << std::endl;
        return (void*) 1;
    }
    pthread_exit(NULL);
}

int main(int argc, char** argv) {
    pthread_t threads[2]; // two threads: server and client
    void* result_server; 
    void* result_client;

    createServerParams csp;
    csp.id = 0;
    csp.path = "../data/TPC-H-small.db";

    int server_t = pthread_create(&threads[0], NULL, CreateServer, &csp);
    usleep(2000); // let the server start up

    int client_t = pthread_create(&threads[1], NULL, CreateClient, (void*) 1);

    if (client_t != 0) {
        return EXIT_FAILURE;
    }

    pthread_join(threads[1], &result_client);
    std::raise(SIGTERM); // shutdown the server

    pthread_join(threads[0], &result_server);
    return EXIT_SUCCESS;
}