#include <cstdlib>
#include <iostream>
#include <pthread.h>

#include <arrow/flight/client.h>
#include <arrow/flight/sql/client.h>
#include <arrow/table.h>

#include "sqlite/sqlite_server.h"

namespace flight = arrow::flight;
namespace flightsql = arrow::flight::sql;

int port = 31337;

void* CreateServer(void *id) {
    std::cout << "Creating server in thread: " << (long) id << std::endl;

    auto location_result = flight::Location::ForGrpcTcp("localhost", port);
    arrow::flight::Location location;
    if (location_result.ok()) {
        location = std::move(location_result).ValueOrDie();
    } else {
        std::cout << "Error occured when assigning location. Stopping.";
        return (void*) 1;
    }

    arrow::flight::FlightServerOptions options(location);
    auto server_result = arrow::flight::sql::sqlite::SQLiteFlightSqlServer::Create();
    std::shared_ptr<arrow::flight::sql::sqlite::SQLiteFlightSqlServer> server;

    if (server_result.ok()) {
        server = std::move(server_result).ValueOrDie();
    } else {
        std::cout << "Error occured when assigning location. Stopping.";
        return (void*) 1;
    }

    if (!server->Init(options).ok()) {
        std::cout << "Error occured when assigning location. Stopping.";
        return (void*) 1;
    }
    // // Exit with a clean error code (0) on SIGTERM
    if (!server->SetShutdownOnSignals({SIGTERM}).ok()) {
        std::cout << "Error occured when assigning location. Stopping.";
        return (void*) 1;
    }

    std::cout << "Server listening on localhost:" << server->port() << std::endl;
    if (!server->Serve().ok()) {
        std::cout << "Error occured when assigning location. Stopping.";
        return (void*) 1;
    }

    // return arrow::Status::OK();
    return (void*) 0;
}

// arrow::Status Main() {
void* CreateClient(void *id) {
//   ARROW_ASSIGN_OR_RAISE(auto location,
//                         flight::Location::ForGrpcTcp("localhost", 31337));
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

    const std::string kQuery = "SELECT 1";

    // Set up the Flight SQL client
    auto flight_client_result = flight::FlightClient::Connect(location);
    std::unique_ptr<flight::FlightClient> flight_client;
    
    // ARROW_ASSIGN_OR_RAISE(flight_client, flight::FlightClient::Connect(location));
    if (flight_client_result.ok()) {
        flight_client = std::move(flight_client_result.ValueOrDie());
    }
    std::unique_ptr<flightsql::FlightSqlClient> client(
        new flightsql::FlightSqlClient(std::move(flight_client)));

    flight::FlightCallOptions call_options;

    // Execute the query, getting a FlightInfo describing how to fetch the results
    std::cout << "Executing query: '" << kQuery << "'" << std::endl;
    auto flight_info_result = client->Execute(call_options, kQuery);
    std::unique_ptr<flight::FlightInfo> flight_info;

    if (flight_info_result.ok()) {
        flight_info = std::move(flight_info_result.ValueOrDie());
    }
    //   ARROW_ASSIGN_OR_RAISE(std::unique_ptr<flight::FlightInfo> flight_info,
    //                         client->Execute(call_options, kQuery));

      // Fetch each partition sequentially (though this can be done in parallel)
      for (const flight::FlightEndpoint& endpoint : flight_info->endpoints()) {
        // Here we assume each partition is on the same server we originally queried, but this
        // isn't true in general: the server may split the query results between multiple
        // other servers, which we would have to connect to.

        // The "ticket" in the endpoint is opaque to the client. The server uses it to
        // identify which part of the query results to return.
        auto stream_result = client->DoGet(call_options, endpoint.ticket);
        std::unique_ptr<arrow::flight::FlightStreamReader> stream;
        if (stream_result.ok()) {
            stream = std::move(stream_result.ValueOrDie());
        }
        // ARROW_ASSIGN_OR_RAISE(auto stream, client->DoGet(call_options, endpoint.ticket));
        // Read all results into an Arrow Table, though we can iteratively process record
        // batches as they arrive as well
        auto table_result = stream->ToTable();
        auto table = std::move(table_result.ValueOrDie());



        // ARROW_ASSIGN_OR_RAISE(auto table, stream->ToTable());
        std::cout << "Read one chunk:" << std::endl;
        std::cout << table->ToString() << std::endl;
      }
    // return (void*) 0;
    pthread_exit(NULL);
}

int main(int argc, char** argv) {
    pthread_t threads[2]; // two threads: server and client
    void* result_server; 
    void* result_client;

    int server_t = pthread_create(&threads[0], NULL, CreateServer, (void*) 0);
    usleep(2000); // let the server start up

    int client_t = pthread_create(&threads[1], NULL, CreateClient, (void*) 1);

    if (client_t != 0) {
        return EXIT_FAILURE;
    }

    pthread_join(threads[0], &result_server);
    pthread_join(threads[1], &result_client);
    return EXIT_SUCCESS;
}