#include <cstdlib>
#include <csignal>
#include <iostream>
#include <fstream>
#include <pthread.h>
#include <filesystem>
#include <vector>
#include <arrow/flight/client.h>
#include <arrow/flight/sql/client.h>
#include <arrow/flight/sql/server.h>
#include <arrow/table.h>
#include <arrow/util/logging.h>
#include <arrow/record_batch.h>
#include <boost/program_options.hpp>

#include "sqlite/sqlite_server.h"
#include "duckdb/duckdb_server.h"
#include "FlightAuthHandler.h"


namespace flight = arrow::flight;
namespace flightsql = arrow::flight::sql;
namespace po = boost::program_options;

int port = 31337;

arrow::Status printResults(
    std::unique_ptr<flight::FlightInfo> &results, 
    std::unique_ptr<flightsql::FlightSqlClient> &client,
    const flight::FlightCallOptions &call_options,
    const bool& print_results_flag) {
        // Fetch each partition sequentially (though this can be done in parallel)
        for (const flight::FlightEndpoint& endpoint : results->endpoints()) {
            // Here we assume each partition is on the same server we originally queried, but this
            // isn't true in general: the server may split the query results between multiple
            // other servers, which we would have to connect to.

            // The "ticket" in the endpoint is opaque to the client. The server uses it to
            // identify which part of the query results to return.
            ARROW_ASSIGN_OR_RAISE(auto stream, client->DoGet(call_options, endpoint.ticket));
            // Read all results into an Arrow Table, though we can iteratively process record
            // batches as they arrive as well
            ARROW_ASSIGN_OR_RAISE(auto table, stream->ToTable());

            if(print_results_flag) std::cout << table->ToString() << std::endl;
        }

    return arrow::Status::OK();
}

std::string readFileIntoString(const std::string& path) {
    auto ss = std::ostringstream{};
    std::ifstream input_file(path);
    if (!input_file.is_open()) {
        std::cerr << "Could not open the file - '"
             << path << "'" << std::endl;
        exit(EXIT_FAILURE);
    }
    ss << input_file.rdbuf();
    return ss.str();
}

bool checkIfSkip(std::string path, int query_id) {
    std::string query_id_str = std::to_string(query_id);

    if (path.find(query_id_str) != std::string::npos) {
        std::cout << "Skipping query: " << query_id << '\n';
        return true;
    }
    return false;
}

arrow::Status runQueries(
        std::unique_ptr<flightsql::FlightSqlClient> &client, 
        const std::string &query_path, 
        const std::vector<int> &skip_queries, 
        flight::FlightCallOptions &call_options,
        const bool& print_results_flag
    ) {
    int skip_vector_it = 0;
    for (const auto & file : std::filesystem::directory_iterator(query_path)) {
        std::cout << "Query: " << file.path() << std::endl;
        if (skip_vector_it < skip_queries.size()) {
            if (checkIfSkip(file.path(), skip_queries.at(skip_vector_it))) {
                ++skip_vector_it;
                continue;
            }
        }
        std::string kQuery = readFileIntoString(file.path());

        if (print_results_flag) std::cout << "Executing query: '" << kQuery << "'" << std::endl;
        ARROW_ASSIGN_OR_RAISE(auto flight_info, client->Execute(call_options, kQuery));

        if (flight_info != nullptr) {
            ARROW_RETURN_NOT_OK(printResults(flight_info, client, call_options, print_results_flag));
        }
    }

    return arrow::Status::OK();
}




arrow::Result<std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>> CreateServer(
        const std::string &db_type, 
        const std::string &db_path
    ) {
    ARROW_ASSIGN_OR_RAISE(auto location,
                        arrow::flight::Location::ForGrpcTls(arrow::flight::GetFlightServerHostname(), port));
    arrow::flight::FlightServerOptions options(location);

    auto header_middleware = std::make_shared<arrow::flight::HeaderAuthServerMiddlewareFactory>();
    auto bearer_middleware = std::make_shared<arrow::flight::BearerAuthServerMiddlewareFactory>();

    options.auth_handler = std::make_unique<arrow::flight::NoOpAuthHandler>();
    options.middleware.push_back({"header-auth-server", header_middleware});
    options.middleware.push_back({"bearer-auth-server", bearer_middleware});

    // Ensure a password is set for the server
    std::string flight_server_password;
    ARROW_CHECK_OK (arrow::flight::GetFlightServerPassword(&flight_server_password));

    // Setup TLS
    ARROW_CHECK_OK(FlightServerTlsCertificates(&options.tls_certificates));

    std::cout << "Using database file: " << db_path << std::endl;

    std::shared_ptr<arrow::flight::sql::FlightSqlServerBase> server = nullptr;

    if (db_type == "sqlite") {
        ARROW_ASSIGN_OR_RAISE(server,
                                arrow::flight::sql::sqlite::SQLiteFlightSqlServer::Create(db_path));
    } else if (db_type == "duckdb") {
        duckdb::DBConfig config;
        ARROW_ASSIGN_OR_RAISE(server,
                                arrow::flight::sql::duckdbflight::DuckDBFlightSqlServer::Create(db_path, config));
    } else {
        std::string err_msg = "Unknown server type: --> ";
        err_msg += db_type;
        return arrow::Status::Invalid(err_msg);
    }

    if (server != nullptr) {
        ARROW_CHECK_OK(server->Init(options));
        // Exit with a clean error code (0) on SIGTERM
        ARROW_CHECK_OK(server->SetShutdownOnSignals({SIGTERM}));

        std::cout << db_type << " server listening on " << location.ToString() << std::endl;
        return server;
    } else {
        std::string err_msg = "Unable to start the server";
        return arrow::Status::Invalid(err_msg);
    }
}

arrow::Result<std::unique_ptr<flightsql::FlightSqlClient>> CreateClient() {
    ARROW_ASSIGN_OR_RAISE(auto location,
                        arrow::flight::Location::ForGrpcTcp("localhost", port));
    arrow::flight::FlightServerOptions options(location);

    ARROW_ASSIGN_OR_RAISE(auto flight_client, flight::FlightClient::Connect(location));
    std::cout << "Connected to server: localhost:" << port << std::endl; 

    std::unique_ptr<flightsql::FlightSqlClient> client(
        new flightsql::FlightSqlClient(std::move(flight_client)));
    std::cout << "Client created." << std::endl;

    return client;
}

arrow::Status Main(const std::string& backend,
                   const std::string& database_file_path,
                   const std::string& database_file_name) {
    std::string database_file_uri = database_file_path + "/" + database_file_name;

    ARROW_ASSIGN_OR_RAISE(auto server, CreateServer(backend, database_file_uri));

//    std::string query_path = "../queries";
//    std::vector<int> skip_queries = {17}; // the rest of the code assumes this is ORDERED vector!
//    ARROW_ASSIGN_OR_RAISE(auto client, CreateClient());
//
//    flight::FlightCallOptions call_options;
//    ARROW_ASSIGN_OR_RAISE(std::unique_ptr<flight::FlightInfo> tables, client->GetTables(call_options, NULL, NULL, NULL, NULL, NULL));
//
//    if (tables != nullptr && print_results_flag) {
//        ARROW_RETURN_NOT_OK(printResults(tables, client, call_options, print_results_flag));
//    }
//
//    ARROW_RETURN_NOT_OK(runQueries(client, query_path, skip_queries, call_options, print_results_flag));

    ARROW_CHECK_OK(server->Serve());

    return arrow::Status::OK();
}

bool string2bool(const std::string &v)
{
    return !v.empty () &&
        (strcasecmp (v.c_str (), "true") == 0 ||
         atoi (v.c_str ()) != 0);
}

int main(int argc, char** argv) {

    // Declare the supported options.
    po::options_description desc("Allowed options");
    desc.add_options()
        ("help", "produce this help message")
        ("backend,B", po::value<std::string>()->default_value("duckdb"), "Specify the database backend. Allowed options: duckdb, sqlite.")
        ("database_file_path,P", po::value<std::string>()->default_value("../data"), "Specify the search path for the database file." )
        ("database_file_name,D", po::value<std::string>()->default_value(""), "Specify the database filename (the file must be in search path)" )
    ;

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    if (vm.count("help")) {
        std::cout << desc << "\n";
        return 1;
    }

    std::string backend = vm["backend"].as<std::string>();
    std::string database_file_path = vm["database_file_path"].as<std::string>();
    std::string database_file_name = vm["database_file_name"].as<std::string>();

    if (database_file_name.empty()) {
        std::cout << "--database_file_name (-D) was not provided on the command line!" << std::endl;
        return 1;
    }

    auto status = Main(backend, database_file_path, database_file_name);
    if (!status.ok()) {
        std::cerr << status << std::endl;
        return 1;
    }

    return EXIT_SUCCESS;
}