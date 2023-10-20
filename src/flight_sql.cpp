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
#include <boost/algorithm/string.hpp>
#include <unistd.h>

#include "sqlite/sqlite_server.h"
#include "duckdb/duckdb_server.h"
#include "FlightAuthHandler.h"


namespace flight = arrow::flight;
namespace flightsql = arrow::flight::sql;
namespace po = boost::program_options;

const int port = 31337;

std::string readFileIntoString(const std::string &path) {
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

bool checkIfSkip(std::string_view path, int query_id) {
    if (std::string query_id_str = std::to_string(query_id); path.find(query_id_str) != std::string::npos) {
        std::cout << "Skipping query: " << query_id << '\n';
        return true;
    }
    return false;
}

#define RUN_INIT_COMMANDS(serverType, init_sql_commands) \
    do { \
        if (init_sql_commands != "") { \
            std::vector<std::string> tokens; \
            boost::split(tokens, init_sql_commands, boost::is_any_of(";")); \
            for (const std::string &init_sql_command: tokens) { \
                if (init_sql_command.empty()) continue; \
                std::cout << "Running Init SQL command: " << std::endl << init_sql_command << ";" << std::endl; \
                ARROW_RETURN_NOT_OK(serverType->ExecuteSql(init_sql_command)); \
            } \
        } \
    } while (false)


arrow::Result<std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>> CreateServer(
        const std::string &db_type,
        const std::string &db_path,
        const std::string &mtls_ca_cert_path,
        const std::string &init_sql_commands,
        const bool &print_queries
) {
    ARROW_ASSIGN_OR_RAISE(auto location,
                          arrow::flight::Location::ForGrpcTls(arrow::flight::GetFlightServerHostname(), port))
    std::cout << "Apache Arrow version: " << ARROW_VERSION_STRING << std::endl;

    arrow::flight::FlightServerOptions options(location);

    auto header_middleware = std::make_shared<arrow::flight::HeaderAuthServerMiddlewareFactory>();
    auto bearer_middleware = std::make_shared<arrow::flight::BearerAuthServerMiddlewareFactory>();

    options.auth_handler = std::make_unique<arrow::flight::NoOpAuthHandler>();
    options.middleware.push_back({"header-auth-server", header_middleware});
    options.middleware.push_back({"bearer-auth-server", bearer_middleware});

    // Ensure a password is set for the server
    std::string flight_server_password;
    ARROW_CHECK_OK (arrow::flight::GetFlightServerPassword(&flight_server_password));

    // Setup TLS - we require it because users are sending their passwords
    ARROW_CHECK_OK(arrow::flight::FlightServerTlsCertificates(&options.tls_certificates));

    if (!mtls_ca_cert_path.empty()) {
        std::cout << "Using mTLS CA certificate: " << mtls_ca_cert_path << std::endl;
        ARROW_CHECK_OK(arrow::flight::FlightServerMtlsCACertificate(mtls_ca_cert_path, &options.root_certificates));
        options.verify_client = true;
    }

    std::shared_ptr<arrow::flight::sql::FlightSqlServerBase> server = nullptr;

    if (db_type == "sqlite") {
        std::shared_ptr<arrow::flight::sql::sqlite::SQLiteFlightSqlServer> sqlite_server = nullptr;
        ARROW_ASSIGN_OR_RAISE(sqlite_server,
                              arrow::flight::sql::sqlite::SQLiteFlightSqlServer::Create(db_path)
        )
        RUN_INIT_COMMANDS(sqlite_server, init_sql_commands);
        server = sqlite_server;
    } else if (db_type == "duckdb") {
        std::shared_ptr<arrow::flight::sql::duckdbflight::DuckDBFlightSqlServer> duckdb_server = nullptr;
        duckdb::DBConfig config;
        ARROW_ASSIGN_OR_RAISE(duckdb_server,
                              arrow::flight::sql::duckdbflight::DuckDBFlightSqlServer::Create(db_path, config,
                                                                                              print_queries)
        )
        RUN_INIT_COMMANDS(duckdb_server, init_sql_commands);
        server = duckdb_server;
    } else {
        std::string err_msg = "Unknown server type: --> ";
        err_msg += db_type;
        return arrow::Status::Invalid(err_msg);
    }

    auto db_realpath = realpath(db_path.c_str(), nullptr);
    std::cout << "Using database file: " << db_realpath << " (resolved from: " << db_path << ")" << std::endl;

    std::cout << "Print Queries option is set to: " << std::boolalpha << print_queries << std::endl;

    if (server != nullptr) {
        ARROW_CHECK_OK(server->Init(options));
        // Exit with a clean error code (0) on SIGTERM
        ARROW_CHECK_OK(server->SetShutdownOnSignals({SIGTERM}));

        std::cout << "Apache Arrow Flight SQL server - with engine: " << db_type << " - listening on "
                  << location.ToString() << std::endl;
        return server;
    } else {
        std::string err_msg = "Unable to start the server";
        return arrow::Status::Invalid(err_msg);
    }
}

arrow::Status printResults(
        std::unique_ptr<flight::FlightInfo> &results,
        std::unique_ptr<flightsql::FlightSqlClient> &client,
        const flight::FlightCallOptions &call_options,
        const bool &print_results_flag) {
    // Fetch each partition sequentially (though this can be done in parallel)
    for (const flight::FlightEndpoint &endpoint: results->endpoints()) {
        // Here we assume each partition is on the same server we originally queried, but this
        // isn't true in general: the server may split the query results between multiple
        // other servers, which we would have to connect to.

        // The "ticket" in the endpoint is opaque to the client. The server uses it to
        // identify which part of the query results to return.
        ARROW_ASSIGN_OR_RAISE(auto stream, client->DoGet(call_options, endpoint.ticket))
        // Read all results into an Arrow Table, though we can iteratively process record
        // batches as they arrive as well
        ARROW_ASSIGN_OR_RAISE(auto table, stream->ToTable())

        if (print_results_flag) std::cout << table->ToString() << std::endl;
    }

    return arrow::Status::OK();
}

struct FlightSQLClientWithCallOptions {
    std::unique_ptr<flightsql::FlightSqlClient> client;
    std::unique_ptr<arrow::flight::FlightCallOptions> call_options;
};

arrow::Result<FlightSQLClientWithCallOptions> CreateClient() {
    ARROW_ASSIGN_OR_RAISE(auto location, arrow::flight::Location::ForGrpcTls("localhost", port))
    arrow::flight::FlightClientOptions options;
    options.disable_server_verification = true;

    ARROW_ASSIGN_OR_RAISE(auto flight_client, flight::FlightClient::Connect(location, options))
    std::cout << "Connected to server: localhost:" << port << std::endl;

    arrow::Result<std::pair<std::string, std::string>> bearer_result =
            flight_client->AuthenticateBasicToken({}, "flight_username", std::string(std::getenv("FLIGHT_PASSWORD")));

    // Use std::make_unique for call_options
    auto call_options = std::make_unique<arrow::flight::FlightCallOptions>();
    call_options->headers.push_back(bearer_result.ValueOrDie());

    auto client = std::make_unique<flightsql::FlightSqlClient>(std::move(flight_client));
    std::cout << "Client created." << std::endl;

    FlightSQLClientWithCallOptions client_with_call_options;
    client_with_call_options.client = std::move(client);
    client_with_call_options.call_options = std::move(call_options);

    return std::move(client_with_call_options);
}

void chdir_string(const std::string &path) {
    // Convert the string to a char array
    // Navigate to the database file directory
    int n = path.length();
    // declaring character array
    char char_array[n + 1];

    // copying the contents of the
    // string to char array
    strcpy(char_array, path.c_str());

    chdir(char_array);
}

arrow::Status Main(const std::string &backend,
                   const std::string &database_file_path,
                   const std::string &database_file_name,
                   const std::string &mtls_ca_cert_path,
                   const bool &print_queries
) {

    // Navigate to the database file directory
    chdir_string(database_file_path);

    std::string database_file_uri = database_file_path + "/" + database_file_name;

    std::string init_sql_commands;

    // Set the autoinstall_known_extensions and autoload_known_extensions flags for DuckDB
    if (backend == "duckdb") {
        init_sql_commands = "SET autoinstall_known_extensions = true; SET autoload_known_extensions = true;";
    }

    // Append the INIT_SQL_COMMANDS environment variable to the init_sql_commands string
    if (auto init_sql_commands_env = std::getenv("INIT_SQL_COMMANDS"); init_sql_commands_env != nullptr) {
        init_sql_commands += init_sql_commands_env;
    }

    ARROW_ASSIGN_OR_RAISE(auto server,
                          CreateServer(backend, database_file_uri, mtls_ca_cert_path, init_sql_commands, print_queries))

    ARROW_CHECK_OK(server->Serve());

    return arrow::Status::OK();
}

bool string2bool(const std::string &v) {
    return !v.empty() &&
           (strcasecmp(v.c_str(), "true") == 0 ||
            atoi(v.c_str()) != 0);
}

int main(int argc, char **argv) {

    // Declare the supported options.
    po::options_description desc("Allowed options");
    desc.add_options()
            ("help", "produce this help message")
            ("backend,B", po::value<std::string>()->default_value("duckdb"),
             "Specify the database backend. Allowed options: duckdb, sqlite.")
            ("database_file_path,P", po::value<std::string>()->default_value("../data"),
             "Specify the search path for the database file.")
            ("database_file_name,D", po::value<std::string>()->default_value(""),
             "Specify the database filename (the file must be in search path)")
            ("mtls_ca_cert_path,M", po::value<std::string>()->default_value(""),
             "Specify an optional mTLS CA certificate path used to verify clients.  The certificate MUST be in PEM format.")
            ("print_queries,Q", po::bool_switch()->default_value(false), "Print queries run by clients to stdout");

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
    std::string mtls_ca_cert_path = vm["mtls_ca_cert_path"].as<std::string>();
    bool print_queries = vm["print_queries"].as<bool>();

    if (database_file_name.empty()) {
        std::cout << "--database_file_name (-D) was not provided on the command line!" << std::endl;
        return 1;
    }

    if (auto status = Main(backend, database_file_path, database_file_name, mtls_ca_cert_path,
                           print_queries); !status.ok()) {
        std::cerr << status << std::endl;
        return 1;
    }

    return EXIT_SUCCESS;
}