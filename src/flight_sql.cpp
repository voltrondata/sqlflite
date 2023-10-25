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
namespace fs = std::filesystem;

const int port = 31337;

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
        const fs::path &db_path,
        const fs::path &tls_cert_path,
        const fs::path &tls_key_path,
        const fs::path &mtls_ca_cert_path,
        const std::string &init_sql_commands,
        const bool &print_queries
) {
    ARROW_ASSIGN_OR_RAISE(auto location,
                          arrow::flight::Location::ForGrpcTls(arrow::flight::GetFlightServerHostname(), port))
    std::cout << "Apache Arrow version: " << ARROW_VERSION_STRING << std::endl;

    arrow::flight::FlightServerOptions options(location);

    // Setup TLS - we require it because users are sending their passwords
    ARROW_CHECK_OK(arrow::flight::FlightServerTlsCertificates(tls_cert_path, tls_key_path, &options.tls_certificates));

    // Setup authentication middleware (using the same TLS certificate keypair)
    auto header_middleware = std::make_shared<arrow::flight::HeaderAuthServerMiddlewareFactory>(
            options.tls_certificates);
    auto bearer_middleware = std::make_shared<arrow::flight::BearerAuthServerMiddlewareFactory>(
            options.tls_certificates);

    options.auth_handler = std::make_unique<arrow::flight::NoOpAuthHandler>();
    options.middleware.push_back({"header-auth-server", header_middleware});
    options.middleware.push_back({"bearer-auth-server", bearer_middleware});

    // Ensure a password is set for the server
    std::string flight_server_password;
    ARROW_CHECK_OK (arrow::flight::GetFlightServerPassword(&flight_server_password));

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

    std::cout << "Using database file: " << db_path << std::endl;

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

arrow::Status Main(const std::string &backend,
                   const fs::path &database_file_name,
                   const fs::path &tls_cert_path,
                   const fs::path &tls_key_path,
                   const fs::path &mtls_ca_cert_path,
                   const std::string &init_sql_commands,
                   const bool &print_queries
) {
    std::string new_init_sql_commands;

    // Set the autoinstall_known_extensions and autoload_known_extensions flags for DuckDB
    if (backend == "duckdb") {
        new_init_sql_commands = "SET autoinstall_known_extensions = true; SET autoload_known_extensions = true;";
    }

    // Append the INIT_SQL_COMMANDS environment variable to the init_sql_commands string
    if (!init_sql_commands.empty()) {
        new_init_sql_commands += init_sql_commands;
    }

    ARROW_ASSIGN_OR_RAISE(auto server,
                          CreateServer(backend, database_file_name, tls_cert_path, tls_key_path, mtls_ca_cert_path,
                                       new_init_sql_commands, print_queries))

    ARROW_CHECK_OK(server->Serve());

    return arrow::Status::OK();
}

int main(int argc, char **argv) {

    std::vector<std::string> tls_token_values;

    // Declare the supported options.
    po::options_description desc("Allowed options");
    desc.add_options()
            ("help", "produce this help message")
            ("backend,B", po::value<std::string>()->default_value("duckdb"),
             "Specify the database backend. Allowed options: duckdb, sqlite.")
            ("database-filename,D", po::value<std::string>()->default_value(""),
             "Specify the database filename (absolute or relative to the current working directory)")
            ("tls,T", po::value<std::vector<std::string>>(&tls_token_values)->multitoken()->default_value(
                     std::vector<std::string>{"tls/cert0.pem", "tls/cert0.key"}, "tls/cert0.pem tls/cert0.key"),
             "Specify the TLS certificate and key file paths.")
            ("init-sql-commands,I", po::value<std::string>()->default_value(""),
             "Specify the SQL commands to run on server startup.")
            ("mtls-ca-cert-filename,M", po::value<std::string>()->default_value(""),
             "Specify an optional mTLS CA certificate path used to verify clients.  The certificate MUST be in PEM format.")
            ("print-queries,Q", po::bool_switch()->default_value(false), "Print queries run by clients to stdout");

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    if (vm.count("help")) {
        std::cout << desc << "\n";
        return 1;
    }

    std::string backend = vm["backend"].as<std::string>();
    fs::path database_file_name = vm["database-filename"].as<std::string>();
    if (database_file_name.empty()) {
        std::cout << "--database-filename (-D) was not provided on the command line!" << std::endl;
        return 1;
    } else {
        // We do not check for existence of the database file, b/c they may want to create a new one
        database_file_name = fs::absolute(database_file_name).string();
    }

    std::vector<std::string> tls_tokens = tls_token_values;
    if (tls_tokens.size() != 2) {
        std::cout << "--tls is a required argument, and requires 2 entries - separated by a space!" << std::endl;
        return 1;
    }
    fs::path tls_cert_path = tls_tokens[0];
    fs::path tls_key_path = tls_tokens[1];

    if (tls_cert_path.empty()) {
        std::cout << "--tls requires a certificate file path as the first entry!" << std::endl;
        return 1;
    } else {
        tls_cert_path = fs::absolute(tls_cert_path).string();
        if (!fs::exists(tls_cert_path)) {
            std::cout << "TLS certificate file does not exist: " << tls_cert_path << std::endl;
            return 1;
        }
    }

    if (tls_key_path.empty()) {
        std::cout << "--tls requires a key file path as the second entry!" << std::endl;
        return 1;
    } else {
        tls_key_path = fs::absolute(tls_key_path).string();
        if (!fs::exists(tls_key_path)) {
            std::cout << "TLS key file does not exist: " << tls_key_path << std::endl;
            return 1;
        }
    }

    std::string init_sql_commands = vm["init-sql-commands"].as<std::string>();
    if (init_sql_commands.empty()) {
        auto env_init_sql_commands = std::getenv("INIT_SQL_COMMANDS");
        if (env_init_sql_commands) {
            init_sql_commands = env_init_sql_commands;
        }
    }

    fs::path mtls_ca_cert_path = vm["mtls-ca-cert-filename"].as<std::string>();

    if (!mtls_ca_cert_path.empty()) {
        mtls_ca_cert_path = fs::absolute(mtls_ca_cert_path).string();
        if (!fs::exists(mtls_ca_cert_path)) {
            std::cout << "mTLS CA certificate file does not exist: " << mtls_ca_cert_path << std::endl;
            return 1;
        }
    }

    bool print_queries = vm["print-queries"].as<bool>();

    if (auto status = Main(backend, database_file_name, tls_cert_path, tls_key_path, mtls_ca_cert_path,
                           init_sql_commands, print_queries); !status.ok()) {
        std::cerr << status << std::endl;
        return 1;
    }

    return EXIT_SUCCESS;
}