#include "flight_sql_library.h"
#include <iostream>
#include <boost/program_options.hpp>

namespace po = boost::program_options;

int main(int argc, char **argv) {

    std::vector<std::string> tls_token_values;

    // Declare the supported options.
    po::options_description desc("Allowed options");
    desc.add_options()
            ("help", "produce this help message")
            ("backend,B", po::value<std::string>()->default_value("duckdb"),
             "Specify the database backend. Allowed options: duckdb, sqlite.")
            ("hostname,H", po::value<std::string>()->default_value(""),
             "Specify the hostname to listen on for the Flight SQL Server.  If not set, we will use env var: 'FLIGHT_HOSTNAME'."
             "If that isn't set, we will use the default of: '0.0.0.0'.")
            ("port,R", po::value<int>()->default_value(DEFAULT_FLIGHT_PORT),
             "Specify the port to listen on for the Flight SQL Server.")
            ("database-filename,D", po::value<std::string>()->default_value(""),
             "Specify the database filename (absolute or relative to the current working directory)")
            ("username,U", po::value<std::string>()->default_value("flight_username"),
             "Specify the username to allow to connect to the Flight SQL Server for clients.")
            ("password,P", po::value<std::string>()->default_value(""),
             "Specify the password to set on the Flight SQL Server for clients to connect with.  If not set, we will use env var: 'FLIGHT_PASSWORD'."
             "If that isn't set, the server will exit with failure.")
            ("secret-key,S", po::value<std::string>()->default_value(""),
             "Specify the secret key used to sign JWTs issued by the Flight SQL Server. "
             "If it isn't set, we use env var: 'SECRET_KEY'.  If that isn't set, the server will create a random secret key.")
            ("tls,T", po::value<std::vector<std::string>>(&tls_token_values)->multitoken()->default_value(
                     std::vector<std::string>{"", ""}, ""),
             "Specify the TLS certificate and key file paths.")
            ("init-sql-commands,I", po::value<std::string>()->default_value(""),
             "Specify the SQL commands to run on server startup."
             "If not set, we will use env var: 'INIT_SQL_COMMANDS'.")
            ("init-sql-commands-file,F", po::value<std::string>()->default_value(""),
             "Specify a file containing SQL commands to run on server startup."
             "If not set, we will use env var: 'INIT_SQL_COMMANDS_FILE'.")
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

    std::string backend_str = vm["backend"].as<std::string>();
    BackendType backend;
    if (backend_str == "duckdb") {
        backend = BackendType::duckdb;
    } else if (backend_str == "sqlite") {
        backend = BackendType::sqlite;
    } else {
        std::cout << "Invalid backend: " << backend_str << std::endl;
        return 1;
    }

    auto database_filename = fs::path(vm["database-filename"].as<std::string>());

    std::string hostname = "";
    if (vm.count("hostname")) {
        hostname = vm["hostname"].as<std::string>();
    }

    int port = vm["port"].as<int>();

    std::string username = "";
    if (vm.count("username")) {
        username = vm["username"].as<std::string>();
    }

    std::string password = "";
    if (vm.count("password")) {
        password = vm["password"].as<std::string>();
    }

    std::string secret_key = "";
    if (vm.count("secret-key")) {
        secret_key = vm["secret-key"].as<std::string>();
    }

    auto tls_cert_path = fs::path();
    auto tls_key_path = fs::path();
    if (vm.count("tls")) {
        std::vector<std::string> tls_tokens = tls_token_values;
        if (tls_tokens.size() != 2) {
            std::cout << "--tls requires 2 entries - separated by a space!" << std::endl;
            return 1;
        }
        tls_cert_path = fs::path(tls_tokens[0]);
        tls_key_path = fs::path(tls_tokens[1]);
    }

    std::string init_sql_commands = "";
    if (vm.count("init-sql-commands")) {
        init_sql_commands = vm["init-sql-commands"].as<std::string>();
    }

    std::string init_sql_commands_file = "";
    if (vm.count("init-sql-commands-file")) {
        init_sql_commands_file = fs::path(vm["init-sql-commands-file"].as<std::string>());
    }

    fs::path mtls_ca_cert_path;
    if (vm.count("mtls-ca-cert-filename")) {
        mtls_ca_cert_path = fs::path(vm["mtls-ca-cert-filename"].as<std::string>());
    }

    bool print_queries = vm["print-queries"].as<bool>();

    return RunFlightSQLServer(backend, database_filename, hostname, port, username, password, secret_key,
                                     tls_cert_path, tls_key_path, mtls_ca_cert_path,
                                     init_sql_commands, init_sql_commands_file, print_queries);
}
