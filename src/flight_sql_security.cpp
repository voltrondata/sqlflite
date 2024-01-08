//
// Created by Philip Moore on 11/14/22.
//
#include "flight_sql_security.h"

namespace fs = std::filesystem;

namespace arrow {
    namespace flight {

        const std::string kJWTIssuer = "flight_sql";
        const int kJWTExpiration = 3600;

        std::string SecurityUtilities::GetFlightServerHostname() {
            const char *c_flight_hostname = std::getenv("FLIGHT_HOSTNAME");
            if (!c_flight_hostname) {
                return "0.0.0.0";
            } else {
                return std::string(c_flight_hostname);
            }
        }

        std::string SecurityUtilities::GetFlightServerPassword() {
            const char *c_flight_password = std::getenv("FLIGHT_PASSWORD");
            return std::string(c_flight_password);
        }

        Status SecurityUtilities::VerifyFlightServerPassword(std::string *out) {
            auto c_flight_password = GetFlightServerPassword();
            if (c_flight_password.empty()) {
                return Status::IOError(
                        "Flight SQL Server env var: FLIGHT_PASSWORD is not set, set this variable to secure the server.");
            }
            *out = c_flight_password;
            return Status::OK();
        }

        std::string SecurityUtilities::GetFlightServerSecretKey() {
            const char *c_flight_secret_key = std::getenv("SECRET_KEY");
            if (c_flight_secret_key) {
                return std::string(c_flight_secret_key);
            } else {
                return "SECRET-" + boost::uuids::to_string(boost::uuids::random_generator()());
            }
        }

        Status SecurityUtilities::FlightServerTlsCertificates(const fs::path &cert_path,
                                                              const fs::path &key_path,
                                                              std::vector<CertKeyPair> *out) {
            std::cout << "Using TLS Cert file: " << cert_path << std::endl;
            std::cout << "Using TLS Key file: " << key_path << std::endl;

            *out = std::vector<CertKeyPair>();
            try {
                std::ifstream cert_file(cert_path);
                if (!cert_file) {
                    return Status::IOError("Could not open certificate: " + cert_path.string());
                }
                std::stringstream cert;
                cert << cert_file.rdbuf();

                std::ifstream key_file(key_path);
                if (!key_file) {
                    return Status::IOError("Could not open key: " + key_path.string());
                }
                std::stringstream key;
                key << key_file.rdbuf();

                out->push_back(CertKeyPair{cert.str(), key.str()});
            } catch (const std::ifstream::failure &e) {
                return Status::IOError(e.what());
            }
            return Status::OK();
        }

        Status SecurityUtilities::FlightServerMtlsCACertificate(const std::string &cert_path,
                                                                std::string *out) {
            try {
                std::ifstream cert_file(cert_path);
                if (!cert_file) {
                    return Status::IOError("Could not open MTLS CA certificate: " + cert_path);
                }
                std::stringstream cert;
                cert << cert_file.rdbuf();

                *out = cert.str();
            } catch (const std::ifstream::failure &e) {
                return Status::IOError(e.what());
            }
            return Status::OK();
        }


        FlightSQLServerAuthHandler::FlightSQLServerAuthHandler(const std::string &username,
                                                               const std::string &password,
                                                               const std::string &secret_key) {
            basic_auth_.username = username;
            basic_auth_.password = password;
            secret_key_ = secret_key;
        }

        FlightSQLServerAuthHandler::~FlightSQLServerAuthHandler() {}

        Status FlightSQLServerAuthHandler::Authenticate(const ServerCallContext &context,
                                                        ServerAuthSender *outgoing,
                                                        ServerAuthReader *incoming) {
            std::string incoming_token;
            RETURN_NOT_OK(incoming->Read(&incoming_token));
            std::cout << "Incoming token: " << incoming_token << std::endl;
            ARROW_ASSIGN_OR_RAISE(BasicAuth incoming_auth, BasicAuth::Deserialize(incoming_token));
            if (incoming_auth.username != basic_auth_.username ||
                incoming_auth.password != basic_auth_.password) {
                return MakeFlightError(FlightStatusCode::Unauthenticated, "Invalid token");
            }

            std::string outgoing_token = CreateJWTToken();
            RETURN_NOT_OK(outgoing->Write(outgoing_token));
            return Status::OK();
        }

        Status FlightSQLServerAuthHandler::IsValid(const ServerCallContext &context,
                                                   const std::string &token,
                                                   std::string *peer_identity) {
            if (!VerifyToken(token)) {
                return MakeFlightError(FlightStatusCode::Unauthenticated, "Invalid token");
            }
            *peer_identity = basic_auth_.username;
            return Status::OK();
        }

        bool FlightSQLServerAuthHandler::VerifyToken(const std::string &token) {
            if (token.empty()) {
                return false;
            }
            auto verify = jwt::verify()
                    .allow_algorithm(jwt::algorithm::hs256{secret_key_})
                    .with_issuer(std::string(kJWTIssuer));

            try {
                auto decoded = jwt::decode(token);
                verify.verify(decoded);
                // If we got this far, the token verified successfully...
                return true;
            }
            catch (const std::exception &e) {
                std::cout << "Bearer Token verification failed with exception: " << e.what() << std::endl;
                return false;
            }
        }

        std::string FlightSQLServerAuthHandler::CreateJWTToken() {
            auto token = jwt::create()
                    .set_issuer(kJWTIssuer)
                    .set_type("JWT")
                    .set_id("flight_sql-server-" + boost::uuids::to_string(boost::uuids::random_generator()()))
                    .set_issued_at(std::chrono::system_clock::now())
                    .set_expires_at(std::chrono::system_clock::now() + std::chrono::seconds{kJWTExpiration})
                    .set_payload_claim("username", jwt::claim(basic_auth_.username))
                    .sign(jwt::algorithm::hs256{secret_key_});

            return token;
        }
    }
}
