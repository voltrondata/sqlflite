//
// Created by Philip Moore on 11/14/22.
//
#include "flight_sql_security.h"

namespace fs = std::filesystem;

namespace arrow {
    namespace flight {

        const std::string kJWTIssuer = "flight_sql";
        const int kJWTExpiration = 5;
        const std::string kValidUsername = "flight_username";
        const std::string kBasicPrefix = "Basic ";
        const std::string kBearerPrefix = "Bearer ";
        const std::string kAuthHeader = "authorization";

        // ----------------------------------------
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

        // Function to look in CallHeaders for a key that has a value starting with prefix and
        // return the rest of the value after the prefix.
        std::string SecurityUtilities::FindKeyValPrefixInCallHeaders(const CallHeaders &incoming_headers,
                                                                     const std::string &key,
                                                                     const std::string &prefix) {
            // Lambda function to compare characters without case sensitivity.
            auto char_compare = [](const char &char1, const char &char2) {
                return (::toupper(char1) == ::toupper(char2));
            };

            auto iter = incoming_headers.find(key);
            if (iter == incoming_headers.end()) {
                return "";
            }
            const std::string val(iter->second);
            if (val.size() > prefix.length()) {
                if (std::equal(val.begin(), val.begin() + prefix.length(), prefix.begin(),
                               char_compare)) {
                    return val.substr(prefix.length());
                }
            }
            return "";
        }

        Status SecurityUtilities::GetAuthHeaderType(const CallHeaders &incoming_headers, std::string *out) {
            if (not FindKeyValPrefixInCallHeaders(incoming_headers, kAuthHeader, kBasicPrefix).empty()) {
                *out = "Basic";
            } else if (not FindKeyValPrefixInCallHeaders(incoming_headers, kAuthHeader, kBearerPrefix).empty()) {
                *out = "Bearer";
            } else {
                return Status::IOError("Invalid Authorization Header type!");
            }
            return Status::OK();
        }

        void SecurityUtilities::ParseBasicHeader(const CallHeaders &incoming_headers, std::string &username,
                                                 std::string &password) {
            std::string encoded_credentials =
                    FindKeyValPrefixInCallHeaders(incoming_headers, kAuthHeader, kBasicPrefix);
            std::stringstream decoded_stream(arrow::util::base64_decode(encoded_credentials));
            std::getline(decoded_stream, username, ':');
            std::getline(decoded_stream, password, ':');
        }

        // ----------------------------------------
        HeaderAuthServerMiddleware::HeaderAuthServerMiddleware(const std::string &username,
                                                               const std::string &secret_key)
                : username_(username), secret_key_(secret_key) {}

        void HeaderAuthServerMiddleware::SendingHeaders(AddCallHeaders *outgoing_headers) {
            auto token = CreateJWTToken();
            outgoing_headers->AddHeader(kAuthHeader, std::string(kBearerPrefix) + token);
        }

        void HeaderAuthServerMiddleware::CallCompleted(const Status &status) {}

        std::string HeaderAuthServerMiddleware::name() const { return "HeaderAuthServerMiddleware"; }

        std::string HeaderAuthServerMiddleware::CreateJWTToken() {
            auto token = jwt::create()
                    .set_issuer(std::string(kJWTIssuer))
                    .set_type("JWT")
                    .set_id("flight_sql-server-" + boost::uuids::to_string(boost::uuids::random_generator()()))
                    .set_issued_at(std::chrono::system_clock::now())
                    .set_expires_at(std::chrono::system_clock::now() + std::chrono::seconds{kJWTExpiration})
                    .set_payload_claim("username", jwt::claim(username_))
                    .sign(jwt::algorithm::hs256{secret_key_});

            return token;
        }

        // ----------------------------------------
        HeaderAuthServerMiddlewareFactory::HeaderAuthServerMiddlewareFactory(const std::string &username,
                                                                             const std::string &password,
                                                                             const std::string &secret_key)
                : username_(username), password_(password), secret_key_(secret_key) {}

        Status HeaderAuthServerMiddlewareFactory::StartCall(const CallInfo &info, const CallHeaders &incoming_headers,
                                                            std::shared_ptr<ServerMiddleware> *middleware) {

            std::string auth_header_type;
            ARROW_RETURN_NOT_OK (SecurityUtilities::GetAuthHeaderType(incoming_headers, &auth_header_type));
            if (auth_header_type == "Basic") {
                std::string username, password;

                SecurityUtilities::ParseBasicHeader(incoming_headers, username, password);

                if ((username == username_) && (password == password_)) {
                    *middleware = std::make_shared<HeaderAuthServerMiddleware>(username, secret_key_);
                } else {
                    return MakeFlightError(FlightStatusCode::Unauthenticated, "Invalid credentials");
                }
            }
            return Status::OK();
        }

        // ----------------------------------------
        BearerAuthServerMiddleware::BearerAuthServerMiddleware(const std::string &secret_key,
                                                               const CallHeaders &incoming_headers,
                                                               std::optional<bool> *isValid)
                : isValid_(isValid) {
            incoming_headers_ = incoming_headers;
            secret_key_ = secret_key;
        }

        void BearerAuthServerMiddleware::SendingHeaders(AddCallHeaders *outgoing_headers) {
            std::string bearer_token =
                    SecurityUtilities::FindKeyValPrefixInCallHeaders(incoming_headers_, kAuthHeader, kBearerPrefix);
            *isValid_ = (VerifyToken(bearer_token));
        }

        void BearerAuthServerMiddleware::CallCompleted(const Status &status) {}

        std::string BearerAuthServerMiddleware::name() const { return "BearerAuthServerMiddleware"; }

        bool BearerAuthServerMiddleware::VerifyToken(const std::string &token) {
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

        // ----------------------------------------
        BearerAuthServerMiddlewareFactory::BearerAuthServerMiddlewareFactory(const std::string &secret_key
        ) : secret_key_(secret_key) {}

        Status BearerAuthServerMiddlewareFactory::StartCall(const CallInfo &info, const CallHeaders &incoming_headers,
                                                            std::shared_ptr<ServerMiddleware> *middleware) {
            const std::pair<CallHeaders::const_iterator, CallHeaders::const_iterator> &iter_pair =
                    incoming_headers.equal_range(kAuthHeader);
            if (iter_pair.first != iter_pair.second) {
                std::string auth_header_type;
                ARROW_RETURN_NOT_OK (SecurityUtilities::GetAuthHeaderType(incoming_headers, &auth_header_type));
                if (auth_header_type == "Bearer") {
                    *middleware =
                            std::make_shared<BearerAuthServerMiddleware>(secret_key_,
                                                                         incoming_headers,
                                                                         &isValid_);
                }
            }
            if (isValid_.has_value() && !*isValid_) {
                isValid_.reset();

                return MakeFlightError(FlightStatusCode::Unauthenticated,
                                       "Invalid bearer token provided");
            }

            return Status::OK();
        }

        std::optional<bool> BearerAuthServerMiddlewareFactory::GetIsValid() { return isValid_; }

    }
}
