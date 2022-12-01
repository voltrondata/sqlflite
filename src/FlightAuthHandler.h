//
// Created by Philip Moore on 11/14/22.
//
#include <arrow/flight/sql/server.h>
#include <arrow/flight/server_auth.h>
#include <arrow/flight/middleware.h>
#include <arrow/flight/server_middleware.h>
#include <arrow/util/base64.h>
#include <sstream>
#include <iostream>
#include "jwt-cpp/jwt.h"
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>



namespace arrow {
    namespace flight {

        const char kJWTIssuer[] = "flight_sql";
        const char kValidUsername[] = "flight_username";
        const char kBasicPrefix[] = "Basic ";
        const char kBearerPrefix[] = "Bearer ";
        const char kAuthHeader[] = "authorization";

        std::string GetFlightServerHostname() {
            const char *c_flight_hostname = std::getenv("FLIGHT_HOSTNAME");
            if (!c_flight_hostname) {
                return "0.0.0.0";
            }
            else {
                return std::string(c_flight_hostname);
            }
        }

        Status GetFlightServerPassword(std::string *out) {
            const char *c_flight_password = std::getenv("FLIGHT_PASSWORD");
            if (!c_flight_password) {
                return Status::IOError(
                        "Flight SQL Server env var: FLIGHT_PASSWORD is not set, set this variable to secure the server.");
            }
            *out = std::string(c_flight_password);
            return Status::OK();
        }

        Status FlightServerTlsCertificates(std::vector<CertKeyPair> *out) {
            std::string root = "../tls";

            *out = std::vector<CertKeyPair>();
            try {
                std::stringstream cert_path;
                cert_path << root << "/cert0.pem";
                std::stringstream key_path;
                key_path << root << "/cert0.key";

                std::ifstream cert_file(cert_path.str());
                if (!cert_file) {
                    return Status::IOError("Could not open certificate: " + cert_path.str());
                }
                std::stringstream cert;
                cert << cert_file.rdbuf();

                std::ifstream key_file(key_path.str());
                if (!key_file) {
                    return Status::IOError("Could not open key: " + key_path.str());
                }
                std::stringstream key;
                key << key_file.rdbuf();

                out->push_back(CertKeyPair{cert.str(), key.str()});
            } catch (const std::ifstream::failure &e) {
                return Status::IOError(e.what());
            }
            return Status::OK();
        }

        class HeaderAuthServerMiddleware : public ServerMiddleware {
        public:
            HeaderAuthServerMiddleware(std::string username) {
                ARROW_CHECK_OK(FlightServerTlsCertificates(&tls_certs_));
                username_ = username;
            }

            void SendingHeaders(AddCallHeaders *outgoing_headers) override {
                auto token = CreateJWTToken();
                outgoing_headers->AddHeader(kAuthHeader, std::string(kBearerPrefix) + token);
            }

            void CallCompleted(const Status &status) override {}

            std::string name() const override { return "HeaderAuthServerMiddleware"; }

        private:
            std::vector<CertKeyPair> tls_certs_;
            std::string username_;

            std::string CreateJWTToken() {
                auto token = jwt::create()
                        .set_issuer(std::string(kJWTIssuer))
                        .set_type("JWT")
                        .set_id("flight_sql-server-" + boost::uuids::to_string(boost::uuids::random_generator()()))
                        .set_issued_at(std::chrono::system_clock::now())
                        .set_expires_at(std::chrono::system_clock::now() + std::chrono::seconds{3600})
                        .set_payload_claim("username", jwt::claim(username_))
                        .sign(jwt::algorithm::rs256("", tls_certs_[0].pem_key, "", ""));

                return token;
            }
        };

        // Function to look in CallHeaders for a key that has a value starting with prefix and
        // return the rest of the value after the prefix.
        std::string FindKeyValPrefixInCallHeaders(const CallHeaders &incoming_headers,
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

        Status GetAuthHeaderType(const CallHeaders &incoming_headers, std::string *out) {
            if ( not FindKeyValPrefixInCallHeaders(incoming_headers, kAuthHeader, kBasicPrefix).empty() ) {
                *out = "Basic";
            }
            else if ( not FindKeyValPrefixInCallHeaders(incoming_headers, kAuthHeader, kBearerPrefix).empty() ) {
                *out = "Bearer";
            }
            else {
                return Status::IOError("Invalid Authorization Header type!");
            }
            return Status::OK();
        }

        void ParseBasicHeader(const CallHeaders &incoming_headers, std::string &username,
                              std::string &password) {
            std::string encoded_credentials =
                    FindKeyValPrefixInCallHeaders(incoming_headers, kAuthHeader, kBasicPrefix);
            std::stringstream decoded_stream(arrow::util::base64_decode(encoded_credentials));
            std::getline(decoded_stream, username, ':');
            std::getline(decoded_stream, password, ':');
        }

        // Factory for base64 header authentication testing.
        class HeaderAuthServerMiddlewareFactory : public ServerMiddlewareFactory {
        public:
            HeaderAuthServerMiddlewareFactory() {
                ARROW_CHECK_OK(FlightServerTlsCertificates(&tls_certs_));
            }

            Status StartCall(const CallInfo &info, const CallHeaders &incoming_headers,
                             std::shared_ptr<ServerMiddleware> *middleware) override {

                std::string auth_header_type;
                ARROW_RETURN_NOT_OK (GetAuthHeaderType(incoming_headers, &auth_header_type));
                if (auth_header_type == "Basic" ) {
                    std::string username, password;

                    ParseBasicHeader(incoming_headers, username, password);
                    std::string flight_server_password;
                    ARROW_RETURN_NOT_OK (GetFlightServerPassword(&flight_server_password));

                    if ((username == kValidUsername) && (password == flight_server_password)) {
                        *middleware = std::make_shared<HeaderAuthServerMiddleware>(username);
                    }
                    else {
                        return MakeFlightError(FlightStatusCode::Unauthenticated, "Invalid credentials");
                    }
                }
                return Status::OK();
            }

        private:
            std::vector<CertKeyPair> tls_certs_;
        };

        // A server middleware for validating incoming bearer header authentication.
        class BearerAuthServerMiddleware : public ServerMiddleware {
        public:
            explicit BearerAuthServerMiddleware(const CallHeaders &incoming_headers, bool *isValid)
                    : isValid_(isValid) {
                incoming_headers_ = incoming_headers;
                ARROW_CHECK_OK(FlightServerTlsCertificates(&tls_certs_));
            }

            void SendingHeaders(AddCallHeaders *outgoing_headers) override {
                std::string bearer_token =
                        FindKeyValPrefixInCallHeaders(incoming_headers_, kAuthHeader, kBearerPrefix);
                *isValid_ = (VerifyToken(bearer_token));
            }

            void CallCompleted(const Status &status) override {}

            std::string name() const override { return "BearerAuthServerMiddleware"; }

        private:
            CallHeaders incoming_headers_;
            bool *isValid_;
            std::vector<CertKeyPair> tls_certs_;

            bool VerifyToken(const std::string &token) {
                if (token.empty()) {
                    return false;
                }
                auto verify = jwt::verify()
                        .allow_algorithm(jwt::algorithm::rs256(tls_certs_[0].pem_cert, "", "", ""))
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
        };

        // Factory for base64 header authentication testing.
        class BearerAuthServerMiddlewareFactory : public ServerMiddlewareFactory {
        public:
            BearerAuthServerMiddlewareFactory() : isValid_(true) {}

            Status StartCall(const CallInfo &info, const CallHeaders &incoming_headers,
                             std::shared_ptr<ServerMiddleware> *middleware) override {
                const std::pair<CallHeaders::const_iterator, CallHeaders::const_iterator> &iter_pair =
                        incoming_headers.equal_range(kAuthHeader);
                if (iter_pair.first != iter_pair.second) {
                    std::string auth_header_type;
                    ARROW_RETURN_NOT_OK (GetAuthHeaderType(incoming_headers, &auth_header_type));
                    if (auth_header_type == "Bearer" ) {
                        *middleware =
                                std::make_shared<BearerAuthServerMiddleware>(incoming_headers, &isValid_);
                    }
                }
                if ( not isValid_ ) {
                    return MakeFlightError(FlightStatusCode::Unauthenticated, "Invalid bearer token provided");
                }
                return Status::OK();
            }

            bool GetIsValid() { return isValid_; }

        private:
            bool isValid_;
        };


//        Status ExampleTlsCertificateRoot(CertKeyPair* out) {
//            std::string root;
//            RETURN_NOT_OK(GetTestResourceRoot(&root));
//
//            std::stringstream path;
//            path << root << "/flight/root-ca.pem";
//
//            try {
//                std::ifstream cert_file(path.str());
//                if (!cert_file) {
//                    return Status::IOError("Could not open certificate: " + path.str());
//                }
//                std::stringstream cert;
//                cert << cert_file.rdbuf();
//                out->pem_cert = cert.str();
//                out->pem_key = "";
//                return Status::OK();
//            } catch (const std::ifstream::failure& e) {
//                return Status::IOError(e.what());
//            }
//        }

    }
}
