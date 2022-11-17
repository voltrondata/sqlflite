//
// Created by Philip Moore on 11/14/22.
//
#include <arrow/flight/sql/server.h>
#include <arrow/flight/server_auth.h>
#include <arrow/flight/middleware.h>
#include <arrow/flight/server_middleware.h>
#include <arrow/util/base64.h>
#include <sstream>


namespace arrow {
    namespace flight {

        const char kValidUsername[] = "flight_username";
        const char kValidPassword[] = "flight_password";
        const char kInvalidUsername[] = "invalid_flight_username";
        const char kInvalidPassword[] = "invalid_flight_password";
        const char kBearerToken[] = "honeybadger";
        const char kBasicPrefix[] = "Basic ";
        const char kBearerPrefix[] = "Bearer ";
        const char kAuthHeader[] = "authorization";

        class HeaderAuthServerMiddleware : public ServerMiddleware {
        public:
            void SendingHeaders(AddCallHeaders *outgoing_headers) override {
                outgoing_headers->AddHeader(kAuthHeader, std::string(kBearerPrefix) + kBearerToken);
            }

            void CallCompleted(const Status &status) override {}

            std::string name() const override { return "HeaderAuthServerMiddleware"; }
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

        void ParseBasicHeader(const CallHeaders &incoming_headers, std::string &username,
                              std::string &password) {
            std::string encoded_credentials =
                    FindKeyValPrefixInCallHeaders(incoming_headers, kAuthHeader, kBasicPrefix);
            std::stringstream decoded_stream(arrow::util::base64_decode(encoded_credentials));
            std::getline(decoded_stream, username, ':');
            std::getline(decoded_stream, password, ':');
        }

        std::string ParseBearerHeader(const CallHeaders &incoming_headers) {
            return FindKeyValPrefixInCallHeaders(incoming_headers, kAuthHeader, kBearerPrefix);
        }

        // Factory for base64 header authentication testing.
        class HeaderAuthServerMiddlewareFactory : public ServerMiddlewareFactory {
        public:
            HeaderAuthServerMiddlewareFactory() {}

            Status StartCall(const CallInfo &info, const CallHeaders &incoming_headers,
                             std::shared_ptr<ServerMiddleware> *middleware) override {
                std::string username, password;

                ParseBasicHeader(incoming_headers, username, password);
                std::string bearer_token = ParseBearerHeader(incoming_headers);
                if ((username == kValidUsername) && (password == kValidPassword)) {
                    *middleware = std::make_shared<HeaderAuthServerMiddleware>();
                } else if (bearer_token != kBearerToken) {
                    return MakeFlightError(FlightStatusCode::Unauthenticated, "Invalid credentials");
                }
                return Status::OK();
            }
        };

        // A server middleware for validating incoming bearer header authentication.
        class BearerAuthServerMiddleware : public ServerMiddleware {
        public:
            explicit BearerAuthServerMiddleware(const CallHeaders &incoming_headers, bool *isValid)
                    : isValid_(isValid) {
                incoming_headers_ = incoming_headers;
            }

            void SendingHeaders(AddCallHeaders *outgoing_headers) override {
                std::string bearer_token =
                        FindKeyValPrefixInCallHeaders(incoming_headers_, kAuthHeader, kBearerPrefix);
                *isValid_ = (bearer_token == std::string(kBearerToken));
            }

            void CallCompleted(const Status &status) override {}

            std::string name() const override { return "BearerAuthServerMiddleware"; }

        private:
            CallHeaders incoming_headers_;
            bool *isValid_;
        };

        // Factory for base64 header authentication testing.
        class BearerAuthServerMiddlewareFactory : public ServerMiddlewareFactory {
        public:
            BearerAuthServerMiddlewareFactory() : isValid_(false) {}

            Status StartCall(const CallInfo &info, const CallHeaders &incoming_headers,
                             std::shared_ptr<ServerMiddleware> *middleware) override {
                const std::pair<CallHeaders::const_iterator, CallHeaders::const_iterator> &iter_pair =
                        incoming_headers.equal_range(kAuthHeader);
                if (iter_pair.first != iter_pair.second) {
                    *middleware =
                            std::make_shared<BearerAuthServerMiddleware>(incoming_headers, &isValid_);
                }
                return Status::OK();
            }

            bool GetIsValid() { return isValid_; }

        private:
            bool isValid_;
        };

        ARROW_FLIGHT_EXPORT
        Status ExampleTlsCertificates(std::vector<CertKeyPair> *out);

        ARROW_FLIGHT_EXPORT
        Status ExampleTlsCertificateRoot(CertKeyPair *out);

    }
}
