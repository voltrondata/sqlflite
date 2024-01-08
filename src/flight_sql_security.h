//
// Created by Philip Moore on 11/14/22.
//
#include <filesystem>
#include <arrow/flight/sql/server.h>
#include <arrow/flight/server_auth.h>
#include <arrow/flight/middleware.h>
#include <arrow/flight/server_middleware.h>
#include <arrow/util/base64.h>
#include <sstream>
#include <iostream>
#include <fstream>
#include "jwt-cpp/jwt.h"
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

namespace fs = std::filesystem;

namespace arrow {
    namespace flight {

        class SecurityUtilities {
        public:
            static std::string GetFlightServerHostname();

            static std::string GetFlightServerPassword();

            static Status VerifyFlightServerPassword(std::string *out);

            static std::string GetFlightServerSecretKey();

            static Status FlightServerTlsCertificates(const fs::path &cert_path,
                                                      const fs::path &key_path,
                                                      std::vector<CertKeyPair> *out);

            static Status FlightServerMtlsCACertificate(const std::string &cert_path,
                                                        std::string *out);
        };

        class FlightSQLServerAuthHandler : public ServerAuthHandler {
        public:
            explicit FlightSQLServerAuthHandler(const std::string &username,
                                                const std::string &password,
                                                const std::string &secret_key);

            ~FlightSQLServerAuthHandler() override;

            Status Authenticate(const ServerCallContext &context, ServerAuthSender *outgoing,
                                ServerAuthReader *incoming) override;

            Status IsValid(const ServerCallContext &context, const std::string &token,
                           std::string *peer_identity) override;

        private:
            BasicAuth basic_auth_;
            std::string secret_key_;

            std::string CreateJWTToken();

            bool VerifyToken(const std::string &token);
        };

    }
}
