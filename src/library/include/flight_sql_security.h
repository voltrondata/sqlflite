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
  static Status FlightServerTlsCertificates(const fs::path &cert_path,
                                            const fs::path &key_path,
                                            std::vector<CertKeyPair> *out);

  static Status FlightServerMtlsCACertificate(const std::string &cert_path,
                                              std::string *out);

  static std::string FindKeyValPrefixInCallHeaders(const CallHeaders &incoming_headers,
                                                   const std::string &key,
                                                   const std::string &prefix);

  static Status GetAuthHeaderType(const CallHeaders &incoming_headers, std::string *out);

  static void ParseBasicHeader(const CallHeaders &incoming_headers, std::string &username,
                               std::string &password);
};

class HeaderAuthServerMiddleware : public ServerMiddleware {
 public:
  HeaderAuthServerMiddleware(const std::string &username, const std::string &secret_key);

  void SendingHeaders(AddCallHeaders *outgoing_headers) override;

  void CallCompleted(const Status &status) override;

  std::string name() const override;

 private:
  std::string username_;
  std::string secret_key_;

  std::string CreateJWTToken() const;
};

class HeaderAuthServerMiddlewareFactory : public ServerMiddlewareFactory {
 public:
  HeaderAuthServerMiddlewareFactory(const std::string &username,
                                    const std::string &password,
                                    const std::string &secret_key);

  Status StartCall(const CallInfo &info, const CallHeaders &incoming_headers,
                   std::shared_ptr<ServerMiddleware> *middleware) override;

 private:
  std::string username_;
  std::string password_;
  std::string secret_key_;
};

class BearerAuthServerMiddleware : public ServerMiddleware {
 public:
  explicit BearerAuthServerMiddleware(const std::string &secret_key,
                                      const CallHeaders &incoming_headers,
                                      std::optional<bool> *isValid);

  void SendingHeaders(AddCallHeaders *outgoing_headers) override;

  void CallCompleted(const Status &status) override;

  std::string name() const override;

 private:
  std::string secret_key_;
  CallHeaders incoming_headers_;
  std::optional<bool> *isValid_;

  bool VerifyToken(const std::string &token) const;
};

class BearerAuthServerMiddlewareFactory : public ServerMiddlewareFactory {
 public:
  explicit BearerAuthServerMiddlewareFactory(const std::string &secret_key);

  Status StartCall(const CallInfo &info, const CallHeaders &incoming_headers,
                   std::shared_ptr<ServerMiddleware> *middleware) override;

  std::optional<bool> GetIsValid();

 private:
  std::optional<bool> isValid_;
  std::string secret_key_;
};

}  // namespace flight
}  // namespace arrow
