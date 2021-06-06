// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#include "external_client.hpp"
#include <string>
#include <utility>
#include "SimpleClient.hpp"
#include "bftclient/fake_comm.h"
#include "utils/secret_retriever.hpp"

namespace concord::external_client {

using bftEngine::ClientMsgFlag;
using config::CommConfig;
using config::ConcordConfiguration;
using namespace config_pool;
using namespace bftEngine;
using std::chrono_literals::operator""s;

using namespace bft::communication;

std::shared_ptr<std::vector<char>> ConcordClient::reply_ = std::make_shared<std::vector<char>>(0);
uint16_t ConcordClient::required_num_of_replicas_ = 0;
uint16_t ConcordClient::num_of_replicas_ = 0;
size_t ConcordClient::max_reply_size_ = 0;
bool ConcordClient::delayed_behaviour_ = false;
bftEngine::OperationResult ConcordClient::clientRequestError_ = SUCCESS;

ConcordClient::ConcordClient(const ConcordConfiguration& config,
                             int client_id,
                             ClientPoolConfig& pool_config,
                             const SimpleClientParams& client_params) {
  logger_ = logging::getLogger("com.vmware.external_client_pool");
  client_id_ = client_id;
  CreateClient(config, pool_config, client_params);
}

ConcordClient::ConcordClient(const ConcordConfiguration& config,
                             int client_id,
                             ConcordClientConfiguration& struct_config,
                             const SimpleClientParams& client_params) {
  logger_ = logging::getLogger("com.vmware.external_client_pool");
  client_id_ = client_id;
  CreateClient(config, struct_config, client_params);
}

ConcordClient::~ConcordClient() noexcept = default;

/*uint32_t ConcordClient::SendRequest(const void* request,
                                    std::uint32_t request_size,
                                    ClientMsgFlag flags,
                                    std::chrono::milliseconds timeout_ms,
                                    std::uint32_t reply_size, uint64_t seq_num,
                                    const std::string correlation_id,
                                    std::string span_context) {
  clientRequestError_ = bftEngine::OperationResult::SUCCESS;
  uint32_t replyBufSize = externalReplyBufferSize ? externalReplyBufferSize : reply_->size();
  char* replyBuffer = externalReplyBuffer ? externalReplyBuffer : reply_->data();

  auto res = bftEngine::OperationResult::SUCCESS;
  if (enable_mock_comm_)
    res =
        client_->sendRequest(ClientMsgFlag::READ_ONLY_REQ, static_cast<const char*>(request), request_size, seq_num,
                             timeout_ms.count(), replyBufSize, replyBuffer, replyBufSize, correlation_id, span_context);
  else
    res = client_->sendRequest(flags, static_cast<const char*>(request), request_size, seq_num, timeout_ms.count(),
                               replyBufSize, replyBuffer, replyBufSize, correlation_id, span_context);

  if (res == bftEngine::OperationResult::TIMEOUT) {
    LOG_ERROR(logger_, "reqSeqNum=" << seq_num << " cid=" << correlation_id << " has failed to invoke, timeout="
                                    << timeout_ms.count() << " ms has been reached");
    clientRequestError_ = bftEngine::OperationResult::TIMEOUT;
  } else if (res == bftEngine::OperationResult::BUFFER_TOO_SMALL) {
    clientRequestError_ = bftEngine::OperationResult::BUFFER_TOO_SMALL;
    LOG_ERROR(logger_, "reqSeqNum=" << seq_num << " cid=" << correlation_id
                                    << " has failed to invoke, buffer size=" << reply_size << " is too small");

  } else if (res == bftEngine::OperationResult::NOT_READY) {
    LOG_ERROR(logger_,
              "reqSeqNum=" << seq_num << " cid=" << correlation_id << " has failed to invoke, replicas are not ready");
    LOG_DEBUG(logger_, "Request has completed processing" << KVLOG(client_id_, seq_num, correlation_id));
    clientRequestError_ = bftEngine::OperationResult::NOT_READY;
  }
  return replyBufSize;
}*/

bft::client::Reply ConcordClient::SendRequest(const bft::client::WriteConfig& config, bft::client::Msg&& request) {
  bft::client::Reply res;
  try {
    res = new_client_->send(config, std::move(request));
  } catch (TimeoutException e) {
    clientRequestError_ = TIMEOUT;
    LOG_ERROR(logger_,
              "reqSeqNum=" << config.request.sequence_number << " cid=" << config.request.correlation_id
                           << " has failed to invoke, timeout=" << config.request.timeout.count()
                           << " ms has been reached");
  }
  LOG_DEBUG(logger_,
            "Request has completed processing"
                << KVLOG(client_id_, config.request.sequence_number, config.request.correlation_id));
  if (externalReplyBuffer) memcpy(externalReplyBuffer, res.matched_data.data(), res.matched_data.size());
  return res;
}

bft::client::Reply ConcordClient::SendRequest(const bft::client::ReadConfig& config, bft::client::Msg&& request) {
  bft::client::Reply res;
  try {
    res = new_client_->send(config, std::move(request));
  } catch (TimeoutException e) {
    clientRequestError_ = TIMEOUT;
    LOG_ERROR(logger_,
              "reqSeqNum=" << config.request.sequence_number << " cid=" << config.request.correlation_id
                           << " has failed to invoke, timeout=" << config.request.timeout.count()
                           << " ms has been reached");
  }
  LOG_DEBUG(logger_,
            "Request has completed processing"
                << KVLOG(client_id_, config.request.sequence_number, config.request.correlation_id));
  if (externalReplyBuffer) {
    memcpy(externalReplyBuffer, res.matched_data.data(), res.matched_data.size());
  }

  return res;
}

void ConcordClient::AddPendingRequest(std::vector<uint8_t>&& request,
                                      bftEngine::ClientMsgFlag flags,
                                      char* reply_buffer,
                                      std::chrono::milliseconds timeout_ms,
                                      std::uint32_t reply_size,
                                      uint64_t seq_num,
                                      const std::string& correlation_id,
                                      const std::string& span_context) {
  bftEngine::ClientRequest pending_request;
  pending_request.lengthOfRequest = request.size();
  pending_request.request = std::move(request);
  pending_request.flags = flags;
  pending_request.timeoutMilli = timeout_ms.count();
  pending_request.reqSeqNum = seq_num;
  pending_request.cid = correlation_id;
  pending_request.span_context = span_context;
  pending_requests_.push_back(std::move(pending_request));

  bftEngine::ClientReply pending_reply;
  if (reply_size) {
    pending_reply.replyBuffer = reply_buffer;
    pending_reply.lengthOfReplyBuffer = reply_size;
  } else {
    pending_reply.replyBuffer = reply_->data() + batching_buffer_reply_offset_ * max_reply_size_;
    LOG_DEBUG(logger_,
              "Given reply size is 0, setting internal buffer with offset="
                  << (batching_buffer_reply_offset_ * max_reply_size_));
    ++batching_buffer_reply_offset_;
    pending_reply.lengthOfReplyBuffer = max_reply_size_;
  }
  pending_reply.actualReplyLength = 0UL;
  pending_reply.cid = correlation_id;
  pending_reply.span_context = span_context;
  pending_replies_.push_back(std::move(pending_reply));
}

std::pair<int8_t, ConcordClient::PendingReplies> ConcordClient::SendPendingRequests() {
  const auto& batch_cid =
      std::to_string(client_id_) + "-" + std::to_string(seqGen_->generateUniqueSequenceNumberForRequest());
  OperationResult ret = OperationResult::SUCCESS;
  std::deque<bft::client::WriteRequest> request_queue;
  std::map<uint64_t, string> seq_num_to_cid;
  for (auto req : pending_requests_) {
    bft::client::WriteConfig single_config;
    single_config.request.timeout = std::chrono::milliseconds(req.timeoutMilli);
    single_config.request.span_context = req.span_context;
    single_config.request.sequence_number = req.reqSeqNum;
    single_config.request.correlation_id = req.cid;
    seq_num_to_cid.insert(std::make_pair(req.reqSeqNum, req.cid));
    single_config.request.span_context = req.span_context;
    single_config.request.pre_execute = req.flags & bftEngine::PRE_PROCESS_REQ;
    request_queue.push_back(bft::client::WriteRequest{single_config, std::move(req.request)});
  }
  try {
    auto res = new_client_->sendBatch(request_queue, batch_cid);
    for (auto rep : res) {
      ClientReply single_reply;
      single_reply.actualReplyLength = rep.second.matched_data.size();
      single_reply.replyBuffer = reinterpret_cast<char*>(rep.second.matched_data.data());
      single_reply.cid = seq_num_to_cid.find(rep.first)->second;
      pending_replies_.push_back(single_reply);
      LOG_DEBUG(logger_, "Request has completed processing" << KVLOG(client_id_, batch_cid, single_reply.cid));
    }
  } catch (BatchTimeoutException& e) {
    LOG_ERROR(logger_, "Batch cid =" << batch_cid << " has failed to invoke, timeout has been reached");
    ret = OperationResult::TIMEOUT;
  }
  batching_buffer_reply_offset_ = 0UL;
  pending_requests_.clear();
  return {ret, std::move(pending_replies_)};
}

void ConcordClient::CreateCommConfig(CommConfig& comm_config,
                                     const ConcordConfiguration& config,
                                     int num_replicas,
                                     const ClientPoolConfig& pool_config) const {
  const auto& nodes = config.subscope(pool_config.PARTICIPANT_NODES, 0);
  const auto& node = nodes.subscope(pool_config.PARTICIPANT_NODE, 0);
  const auto& external_clients_conf = node.subscope(pool_config.EXTERNAL_CLIENTS, client_id_);
  const auto& client_conf = external_clients_conf.subscope(pool_config.CLIENT, 0);
  comm_config.commType = config.getValue<decltype(comm_config.commType)>(pool_config.COMM_PROTOCOL);
  comm_config.listenIp = "external_client";
  comm_config.listenPort = client_conf.getValue<decltype(comm_config.listenPort)>(pool_config.CLIENT_PORT);
  comm_config.bufferLength = config.getValue<decltype(comm_config.bufferLength)>(pool_config.COMM_BUFF_LEN);
  comm_config.selfId = client_conf.getValue<decltype(comm_config.selfId)>(pool_config.CLIENT_ID);

  if (comm_config.commType == "tcp" || comm_config.commType == "tls") {
    comm_config.maxServerId = num_replicas - 1;
  }

  if (comm_config.commType == "tls") {
    comm_config.certificatesRootPath =
        config.getValue<decltype(comm_config.certificatesRootPath)>(pool_config.CERT_FOLDER);
    comm_config.cipherSuite = config.getValue<decltype(comm_config.cipherSuite)>(pool_config.CIPHER_SUITE);
  }
  comm_config.statusCallback = nullptr;
  if (config.hasValue<bool>("encrypted_config_enabled") && config.getValue<bool>("encrypted_config_enabled")) {
    comm_config.secretData = concord::secretsretriever::SecretRetrieve(config.getValue<std::string>("secrets_url"));
  } else {
    comm_config.secretData = std::nullopt;
  }
}

void ConcordClient::CreateCommConfig(CommConfig& comm_config,
                                     const ConcordConfiguration& config,
                                     int num_replicas,
                                     const ConcordClientConfiguration& config_struct) const {  // I added

  const auto& client_conf = config_struct.participant_nodes.at(0).externalClients.at(client_id_);
  ;
  comm_config.commType = config_struct.comm_to_use;
  comm_config.listenIp = "external_client";
  comm_config.listenPort = client_conf.client_port;
  comm_config.bufferLength = std::stoul(config_struct.concord_bft_communication_buffer_length);
  comm_config.selfId = client_conf.principal_id;

  if (comm_config.commType == "tcp" || comm_config.commType == "tls") {
    comm_config.maxServerId = num_replicas - 1;
  }

  if (comm_config.commType == "tls") {
    comm_config.certificatesRootPath = config_struct.tls_certificates_folder_path;
    comm_config.cipherSuite = config_struct.tls_cipher_suite_list;
  }
  comm_config.statusCallback = nullptr;
  if (config_struct.encrypted_config_enabled) {
    comm_config.secretData = concord::secretsretriever::SecretRetrieve(config_struct.secrets_url);
  } else {
    comm_config.secretData = std::nullopt;
  }
}

void ConcordClient::CreateClient(const ConcordConfiguration& config,
                                 ClientPoolConfig& pool_config,
                                 const SimpleClientParams& client_params) {
  const auto num_replicas = config.getValue<std::uint16_t>(pool_config.NUM_REPLICAS);
  const auto& nodes = config.subscope(pool_config.PARTICIPANT_NODES, 0);
  const auto& node = nodes.subscope(pool_config.PARTICIPANT_NODE, 0);
  const auto& external_clients_conf = node.subscope(pool_config.EXTERNAL_CLIENTS, client_id_);
  const auto& client_conf = external_clients_conf.subscope(pool_config.CLIENT, 0);
  auto fVal = config.getValue<uint16_t>(pool_config.F_VAL);
  auto cVal = config.getValue<uint16_t>(pool_config.C_VAL);
  auto clientId = client_conf.getValue<uint16_t>(pool_config.CLIENT_ID);
  if (config.hasValue<bool>(pool_config.ENABLE_MOCK_COMM))
    enable_mock_comm_ = config.getValue<bool>(pool_config.ENABLE_MOCK_COMM);
  CommConfig comm_config;
  CreateCommConfig(comm_config, config, num_replicas, pool_config);
  client_id_ = clientId;
  std::set<ReplicaId> all_replicas;
  for (auto i = 0u; i < num_replicas; ++i) {
    const auto& node_conf = config.subscope(pool_config.NODE_VAR, i);
    const auto& replica_conf = node_conf.subscope(pool_config.REPLICA_VAR, 0);
    const auto replica_id = replica_conf.getValue<decltype(comm_config.nodes)::key_type>(pool_config.CLIENT_ID);
    NodeInfo node_info;
    node_info.host = replica_conf.getValue<decltype(node_info.host)>(pool_config.REPLICA_HOST);
    node_info.port = replica_conf.getValue<decltype(node_info.port)>(pool_config.REPLICA_PORT);
    node_info.isReplica = true;
    comm_config.nodes[replica_id] = node_info;
    all_replicas.insert(ReplicaId{static_cast<uint16_t>(i)});
  }
  // Ensure exception safety by creating local pointers and only moving to
  // object members if construction and startup haven't thrown.
  LOG_DEBUG(logger_, "Client_id=" << client_id_ << " Creating communication module");
  if (enable_mock_comm_) {
    if (delayed_behaviour_) {
      std::unique_ptr<FakeCommunication> fakecomm(new FakeCommunication(delayedBehaviour));
      comm_ = std::move(fakecomm);
    } else {
      std::unique_ptr<FakeCommunication> fakecomm(new FakeCommunication(immideateBehaviour));
      comm_ = std::move(fakecomm);
    }
  } else {
    auto comm = pool_config.ToCommunication(comm_config);
    comm_ = std::move(comm);
  }
  static constexpr char transaction_signing_plain_file_name[] = "transaction_signing_priv.pem";
  static constexpr char transaction_signing_enc_file_name[] = "transaction_signing_priv.pem.enc";

  LOG_DEBUG(logger_, "Client_id=" << client_id_ << " starting communication and creating new bft client instance");
  ClientConfig cfg;
  cfg.all_replicas = all_replicas;
  cfg.c_val = cVal;
  cfg.f_val = fVal;
  cfg.id = ClientId{clientId};
  cfg.retry_timeout_config = RetryTimeoutConfig{std::chrono::milliseconds(client_params.clientInitialRetryTimeoutMilli),
                                                std::chrono::milliseconds(client_params.clientMinRetryTimeoutMilli),
                                                std::chrono::milliseconds(client_params.clientMaxRetryTimeoutMilli),
                                                client_params.numberOfStandardDeviationsToTolerate,
                                                client_params.samplesPerEvaluation,
                                                static_cast<int16_t>(client_params.samplesUntilReset)};

  bool transaction_signing_enabled = false;
  if (config.hasValue<bool>(pool_config.TRANSACTION_SIGNING_ENABLED))
    transaction_signing_enabled = config.getValue<bool>(pool_config.TRANSACTION_SIGNING_ENABLED);
  if (transaction_signing_enabled) {
    std::string priv_key_path = config.getValue<std::string>(pool_config.TRANSACTION_SIGNING_KEY_PATH);
    const char* transaction_signing_file_name = transaction_signing_plain_file_name;

    if (comm_config.secretData) {
      transaction_signing_file_name = transaction_signing_enc_file_name;
      cfg.secrets_manager_config = comm_config.secretData;
    }
    cfg.transaction_signing_private_key_file_path = priv_key_path + std::string("/") + transaction_signing_file_name;
  }
  auto new_client = std::unique_ptr<bft::client::Client>{new bft::client::Client(std::move(comm_), cfg)};
  new_client_ = std::move(new_client);
  seqGen_ = bftEngine::SeqNumberGeneratorForClientRequests::createSeqNumberGeneratorForClientRequests();
  LOG_INFO(logger_, "client_id=" << client_id_ << " creation succeeded");
}  // namespace concord::external_client

void ConcordClient::CreateClient(const ConcordConfiguration& config,
                                 ConcordClientConfiguration& config_struct,
                                 const SimpleClientParams& client_params) {
  const auto num_replicas = config_struct.num_replicas;
  const auto& nodes = std::move(config_struct.participant_nodes);
  const auto& node = nodes.at(0);
  const auto& external_clients_conf = node.externalClients;
  const auto& client_conf = external_clients_conf.at(0);
  auto fVal = config_struct.f_val;
  auto cVal = config_struct.c_val;
  auto clientId = client_conf.principal_id;
  enable_mock_comm_ = config_struct.enable_mock_comm;
  CommConfig comm_config;
  CreateCommConfig(comm_config, config, num_replicas, config_struct);
  client_id_ = clientId;
  std::set<ReplicaId> all_replicas;
  const auto& node_conf = std::move(config_struct.node);
  for (auto i = 0u; i < num_replicas; ++i) {
    const auto& replica_conf = node_conf[0];
    const auto replica_id = replica_conf.principal_id;
    NodeInfo node_info;
    node_info.host = replica_conf.replica_host;
    node_info.port = replica_conf.replica_port;
    node_info.isReplica = true;
    comm_config.nodes[replica_id] = node_info;
    all_replicas.insert(ReplicaId{static_cast<uint16_t>(i)});
  }
  // Ensure exception safety by creating local pointers and only moving to
  // object members if construction and startup haven't thrown.
  LOG_DEBUG(logger_, "Client_id=" << client_id_ << " Creating communication module");
  if (enable_mock_comm_) {
    if (delayed_behaviour_) {
      std::unique_ptr<FakeCommunication> fakecomm(new FakeCommunication(delayedBehaviour));
      comm_ = std::move(fakecomm);
    } else {
      std::unique_ptr<FakeCommunication> fakecomm(new FakeCommunication(immideateBehaviour));
      comm_ = std::move(fakecomm);
    }
  } else {
    auto comm = ClientPoolConfig::ToCommunication(comm_config);
    comm_ = std::move(comm);
  }
  static constexpr char transaction_signing_plain_file_name[] = "transaction_signing_priv.pem";
  static constexpr char transaction_signing_enc_file_name[] = "transaction_signing_priv.pem.enc";

  LOG_DEBUG(logger_, "Client_id=" << client_id_ << " starting communication and creating new bft client instance");
  ClientConfig cfg;
  cfg.all_replicas = all_replicas;
  cfg.c_val = cVal;
  cfg.f_val = fVal;
  cfg.id = ClientId{clientId};
  cfg.retry_timeout_config = RetryTimeoutConfig{std::chrono::milliseconds(client_params.clientInitialRetryTimeoutMilli),
                                                std::chrono::milliseconds(client_params.clientMinRetryTimeoutMilli),
                                                std::chrono::milliseconds(client_params.clientMaxRetryTimeoutMilli),
                                                client_params.numberOfStandardDeviationsToTolerate,
                                                client_params.samplesPerEvaluation,
                                                static_cast<int16_t>(client_params.samplesUntilReset)};

  bool transaction_signing_enabled = config_struct.transaction_signing_enabled;
  if (transaction_signing_enabled) {
    std::string priv_key_path = std::move(config_struct.signing_key_path);
    const char* transaction_signing_file_name = transaction_signing_plain_file_name;

    if (comm_config.secretData) {
      transaction_signing_file_name = transaction_signing_enc_file_name;
      cfg.secrets_manager_config = comm_config.secretData;
    }
    cfg.transaction_signing_private_key_file_path = priv_key_path + std::string("/") + transaction_signing_file_name;
  }
  auto new_client = std::unique_ptr<bft::client::Client>{new bft::client::Client(std::move(comm_), cfg)};
  new_client_ = std::move(new_client);
  seqGen_ = bftEngine::SeqNumberGeneratorForClientRequests::createSeqNumberGeneratorForClientRequests();
  LOG_INFO(logger_, "client_id=" << client_id_ << " creation succeeded");
}

int ConcordClient::getClientId() const { return client_id_; }

uint64_t ConcordClient::generateClientSeqNum() { return seqGen_->generateUniqueSequenceNumberForRequest(); }

void ConcordClient::setStartRequestTime() { start_job_time_ = std::chrono::steady_clock::now(); }

std::chrono::steady_clock::time_point ConcordClient::getStartRequestTime() const { return start_job_time_; }

void ConcordClient::setStartWaitingTime() { waiting_job_time_ = std::chrono::steady_clock::now(); }

std::chrono::steady_clock::time_point ConcordClient::getWaitingTime() const { return waiting_job_time_; }

bool ConcordClient::isServing() const { return new_client_->isServing(num_of_replicas_, required_num_of_replicas_); }

void ConcordClient::setStatics(uint16_t required_num_of_replicas,
                               uint16_t num_of_replicas,
                               uint32_t max_reply_size,
                               size_t batch_size) {
  ConcordClient::max_reply_size_ = max_reply_size;
  ConcordClient::reply_->resize((batch_size) ? batch_size * max_reply_size : max_reply_size_);
  ConcordClient::required_num_of_replicas_ = required_num_of_replicas;
  ConcordClient::num_of_replicas_ = num_of_replicas;
}

void ConcordClient::setReplyBuffer(char* buf, uint32_t size) {
  externalReplyBuffer = buf;
  externalReplyBufferSize = size;
}

void ConcordClient::unsetReplyBuffer() {
  externalReplyBuffer = nullptr;
  externalReplyBufferSize = 0;
}

void ConcordClient::setDelayFlagForTest(bool delay) { ConcordClient::delayed_behaviour_ = delay; }

bftEngine::OperationResult ConcordClient::getClientRequestError() { return clientRequestError_; }

void ConcordClient::stopClientComm() { new_client_->stop(); }

}  // namespace concord::external_client
