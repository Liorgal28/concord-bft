// Concord
//
// Copyright (c) 2020-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#include "concord_client_pool.hpp"

#include <sparse_merkle/base_types.h>
#include <mutex>
#include <thread>
#include <utility>

#include <opentracing/tracer.h>

#include "KeyExchangeMsg.hpp"
#include "OpenTracing.hpp"
#include "trace_contexts.hpp"

namespace concord::concord_client_pool {

using bftEngine::ClientMsgFlag;
using config::ConcordConfiguration;
using opentracing::Tracer;
using namespace config_pool;
using namespace bftEngine;
using namespace tracing;

static inline const std::string kEmptySpanContext = std::string("");

std::string TextMapToString(const TextMap &text_map) {
  return std::accumulate(text_map.begin(),
                         text_map.end(),
                         std::string(),
                         [](const std::string &s, const std::pair<const std::string, std::string> &p) {
                           return s + (s.empty() ? std::string() : "|") + p.first + "=" + p.second;
                         });
}

SubmitResult ConcordClientPool::SendTracedRequest(std::vector<uint8_t> &&request,
                                                  ClientMsgFlag flags,
                                                  std::chrono::milliseconds timeout_ms,
                                                  char *reply_buffer,
                                                  std::uint32_t max_reply_size,
                                                  uint64_t seq_num,
                                                  std::string correlation_id,
                                                  const TextMap &w3c_textmap_span_context) {
  if (w3c_textmap_span_context.find("traceparent") != w3c_textmap_span_context.end()) {
    LOG_DEBUG(logger_,
              "W3C traceparent entry found in W3C text map '" << TextMapToString(w3c_textmap_span_context) << "'");
  }

  // Demonstrate how to connect a child span to one created by OpenTelemetry
  // in the ledger API server.
  auto span_context = W3cTextMapSpanContextToJaeger(w3c_textmap_span_context);
  if (span_context.isValid()) {
    LOG_DEBUG(logger_, "valid span context extracted from W3C text map");

    auto child_span_wrapper = concordUtils::startChildSpanFromContext(span_context, "bft_client_SendTracedRequest");
    LOG_DEBUG(logger_, "child span context created");

    auto span_context_blob = OpenTracingSpanContextToBlob(child_span_wrapper.impl()->context());
    LOG_DEBUG(logger_, "child span context converted to blob of size " << span_context_blob.size());

    return ConcordClientPool::SendRequest(std::forward<std::vector<uint8_t>>(request),
                                          flags,
                                          timeout_ms,
                                          reply_buffer,
                                          max_reply_size,
                                          seq_num,
                                          correlation_id,
                                          SpanSampler(span_context_blob));
  } else {
    // This isn't an error per se, as OpenTelemetry could be left unconfigured
    // in some deployments.
    LOG_DEBUG(logger_, "couldn't extract a valid span context from W3C text map");

    return ConcordClientPool::SendRequest(std::forward<std::vector<uint8_t>>(request),
                                          flags,
                                          timeout_ms,
                                          reply_buffer,
                                          max_reply_size,
                                          seq_num,
                                          correlation_id,
                                          kEmptySpanContext);
  }
}

static auto IsGoodForBatching(ClientMsgFlag flags, bool client_batching_enabled) {
  return flags & ClientMsgFlag::PRE_PROCESS_REQ && client_batching_enabled;
}

SubmitResult ConcordClientPool::SendRequest(std::vector<uint8_t> &&request,
                                            ClientMsgFlag flags,
                                            std::chrono::milliseconds timeout_ms,
                                            char *reply_buffer,
                                            std::uint32_t max_reply_size,
                                            uint64_t seq_num,
                                            std::string correlation_id,
                                            std::string span_context) {
  LOG_INFO(KEY_EX_LOG, timeout_ms.count());
  externalRequest external_request;
  std::unique_lock<std::mutex> lock(clients_queue_lock_);
  bool is_request_processed = false;
  auto serving_candidates = clients_.size();
  int client_id = 0;
  while (!clients_.empty() && !is_request_processed && serving_candidates != 0) {
    auto client = clients_.front();
    client_id = client->getClientId();
    if (is_overloaded_) {
      std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
      (void)end;
      /*waiting_time_summary_.Observe(
          std::chrono::duration_cast<std::chrono::milliseconds>(end - client->getWaitingTime()).count());*/
      is_overloaded_ = false;
    }
    if (!client->isServing()) {
      clients_.pop_front();
      clients_.push_back(client);
      --serving_candidates;
      continue;
    }
    if (0 == seq_num) {
      seq_num = client->generateClientSeqNum();
    }
    if (IsGoodForBatching(flags, client_batching_enabled_)) {
      if (0 == client->PendingRequestsCount()) {
        LOG_TRACE(logger_, "Set batching timer" << KVLOG(client_id));
        batch_timer_->set(client);
      }
      client->AddPendingRequest(
          std::move(request), flags, reply_buffer, timeout_ms, max_reply_size, seq_num, correlation_id, span_context);
      if (correlation_id.length() > SECOND_LEG_CID_LEN)
        metrics_->metrics_.first_leg_counter_++;
      else
        metrics_->metrics_.second_leg_counter_++;
      LOG_DEBUG(
          logger_,
          "Added request" << KVLOG(seq_num, correlation_id, client->PendingRequestsCount(), batch_size_, client_id));
      if (client->PendingRequestsCount() >= batch_size_) {
        clients_.pop_front();
        LOG_TRACE(logger_, "Cancel batching timer" << KVLOG(client_id));
        auto batch_wait_time = batch_timer_->cancel();
        (void)batch_wait_time;
        // batch_handle_dur_ms_.Observe(batch_wait_time.count());
        metrics_->metrics_.full_batch_counter_++;
        assignJobToClient(client);
      }
      is_request_processed = true;
    } else {
      clients_.pop_front();
      if (0 != client->PendingRequestsCount()) {
        LOG_TRACE(logger_, "Cancel batching timer" << KVLOG(client_id));
        auto batch_wait_time = batch_timer_->cancel();
        (void)batch_wait_time;
        // batch_handle_dur_ms_.Observe(batch_wait_time.count());
        metrics_->metrics_.full_batch_counter_++;
        assignJobToClient(client);
      } else {
        assignJobToClient(std::move(client),
                          std::move(request),
                          flags,
                          timeout_ms,
                          reply_buffer,
                          max_reply_size,
                          seq_num,
                          correlation_id,
                          span_context);
        is_request_processed = true;
      }
    }
  }
  if (!is_request_processed) {
    if (external_requests_queue_.size() < jobs_queue_max_size_) {
      LOG_DEBUG(logger_, "Request has been inserted to the wait queue" << KVLOG(correlation_id, seq_num));
      external_requests_queue_.emplace_back(externalRequest{std::move(request),
                                                            flags,
                                                            timeout_ms,
                                                            seq_num,
                                                            std::move(correlation_id),
                                                            std::move(span_context),
                                                            std::chrono::steady_clock::now(),
                                                            reply_buffer,
                                                            max_reply_size});
      return SubmitResult::Acknowledged;
    } else {
      metrics_->metrics_.rejected_counter_++;
      is_overloaded_ = true;
      LOG_WARN(logger_, "Cannot allocate client for" << KVLOG(correlation_id));
      return SubmitResult::Overloaded;
    }
  }
  LOG_INFO(logger_, "Request Acknowledged" << KVLOG(client_id, correlation_id, seq_num, flags));
  return SubmitResult::Acknowledged;
}

void ConcordClientPool::assignJobToClient(const ClientPtr &client) {
  LOG_TRACE(logger_, "Launching a batch job for" << KVLOG(client->getClientId()));
  client->setStartRequestTime();
  auto *job = new BatchRequestProcessingJob(*this, client);
  metrics_->metrics_.requests_counter_ += client->PendingRequestsCount();
  metrics_->metrics_.size_of_batch_gauge_ = client->PendingRequestsCount();
  metrics_->metrics_.clients_gauge_--;
  jobs_thread_pool_.add(job);
}

void ConcordClientPool::assignJobToClient(ClientPtr client,
                                          std::vector<uint8_t> &&request,
                                          ClientMsgFlag flags,
                                          std::chrono::milliseconds timeout_ms,
                                          char *reply_buffer,
                                          std::uint32_t max_reply_size,
                                          uint64_t seq_num,
                                          std::string correlation_id,
                                          std::string span_context) {
  if (max_reply_size) client->setReplyBuffer(reply_buffer, max_reply_size);

  LOG_INFO(logger_,
           "client_id=" << client->getClientId() << " starts handling reqSeqNum=" << seq_num << " cid="
                        << correlation_id << " span_context exists=" << !span_context.empty() << " flags=" << flags
                        << " request_size=" << request.size() << " timeout_ms=" << timeout_ms.count());

  client->setStartRequestTime();
  auto *job = new SingleRequestProcessingJob(
      *this, client, std::move(request), flags, timeout_ms, correlation_id, seq_num, span_context);
  metrics_->metrics_.requests_counter_++;
  metrics_->metrics_.clients_gauge_--;
  jobs_thread_pool_.add(job);
}

SubmitResult ConcordClientPool::SendRequest(const bft::client::WriteConfig &config, bft::client::Msg &&request) {
  LOG_DEBUG(logger_, "Received write request with cid=" << config.request.correlation_id);
  auto request_flag = ClientMsgFlag::EMPTY_FLAGS_REQ;
  if (config.request.pre_execute) request_flag = ClientMsgFlag::PRE_PROCESS_REQ;
  return SendRequest(std::forward<std::vector<uint8_t>>(request),
                     request_flag,
                     config.request.timeout,
                     nullptr,
                     0,
                     config.request.sequence_number,
                     config.request.correlation_id,
                     config.request.span_context);
}

SubmitResult ConcordClientPool::SendTracedRequest(const bft::client::WriteConfig &config,
                                                  bft::client::Msg &&request,
                                                  const TextMap &w3c_textmap_span_context) {
  LOG_DEBUG(logger_, "Received write request received with cid=" << config.request.correlation_id);
  auto request_flag = ClientMsgFlag::EMPTY_FLAGS_REQ;
  if (config.request.pre_execute) request_flag = ClientMsgFlag::PRE_PROCESS_REQ;
  return SendTracedRequest(std::forward<std::vector<uint8_t>>(request),
                           request_flag,
                           config.request.timeout,
                           nullptr,
                           0,
                           config.request.sequence_number,
                           config.request.correlation_id,
                           w3c_textmap_span_context);
}

SubmitResult ConcordClientPool::SendRequest(const bft::client::ReadConfig &config, bft::client::Msg &&request) {
  LOG_INFO(logger_, "Received read request with cid=" << config.request.correlation_id);
  return SendRequest(std::forward<std::vector<uint8_t>>(request),
                     ClientMsgFlag::READ_ONLY_REQ,
                     config.request.timeout,
                     nullptr,
                     0,
                     config.request.sequence_number,
                     config.request.correlation_id,
                     config.request.span_context);
}

/*std::unique_ptr<ConcordClientPool> ConcordClientPool::create(ConcordClientConfiguration config_struct) {
  return std::make_unique<ConcordClientPool>(config_struct);
}*/

ConcordClientPool::ConcordClientPool(ConcordClientConfiguration &config_struct, ConcordClientPoolMetrics *metrics)
    : metrics_(metrics), logger_(logging::getLogger("com.vmware.external_client_pool")) {
  ConcordConfiguration config;
  try {
    CreatePool(config_struct, config);
  } catch (config::ConfigurationResourceNotFoundException &e) {
    throw InternalError();
  } catch (std::invalid_argument &e) {
    LOG_ERROR(logger_, "Communication protocol=" << config_struct.comm_to_use << " is not supported");
    throw InternalError();
  } catch (config::InvalidConfigurationInputException &e) {
    throw InternalError();
  }
}

ConcordClientPool::ConcordClientPool(ConcordClientConfiguration &config_struct,
                                     ConcordClientPoolMetrics *metrics,
                                     bool delay_behavior)
    : metrics_(metrics), logger_(logging::getLogger("com.vmware.external_client_pool")) {
  ConcordConfiguration config;
  concord::external_client::ConcordClient::setDelayFlagForTest(delay_behavior);
  try {
    LOG_ERROR(KEY_EX_LOG, "LIOR1=" << config_struct.comm_to_use);
    CreatePool(config_struct, config);
  } catch (config::ConfigurationResourceNotFoundException &e) {
    throw InternalError();
  } catch (std::invalid_argument &e) {
    LOG_ERROR(logger_, "Communication protocol=" << config_struct.comm_to_use << " is not supported");
    throw InternalError();
  } catch (config::InvalidConfigurationInputException &e) {
    throw InternalError();
  }
}

void ConcordClientPool::setUpClientParams(SimpleClientParams &client_params,
                                          const ConcordConfiguration &config,
                                          const ClientPoolConfig &pool_config) {
  client_params.clientInitialRetryTimeoutMilli = config.getValue<uint16_t>(pool_config.INITIAL_RETRY_TIMEOUT);
  client_params.clientMinRetryTimeoutMilli = config.getValue<uint16_t>(pool_config.MIN_RETRY_TIMEOUT);
  client_params.clientMaxRetryTimeoutMilli = config.getValue<uint16_t>(pool_config.MAX_RETRY_TIMEOUT);
  if (client_params.clientInitialRetryTimeoutMilli < client_params.clientMinRetryTimeoutMilli ||
      client_params.clientInitialRetryTimeoutMilli > client_params.clientMaxRetryTimeoutMilli) {
    throw config::InvalidConfigurationInputException{
        "the initial timeout= " + std::to_string(client_params.clientInitialRetryTimeoutMilli) +
        " should be between min timeout= " + std::to_string(client_params.clientMinRetryTimeoutMilli) +
        " to max timeout= " + std::to_string(client_params.clientMaxRetryTimeoutMilli)};
  }
  client_params.numberOfStandardDeviationsToTolerate =
      config.getValue<uint16_t>(pool_config.STANDARD_DEVIATIONS_TO_TOLERATE);
  client_params.samplesPerEvaluation = config.getValue<uint16_t>(pool_config.SAMPLES_PER_EVALUATION);
  client_params.samplesUntilReset = config.getValue<uint16_t>(pool_config.SAMPLES_UNTIL_RESET);
  client_params.clientSendsRequestToAllReplicasFirstThresh = config.getValue<uint16_t>(pool_config.FIRST_THRESH);
  client_params.clientSendsRequestToAllReplicasPeriodThresh = config.getValue<uint16_t>(pool_config.PERIODIC_THRESH);
  client_params.clientPeriodicResetThresh = config.getValue<uint16_t>(pool_config.RESET_THRESH);
  LOG_INFO(logger_,
           "clientInitialRetryTimeoutMilli="
               << client_params.clientInitialRetryTimeoutMilli
               << " clientMinRetryTimeoutMilli=" << client_params.clientMinRetryTimeoutMilli
               << " clientMaxRetryTimeoutMilli=" << client_params.clientMaxRetryTimeoutMilli
               << " numberOfStandardDeviationsToTolerate=" << client_params.numberOfStandardDeviationsToTolerate
               << " samplesPerEvaluation=" << client_params.samplesPerEvaluation << " samplesUntilReset="
               << client_params.samplesUntilReset << " clientSendsRequestToAllReplicasFirstThresh="
               << client_params.clientSendsRequestToAllReplicasFirstThresh
               << " clientSendsRequestToAllReplicasPeriodThresh="
               << client_params.clientSendsRequestToAllReplicasPeriodThresh
               << " clientPeriodicResetThresh=" << client_params.clientPeriodicResetThresh);
}

void ConcordClientPool::setUpClientParams(SimpleClientParams &client_params,
                                          const concord::config_pool::ConcordClientConfiguration &struct_config) {
  client_params.clientInitialRetryTimeoutMilli = struct_config.client_initial_retry_timeout_milli;
  client_params.clientMinRetryTimeoutMilli = struct_config.client_min_retry_timeout_milli;
  client_params.clientMaxRetryTimeoutMilli = struct_config.client_max_retry_timeout_milli;
  if (client_params.clientInitialRetryTimeoutMilli < client_params.clientMinRetryTimeoutMilli ||
      client_params.clientInitialRetryTimeoutMilli > client_params.clientMaxRetryTimeoutMilli) {
    throw config::InvalidConfigurationInputException{
        "the initial timeout= " + std::to_string(client_params.clientInitialRetryTimeoutMilli) +
        " should be between min timeout= " + std::to_string(client_params.clientMinRetryTimeoutMilli) +
        " to max timeout= " + std::to_string(client_params.clientMaxRetryTimeoutMilli)};
  }
  client_params.numberOfStandardDeviationsToTolerate = struct_config.client_number_of_standard_deviations_to_tolerate;
  client_params.samplesPerEvaluation = struct_config.client_samples_per_evaluation;
  client_params.samplesUntilReset = struct_config.client_samples_until_reset;
  client_params.clientSendsRequestToAllReplicasFirstThresh =
      struct_config.client_sends_request_to_all_replicas_first_thresh;
  client_params.clientSendsRequestToAllReplicasPeriodThresh =
      struct_config.client_sends_request_to_all_replicas_period_thresh;
  client_params.clientPeriodicResetThresh = struct_config.client_periodic_reset_thresh;
  LOG_INFO(logger_,
           "clientInitialRetryTimeoutMilli="
               << client_params.clientInitialRetryTimeoutMilli
               << " clientMinRetryTimeoutMilli=" << client_params.clientMinRetryTimeoutMilli
               << " clientMaxRetryTimeoutMilli=" << client_params.clientMaxRetryTimeoutMilli
               << " numberOfStandardDeviationsToTolerate=" << client_params.numberOfStandardDeviationsToTolerate
               << " samplesPerEvaluation=" << client_params.samplesPerEvaluation << " samplesUntilReset="
               << client_params.samplesUntilReset << " clientSendsRequestToAllReplicasFirstThresh="
               << client_params.clientSendsRequestToAllReplicasFirstThresh
               << " clientSendsRequestToAllReplicasPeriodThresh="
               << client_params.clientSendsRequestToAllReplicasPeriodThresh
               << " clientPeriodicResetThresh=" << client_params.clientPeriodicResetThresh);
}

void ConcordClientPool::CreatePool(std::istream &config_stream, ConcordConfiguration &config) {
  auto pool_config = std::make_unique<config_pool::ClientPoolConfig>();
  ConfigInit(config, *pool_config, config_stream);
  PrometheusInit(config, *pool_config);
  auto num_clients = config.getValue<std::uint16_t>(pool_config->NUM_EXTERNAL_CLIENTS);
  metrics_->metrics_.clients_gauge_ = num_clients;
  LOG_INFO(logger_, "Creating pool" << KVLOG(num_clients));
  auto f_val = config.getValue<uint16_t>(pool_config->F_VAL);
  auto c_val = config.getValue<uint16_t>(pool_config->C_VAL);
  auto clients_per_replica = config.getValue<uint16_t>(pool_config->CLIENT_PROXIES_PER_REPLICA);
  transaction_count = 0;
  if (config.hasValue<std::uint32_t>(pool_config->TRACE_SAMPLING_RATE)) {
    span_rate = config.getValue<std::uint32_t>(pool_config->TRACE_SAMPLING_RATE);
  } else {
    LOG_INFO(logger_, "Trace sampling rate not configured");
    span_rate = 0;
  }
  LOG_INFO(logger_, "Trace sampling rate set to: " << span_rate);

  auto max_buf_size = stol(config.getValue<std::string>(pool_config->COMM_BUFF_LEN));
  const auto num_replicas = 3 * f_val + 2 * c_val + 1;
  const auto required_num_of_replicas = 2 * f_val + 1;

  auto timeout = std::chrono::milliseconds{0UL};
  if (config.hasValue<bool>(pool_config->CLIENT_BATCHING_ENABLED) &&
      config.getValue<bool>(pool_config->CLIENT_BATCHING_ENABLED)) {
    batch_size_ = config.getValue<size_t>(pool_config->CLIENT_BATCHING_MAX_MSG_NUM);
    timeout = std::chrono::milliseconds(config.getValue<uint64_t>(pool_config->CLIENT_BATCHING_TIMEOUT_MILLI));
    client_batching_enabled_ = true;
    LOG_INFO(logger_,
             "Batching for client pool is enabled with the next params: "
             "timeout="
                 << timeout.count() << " ms, batch size=" << batch_size_);
  } else {
    LOG_INFO(logger_, "Batching for client pool is disabled");
  }
  batch_timer_ = std::make_unique<Timer_t>(timeout, [this](ClientPtr client) -> void { OnBatchingTimeout(client); });
  external_client::ConcordClient::setStatics(required_num_of_replicas, num_replicas, max_buf_size, batch_size_);
  auto num_of_principals = num_replicas * clients_per_replica + num_replicas;
  bftEngine::SimpleClientParams clientParams;
  setUpClientParams(clientParams, config, *pool_config);
  for (int i = 0; i < num_clients; i++) {
    auto client_id = num_of_principals + i;
    LOG_DEBUG(logger_, "Creating client" << KVLOG(client_id));
    clients_.push_back(std::make_shared<external_client::ConcordClient>(config, i, *pool_config, clientParams));
  }
  jobs_thread_pool_.start(num_clients);
  if (config.hasValue<std::uint32_t>("external_requests_queue_size") &&
      config.getValue<std::uint32_t>("external_requests_queue_size") >= 0)
    jobs_queue_max_size_ = config.getValue<std::uint32_t>("external_requests_queue_size");
  else
    jobs_queue_max_size_ = 0;
}

void ConcordClientPool::CreatePool(concord::config_pool::ConcordClientConfiguration &config_struct,
                                   ConcordConfiguration &config) {
  auto num_clients = config_struct.clients_per_participant_node;
  metrics_->metrics_.clients_gauge_ = num_clients;
  LOG_INFO(logger_, "Creating pool" << KVLOG(num_clients));
  auto f_val = config_struct.f_val;
  auto c_val = config_struct.c_val;
  auto max_buf_size = stol(config_struct.concord_bft_communication_buffer_length);
  const auto num_replicas = 3 * f_val + 2 * c_val + 1;
  const auto required_num_of_replicas = 2 * f_val + 1;

  auto timeout = std::chrono::milliseconds{0UL};
  if (config_struct.client_batching_enabled) {
    batch_size_ = config_struct.client_batching_max_messages_nbr;
    timeout = std::chrono::milliseconds(config_struct.client_batching_flush_timeout_ms);
    client_batching_enabled_ = true;
    LOG_INFO(logger_,
             "Batching for client pool is enabled with the next params: "
             "timeout="
                 << timeout.count() << " ms, batch size=" << batch_size_);
  } else {
    LOG_INFO(logger_, "Batching for client pool is disabled");
  }
  batch_timer_ = std::make_unique<Timer_t>(timeout, [this](ClientPtr client) -> void { OnBatchingTimeout(client); });
  external_client::ConcordClient::setStatics(required_num_of_replicas, num_replicas, max_buf_size, batch_size_);
  bftEngine::SimpleClientParams clientParams;
  setUpClientParams(clientParams, config_struct);

  for (int i = 0; i < num_clients; i++) {
    clients_.push_back(std::make_shared<external_client::ConcordClient>(config, i, config_struct, clientParams));
  }
  jobs_thread_pool_.start(num_clients);
  jobs_queue_max_size_ = config_struct.external_requests_queue_size;
}

void ConcordClientPool::OnBatchingTimeout(ClientPtr client) {
  {
    std::unique_lock<std::mutex> lock(clients_queue_lock_);
    const auto client_id = client->getClientId();
    LOG_INFO(logger_,
             "Client reached batching timeout" << KVLOG(client_id, batch_size_, client->PendingRequestsCount()));
    if (client != clients_.front()) {
      LOG_DEBUG(logger_, "Client is already processing other requests" << KVLOG(client_id));
      return;
    }
    clients_.pop_front();
  }
  metrics_->metrics_.partial_batch_counter_++;
  assignJobToClient(client);
}

void ConcordClientPool::ConfigInit(config::ConcordConfiguration &config,
                                   ClientPoolConfig &pool_config,
                                   std::istream &config_stream) {
  pool_config.ParseConfig(config_stream, config);
}

ConcordClientPool::~ConcordClientPool() {
  batch_timer_->stop();
  jobs_thread_pool_.stop(true);
  std::unique_lock<std::mutex> clients_lock(clients_queue_lock_);
  for (auto &client : clients_) {
    client->stopClientComm();
  }
  clients_.clear();
  LOG_INFO(logger_, "Clients cleanup complete");
}

void ConcordClientPool::SetDoneCallback(EXT_DONE_CALLBACK cb) { done_callback_ = std::move(cb); }

void ConcordClientPool::Done(std::pair<int8_t, external_client::ConcordClient::PendingReplies> &&replies) {
  if (done_callback_) {
    for (const auto &reply : replies.second) {
      LOG_DEBUG(logger_, "Return client reply to the sender" << KVLOG(reply.cid, reply.actualReplyLength));
      done_callback_(reply.cid, reply.actualReplyLength);
    }
  }
}

void BatchRequestProcessingJob::execute() {
  clients_pool_.InsertClientToQueue(processing_client_, processing_client_->SendPendingRequests());
}

void SingleRequestProcessingJob::execute() {
  uint32_t reply_size;
  bft::client::Reply res;
  if (flags_ & READ_ONLY_REQ) {
    read_config_.request.timeout = timeout_ms_;
    read_config_.request.sequence_number = seq_num_;
    read_config_.request.correlation_id = correlation_id_;
    read_config_.request.span_context = span_context_;
    res = processing_client_->SendRequest(read_config_, std::move(request_));
    reply_size = res.matched_data.size();
  } else {
    write_config_.request.timeout = timeout_ms_;
    write_config_.request.sequence_number = seq_num_;
    write_config_.request.correlation_id = correlation_id_;
    write_config_.request.span_context = span_context_;
    write_config_.request.pre_execute = flags_ & PRE_PROCESS_REQ;
    res = processing_client_->SendRequest(write_config_, std::move(request_));
    reply_size = res.matched_data.size();
  }
  external_client::ConcordClient::PendingReplies replies;
  replies.push_back(ClientReply{static_cast<uint32_t>(request_.size()),
                                nullptr,
                                reply_size,
                                OperationResult::SUCCESS,
                                correlation_id_,
                                span_context_});
  clients_pool_.InsertClientToQueue(processing_client_, {0, std::move(replies)});
}

void ConcordClientPool::InsertClientToQueue(
    ClientPtr &client, std::pair<int8_t, external_client::ConcordClient::PendingReplies> &&replies) {
  const auto client_id = client->getClientId();
  LOG_DEBUG(logger_, "Client has completed processing request" << KVLOG(client_id));
  std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - client->getStartRequestTime()).count();
  metrics_->metrics_.last_request_time_gauge_ = duration;
  // request_handle_dur_ms_.Observe(duration);
  metrics_->metrics_.clients_gauge_++;
  metrics_->metrics_.executed_requests_counter_++;
  client->unsetReplyBuffer();
  {
    std::unique_lock<std::mutex> lock(clients_queue_lock_);
    while (!external_requests_queue_.empty() && client->PendingRequestsCount() < batch_size_) {
      auto &req = external_requests_queue_.front();
      auto remaining_time =
          std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - req.arrival_time);

      if (remaining_time > req.timeout_ms) {
        LOG_INFO(logger_,
                 "Dropping request due to timeout"
                     << KVLOG(client_id, req.seq_num, req.correlation_id, req.timeout_ms.count()));
        external_requests_queue_.pop_front();
        continue;
      }

      if (IsGoodForBatching(req.flags, client_batching_enabled_)) {
        if (0 == client->PendingRequestsCount()) {
          LOG_TRACE(logger_, "Set batching timer for client" << KVLOG(client_id));
          batch_timer_->set(client);
        }
        client->AddPendingRequest(std::move(req.request),
                                  req.flags,
                                  req.reply_buffer,
                                  req.timeout_ms,
                                  req.reply_size,
                                  req.seq_num,
                                  req.correlation_id,
                                  req.span_context);

        LOG_DEBUG(logger_,
                  "Added request to the client" << KVLOG(
                      client_id, req.seq_num, req.correlation_id, client->PendingRequestsCount(), batch_size_));
        external_requests_queue_.pop_front();
      } else {
        // No need to loop anymore
        break;
      }
    }
    if (client->PendingRequestsCount() > 0) {
      if (client->PendingRequestsCount() >= batch_size_) {
        LOG_TRACE(logger_, "Cancel batching timer for client_id=" << client->getClientId());
        auto batch_wait_time = batch_timer_->cancel();
        (void)batch_wait_time;
        // batch_handle_dur_ms_.Observe(batch_wait_time.count());
        metrics_->metrics_.full_batch_counter_++;
        assignJobToClient(client);
      } else {
        if (is_overloaded_) {
          client->setStartWaitingTime();
        }
        LOG_TRACE(logger_, "Return client with pending jobs to the queue" << KVLOG(client_id));
        clients_.push_back(client);
      }
    } else {
      if (!external_requests_queue_.empty()) {
        auto req = std::move(external_requests_queue_.front());
        external_requests_queue_.pop_front();

        assignJobToClient(std::move(client),
                          std::move(req.request),
                          req.flags,
                          req.timeout_ms,
                          req.reply_buffer,
                          req.reply_size,
                          req.seq_num,
                          req.correlation_id,
                          req.span_context);
      } else {
        clients_.push_back(client);
      }
    }
  }
  Done(std::move(replies));
}

PoolStatus ConcordClientPool::HealthStatus() {
  std::unique_lock<std::mutex> lock(clients_queue_lock_);
  for (auto &client : clients_) {
    if (client->isServing()) {
      if (!hasKeys_ && !(hasKeys_ = clusterHasKeys(client))) {
        break;
      }
      LOG_INFO(logger_, "client_id=" << client->getClientId() << " is serving - the pool is ready");
      return PoolStatus::Serving;
    }
  }
  LOG_DEBUG(logger_, "None of clients is serving - the pool is not ready");
  return PoolStatus::NotServing;
}

bool ConcordClientPool::clusterHasKeys(ClientPtr &cl) {
  KeyExchangeMsg msg;
  msg.op = KeyExchangeMsg::HAS_KEYS;
  std::stringstream ss;
  concord::serialize::Serializable::serialize(ss, msg);
  auto request = ss.str();

  auto now = std::chrono::steady_clock::now().time_since_epoch();
  auto now_ms = std::chrono::duration_cast<std::chrono::microseconds>(now);
  auto sn = now_ms.count();
  auto trueReply = std::string(KeyExchangeMsg::hasKeysTrueReply);
  bft::client::ReadConfig config;
  config.request.max_reply_size = 32;
  config.request.correlation_id = std::string{"HAS-KEYS-"} + std::to_string(sn);
  config.request.key_exchange = true;
  config.request.timeout = std::chrono::milliseconds(60000);
  config.request.sequence_number = sn;
  auto res = cl->SendRequest(config, bft::client::Msg{request.begin(), request.end()});
  std::string result(res.matched_data.begin(), res.matched_data.end());
  LOG_INFO(logger_,
           "Reply for HAS_KEYS request [" << config.request.correlation_id << "] is " << std::boolalpha
                                          << (result == trueReply) << std::noboolalpha);
  return result == trueReply;
}

std::string ConcordClientPool::SpanSampler(std::string &span_blob) {
  if (span_rate <= 0) return kEmptySpanContext;
  if (span_rate == 1) return span_blob;
  if (transaction_count == 0) {
    transaction_count++;
    return span_blob;
  }
  transaction_count++;
  if (transaction_count == span_rate) {
    transaction_count = 0;
  }
  return kEmptySpanContext;
}

}  // namespace concord::concord_client_pool
