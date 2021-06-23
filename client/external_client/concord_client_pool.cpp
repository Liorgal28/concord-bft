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

#include <mutex>
#include <thread>
#include <utility>

#include "KeyExchangeMsg.hpp"
#include "concord_client_pool.hpp"

namespace concord::concord_client_pool {

using bftEngine::ClientMsgFlag;
using namespace config_pool;
using namespace bftEngine;

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
  externalRequest external_request;
  std::unique_lock<std::mutex> lock(clients_queue_lock_);
  metricsComponent_.UpdateAggregator();
  bool is_request_processed = false;
  auto serving_candidates = clients_.size();
  int client_id = 0;
  while (!clients_.empty() && !is_request_processed && serving_candidates != 0) {
    auto client = clients_.front();
    client_id = client->getClientId();
    if (is_overloaded_) {
      std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
      (void)end;
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
        ClientPoolMetrics_.first_leg_counter_.Get().Inc();
      else
        ClientPoolMetrics_.second_leg_counter_.Get().Inc();
      LOG_DEBUG(
          logger_,
          "Added request" << KVLOG(seq_num, correlation_id, client->PendingRequestsCount(), batch_size_, client_id));
      if (client->PendingRequestsCount() >= batch_size_) {
        clients_.pop_front();
        LOG_TRACE(logger_, "Cancel batching timer" << KVLOG(client_id));
        auto batch_wait_time = batch_timer_->cancel();
        (void)batch_wait_time;
        ClientPoolMetrics_.full_batch_counter_.Get().Inc();
        assignJobToClient(client);
      }
      is_request_processed = true;
    } else {
      clients_.pop_front();
      if (0 != client->PendingRequestsCount()) {
        LOG_TRACE(logger_, "Cancel batching timer" << KVLOG(client_id));
        auto batch_wait_time = batch_timer_->cancel();
        (void)batch_wait_time;
        ClientPoolMetrics_.full_batch_counter_.Get().Inc();
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
      ClientPoolMetrics_.rejected_counter_.Get().Inc();
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
  ClientPoolMetrics_.requests_counter_.Get().Inc(client->PendingRequestsCount());
  ClientPoolMetrics_.size_of_batch_gauge_.Get().Set(client->PendingRequestsCount());
  ClientPoolMetrics_.clients_gauge_.Get().Dec();
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
  ClientPoolMetrics_.requests_counter_.Get().Inc();
  ClientPoolMetrics_.clients_gauge_.Get().Dec();
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

std::unique_ptr<ConcordClientPool> ConcordClientPool::create(ConcordClientConfiguration config_struct,
                                                             std::shared_ptr<concordMetrics::Aggregator> aggregator) {
  return std::make_unique<ConcordClientPool>(config_struct, aggregator);
}

ConcordClientPool::ConcordClientPool(ConcordClientConfiguration &config_struct,
                                     std::shared_ptr<concordMetrics::Aggregator> aggregator)
    : logger_(logging::getLogger("com.vmware.external_client_pool")),
      metricsComponent_{concordMetrics::Component("preProcessor", std::make_shared<concordMetrics::Aggregator>())},
      ClientPoolMetrics_{metricsComponent_.RegisterCounter("requests_counter"),
                         metricsComponent_.RegisterCounter("executed_requests_counter"),
                         metricsComponent_.RegisterCounter("rejected_counter"),
                         metricsComponent_.RegisterCounter("full_batch_counter"),
                         metricsComponent_.RegisterCounter("partial_batch_counter"),
                         metricsComponent_.RegisterCounter("first_leg_counter"),
                         metricsComponent_.RegisterCounter("second_leg_counter"),
                         metricsComponent_.RegisterGauge("size_of_batch_gauge", 0),
                         metricsComponent_.RegisterGauge("clients_gauge", 0),
                         metricsComponent_.RegisterGauge("last_request_time_gauge", 0)} {
  try {
    CreatePool(config_struct);
  } catch (std::invalid_argument &e) {
    LOG_ERROR(logger_, "Communication protocol=" << config_struct.comm_to_use << " is not supported");
    throw InternalError();
  } catch (std::exception &e) {
    throw InternalError();
  }
}

ConcordClientPool::ConcordClientPool(ConcordClientConfiguration &config_struct,
                                     std::shared_ptr<concordMetrics::Aggregator> aggregator,
                                     bool delay_behavior)
    : logger_(logging::getLogger("com.vmware.external_client_pool")),
      metricsComponent_{concordMetrics::Component("preProcessor", std::make_shared<concordMetrics::Aggregator>())},
      ClientPoolMetrics_{metricsComponent_.RegisterCounter("requests_counter"),
                         metricsComponent_.RegisterCounter("executed_requests_counter"),
                         metricsComponent_.RegisterCounter("rejected_counter"),
                         metricsComponent_.RegisterCounter("full_batch_counter"),
                         metricsComponent_.RegisterCounter("partial_batch_counter"),
                         metricsComponent_.RegisterCounter("first_leg_counter"),
                         metricsComponent_.RegisterCounter("second_leg_counter"),
                         metricsComponent_.RegisterGauge("size_of_batch_gauge", 0),
                         metricsComponent_.RegisterGauge("clients_gauge", 0),
                         metricsComponent_.RegisterGauge("last_request_time_gauge", 0)} {
  concord::external_client::ConcordClient::setDelayFlagForTest(delay_behavior);
  try {
    CreatePool(config_struct);
  } catch (std::invalid_argument &e) {
    LOG_ERROR(logger_, "Communication protocol=" << config_struct.comm_to_use << " is not supported");
    throw InternalError();
  } catch (std::exception &e) {
    throw InternalError();
  }
}

void ConcordClientPool::setUpClientParams(SimpleClientParams &client_params,
                                          const concord::config_pool::ConcordClientConfiguration &struct_config) {
  client_params.clientInitialRetryTimeoutMilli = struct_config.client_initial_retry_timeout_milli;
  client_params.clientMinRetryTimeoutMilli = struct_config.client_min_retry_timeout_milli;
  client_params.clientMaxRetryTimeoutMilli = struct_config.client_max_retry_timeout_milli;
  if (client_params.clientInitialRetryTimeoutMilli < client_params.clientMinRetryTimeoutMilli ||
      client_params.clientInitialRetryTimeoutMilli > client_params.clientMaxRetryTimeoutMilli) {
    throw std::invalid_argument{
        "the initial timeout= " + std::to_string(client_params.clientInitialRetryTimeoutMilli) +
        " should be between min timeout= " + std::to_string(client_params.clientMinRetryTimeoutMilli) +
        " to max timeout= " + std::to_string(client_params.clientMaxRetryTimeoutMilli)};
    /*throw config::InvalidConfigurationInputException{
        "the initial timeout= " + std::to_string(client_params.clientInitialRetryTimeoutMilli) +
        " should be between min timeout= " + std::to_string(client_params.clientMinRetryTimeoutMilli) +
        " to max timeout= " + std::to_string(client_params.clientMaxRetryTimeoutMilli)};*/
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

void ConcordClientPool::CreatePool(concord::config_pool::ConcordClientConfiguration &config_struct) {
  auto num_clients = config_struct.clients_per_participant_node;
  ClientPoolMetrics_.clients_gauge_.Get().Set(num_clients);
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
    clients_.push_back(std::make_shared<external_client::ConcordClient>(i, config_struct, clientParams));
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
  ClientPoolMetrics_.partial_batch_counter_.Get().Inc();
  assignJobToClient(client);
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
  ClientPoolMetrics_.last_request_time_gauge_.Get().Set(duration);
  // request_handle_dur_ms_.Observe(duration);
  ClientPoolMetrics_.clients_gauge_.Get().Inc();
  ClientPoolMetrics_.executed_requests_counter_.Get().Inc();
  client->unsetReplyBuffer();
  {
    std::unique_lock<std::mutex> lock(clients_queue_lock_);
    metricsComponent_.UpdateAggregator();
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
        ClientPoolMetrics_.full_batch_counter_.Get().Inc();
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

}  // namespace concord::concord_client_pool