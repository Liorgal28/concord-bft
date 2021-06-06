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

#include "client_pool_config.hpp"

namespace concord::config_pool {
using bft::communication::CommFactory;
using bft::communication::ICommunication;
using bft::communication::PlainUdpConfig;
using bft::communication::TlsTcpConfig;
using config::CommConfig;
using config::ConcordConfiguration;
using config::ConfigurationResourceNotFoundException;
using config::kConcordExternalClientConfigurationStateLabel;
using config::YAMLConfigurationInput;

ClientPoolConfig::ClientPoolConfig() { logger_ = logging::getLogger("com.vmware.external_client_pool"); }
void ClientPoolConfig::ParseConfig(std::istream& config_stream, config::ConcordConfiguration& config) {
  config::specifyExternalClientConfiguration(config);
  config.setConfigurationStateLabel(FILE_NAME);
  YAMLConfigurationInput yaml{config_stream};
  yaml.parseInput();
  // First, load all template parameters.
  yaml.loadConfiguration(config,
                         config.begin(ConcordConfiguration::kIterateAllTemplateParameters),
                         config.end(ConcordConfiguration::kIterateAllTemplateParameters));

  // Instantiate the replica scope before node scope.
  config.subscope(NODE_VAR).instantiateScope(REPLICA_VAR);
  config.instantiateScope(NODE_VAR);
  config.subscope(PARTICIPANT_NODES).subscope(PARTICIPANT_NODE).subscope(EXTERNAL_CLIENTS).instantiateScope(CLIENT);
  config.subscope(PARTICIPANT_NODES).subscope(PARTICIPANT_NODE).instantiateScope(EXTERNAL_CLIENTS);
  config.subscope(PARTICIPANT_NODES).instantiateScope(PARTICIPANT_NODE);
  config.instantiateScope(PARTICIPANT_NODES);

  // num_ro_replicas is required for validating principal id, but it should be
  // optional for backward compatibility. For this reason its default value is
  // loaded to cover the cases where the parameter is not set in the
  // configuration.
  config.loadDefault("num_ro_replicas", nullptr, false);

  // Next, load all features.
  yaml.loadConfiguration(
      config, config.begin(ConcordConfiguration::kIterateAll), config.end(ConcordConfiguration::kIterateAll));

  if (config.validateAll() != ConcordConfiguration::ParameterStatus::VALID) {
    LOG_ERROR(logger_, "Configuration file=" << config.getValue<std::string>(FILE_NAME) << " validation failed");
    throw ConfigurationResourceNotFoundException{"Node configuration complete validation failed."};
  }

  InitializeStructConfig(config);
}

void ClientPoolConfig::InitializeStructConfig(config::ConcordConfiguration& config) {
  if (config.hasValue<std::string>(FILE_NAME)) struct_config_.file_name = config.getValue<std::string>(FILE_NAME);
  if (config.hasValue<std::uint16_t>(STANDARD_DEVIATIONS_TO_TOLERATE))
    struct_config_.client_number_of_standard_deviations_to_tolerate =
        config.getValue<std::uint16_t>(STANDARD_DEVIATIONS_TO_TOLERATE);
  if (config.hasValue<std::uint16_t>(SAMPLES_PER_EVALUATION))
    struct_config_.client_samples_per_evaluation = config.getValue<std::uint16_t>(SAMPLES_PER_EVALUATION);
  if (config.hasValue<std::uint16_t>(SAMPLES_UNTIL_RESET))
    struct_config_.client_samples_until_reset = config.getValue<std::uint16_t>(SAMPLES_UNTIL_RESET);
  if (config.hasValue<std::uint16_t>(FIRST_THRESH))
    struct_config_.client_sends_request_to_all_replicas_first_thresh = config.getValue<std::uint16_t>(FIRST_THRESH);
  if (config.hasValue<std::uint16_t>(PERIODIC_THRESH))
    struct_config_.client_sends_request_to_all_replicas_period_thresh = config.getValue<std::uint16_t>(PERIODIC_THRESH);
  if (config.hasValue<std::uint16_t>(RESET_THRESH))
    struct_config_.client_periodic_reset_thresh = config.getValue<std::uint16_t>(RESET_THRESH);
  if (config.hasValue<std::uint16_t>(CLIENT_PROXIES_PER_REPLICA))
    struct_config_.client_proxies_per_replica = config.getValue<std::uint16_t>(CLIENT_PROXIES_PER_REPLICA);
  struct_config_.num_replicas = config.getValue<std::uint16_t>(NUM_REPLICAS);
  struct_config_.f_val = config.getValue<std::uint16_t>(F_VAL);
  struct_config_.c_val = config.getValue<uint16_t>(C_VAL);
  struct_config_.clients_per_participant_node = config.getValue<std::uint16_t>(NUM_EXTERNAL_CLIENTS);
  struct_config_.concord_bft_communication_buffer_length =
      std::to_string(stol(config.getValue<std::string>(COMM_BUFF_LEN)));
  struct_config_.client_batching_enabled =
      config.hasValue<bool>(CLIENT_BATCHING_ENABLED) && config.getValue<bool>(CLIENT_BATCHING_ENABLED);
  struct_config_.enable_mock_comm = config.hasValue<bool>(ENABLE_MOCK_COMM) && config.getValue<bool>(ENABLE_MOCK_COMM);
  if (struct_config_.client_batching_enabled) {
    struct_config_.client_batching_max_messages_nbr = config.getValue<size_t>(CLIENT_BATCHING_MAX_MSG_NUM);
    struct_config_.client_batching_flush_timeout_ms = config.getValue<uint64_t>(CLIENT_BATCHING_TIMEOUT_MILLI);
  }
  if (config.hasValue<std::uint32_t>(TRACE_SAMPLING_RATE))
    struct_config_.trace_sampling_rate = config.getValue<std::uint32_t>(TRACE_SAMPLING_RATE);
  if (config.hasValue<std::uint32_t>("external_requests_queue_size") &&
      config.getValue<std::uint32_t>("external_requests_queue_size") >= 0)
    struct_config_.external_requests_queue_size = config.getValue<std::uint32_t>("external_requests_queue_size");
  struct_config_.comm_to_use = config.getValue<std::string>(COMM_PROTOCOL);
  struct_config_.tls_certificates_folder_path = config.getValue<std::string>(CERT_FOLDER);
  struct_config_.tls_cipher_suite_list = config.getValue<std::string>(CIPHER_SUITE);
  struct_config_.encrypted_config_enabled =
      config.hasValue<bool>("encrypted_config_enabled") && config.getValue<bool>("encrypted_config_enabled");
  struct_config_.transaction_signing_enabled =
      config.hasValue<bool>(TRANSACTION_SIGNING_ENABLED) && config.getValue<bool>(TRANSACTION_SIGNING_ENABLED);

  if (struct_config_.encrypted_config_enabled) struct_config_.secrets_url = config.getValue<std::string>("secrets_url");
  if (struct_config_.transaction_signing_enabled)
    struct_config_.signing_key_path = config.getValue<std::string>(TRANSACTION_SIGNING_KEY_PATH);
  if (config.hasValue<std::string>(PROMETHEUS_PORT))
    struct_config_.prometheus_port = config.getValue<std::string>(PROMETHEUS_PORT);

  InitializeExternalClient(config);
  InitializeReplicaConfig(config);
}

void ClientPoolConfig::InitializeExternalClient(ConcordConfiguration& config) {
  std::unique_ptr<ParticipantNode> p_node = std::make_unique<ParticipantNode>();
  const auto& nodes = config.subscope(PARTICIPANT_NODES, 0);
  const auto& node = nodes.subscope(PARTICIPANT_NODE, 0);
  p_node->participant_node_host = node.getValue<std::string>(PROMETHEUS_HOST);
  for (int i = 0; i < struct_config_.clients_per_participant_node; i++) {
    const auto& external_clients_conf = node.subscope(EXTERNAL_CLIENTS, i);
    std::unique_ptr<ExternalClient> external_client = std::make_unique<ExternalClient>();
    const auto& client_conf = external_clients_conf.subscope(CLIENT, 0);
    external_client->principal_id = client_conf.getValue<uint16_t>(CLIENT_ID);
    external_client->client_port = client_conf.getValue<uint16_t>(CLIENT_PORT);
    p_node->externalClients.push_back(std::move(*external_client));
  }
  struct_config_.participant_nodes.push_back(std::move(*p_node));
}

void ClientPoolConfig::InitializeReplicaConfig(ConcordConfiguration& config) {
  for (auto i = 0u; i < struct_config_.num_replicas; ++i) {
    const auto& node_conf = config.subscope(NODE_VAR, i);
    const auto& replica_conf = node_conf.subscope(REPLICA_VAR, 0);
    std::unique_ptr<Replica> replica = std::make_unique<Replica>();
    replica->principal_id = replica_conf.getValue<bft::communication::NodeNum>(CLIENT_ID);
    replica->replica_host = replica_conf.getValue<std::string>(REPLICA_HOST);
    replica->replica_port = replica_conf.getValue<std::uint32_t>(REPLICA_PORT);
    struct_config_.node.push_back(std::move(*replica));
  }
}

std::unique_ptr<ICommunication> ClientPoolConfig::ToCommunication(const CommConfig& comm_config) {
  if (comm_config.commType == "tls") {
    const auto tls_config = TlsTcpConfig{comm_config.listenIp,
                                         comm_config.listenPort,
                                         comm_config.bufferLength,
                                         comm_config.nodes,
                                         static_cast<std::int32_t>(comm_config.maxServerId),
                                         comm_config.selfId,
                                         comm_config.certificatesRootPath,
                                         comm_config.cipherSuite,
                                         comm_config.statusCallback,
                                         comm_config.secretData};
    return std::unique_ptr<ICommunication>{CommFactory::create(tls_config)};
  } else if (comm_config.commType == "udp") {
    const auto udp_config = PlainUdpConfig{comm_config.listenIp,
                                           comm_config.listenPort,
                                           comm_config.bufferLength,
                                           comm_config.nodes,
                                           comm_config.selfId,
                                           comm_config.statusCallback};
    return std::unique_ptr<ICommunication>{CommFactory::create(udp_config)};
  }
  throw std::invalid_argument{"Unknown communication module type=" + comm_config.commType};
}

}  // namespace concord::config_pool
