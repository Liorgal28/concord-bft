// Copyright 2021 VMware, all rights reserved

#include <boost/program_options.hpp>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <string>
#include <vector>
#include "../../external_client_facade/concord_client_pool_facade.h"
#include "concord.pb.h"

using com::vmware::concord::ConcordRequest;
using com::vmware::concord::ConcordResponse;
using com::vmware::concord::TeeRequest;
using com::vmware::concord::TeeResponse;

using namespace concord::concord_client_pool;

static const std::string DEFAULT_CLIENT_POOL_CONFIG = "/concord/config-public/bftclient_apollo.config";

static std::chrono::milliseconds REQ_TIMEOUT_MS{10000};
static const uint32_t REPLY_BUF_SIZE{1 << 8};

std::mutex mutex;
std::condition_variable cv;
std::atomic_uint32_t num_replies = 0;

std::vector<std::shared_ptr<char[]>> replies;

void poolDoneCallback(const std::string cid, uint32_t reply_size) {
  std::cout << "Done: " << cid << std::endl;

  {
    std::lock_guard<std::mutex> lock(mutex);
    num_replies++;
    cv.notify_all();
  }
}

void run(std::string& client_pool_config, int num_requests) {
  // Setup ConcordClientPool
  auto pool = std::make_unique<concord::concord_client_pool_facade::ConcordClientPoolFacade>(client_pool_config);
  pool->SetDoneCallback(poolDoneCallback);

  while (PoolStatus::NotServing == pool->HealthStatus()) {
    std::cout << "Waiting for client pool to connect" << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(2));
  }
  std::cout << "Client Pool ready to go!!!" << std::endl;

  int i = 0;
  while (i < num_requests) {
    // Build request
    auto conc_req = std::make_unique<ConcordRequest>();
    TeeRequest* tee_req = conc_req->mutable_tee_request();
    tee_req->set_tee_input("EchoEcho");

    std::string serialized_req;
    conc_req->SerializeToString(&serialized_req);

    auto flags = bftEngine::ClientMsgFlag::EMPTY_FLAGS_REQ;
    uint64_t seq_num = 1337 + i;
    std::string cid = "echo" + std::to_string(seq_num);

    std::vector<uint8_t> request{serialized_req.c_str(), serialized_req.c_str() + serialized_req.size()};

    std::shared_ptr<char[]> reply_buffer(new char[REPLY_BUF_SIZE]);

    // Send request
    auto result =
        pool->SendRequest(std::move(request), flags, REQ_TIMEOUT_MS, reply_buffer.get(), REPLY_BUF_SIZE, seq_num, cid);
    if (result != SubmitResult::Acknowledged) {
      std::cout << "Client Pool overloaded, sleeping ..." << std::endl;
      std::this_thread::sleep_for(std::chrono::seconds(2));
      continue;
    }

    replies.push_back(reply_buffer);
    i++;
  }

  // Wait for all num_requests replies
  {
    std::unique_lock<std::mutex> lock(mutex);
    cv.wait(lock, [&num_requests] { return num_replies >= num_requests; });
  }

  // Print all responses
  for (const auto& reply : replies) {
    ConcordResponse conc_res;
    conc_res.ParseFromArray(reply.get(), REPLY_BUF_SIZE);
    assert(conc_res.has_tee_response());
    std::cout << conc_res.tee_response().tee_output() << std::endl;
  }
}

int main(int argc, char** argv) {
  std::string config;
  int num_requests;

  // Build commandline parser
  namespace po = boost::program_options;
  po::options_description desc;

  // clang-format off
    desc.add_options()
            ("help,h", "This help message")
            ("client-pool-config,c",
             po::value<std::string>(&config)->default_value(DEFAULT_CLIENT_POOL_CONFIG),
             "Path to client pool confiugration file")
            ("num-requests,n",
             po::value<int>(&num_requests)->default_value(1),
             "Number of requests")
            ;
  // clang-format on

  // Parse commandline options
  po::variables_map vm;
  try {
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);
  } catch (const std::exception& e) {
    std::cerr << "Failed to parse options. Use -h/--help for more information." << std::endl;
    return 1;
  }

  if (vm.count("help")) {
    std::cout << "Usage: " << argv[0] << " [options]" << std::endl;
    std::cout << desc;
    return 0;
  }

  try {
    run(config, num_requests);
  } catch (const std::exception& e) {
    std::cerr << "=========Caught an exception=========\n" << e.what() << std::endl;
  }
}
