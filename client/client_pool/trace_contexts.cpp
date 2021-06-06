// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#include "trace_contexts.hpp"

#include <jaegertracing/propagation/W3CPropagator.h>

namespace concord::concord_client_pool::tracing {

using jaegertracing::propagation::W3CTextMapPropagator;

// From
// https://github.com/opentracing/opentracing-cpp#extract-span-context-from-a-textmapreader
class SimpleTextMapReader : public opentracing::TextMapReader {
 public:
  explicit SimpleTextMapReader(const TextMap &text_map) : text_map{text_map} {}

  using F = std::function<opentracing::expected<void>(opentracing::string_view, opentracing::string_view)>;

  opentracing::expected<void> ForeachKey(F f) const override {
    for (auto &key_value : text_map) {
      auto was_successful = f(key_value.first, key_value.second);
      if (!was_successful) {
        return was_successful;
      }
    }
    return {};
  }

  // Defining TextMapReader::LookupKey allows for faster extraction.
  opentracing::expected<opentracing::string_view> LookupKey(opentracing::string_view key) const override {
    auto iter = text_map.find(key);
    if (iter != text_map.end()) {
      return opentracing::make_unexpected(opentracing::key_not_found_error);
    }
    return opentracing::string_view{iter->second};
  }

 private:
  const TextMap &text_map;
};

std::string OpenTracingSpanContextToBlob(const opentracing::SpanContext &span_context) {
  std::ostringstream carrier;
  opentracing::Tracer::Global()->Inject(span_context, carrier);
  return carrier.str();
}

jaegertracing::SpanContext W3cTextMapSpanContextToJaeger(const TextMap &w3c_textmap_span_context) {
  W3CTextMapPropagator text_map_propagator;
  SimpleTextMapReader text_map_reader(w3c_textmap_span_context);
  return text_map_propagator.extract(text_map_reader);
}

}  // namespace concord::concord_client_pool::tracing