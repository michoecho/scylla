/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "s3_retry_strategy.hh"
#include "aws_error.hh"
#include "utils/log.hh"

using seastar::log_level;

using namespace std::chrono_literals;

namespace aws {

static logging::logger s3_retry_logger("s3_retry_strategy");

s3_retry_strategy::s3_retry_strategy(credentials_refresher creds_refresher, unsigned max_retries, unsigned scale_factor)
    : default_retry_strategy(max_retries, scale_factor), _creds_refresher(std::move(creds_refresher)) {
}

seastar::future<bool> s3_retry_strategy::should_retry(const aws_error& error, unsigned attempted_retries) const {
    if (attempted_retries < _max_retries && error.get_error_type() == aws_error_type::EXPIRED_TOKEN) {
        LOGMACRO(s3_retry_logger, log_level::info, "Credentials are expired, renewing");
        co_await _creds_refresher();
        co_return true;
    }
    co_return co_await default_retry_strategy::should_retry(error, attempted_retries);
}

} // namespace aws
