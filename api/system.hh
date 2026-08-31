/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

namespace seastar::httpd {
class routes;
}

namespace cql3 {
class query_processor;
}

namespace seastar {
template <typename T>
class sharded;
}

namespace api {

struct http_context;
void set_system(http_context& ctx, seastar::httpd::routes& r, seastar::sharded<cql3::query_processor>& qp);

}
