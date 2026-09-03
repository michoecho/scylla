#include <doctest/doctest.h>

#include <sqlite3.h>

#include <memory>
#include <string>

namespace {

struct DatabaseDeleter {
    void operator()(sqlite3* database) const {
        if (database != nullptr) {
            sqlite3_close(database);
        }
    }
};

struct StatementDeleter {
    void operator()(sqlite3_stmt* statement) const {
        if (statement != nullptr) {
            sqlite3_finalize(statement);
        }
    }
};

}  // namespace

TEST_CASE("sqlite writes a row and reads it back") {
    sqlite3* raw_database = nullptr;
    REQUIRE(sqlite3_open(":memory:", &raw_database) == SQLITE_OK);
    std::unique_ptr<sqlite3, DatabaseDeleter> database(raw_database);

    REQUIRE(sqlite3_exec(
                database.get(),
                "CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT NOT NULL);",
                nullptr,
                nullptr,
                nullptr) == SQLITE_OK);

    sqlite3_stmt* raw_insert = nullptr;
    REQUIRE(sqlite3_prepare_v2(
                database.get(),
                "INSERT INTO people (name) VALUES (?);",
                -1,
                &raw_insert,
                nullptr) == SQLITE_OK);
    std::unique_ptr<sqlite3_stmt, StatementDeleter> insert(raw_insert);
    REQUIRE(sqlite3_bind_text(insert.get(), 1, "Ada", -1, SQLITE_STATIC) == SQLITE_OK);
    REQUIRE(sqlite3_step(insert.get()) == SQLITE_DONE);

    sqlite3_stmt* raw_select = nullptr;
    REQUIRE(sqlite3_prepare_v2(
                database.get(),
                "SELECT id, name FROM people WHERE name = ?;",
                -1,
                &raw_select,
                nullptr) == SQLITE_OK);
    std::unique_ptr<sqlite3_stmt, StatementDeleter> select(raw_select);
    REQUIRE(sqlite3_bind_text(select.get(), 1, "Ada", -1, SQLITE_STATIC) == SQLITE_OK);
    REQUIRE(sqlite3_step(select.get()) == SQLITE_ROW);

    CHECK(sqlite3_column_int(select.get(), 0) == 1);
    CHECK(std::string(reinterpret_cast<const char*>(sqlite3_column_text(select.get(), 1))) == "Ada");
    CHECK(sqlite3_step(select.get()) == SQLITE_DONE);
}
