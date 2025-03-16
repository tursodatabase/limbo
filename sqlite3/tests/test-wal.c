#include "check.h"

#include <sqlite3.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdio.h>

void test_wal_checkpoint(void)
{
    sqlite3 *db;

    // Test with NULL db handle
    CHECK_EQUAL(SQLITE_MISUSE, sqlite3_wal_checkpoint(NULL, NULL));

    // Test with valid db
    CHECK_EQUAL(SQLITE_OK, sqlite3_open("../../testing/testing.db", &db));
    CHECK_EQUAL(SQLITE_OK, sqlite3_wal_checkpoint(db, NULL));
    CHECK_EQUAL(SQLITE_OK, sqlite3_close(db));
}

void test_wal_checkpoint_v2(void)
{
    sqlite3 *db;
    int log_size, checkpoint_count;

    // Test with NULL db handle
    CHECK_EQUAL(SQLITE_MISUSE, sqlite3_wal_checkpoint_v2(NULL, NULL, SQLITE_CHECKPOINT_PASSIVE, NULL, NULL));

    // Test with valid db
    CHECK_EQUAL(SQLITE_OK, sqlite3_open("../../testing/testing.db", &db));
    
    // Test different checkpoint modes
    CHECK_EQUAL(SQLITE_OK, sqlite3_wal_checkpoint_v2(db, NULL, SQLITE_CHECKPOINT_PASSIVE, &log_size, &checkpoint_count));
    CHECK_EQUAL(SQLITE_OK, sqlite3_wal_checkpoint_v2(db, NULL, SQLITE_CHECKPOINT_FULL, &log_size, &checkpoint_count));
    CHECK_EQUAL(SQLITE_OK, sqlite3_wal_checkpoint_v2(db, NULL, SQLITE_CHECKPOINT_RESTART, &log_size, &checkpoint_count));
    CHECK_EQUAL(SQLITE_OK, sqlite3_wal_checkpoint_v2(db, NULL, SQLITE_CHECKPOINT_TRUNCATE, &log_size, &checkpoint_count));

    CHECK_EQUAL(SQLITE_OK, sqlite3_close(db));
} 