/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <atomic>
#include <map>
#include <string>

#include "transaction/transaction.h"
#include "transaction/concurrency/lock_manager.h"
#include "recovery/log_manager.h"

class TransactionManager;

// used for data_send
static int const_offset = -1;

inline std::atomic<bool> g_output_file_enabled{true};

inline bool output_file_enabled() {
    return g_output_file_enabled.load(std::memory_order_relaxed);
}

inline void set_output_file_enabled(bool enabled) {
    g_output_file_enabled.store(enabled, std::memory_order_relaxed);
}

class Context {
public:
    Context (LockManager *lock_mgr, LogManager *log_mgr, 
            Transaction *txn, char *data_send = nullptr, int *offset = &const_offset,
            TransactionManager *txn_mgr = nullptr)
        : lock_mgr_(lock_mgr), log_mgr_(log_mgr), txn_(txn), txn_mgr_(txn_mgr),
          data_send_(data_send), offset_(offset) {
            ellipsis_ = false;
          }

    TransactionManager *txn_mgr_;
    LockManager *lock_mgr_;
    LogManager *log_mgr_;
    Transaction *txn_;
    char *data_send_;
    int *offset_;
    bool ellipsis_;
    std::map<std::string, std::string> explain_tab_aliases_;
    bool explain_select_all_ = false;
    bool is_explain_analyze_ = false;
    bool explain_has_explicit_join_ = false;
    IsolationLevel session_isolation_ = IsolationLevel::SERIALIZABLE;
    bool session_mvcc_enabled_ = true;
    bool collect_select_reads_ = false;
};
