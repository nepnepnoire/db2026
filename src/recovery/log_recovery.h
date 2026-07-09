/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2. */

#pragma once

#include <set>
#include <string>
#include <vector>

#include "log_manager.h"
#include "storage/disk_manager.h"
#include "system/sm_manager.h"

class RecoveryManager {
public:
    RecoveryManager(DiskManager* disk_manager, BufferPoolManager* buffer_pool_manager, SmManager* sm_manager) {
        disk_manager_ = disk_manager;
        buffer_pool_manager_ = buffer_pool_manager;
        sm_manager_ = sm_manager;
    }

    void analyze();
    void redo();
    void undo();
    txn_id_t max_txn_id() const { return max_txn_id_; }
    lsn_t max_lsn() const { return max_lsn_; }

private:
    enum class RecoveryOpType { INSERT, DELETE, UPDATE };
    struct RecoveryOp {
        RecoveryOpType type;
        lsn_t lsn{INVALID_LSN};
        txn_id_t txn_id;
        std::string table_name;
        Rid rid;
        std::vector<char> before;
        std::vector<char> after;
    };

    void apply_redo(const RecoveryOp &op);
    void apply_undo(const RecoveryOp &op);
    void rebuild_and_flush_touched_tables();

    LogBuffer buffer_;
    DiskManager* disk_manager_;
    BufferPoolManager* buffer_pool_manager_;
    SmManager* sm_manager_;
    std::vector<RecoveryOp> ops_;
    std::set<txn_id_t> committed_txns_;
    std::set<txn_id_t> finished_txns_;
    std::set<std::string> touched_tables_;
    long restart_offset_{0};
    txn_id_t max_txn_id_{0};
    lsn_t max_lsn_{INVALID_LSN};
};
