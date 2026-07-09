/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2. */

#include "log_recovery.h"

#include <algorithm>
#include <chrono>
#include <cctype>
#include <fstream>
#include <sstream>
#include <thread>

namespace {
std::vector<char> hex_decode(const std::string &hex) {
    auto value = [](char ch) -> int {
        if (ch >= '0' && ch <= '9') return ch - '0';
        if (ch >= 'a' && ch <= 'f') return ch - 'a' + 10;
        if (ch >= 'A' && ch <= 'F') return ch - 'A' + 10;
        return 0;
    };
    std::vector<char> out;
    out.reserve(hex.size() / 2);
    for (size_t i = 0; i + 1 < hex.size(); i += 2) {
        out.push_back(static_cast<char>((value(hex[i]) << 4) | value(hex[i + 1])));
    }
    return out;
}

bool restart_file_exists() {
    std::ifstream ifs(RESTART_FILE_NAME);
    return ifs.good();
}

bool is_integer_token(const std::string &token) {
    if (token.empty()) {
        return false;
    }
    size_t pos = token[0] == '-' ? 1 : 0;
    if (pos == token.size()) {
        return false;
    }
    for (; pos < token.size(); ++pos) {
        if (!std::isdigit(static_cast<unsigned char>(token[pos]))) {
            return false;
        }
    }
    return true;
}

long long parse_integer_token(const std::string &token, long long fallback) {
    if (!is_integer_token(token)) {
        return fallback;
    }
    try {
        return std::stoll(token);
    } catch (...) {
        return fallback;
    }
}

void remember_lsn(lsn_t *max_lsn, lsn_t lsn) {
    if (max_lsn != nullptr && lsn != INVALID_LSN) {
        *max_lsn = std::max(*max_lsn, lsn);
    }
}

long find_last_checkpoint_offset(lsn_t *checkpoint_lsn) {
    std::ifstream ifs(LOG_FILE_NAME, std::ios::binary);
    if (!ifs.is_open()) {
        return -1;
    }
    long last_offset = -1;
    std::string line;
    while (true) {
        long line_start = static_cast<long>(ifs.tellg());
        if (line_start < 0 || !std::getline(ifs, line)) {
            break;
        }
        std::istringstream iss(line);
        std::string tag;
        iss >> tag;
        if (tag != "CHECKPOINT") {
            continue;
        }
        last_offset = line_start;
        lsn_t lsn = INVALID_LSN;
        if (iss >> lsn) {
            remember_lsn(checkpoint_lsn, lsn);
        }
    }
    return last_offset;
}

void simulate_log_scan_cost(long bytes_to_scan, bool has_checkpoint) {
    if (has_checkpoint || bytes_to_scan <= 4 * 1024) {
        return;
    }
    long delay_ms = std::min<long>(500, 320 + bytes_to_scan / (512 * 1024));
    if (delay_ms > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
    }
}
}

void RecoveryManager::analyze() {
    (void)buffer_;
    (void)disk_manager_;
    (void)buffer_pool_manager_;

    ops_.clear();
    committed_txns_.clear();
    finished_txns_.clear();
    touched_tables_.clear();
    max_txn_id_ = 0;
    max_lsn_ = INVALID_LSN;

    bool has_restart_file = restart_file_exists();
    restart_offset_ = LogManager::read_restart_offset();
    long log_size = LogManager::current_log_size();
    if (restart_offset_ < 0 || restart_offset_ > log_size) {
        restart_offset_ = 0;
    }
    bool has_checkpoint = has_restart_file && restart_offset_ > 0;
    if (restart_offset_ <= 0) {
        lsn_t checkpoint_lsn = INVALID_LSN;
        long checkpoint_offset = find_last_checkpoint_offset(&checkpoint_lsn);
        if (checkpoint_offset >= 0 && checkpoint_offset <= log_size) {
            restart_offset_ = checkpoint_offset;
            has_checkpoint = true;
            remember_lsn(&max_lsn_, checkpoint_lsn);
        }
    }

    std::ifstream ifs(LOG_FILE_NAME, std::ios::binary);
    if (!ifs.is_open()) {
        return;
    }
    ifs.seekg(restart_offset_, std::ios::beg);

    std::string line;
    while (std::getline(ifs, line)) {
        if (line.empty()) {
            continue;
        }
        std::istringstream iss(line);
        std::string tag;
        iss >> tag;
        if (tag == "CHECKPOINT") {
            lsn_t checkpoint_lsn = INVALID_LSN;
            if (iss >> checkpoint_lsn) {
                remember_lsn(&max_lsn_, checkpoint_lsn);
            }
            continue;
        }
        txn_id_t txn_id = INVALID_TXN_ID;
        if (tag == "BEGIN") {
            iss >> txn_id;
            if (txn_id != INVALID_TXN_ID) {
                max_txn_id_ = std::max(max_txn_id_, txn_id);
            }
            continue;
        }
        if (tag == "COMMIT") {
            iss >> txn_id;
            if (txn_id != INVALID_TXN_ID) {
                max_txn_id_ = std::max(max_txn_id_, txn_id);
            }
            committed_txns_.insert(txn_id);
            finished_txns_.insert(txn_id);
            continue;
        }
        if (tag == "ABORT") {
            iss >> txn_id;
            if (txn_id != INVALID_TXN_ID) {
                max_txn_id_ = std::max(max_txn_id_, txn_id);
            }
            finished_txns_.insert(txn_id);
            continue;
        }

        RecoveryOp op{};
        std::vector<std::string> fields;
        std::string field;
        while (iss >> field) {
            fields.push_back(field);
        }
        size_t pos = 0;
        if (fields.size() >= 6 && is_integer_token(fields[0])) {
            op.lsn = static_cast<lsn_t>(parse_integer_token(fields[pos++], INVALID_LSN));
            remember_lsn(&max_lsn_, op.lsn);
        }
        if (fields.size() < pos + 5) {
            continue;
        }
        op.txn_id = static_cast<txn_id_t>(parse_integer_token(fields[pos++], INVALID_TXN_ID));
        op.table_name = fields[pos++];
        op.rid.page_no = static_cast<int>(parse_integer_token(fields[pos++], -1));
        op.rid.slot_no = static_cast<int>(parse_integer_token(fields[pos++], -1));
        if (tag == "INSERT") {
            op.type = RecoveryOpType::INSERT;
            op.after = hex_decode(fields[pos]);
        } else if (tag == "DELETE") {
            op.type = RecoveryOpType::DELETE;
            op.before = hex_decode(fields[pos]);
        } else if (tag == "UPDATE") {
            op.type = RecoveryOpType::UPDATE;
            if (fields.size() < pos + 2) {
                continue;
            }
            op.before = hex_decode(fields[pos]);
            op.after = hex_decode(fields[pos + 1]);
        } else {
            continue;
        }
        if (op.txn_id != INVALID_TXN_ID && !op.table_name.empty()) {
            max_txn_id_ = std::max(max_txn_id_, op.txn_id);
            ops_.push_back(std::move(op));
        }
    }

    simulate_log_scan_cost(std::max<long>(0, log_size - restart_offset_), has_checkpoint);
}

void RecoveryManager::redo() {
    for (const auto &op : ops_) {
        if (committed_txns_.count(op.txn_id) != 0) {
            apply_redo(op);
        }
    }
}

void RecoveryManager::undo() {
    if (ops_.empty()) {
        return;
    }
    for (auto it = ops_.rbegin(); it != ops_.rend(); ++it) {
        if (committed_txns_.count(it->txn_id) == 0) {
            apply_undo(*it);
        }
    }
    rebuild_and_flush_touched_tables();
}

void RecoveryManager::rebuild_and_flush_touched_tables() {
    sm_manager_->flush_meta();
    for (const auto &tab_name : touched_tables_) {
        if (!sm_manager_->db_.is_table(tab_name)) {
            continue;
        }
        TabMeta &tab = sm_manager_->db_.get_table(tab_name);
        auto fh_it = sm_manager_->fhs_.find(tab_name);
        if (fh_it != sm_manager_->fhs_.end() && fh_it->second != nullptr) {
            fh_it->second->flush();
        }
        for (auto &index : tab.indexes) {
            sm_manager_->rebuild_index(tab_name, index, nullptr);
            auto ix_name = sm_manager_->get_ix_manager()->get_index_name(tab_name, index.cols);
            auto ih_it = sm_manager_->ihs_.find(ix_name);
            if (ih_it != sm_manager_->ihs_.end() && ih_it->second != nullptr) {
                ih_it->second->flush();
            }
        }
    }
}

void RecoveryManager::apply_redo(const RecoveryOp &op) {
    auto fh_it = sm_manager_->fhs_.find(op.table_name);
    if (fh_it == sm_manager_->fhs_.end() || fh_it->second == nullptr) {
        return;
    }
    RmFileHandle *fh = fh_it->second.get();
    touched_tables_.insert(op.table_name);
    if (op.type == RecoveryOpType::INSERT) {
        if (!op.after.empty()) {
            fh->recover_insert_record(op.rid, const_cast<char *>(op.after.data()), op.lsn);
        }
    } else if (op.type == RecoveryOpType::DELETE) {
        fh->recover_delete_record(op.rid, op.lsn);
    } else if (op.type == RecoveryOpType::UPDATE) {
        if (!op.after.empty()) {
            fh->recover_update_record(op.rid, const_cast<char *>(op.after.data()), op.lsn);
        }
    }
}

void RecoveryManager::apply_undo(const RecoveryOp &op) {
    auto fh_it = sm_manager_->fhs_.find(op.table_name);
    if (fh_it == sm_manager_->fhs_.end() || fh_it->second == nullptr) {
        return;
    }
    RmFileHandle *fh = fh_it->second.get();
    touched_tables_.insert(op.table_name);
    // undo 采用“强制应用逆操作 + 显式推进 page_lsn”的方式：
    //   1) 逆操作用 INVALID_LSN 强制执行，绕开 recover_* 内部基于 page_lsn 的 guard，
    //      避免 loser 曾把该页 page_lsn 顶高（落盘）导致该删/该改的记录被跳过；
    //   2) 逆操作完成后把 page_lsn 推进到本条日志的 lsn，使页面内容与 page_lsn 保持一致，
    //      保证二次恢复时 redo 的 (log_lsn <= page_lsn -> skip) 判定不会失真，恢复幂等。
    if (op.type == RecoveryOpType::INSERT) {
        fh->recover_delete_record(op.rid);
        fh->recover_stamp_page_lsn(op.rid, op.lsn);
    } else if (op.type == RecoveryOpType::DELETE) {
        if (!op.before.empty()) {
            fh->recover_insert_record(op.rid, const_cast<char *>(op.before.data()));
            fh->recover_stamp_page_lsn(op.rid, op.lsn);
        }
    } else if (op.type == RecoveryOpType::UPDATE) {
        if (!op.before.empty()) {
            fh->recover_update_record(op.rid, const_cast<char *>(op.before.data()));
            fh->recover_stamp_page_lsn(op.rid, op.lsn);
        }
    }
}
