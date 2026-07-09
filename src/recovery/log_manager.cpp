/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include <cstring>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <unordered_map>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include "log_manager.h"

namespace {
void ensure_log_file_exists() {
    std::ofstream ofs(LOG_FILE_NAME, std::ios::binary | std::ios::app);
}

std::string hex_encode(const char *data, int size) {
    static constexpr char kHex[] = "0123456789ABCDEF";
    std::string out;
    out.reserve(static_cast<size_t>(size) * 2);
    for (int i = 0; i < size; ++i) {
        unsigned char c = static_cast<unsigned char>(data[i]);
        out.push_back(kHex[c >> 4]);
        out.push_back(kHex[c & 0x0F]);
    }
    return out;
}

void flush_stream_to_disk(std::ofstream &ofs) {
    ofs.flush();
}
}

/**
 * @description: 添加日志记录到日志缓冲区中，并返回日志记录号
 * @param {LogRecord*} log_record 要写入缓冲区的日志记录
 * @return {lsn_t} 返回该日志的日志记录号
 */
lsn_t LogManager::add_log_to_buffer(LogRecord* log_record) {
    if (log_record == nullptr) {
        return INVALID_LSN;
    }
    std::lock_guard<std::mutex> guard(latch_);
    if (disk_manager_->GetLogFd() == -1) {
        ensure_log_file_exists();
    }

    if (log_buffer_.is_full(static_cast<int>(log_record->log_tot_len_)) && log_buffer_.offset_ > 0) {
        disk_manager_->write_log(log_buffer_.buffer_, log_buffer_.offset_);
        log_buffer_.offset_ = 0;
    }

    lsn_t lsn = global_lsn_.fetch_add(1);
    log_record->lsn_ = lsn;
    if (log_record->log_tot_len_ > LOG_BUFFER_SIZE) {
        std::vector<char> tmp(log_record->log_tot_len_);
        log_record->serialize(tmp.data());
        disk_manager_->write_log(tmp.data(), static_cast<int>(tmp.size()));
        persist_lsn_ = lsn;
        return lsn;
    }

    log_record->serialize(log_buffer_.buffer_ + log_buffer_.offset_);
    log_buffer_.offset_ += static_cast<int>(log_record->log_tot_len_);
    return lsn;
}

/**
 * @description: 把日志缓冲区的内容刷到磁盘中，由于目前只设置了一个缓冲区，因此需要阻塞其他日志操作
 */
void LogManager::flush_log_to_disk() {
    std::lock_guard<std::mutex> guard(latch_);
    if (log_buffer_.offset_ <= 0) {
        if (disk_manager_->GetLogFd() == -1) {
            ensure_log_file_exists();
        }
        return;
    }
    if (disk_manager_->GetLogFd() == -1) {
        ensure_log_file_exists();
    }
    disk_manager_->write_log(log_buffer_.buffer_, log_buffer_.offset_);
    persist_lsn_ = global_lsn_.load() - 1;
    log_buffer_.offset_ = 0;
}

lsn_t LogManager::append_recovery_line(const std::string &line) {
    std::lock_guard<std::mutex> guard(latch_);
    if (disk_manager_->GetLogFd() == -1) {
        ensure_log_file_exists();
    }
    if (log_buffer_.offset_ > 0) {
        disk_manager_->write_log(log_buffer_.buffer_, log_buffer_.offset_);
        persist_lsn_ = global_lsn_.load() - 1;
        log_buffer_.offset_ = 0;
    }
    lsn_t lsn = global_lsn_.fetch_add(1);
    std::string record = line + "\n";
    disk_manager_->write_log(record.data(), static_cast<int>(record.size()));
    persist_lsn_ = lsn;
    return lsn;
}

void LogManager::append_begin(txn_id_t txn_id) {
    append_recovery_line("BEGIN " + std::to_string(txn_id));
}

void LogManager::append_commit(txn_id_t txn_id) {
    append_recovery_line("COMMIT " + std::to_string(txn_id));
}

void LogManager::append_abort(txn_id_t txn_id) {
    append_recovery_line("ABORT " + std::to_string(txn_id));
}

lsn_t LogManager::append_recovery_op_line(const std::string &tag, txn_id_t txn_id, const std::string &table_name,
                                          const Rid &rid, const std::string &payload) {
    std::lock_guard<std::mutex> guard(latch_);
    if (disk_manager_->GetLogFd() == -1) {
        ensure_log_file_exists();
    }
    if (log_buffer_.offset_ > 0) {
        disk_manager_->write_log(log_buffer_.buffer_, log_buffer_.offset_);
        persist_lsn_ = global_lsn_.load() - 1;
        log_buffer_.offset_ = 0;
    }
    lsn_t lsn = global_lsn_.fetch_add(1);
    std::string record = tag + " " + std::to_string(lsn) + " " + std::to_string(txn_id) + " " + table_name + " " +
                         std::to_string(rid.page_no) + " " + std::to_string(rid.slot_no) + " " + payload + "\n";
    disk_manager_->write_log(record.data(), static_cast<int>(record.size()));
    persist_lsn_ = lsn;
    return lsn;
}

lsn_t LogManager::append_insert(txn_id_t txn_id, const std::string &table_name, const Rid &rid,
                               const char *data, int size) {
    return append_recovery_op_line("INSERT", txn_id, table_name, rid, hex_encode(data, size));
}

lsn_t LogManager::append_delete(txn_id_t txn_id, const std::string &table_name, const Rid &rid,
                               const char *old_data, int size) {
    return append_recovery_op_line("DELETE", txn_id, table_name, rid, hex_encode(old_data, size));
}

lsn_t LogManager::append_update(txn_id_t txn_id, const std::string &table_name, const Rid &rid,
                               const char *old_data, const char *new_data, int size) {
    return append_recovery_op_line("UPDATE", txn_id, table_name, rid, hex_encode(old_data, size) + " " + hex_encode(new_data, size));
}

void LogManager::advance_next_lsn(lsn_t max_seen_lsn) {
    if (max_seen_lsn == INVALID_LSN) {
        return;
    }
    lsn_t target = max_seen_lsn + 1;
    lsn_t cur = global_lsn_.load();
    while (cur < target && !global_lsn_.compare_exchange_weak(cur, target)) {
    }
    if (persist_lsn_ == INVALID_LSN || persist_lsn_ < max_seen_lsn) {
        persist_lsn_ = max_seen_lsn;
    }
}

long LogManager::create_static_checkpoint_record(bool advance_to_active_only) {
    std::lock_guard<std::mutex> guard(latch_);
    ensure_log_file_exists();
    if (log_buffer_.offset_ > 0) {
        disk_manager_->write_log(log_buffer_.buffer_, log_buffer_.offset_);
        persist_lsn_ = global_lsn_.load() - 1;
        log_buffer_.offset_ = 0;
    }

    long checkpoint_offset = current_log_size();
    std::string record = "CHECKPOINT " + std::to_string(global_lsn_.fetch_add(1)) + "\n";
    disk_manager_->write_log(record.data(), static_cast<int>(record.size()));
    // 无活跃事务：安全起点就是 checkpoint 记录处（此前的日志都可跳过）。
    // 有活跃事务（fuzzy checkpoint）：起点必须回退到最早仍活跃事务的 BEGIN，
    // 否则该 loser 的 undo 记录会被 analyze 跳过，导致其未提交改动无法回滚。
    long restart_offset = checkpoint_offset;
    if (advance_to_active_only) {
        restart_offset = compute_min_active_begin_offset(checkpoint_offset);
    }
    write_restart_offset(restart_offset);
    return restart_offset;
}

long LogManager::compute_min_active_begin_offset(long fallback_offset) {
    std::ifstream ifs(LOG_FILE_NAME, std::ios::binary);
    if (!ifs.is_open()) {
        return fallback_offset;
    }
    // 记录每个仍活跃事务的 BEGIN 偏移；遇到 COMMIT/ABORT 则移除。
    std::unordered_map<long long, long> active_begin_offset;
    std::string line;
    while (true) {
        long line_start = static_cast<long>(ifs.tellg());
        if (line_start < 0 || !std::getline(ifs, line)) {
            break;
        }
        if (line.empty()) {
            continue;
        }
        std::istringstream iss(line);
        std::string tag;
        iss >> tag;
        long long txn_id = -1;
        if (tag == "BEGIN") {
            if (iss >> txn_id) {
                active_begin_offset[txn_id] = line_start;
            }
        } else if (tag == "COMMIT" || tag == "ABORT") {
            if (iss >> txn_id) {
                active_begin_offset.erase(txn_id);
            }
        }
    }
    long min_offset = fallback_offset;
    bool found = false;
    for (const auto &entry : active_begin_offset) {
        if (!found || entry.second < min_offset) {
            min_offset = entry.second;
            found = true;
        }
    }
    return found ? min_offset : fallback_offset;
}


long LogManager::read_restart_offset() {
    std::ifstream ifs(RESTART_FILE_NAME);
    long offset = 0;
    if (ifs >> offset && offset >= 0) {
        return offset;
    }
    return 0;
}

void LogManager::write_restart_offset(long offset) {
    std::string data = std::to_string(offset) + "\n";
    int fd = open(RESTART_FILE_NAME.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
    if (fd == -1) {
        return;
    }
    ssize_t bytes_write = write(fd, data.data(), data.size());
    if (bytes_write == static_cast<ssize_t>(data.size())) {
#ifndef _WIN32
        fsync(fd);
#endif
    }
    close(fd);
}

long LogManager::current_log_size() {
    struct stat st {};
    if (stat(LOG_FILE_NAME.c_str(), &st) != 0) {
        return 0;
    }
    return static_cast<long>(st.st_size);
}
