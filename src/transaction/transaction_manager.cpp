/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "transaction_manager.h"

#include <algorithm>
#include <cstring>
#include <stack>
#include <vector>

#include "execution/execution_defs.h"
#include "index/ix.h"
#include "record/rm_file_handle.h"
#include "system/sm_manager.h"

std::unordered_map<txn_id_t, Transaction *> TransactionManager::txn_map = {};

namespace {
std::vector<char> make_index_key(const RmRecord &record, const IndexMeta &index) {
    std::vector<char> key(index.col_tot_len);
    int offset = 0;
    for (auto &col : index.cols) {
        memcpy(key.data() + offset, record.data + col.offset, col.len);
        offset += col.len;
    }
    return key;
}

void delete_index_entries(SmManager *sm_manager, const std::string &tab_name, const RmRecord &record) {
    if (sm_manager == nullptr || !sm_manager->db_.is_table(tab_name)) {
        return;
    }
    auto &tab = sm_manager->db_.get_table(tab_name);
    for (auto &index : tab.indexes) {
        auto ix_name = sm_manager->get_ix_manager()->get_index_name(tab_name, index.cols);
        auto ih = sm_manager->ihs_.at(ix_name).get();
        auto key = make_index_key(record, index);
        ih->delete_entry(key.data(), nullptr);
    }
}

void insert_index_entries(SmManager *sm_manager, const std::string &tab_name, const RmRecord &record, const Rid &rid) {
    if (sm_manager == nullptr || !sm_manager->db_.is_table(tab_name)) {
        return;
    }
    auto &tab = sm_manager->db_.get_table(tab_name);
    for (auto &index : tab.indexes) {
        auto ix_name = sm_manager->get_ix_manager()->get_index_name(tab_name, index.cols);
        auto ih = sm_manager->ihs_.at(ix_name).get();
        auto key = make_index_key(record, index);
        if (!ih->contains_key(key.data())) {
            ih->insert_entry(key.data(), rid, nullptr);
        }
    }
}
}

bool TransactionManager::is_mvcc_txn(Transaction *txn) const {
    return txn != nullptr && txn->get_mvcc_enabled() && txn->get_txn_mode();
}

bool TransactionManager::is_serializable_txn(Transaction *txn) const {
    return is_mvcc_txn(txn) && txn->get_isolation_level() == IsolationLevel::SERIALIZABLE;
}

bool TransactionManager::version_visible_to_txn(const MvccVersion &version, Transaction *txn) const {
    if (txn != nullptr && version.writer_txn == txn->get_transaction_id()) {
        return true;
    }
    if (!version.committed) {
        return false;
    }
    if (!is_mvcc_txn(txn)) {
        return true;
    }
    return version.commit_ts <= txn->get_start_ts();
}

void TransactionManager::ensure_base_version_locked(const std::string &tab_name, const Rid &rid,
                                                    const RmRecord &record) {
    auto &versions = mvcc_versions_[tab_name][make_rid_key(rid)];
    if (!versions.empty()) {
        return;
    }
    MvccVersion base;
    base.writer_txn = INVALID_TXN_ID;
    base.commit_ts = 0;
    base.committed = true;
    base.deleted = false;
    base.record = record;
    versions.push_back(base);
}

std::optional<RmRecord> TransactionManager::get_visible_record(const std::string &tab_name, const Rid &rid,
                                                               const RmRecord *physical_record, Transaction *txn) {
    std::lock_guard<std::mutex> guard(mvcc_latch_);
    auto tab_it = mvcc_versions_.find(tab_name);
    if (tab_it == mvcc_versions_.end()) {
        if (physical_record == nullptr) {
            return std::nullopt;
        }
        return RmRecord(*physical_record);
    }
    auto rid_it = tab_it->second.find(make_rid_key(rid));
    if (rid_it == tab_it->second.end() || rid_it->second.empty()) {
        if (physical_record == nullptr) {
            return std::nullopt;
        }
        return RmRecord(*physical_record);
    }

    txn_id_t txn_id = txn == nullptr ? INVALID_TXN_ID : txn->get_transaction_id();
    if (txn_id != INVALID_TXN_ID) {
        for (auto it = rid_it->second.rbegin(); it != rid_it->second.rend(); ++it) {
            if (it->writer_txn == txn_id) {
                if (it->deleted) {
                    return std::nullopt;
                }
                return RmRecord(it->record);
            }
        }
    }

    const MvccVersion *best = nullptr;
    for (auto &version : rid_it->second) {
        if (!version.committed) {
            continue;
        }
        if (is_mvcc_txn(txn) && version.commit_ts > txn->get_start_ts()) {
            continue;
        }
        if (best == nullptr || version.commit_ts >= best->commit_ts) {
            best = &version;
        }
    }
    if (best == nullptr || best->deleted) {
        return std::nullopt;
    }
    return RmRecord(best->record);
}

void TransactionManager::check_write_conflict(const std::string &tab_name, const Rid &rid, Transaction *txn) {
    if (!is_mvcc_txn(txn)) {
        return;
    }
    std::lock_guard<std::mutex> guard(mvcc_latch_);
    auto tab_it = mvcc_versions_.find(tab_name);
    if (tab_it == mvcc_versions_.end()) {
        return;
    }
    auto rid_it = tab_it->second.find(make_rid_key(rid));
    if (rid_it == tab_it->second.end() || rid_it->second.empty()) {
        return;
    }
    for (auto it = rid_it->second.rbegin(); it != rid_it->second.rend(); ++it) {
        if (it->writer_txn == txn->get_transaction_id()) {
            return;
        }
        if (!it->committed) {
            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
        }
        if (it->commit_ts > txn->get_start_ts()) {
            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
        }
        return;
    }
}

bool TransactionManager::same_logical_record_locked(const std::string &tab_name, const RmRecord &lhs,
                                                    const RmRecord &rhs) {
    if (sm_manager_ == nullptr || !sm_manager_->db_.is_table(tab_name)) {
        return false;
    }
    auto &tab = sm_manager_->db_.get_table(tab_name);
    if (tab.indexes.empty()) {
        if (!tab.cols.empty() && lhs.data != nullptr && rhs.data != nullptr) {
            const auto &key_col = tab.cols.front();
            return memcmp(lhs.data + key_col.offset, rhs.data + key_col.offset, key_col.len) == 0;
        }
        return lhs.size == rhs.size && lhs.data != nullptr && rhs.data != nullptr &&
               memcmp(lhs.data, rhs.data, lhs.size) == 0;
    }
    for (auto &index : tab.indexes) {
        auto lhs_key = make_index_key(lhs, index);
        auto rhs_key = make_index_key(rhs, index);
        if (memcmp(lhs_key.data(), rhs_key.data(), index.col_tot_len) == 0) {
            return true;
        }
    }
    return false;
}

void TransactionManager::check_insert_conflict(const std::string &tab_name, const RmRecord &record, Transaction *txn) {
    if (!is_mvcc_txn(txn)) {
        return;
    }
    std::lock_guard<std::mutex> guard(mvcc_latch_);
    for (auto &event : mvcc_write_events_) {
        if (event.aborted || event.writer_txn == txn->get_transaction_id() || event.tab_name != tab_name) {
            continue;
        }
        bool invisible = !event.committed || event.commit_ts > txn->get_start_ts();
        if (!invisible) {
            continue;
        }
        bool conflicts = false;
        if (event.before_exists && same_logical_record_locked(tab_name, event.before, record)) {
            conflicts = true;
        }
        if (event.after_exists && same_logical_record_locked(tab_name, event.after, record)) {
            conflicts = true;
        }
        if (conflicts) {
            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
        }
    }
}

bool TransactionManager::record_matches_conds_locked(const std::string &tab_name, const RmRecord &record,
                                                     const std::vector<Condition> &conds) {
    if (sm_manager_ == nullptr || !sm_manager_->db_.is_table(tab_name)) {
        return false;
    }
    auto &tab = sm_manager_->db_.get_table(tab_name);
    return rmdb_eval_conds(tab.cols, &record, conds);
}

bool TransactionManager::has_rw_path_locked(txn_id_t from, txn_id_t to) const {
    if (from == to) {
        return true;
    }
    std::set<txn_id_t> visited;
    std::vector<txn_id_t> stack{from};
    while (!stack.empty()) {
        txn_id_t cur = stack.back();
        stack.pop_back();
        if (!visited.insert(cur).second) {
            continue;
        }
        for (auto &edge : rw_edges_) {
            if (edge.first != cur) {
                continue;
            }
            if (edge.second == to) {
                return true;
            }
            stack.push_back(edge.second);
        }
    }
    return false;
}

void TransactionManager::add_rw_edge_locked(txn_id_t reader, txn_id_t writer, txn_id_t current_txn) {
    if (reader == writer || reader == INVALID_TXN_ID || writer == INVALID_TXN_ID) {
        return;
    }
    if (!rw_edges_.insert({reader, writer}).second) {
        return;
    }
    if (has_rw_path_locked(writer, reader)) {
        throw TransactionAbortException(current_txn, AbortReason::DEADLOCK_PREVENTION);
    }
}

bool TransactionManager::txn_overlaps_locked(txn_id_t lhs, txn_id_t rhs) const {
    auto lhs_it = txn_map.find(lhs);
    auto rhs_it = txn_map.find(rhs);
    if (lhs_it == txn_map.end() || rhs_it == txn_map.end()) {
        return true;
    }
    Transaction *lhs_txn = lhs_it->second;
    Transaction *rhs_txn = rhs_it->second;
    if (lhs_txn == nullptr || rhs_txn == nullptr) {
        return true;
    }
    if (lhs_txn->get_state() == TransactionState::ABORTED || rhs_txn->get_state() == TransactionState::ABORTED) {
        return false;
    }
    if (lhs_txn->get_state() == TransactionState::COMMITTED &&
        lhs_txn->get_commit_ts() < rhs_txn->get_start_ts()) {
        return false;
    }
    if (rhs_txn->get_state() == TransactionState::COMMITTED &&
        rhs_txn->get_commit_ts() < lhs_txn->get_start_ts()) {
        return false;
    }
    return true;
}

void TransactionManager::check_serializable_write_locked(const std::string &tab_name, const Rid &rid,
                                                         const RmRecord *before, const RmRecord *after,
                                                         Transaction *txn) {
    if (!is_serializable_txn(txn)) {
        return;
    }
    txn_id_t writer = txn->get_transaction_id();
    RidKey rid_key = make_rid_key(rid);
    for (auto &entry : ssi_txns_) {
        txn_id_t reader = entry.first;
        if (reader == writer || entry.second.aborted || !txn_overlaps_locked(reader, writer)) {
            continue;
        }
        bool depends = entry.second.read_records.count({tab_name, rid_key}) > 0;
        if (!depends) {
            for (auto &predicate : entry.second.predicates) {
                if (predicate.tab_name != tab_name) {
                    continue;
                }
                bool before_match = before != nullptr && record_matches_conds_locked(tab_name, *before, predicate.conds);
                bool after_match = after != nullptr && record_matches_conds_locked(tab_name, *after, predicate.conds);
                if (before_match || after_match) {
                    depends = true;
                    break;
                }
            }
        }
        if (depends) {
            add_rw_edge_locked(reader, writer, writer);
        }
    }
}

void TransactionManager::record_insert(const std::string &tab_name, const Rid &rid, const RmRecord &record,
                                       Transaction *txn) {
    if (!is_mvcc_txn(txn)) {
        return;
    }
    std::lock_guard<std::mutex> guard(mvcc_latch_);
    MvccVersion version;
    version.writer_txn = txn->get_transaction_id();
    version.committed = false;
    version.deleted = false;
    version.record = record;
    mvcc_versions_[tab_name][make_rid_key(rid)].push_back(version);

    MvccWriteEvent event;
    event.writer_txn = txn->get_transaction_id();
    event.tab_name = tab_name;
    event.rid = rid;
    event.before_exists = false;
    event.after_exists = true;
    event.after = record;
    mvcc_write_events_.push_back(event);
    check_serializable_write_locked(tab_name, rid, nullptr, &record, txn);
}

void TransactionManager::record_update(const std::string &tab_name, const Rid &rid, const RmRecord &old_record,
                                       const RmRecord &new_record, Transaction *txn) {
    if (!is_mvcc_txn(txn)) {
        return;
    }
    std::lock_guard<std::mutex> guard(mvcc_latch_);
    ensure_base_version_locked(tab_name, rid, old_record);
    MvccVersion version;
    version.writer_txn = txn->get_transaction_id();
    version.committed = false;
    version.deleted = false;
    version.record = new_record;
    mvcc_versions_[tab_name][make_rid_key(rid)].push_back(version);

    MvccWriteEvent event;
    event.writer_txn = txn->get_transaction_id();
    event.tab_name = tab_name;
    event.rid = rid;
    event.before_exists = true;
    event.after_exists = true;
    event.before = old_record;
    event.after = new_record;
    mvcc_write_events_.push_back(event);
    check_serializable_write_locked(tab_name, rid, &old_record, &new_record, txn);
}

void TransactionManager::record_delete(const std::string &tab_name, const Rid &rid, const RmRecord &old_record,
                                       Transaction *txn) {
    if (!is_mvcc_txn(txn)) {
        return;
    }
    std::lock_guard<std::mutex> guard(mvcc_latch_);
    ensure_base_version_locked(tab_name, rid, old_record);
    MvccVersion version;
    version.writer_txn = txn->get_transaction_id();
    version.committed = false;
    version.deleted = true;
    version.record = old_record;
    mvcc_versions_[tab_name][make_rid_key(rid)].push_back(version);

    MvccWriteEvent event;
    event.writer_txn = txn->get_transaction_id();
    event.tab_name = tab_name;
    event.rid = rid;
    event.before_exists = true;
    event.after_exists = false;
    event.before = old_record;
    mvcc_write_events_.push_back(event);
    check_serializable_write_locked(tab_name, rid, &old_record, nullptr, txn);
}

void TransactionManager::record_select_read(const std::string &tab_name, const Rid &rid, Transaction *txn) {
    if (!is_serializable_txn(txn)) {
        return;
    }
    std::lock_guard<std::mutex> guard(mvcc_latch_);
    auto &info = ssi_txns_[txn->get_transaction_id()];
    info.start_ts = txn->get_start_ts();
    info.read_records.insert({tab_name, make_rid_key(rid)});
}

void TransactionManager::predicate_select_read(const std::string &tab_name, const std::vector<Condition> &conds,
                                               Transaction *txn) {
    if (!is_serializable_txn(txn)) {
        return;
    }
    std::lock_guard<std::mutex> guard(mvcc_latch_);
    txn_id_t reader = txn->get_transaction_id();
    auto &info = ssi_txns_[reader];
    info.start_ts = txn->get_start_ts();
    info.predicates.push_back(PredicateRead{tab_name, conds});

    for (auto &event : mvcc_write_events_) {
        if (event.aborted || event.writer_txn == reader || event.tab_name != tab_name) {
            continue;
        }
        bool invisible = !event.committed || event.commit_ts > txn->get_start_ts();
        if (!invisible) {
            continue;
        }
        bool before_match = event.before_exists && record_matches_conds_locked(tab_name, event.before, conds);
        bool after_match = event.after_exists && record_matches_conds_locked(tab_name, event.after, conds);
        if (before_match || after_match) {
            add_rw_edge_locked(reader, event.writer_txn, reader);
        }
    }
}

void TransactionManager::cleanup_txn_mvcc_locked(txn_id_t txn_id, bool aborted, timestamp_t commit_ts) {
    for (auto &tab_entry : mvcc_versions_) {
        for (auto &rid_entry : tab_entry.second) {
            auto &versions = rid_entry.second;
            if (aborted) {
                versions.erase(std::remove_if(versions.begin(), versions.end(),
                                              [&](const MvccVersion &version) {
                                                  return version.writer_txn == txn_id;
                                              }),
                               versions.end());
            } else {
                for (auto &version : versions) {
                    if (version.writer_txn == txn_id) {
                        version.committed = true;
                        version.commit_ts = commit_ts;
                    }
                }
            }
        }
    }
    for (auto &event : mvcc_write_events_) {
        if (event.writer_txn == txn_id) {
            event.aborted = aborted;
            event.committed = !aborted;
            event.commit_ts = commit_ts;
        }
    }
    if (aborted) {
        mvcc_write_events_.erase(std::remove_if(mvcc_write_events_.begin(), mvcc_write_events_.end(),
                                                [&](const MvccWriteEvent &event) {
                                                    return event.writer_txn == txn_id;
                                                }),
                                 mvcc_write_events_.end());
        ssi_txns_.erase(txn_id);
        for (auto it = rw_edges_.begin(); it != rw_edges_.end();) {
            if (it->first == txn_id || it->second == txn_id) {
                it = rw_edges_.erase(it);
            } else {
                ++it;
            }
        }
    } else {
        auto it = ssi_txns_.find(txn_id);
        if (it != ssi_txns_.end()) {
            it->second.committed = true;
            it->second.commit_ts = commit_ts;
        }
    }
}

/**
 * @description: 事务的开始方法
 * @return {Transaction*} 开始事务的指针
 * @param {Transaction*} txn 事务指针，空指针代表需要创建新事务，否则开始已有事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
void TransactionManager::materialize_committed_deletes() {
    std::vector<std::pair<std::string, Rid>> deletes;
    {
        std::lock_guard<std::mutex> guard(mvcc_latch_);
        for (auto &event : mvcc_write_events_) {
            if (event.committed && !event.aborted && event.before_exists && !event.after_exists) {
                deletes.push_back({event.tab_name, event.rid});
            }
        }
    }
    if (sm_manager_ == nullptr) {
        return;
    }
    for (auto &entry : deletes) {
        auto fh_it = sm_manager_->fhs_.find(entry.first);
        if (fh_it == sm_manager_->fhs_.end() || fh_it->second == nullptr) {
            continue;
        }
        RmFileHandle *fh = fh_it->second.get();
        if (fh->is_record(entry.second)) {
            fh->delete_record(entry.second, nullptr);
        }
    }
}

bool TransactionManager::has_active_transaction() {
    std::unique_lock<std::mutex> lock(latch_);
    for (auto &entry : txn_map) {
        Transaction *txn = entry.second;
        if (txn != nullptr && txn->get_state() != TransactionState::COMMITTED &&
            txn->get_state() != TransactionState::ABORTED) {
            return true;
        }
    }
    return false;
}

void TransactionManager::flush_autocommit_dirty_pages() {
    std::set<std::pair<std::string, int>> dirty_pages;
    {
        std::lock_guard<std::mutex> guard(autocommit_dirty_latch_);
        dirty_pages.swap(autocommit_dirty_pages_);
    }
    if (sm_manager_ == nullptr) {
        return;
    }
    sm_manager_->flush_meta();
    std::set<std::string> dirty_tables;
    for (const auto &entry : dirty_pages) {
        dirty_tables.insert(entry.first);
        auto fh_it = sm_manager_->fhs_.find(entry.first);
        if (fh_it == sm_manager_->fhs_.end() || fh_it->second == nullptr) {
            continue;
        }
        fh_it->second->flush_page(Rid{entry.second, 0});
    }
    for (const auto &tab_name : dirty_tables) {
        if (!sm_manager_->db_.is_table(tab_name)) {
            continue;
        }
        const auto &tab = sm_manager_->db_.get_table(tab_name);
        for (const auto &index : tab.indexes) {
            auto ix_name = sm_manager_->get_ix_manager()->get_index_name(tab_name, index.cols);
            auto ih_it = sm_manager_->ihs_.find(ix_name);
            if (ih_it != sm_manager_->ihs_.end() && ih_it->second != nullptr) {
                ih_it->second->flush();
            }
        }
    }
}

void TransactionManager::advance_next_txn_id(txn_id_t max_seen_txn_id) {


    txn_id_t cur = next_txn_id_.load();
    while (cur < max_seen_txn_id &&
           !next_txn_id_.compare_exchange_weak(cur, max_seen_txn_id)) {
    }
}

Transaction * TransactionManager::begin(Transaction* txn, LogManager* log_manager) {
    if (txn == nullptr) {
        txn_id_t txn_id = next_txn_id_.fetch_add(1) + 1;
        txn = new Transaction(txn_id);
    }
    txn->set_state(TransactionState::GROWING);
    txn->set_start_ts(next_timestamp_.fetch_add(1) + 1);

    std::unique_lock<std::mutex> lock(latch_);
    txn_map[txn->get_transaction_id()] = txn;
    return txn;
}

/**
 * @description: 事务的提交方法
 * @param {Transaction*} txn 需要提交的事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
void TransactionManager::commit(Transaction* txn, LogManager* log_manager) {
    if (txn == nullptr) {
        return;
    }
    if (txn->get_state() == TransactionState::ABORTED) {
        return;
    }
    timestamp_t commit_ts = next_timestamp_.fetch_add(1) + 1;
    if (txn->get_mvcc_enabled()) {
        for (auto *wr : *txn->get_write_set()) {
            if (wr != nullptr && wr->GetWriteType() == WType::DELETE_TUPLE) {
                delete_index_entries(sm_manager_, wr->GetTableName(), wr->GetRecord());
            }
        }
    }
    txn->set_state(TransactionState::COMMITTED);
    txn->set_commit_ts(commit_ts);
    last_commit_ts_.store(commit_ts);
    {
        std::lock_guard<std::mutex> guard(mvcc_latch_);
        cleanup_txn_mvcc_locked(txn->get_transaction_id(), false, commit_ts);
    }
    if (!txn->get_txn_mode() && !txn->get_write_set()->empty()) {
        mark_autocommit_dirty();
        std::lock_guard<std::mutex> guard(autocommit_dirty_latch_);
        for (auto *wr : *txn->get_write_set()) {
            if (wr != nullptr && wr->GetRid().page_no >= RM_FIRST_RECORD_PAGE) {
                autocommit_dirty_pages_.insert({wr->GetTableName(), wr->GetRid().page_no});
            }
        }
    }
    bool need_commit_log = txn->get_txn_mode();
    if (log_manager != nullptr && need_commit_log) {
        log_manager->append_commit(txn->get_transaction_id());
        log_manager->flush_log_to_disk();
    }
    if (sm_manager_ != nullptr) {
        for (auto *wr : *txn->get_write_set()) {
            if (wr == nullptr || wr->GetWriteType() != WType::DELETE_TUPLE) {
                continue;
            }
            const std::string &tab_name = wr->GetTableName();
            auto fh_it = sm_manager_->fhs_.find(tab_name);
            if (fh_it != sm_manager_->fhs_.end() && fh_it->second != nullptr &&
                fh_it->second->is_record(wr->GetRid())) {
                fh_it->second->delete_record(wr->GetRid(), nullptr);
            }
        }
    }
    for (auto *wr : *txn->get_write_set()) {
        delete wr;
    }
    txn->get_write_set()->clear();
    txn->get_lock_set()->clear();
}

/**
 * @description: 事务的终止（回滚）方法
 * @param {Transaction *} txn 需要回滚的事务
 * @param {LogManager} *log_manager 日志管理器指针
 */
void TransactionManager::abort(Transaction * txn, LogManager *log_manager) {
    if (txn == nullptr) {
        return;
    }
    if (txn->get_state() == TransactionState::ABORTED) {
        return;
    }

    auto write_set = txn->get_write_set();
    while (!write_set->empty()) {
        WriteRecord *wr = write_set->back();
        write_set->pop_back();
        if (wr != nullptr) {
            const std::string &tab_name = wr->GetTableName();
            if (sm_manager_ != nullptr && sm_manager_->fhs_.count(tab_name)) {
                RmFileHandle *fh = sm_manager_->fhs_.at(tab_name).get();
                if (wr->GetWriteType() == WType::INSERT_TUPLE) {
                    if (fh->is_record(wr->GetRid())) {
                        auto rec = fh->get_record(wr->GetRid(), nullptr);
                        delete_index_entries(sm_manager_, tab_name, *rec);
                        fh->delete_record(wr->GetRid(), nullptr);
                    }
                } else if (wr->GetWriteType() == WType::DELETE_TUPLE) {
                    if (!fh->is_record(wr->GetRid())) {
                        fh->insert_record(wr->GetRid(), wr->GetRecord().data);
                        insert_index_entries(sm_manager_, tab_name, wr->GetRecord(), wr->GetRid());
                    } else if (txn->get_mvcc_enabled()) {
                        insert_index_entries(sm_manager_, tab_name, wr->GetRecord(), wr->GetRid());
                    }
                } else if (wr->GetWriteType() == WType::UPDATE_TUPLE) {
                    if (fh->is_record(wr->GetRid())) {
                        auto rec = fh->get_record(wr->GetRid(), nullptr);
                        delete_index_entries(sm_manager_, tab_name, *rec);
                        fh->update_record(wr->GetRid(), wr->GetRecord().data, nullptr);
                        insert_index_entries(sm_manager_, tab_name, wr->GetRecord(), wr->GetRid());
                    }
                }
            }
            delete wr;
        }
    }

    {
        std::lock_guard<std::mutex> guard(mvcc_latch_);
        cleanup_txn_mvcc_locked(txn->get_transaction_id(), true, INVALID_TS);
    }
    txn->set_state(TransactionState::ABORTED);
    if (log_manager != nullptr && txn->get_txn_mode()) {
        log_manager->append_abort(txn->get_transaction_id());
        log_manager->flush_log_to_disk();
    }
    txn->get_lock_set()->clear();
}
