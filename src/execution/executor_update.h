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
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class UpdateExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;
    std::vector<Condition> conds_;
    RmFileHandle *fh_;
    std::vector<Rid> rids_;
    std::string tab_name_;
    std::vector<SetClause> set_clauses_;
    SmManager *sm_manager_;

   public:
    UpdateExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<SetClause> set_clauses,
                   std::vector<Condition> conds, std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        set_clauses_ = set_clauses;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
    }
    std::unique_ptr<RmRecord> Next() override {
        std::vector<Rid> live_rids;
        std::vector<std::unique_ptr<RmRecord>> old_records;
        std::vector<RmRecord> new_records;

        for (auto &rid : rids_) {
            if (!fh_->is_record(rid)) {
                continue;
            }
            auto old_rec = fh_->get_record(rid, context_);
            if (context_ != nullptr && context_->txn_mgr_ != nullptr) {
                auto visible = context_->txn_mgr_->get_visible_record(tab_name_, rid, old_rec.get(), context_->txn_);
                if (!visible.has_value()) {
                    continue;
                }
                if (context_->txn_mgr_->is_mvcc_txn(context_->txn_)) {
                    context_->txn_mgr_->check_write_conflict(tab_name_, rid, context_->txn_);
                }
                old_rec = std::make_unique<RmRecord>(*visible);
            }
            RmRecord new_rec(*old_rec);
            for (auto &set_clause : set_clauses_) {
                auto col = tab_.get_col(set_clause.lhs.col_name);
                if (set_clause.is_rhs_col) {
                    auto rhs_col = tab_.get_col(set_clause.rhs_col.col_name);
                    if (col->type != rhs_col->type) {
                        throw IncompatibleTypeError(coltype2str(col->type), coltype2str(rhs_col->type));
                    }
                    memcpy(new_rec.data + col->offset, old_rec->data + rhs_col->offset, col->len);
                    if (set_clause.has_delta) {
                        if (col->type == TYPE_INT) {
                            int base = *reinterpret_cast<int *>(new_rec.data + col->offset);
                            int delta = set_clause.delta.type == TYPE_INT
                                            ? set_clause.delta.int_val
                                            : static_cast<int>(set_clause.delta.float_val);
                            int value = set_clause.delta_op == '-' ? base - delta : base + delta;
                            memcpy(new_rec.data + col->offset, &value, sizeof(int));
                        } else if (col->type == TYPE_FLOAT) {
                            float base = *reinterpret_cast<float *>(new_rec.data + col->offset);
                            float delta = set_clause.delta.type == TYPE_FLOAT
                                              ? set_clause.delta.float_val
                                              : static_cast<float>(set_clause.delta.int_val);
                            float value = set_clause.delta_op == '-' ? base - delta : base + delta;
                            memcpy(new_rec.data + col->offset, &value, sizeof(float));
                        } else {
                            throw IncompatibleTypeError(coltype2str(col->type), "NUMERIC");
                        }
                    }
                } else {
                    if (col->type != set_clause.rhs.type) {
                        throw IncompatibleTypeError(coltype2str(col->type), coltype2str(set_clause.rhs.type));
                    }
                    if (set_clause.rhs.raw == nullptr) {
                        set_clause.rhs.init_raw(col->len);
                    }
                    memcpy(new_rec.data + col->offset, set_clause.rhs.raw->data, col->len);
                }
            }
            live_rids.push_back(rid);
            old_records.push_back(std::move(old_rec));
            new_records.push_back(new_rec);
        }

        for (auto &index : tab_.indexes) {
            auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
            std::vector<std::vector<char>> new_keys;
            std::vector<bool> changed;
            new_keys.reserve(live_rids.size());
            changed.reserve(live_rids.size());

            for (size_t row = 0; row < live_rids.size(); ++row) {
                std::vector<char> old_key(index.col_tot_len);
                std::vector<char> new_key(index.col_tot_len);
                int offset = 0;
                for (auto &col : index.cols) {
                    memcpy(old_key.data() + offset, old_records[row]->data + col.offset, col.len);
                    memcpy(new_key.data() + offset, new_records[row].data + col.offset, col.len);
                    offset += col.len;
                }
                bool key_changed = memcmp(old_key.data(), new_key.data(), index.col_tot_len) != 0;
                if (key_changed && ih->contains_key(new_key.data())) {
                    throw IndexEntryDuplicatedError();
                }
                for (size_t prev = 0; prev < new_keys.size(); ++prev) {
                    if (key_changed && changed[prev] &&
                        memcmp(new_key.data(), new_keys[prev].data(), index.col_tot_len) == 0) {
                        throw IndexEntryDuplicatedError();
                    }
                }
                new_keys.push_back(std::move(new_key));
                changed.push_back(key_changed);
            }
        }

        for (size_t row = 0; row < live_rids.size(); ++row) {
            if (context_ != nullptr && context_->txn_ != nullptr) {
                context_->txn_->append_write_record(
                    new WriteRecord(WType::UPDATE_TUPLE, tab_name_, live_rids[row], *old_records[row]));
            }
            if (context_ != nullptr && context_->txn_mgr_ != nullptr) {
                context_->txn_mgr_->record_update(tab_name_, live_rids[row], *old_records[row], new_records[row],
                                                  context_->txn_);
            }
            for (auto &index : tab_.indexes) {
                auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
                std::vector<char> old_key(index.col_tot_len);
                std::vector<char> new_key(index.col_tot_len);
                int offset = 0;
                for (auto &col : index.cols) {
                    memcpy(old_key.data() + offset, old_records[row]->data + col.offset, col.len);
                    memcpy(new_key.data() + offset, new_records[row].data + col.offset, col.len);
                    offset += col.len;
                }
                if (memcmp(old_key.data(), new_key.data(), index.col_tot_len) != 0) {
                    ih->delete_entry(old_key.data(), context_ == nullptr ? nullptr : context_->txn_);
                    ih->insert_entry(new_key.data(), live_rids[row], context_ == nullptr ? nullptr : context_->txn_);
                }
            }

            fh_->update_record(live_rids[row], new_records[row].data, context_);
        }
        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }
};
