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

class DeleteExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;                   // 表的元数据
    std::vector<Condition> conds_;  // delete的条件
    RmFileHandle *fh_;              // 表的数据文件句柄
    std::vector<Rid> rids_;         // 需要删除的记录的位置
    std::string tab_name_;          // 表名称
    SmManager *sm_manager_;

   public:
    DeleteExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<Condition> conds,
                   std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
    }

    std::unique_ptr<RmRecord> Next() override {
        for (auto &rid : rids_) {
            if (!fh_->is_record(rid)) {
                continue;
            }
            auto rec = fh_->get_record(rid, context_);
            bool mvcc_delete = false;
            if (context_ != nullptr && context_->txn_mgr_ != nullptr) {
                auto visible = context_->txn_mgr_->get_visible_record(tab_name_, rid, rec.get(), context_->txn_);
                if (!visible.has_value()) {
                    continue;
                }
                if (context_->txn_mgr_->is_mvcc_txn(context_->txn_)) {
                    context_->txn_mgr_->check_write_conflict(tab_name_, rid, context_->txn_);
                    mvcc_delete = true;
                }
                rec = std::make_unique<RmRecord>(*visible);
            }
            if (context_ != nullptr && context_->txn_ != nullptr) {
                context_->txn_->append_write_record(new WriteRecord(WType::DELETE_TUPLE, tab_name_, rid, *rec));
            }
            if (mvcc_delete) {
                if (context_ != nullptr && context_->log_mgr_ != nullptr && context_->txn_ != nullptr &&
                    context_->txn_->get_txn_mode()) {
                    context_->log_mgr_->append_delete(context_->txn_->get_transaction_id(), tab_name_, rid,
                                                       rec->data, rec->size);
                }
                context_->txn_mgr_->record_delete(tab_name_, rid, *rec, context_->txn_);
                continue;
            }
            for (auto &index : tab_.indexes) {
                auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
                std::vector<char> key(index.col_tot_len);
                int offset = 0;
                for (auto &col : index.cols) {
                    memcpy(key.data() + offset, rec->data + col.offset, col.len);
                    offset += col.len;
                }
                ih->delete_entry(key.data(), context_ == nullptr ? nullptr : context_->txn_);
            }
            fh_->delete_record(rid, context_);
        }
        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }
};
