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
#include "record/rm_scan.h"
#include "system/sm.h"

class SeqScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;              // 表的名称
    std::vector<Condition> conds_;      // scan的条件
    RmFileHandle *fh_;                  // 表的数据文件句柄
    std::vector<ColMeta> cols_;         // scan后生成的记录的字段
    size_t len_;                        // scan后生成的每条记录的长度
    std::vector<Condition> fed_conds_;  // 同conds_，两个字段相同

    Rid rid_;
    std::unique_ptr<RmRecord> curr_rec_;
    std::unique_ptr<RecScan> scan_;     // table_iterator

    SmManager *sm_manager_;

    void advance_to_match() {
        while (scan_ != nullptr && !scan_->is_end()) {
            Rid curr = scan_->rid();
            auto physical = fh_->get_record(curr, context_);
            std::unique_ptr<RmRecord> rec = std::move(physical);
            if (context_ != nullptr && context_->txn_mgr_ != nullptr) {
                auto visible = context_->txn_mgr_->get_visible_record(tab_name_, curr, rec.get(), context_->txn_);
                if (!visible.has_value()) {
                    scan_->next();
                    continue;
                }
                rec = std::make_unique<RmRecord>(*visible);
            }
            if (rmdb_eval_conds(cols_, rec.get(), fed_conds_)) {
                rid_ = curr;
                curr_rec_ = std::move(rec);
                if (context_ != nullptr && context_->collect_select_reads_ && context_->txn_mgr_ != nullptr) {
                    context_->txn_mgr_->record_select_read(tab_name_, curr, context_->txn_);
                }
                return;
            }
            scan_->next();
        }
        rid_ = {RM_NO_PAGE, -1};
        curr_rec_.reset();
    }

   public:
    SeqScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = std::move(tab_name);
        conds_ = std::move(conds);
        TabMeta &tab = sm_manager_->db_.get_table(tab_name_);
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab.cols;
        len_ = cols_.back().offset + cols_.back().len;

        context_ = context;

        fed_conds_ = conds_;
        rid_ = {RM_NO_PAGE, -1};
    }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }

    std::string getType() override { return "SeqScanExecutor"; }

    void beginTuple() override {
        if (context_ != nullptr && context_->collect_select_reads_ && context_->txn_mgr_ != nullptr) {
            context_->txn_mgr_->predicate_select_read(tab_name_, fed_conds_, context_->txn_);
        }
        scan_ = std::make_unique<RmScan>(fh_);
        advance_to_match();
    }

    void nextTuple() override {
        if (scan_ == nullptr || scan_->is_end()) {
            rid_ = {RM_NO_PAGE, -1};
            return;
        }
        scan_->next();
        advance_to_match();
    }

    bool is_end() const override {
        return scan_ == nullptr || scan_->is_end();
    }

    std::unique_ptr<RmRecord> Next() override {
        if (is_end()) {
            return nullptr;
        }
        if (curr_rec_ != nullptr) {
            return std::make_unique<RmRecord>(*curr_rec_);
        }
        return fh_->get_record(rid_, context_);
    }

    ColMeta get_col_offset(const TabCol &target) override {
        return *get_col(cols_, target);
    }

    Rid &rid() override { return rid_; }
};
