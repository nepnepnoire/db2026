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

#include <limits>

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "record/rm_scan.h"
#include "system/sm.h"

class IndexScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;                      // 表名称
    TabMeta tab_;                               // 表的元数据
    std::vector<Condition> conds_;              // 扫描条件
    RmFileHandle *fh_;                          // 表的数据文件句柄
    std::vector<ColMeta> cols_;                 // 需要读取的字段
    size_t len_;                                // 选取出来的一条记录的长度
    std::vector<Condition> fed_conds_;          // 扫描条件，和conds_字段相同

    std::vector<std::string> index_col_names_;  // index scan涉及到的索引包含的字段
    IndexMeta index_meta_;                      // index scan涉及到的索引元数据

    Rid rid_;
    std::unique_ptr<RmRecord> curr_rec_;
    std::unique_ptr<RecScan> scan_;

    SmManager *sm_manager_;

    void fill_extreme(char *dest, const ColMeta &col, bool high) {
        if (col.type == TYPE_INT) {
            int value = high ? std::numeric_limits<int>::max() : std::numeric_limits<int>::min();
            memcpy(dest, &value, sizeof(int));
        } else if (col.type == TYPE_FLOAT) {
            float value = high ? std::numeric_limits<float>::max() : std::numeric_limits<float>::lowest();
            memcpy(dest, &value, sizeof(float));
        } else {
            memset(dest, high ? 0xff : 0x00, col.len);
        }
    }

    int compare_value(const ColMeta &col, const char *lhs, const char *rhs) {
        return ix_compare(lhs, rhs, col.type, col.len);
    }

    int compare_index_key(const std::vector<char> &lhs, const std::vector<char> &rhs) {
        std::vector<ColType> col_types;
        std::vector<int> col_lens;
        for (auto &col : index_meta_.cols) {
            col_types.push_back(col.type);
            col_lens.push_back(col.len);
        }
        return ix_compare(lhs.data(), rhs.data(), col_types, col_lens);
    }

    void build_scan_range(IxIndexHandle *ih, Iid &lower, Iid &upper) {
        std::vector<char> lower_key(index_meta_.col_tot_len);
        std::vector<char> upper_key(index_meta_.col_tot_len);
        bool lower_inclusive = true;
        bool upper_inclusive = true;
        bool stop_prefix = false;
        int offset = 0;

        for (auto &col : index_meta_.cols) {
            fill_extreme(lower_key.data() + offset, col, false);
            fill_extreme(upper_key.data() + offset, col, true);
            if (stop_prefix) {
                offset += col.len;
                continue;
            }

            const Condition *eq_cond = nullptr;
            const Condition *lower_cond = nullptr;
            const Condition *upper_cond = nullptr;
            bool lower_strict = false;
            bool upper_strict = false;

            for (auto &cond : conds_) {
                if (!cond.is_rhs_val || cond.lhs_col.tab_name != tab_name_ || cond.lhs_col.col_name != col.name) {
                    continue;
                }
                if (cond.op == OP_EQ) {
                    eq_cond = &cond;
                } else if (cond.op == OP_GT || cond.op == OP_GE) {
                    if (lower_cond == nullptr) {
                        lower_cond = &cond;
                        lower_strict = cond.op == OP_GT;
                    } else {
                        int cmp = compare_value(col, cond.rhs_val.raw->data, lower_cond->rhs_val.raw->data);
                        if (cmp > 0 || (cmp == 0 && cond.op == OP_GT && !lower_strict)) {
                            lower_cond = &cond;
                            lower_strict = cond.op == OP_GT;
                        }
                    }
                } else if (cond.op == OP_LT || cond.op == OP_LE) {
                    if (upper_cond == nullptr) {
                        upper_cond = &cond;
                        upper_strict = cond.op == OP_LT;
                    } else {
                        int cmp = compare_value(col, cond.rhs_val.raw->data, upper_cond->rhs_val.raw->data);
                        if (cmp < 0 || (cmp == 0 && cond.op == OP_LT && !upper_strict)) {
                            upper_cond = &cond;
                            upper_strict = cond.op == OP_LT;
                        }
                    }
                }
            }

            if (eq_cond != nullptr) {
                memcpy(lower_key.data() + offset, eq_cond->rhs_val.raw->data, col.len);
                memcpy(upper_key.data() + offset, eq_cond->rhs_val.raw->data, col.len);
            } else {
                if (lower_cond != nullptr) {
                    memcpy(lower_key.data() + offset, lower_cond->rhs_val.raw->data, col.len);
                    lower_inclusive = !lower_strict;
                }
                if (upper_cond != nullptr) {
                    memcpy(upper_key.data() + offset, upper_cond->rhs_val.raw->data, col.len);
                    upper_inclusive = !upper_strict;
                }
                stop_prefix = true;
            }
            offset += col.len;
        }

        int cmp = compare_index_key(lower_key, upper_key);
        if (cmp > 0 || (cmp == 0 && (!lower_inclusive || !upper_inclusive))) {
            lower = ih->leaf_end();
            upper = ih->leaf_end();
            return;
        }
        lower = lower_inclusive ? ih->lower_bound(lower_key.data()) : ih->upper_bound(lower_key.data());
        upper = upper_inclusive ? ih->upper_bound(upper_key.data()) : ih->lower_bound(upper_key.data());
    }

    void advance_to_match() {
        while (scan_ != nullptr && !scan_->is_end()) {
            Rid curr = scan_->rid();
            if (!fh_->is_record(curr)) {
                scan_->next();
                continue;
            }
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
    IndexScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, std::vector<std::string> index_col_names,
                    Context *context) {
        sm_manager_ = sm_manager;
        context_ = context;
        tab_name_ = std::move(tab_name);
        tab_ = sm_manager_->db_.get_table(tab_name_);
        conds_ = std::move(conds);
        index_col_names_ = index_col_names; 
        if (!index_col_names_.empty()) {
            index_meta_ = *(tab_.get_index_meta(index_col_names_));
        }
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab_.cols;
        len_ = cols_.back().offset + cols_.back().len;
        std::map<CompOp, CompOp> swap_op = {
            {OP_EQ, OP_EQ}, {OP_NE, OP_NE}, {OP_LT, OP_GT}, {OP_GT, OP_LT}, {OP_LE, OP_GE}, {OP_GE, OP_LE},
        };

        for (auto &cond : conds_) {
            if (cond.lhs_col.tab_name != tab_name_) {
                assert(!cond.is_rhs_val && cond.rhs_col.tab_name == tab_name_);
                std::swap(cond.lhs_col, cond.rhs_col);
                cond.op = swap_op.at(cond.op);
            }
        }
        fed_conds_ = conds_;
        rid_ = {RM_NO_PAGE, -1};
    }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }

    std::string getType() override { return "IndexScanExecutor"; }

    void beginTuple() override {
        if (context_ != nullptr && context_->collect_select_reads_ && context_->txn_mgr_ != nullptr) {
            context_->txn_mgr_->predicate_select_read(tab_name_, fed_conds_, context_->txn_);
        }
        auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index_meta_.cols)).get();
        Iid lower, upper;
        build_scan_range(ih, lower, upper);
        scan_ = std::make_unique<IxScan>(ih, lower, upper, sm_manager_->get_bpm());
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

    bool is_end() const override { return scan_ == nullptr || scan_->is_end(); }

    std::unique_ptr<RmRecord> Next() override {
        if (is_end()) {
            return nullptr;
        }
        if (curr_rec_ != nullptr) {
            return std::make_unique<RmRecord>(*curr_rec_);
        }
        return fh_->get_record(rid_, context_);
    }

    ColMeta get_col_offset(const TabCol &target) override { return *get_col(cols_, target); }

    Rid &rid() override { return rid_; }
};
