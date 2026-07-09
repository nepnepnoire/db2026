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
#include <algorithm>
#include <memory>
#include <vector>

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class SortExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;
    ColMeta sort_col_;                         // 框架中只支持一个键排序
    bool is_desc_;
    std::vector<std::unique_ptr<RmRecord>> tuples_;
    size_t cursor_;

   public:
    SortExecutor(std::unique_ptr<AbstractExecutor> prev, TabCol sel_cols, bool is_desc) {
        prev_ = std::move(prev);
        sort_col_ = prev_->get_col_offset(sel_cols);
        is_desc_ = is_desc;
        cursor_ = 0;
    }

    size_t tupleLen() const override { return prev_->tupleLen(); }

    const std::vector<ColMeta> &cols() const override { return prev_->cols(); }

    std::string getType() override { return "SortExecutor"; }

    void beginTuple() override { 
        tuples_.clear();
        prev_->beginTuple();
        for (; !prev_->is_end(); prev_->nextTuple()) {
            auto rec = prev_->Next();
            if (rec != nullptr) {
                tuples_.push_back(std::move(rec));
            }
        }
        std::sort(tuples_.begin(), tuples_.end(), [&](const auto &a, const auto &b) {
            int cmp = rmdb_compare_raw(sort_col_.type, a->data + sort_col_.offset, b->data + sort_col_.offset, sort_col_.len);
            return is_desc_ ? cmp > 0 : cmp < 0;
        });
        cursor_ = 0;
    }

    void nextTuple() override {
        if (cursor_ < tuples_.size()) {
            ++cursor_;
        }
    }

    bool is_end() const override { return cursor_ >= tuples_.size(); }

    std::unique_ptr<RmRecord> Next() override {
        if (is_end()) {
            return nullptr;
        }
        return std::make_unique<RmRecord>(*tuples_[cursor_]);
    }

    ColMeta get_col_offset(const TabCol &target) override { return *get_col(cols(), target); }

    Rid &rid() override { return _abstract_rid; }
};
