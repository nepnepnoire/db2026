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
#include <cstring>
#include <vector>

#include "defs.h"
#include "errors.h"
#include "common/common.h"
#include "system/sm_meta.h"

inline int rmdb_compare_raw(ColType type, const char *lhs, const char *rhs, int len) {
    if (type == TYPE_INT) {
        int l = *reinterpret_cast<const int *>(lhs);
        int r = *reinterpret_cast<const int *>(rhs);
        return (l > r) - (l < r);
    }
    if (type == TYPE_FLOAT) {
        float l = *reinterpret_cast<const float *>(lhs);
        float r = *reinterpret_cast<const float *>(rhs);
        return (l > r) - (l < r);
    }
    return std::strncmp(lhs, rhs, len);
}

inline bool rmdb_compare_result(int cmp, CompOp op) {
    switch (op) {
        case OP_EQ: return cmp == 0;
        case OP_NE: return cmp != 0;
        case OP_LT: return cmp < 0;
        case OP_GT: return cmp > 0;
        case OP_LE: return cmp <= 0;
        case OP_GE: return cmp >= 0;
    }
    return false;
}

inline std::vector<ColMeta>::const_iterator rmdb_find_col(const std::vector<ColMeta> &cols, const TabCol &target) {
    auto pos = std::find_if(cols.begin(), cols.end(), [&](const ColMeta &col) {
        return col.tab_name == target.tab_name && col.name == target.col_name;
    });
    if (pos == cols.end()) {
        throw ColumnNotFoundError(target.tab_name + "." + target.col_name);
    }
    return pos;
}

inline bool rmdb_eval_cond(const std::vector<ColMeta> &cols, const RmRecord *rec, const Condition &cond) {
    auto lhs_col = rmdb_find_col(cols, cond.lhs_col);
    const char *lhs = rec->data + lhs_col->offset;
    const char *rhs = nullptr;
    ColType rhs_type;
    int rhs_len;

    if (cond.is_rhs_val) {
        if (cond.rhs_val.raw == nullptr) {
            throw InternalError("Condition rhs value is not initialized");
        }
        rhs = cond.rhs_val.raw->data;
        rhs_type = cond.rhs_val.type;
        rhs_len = lhs_col->len;
    } else {
        auto rhs_col = rmdb_find_col(cols, cond.rhs_col);
        rhs = rec->data + rhs_col->offset;
        rhs_type = rhs_col->type;
        rhs_len = rhs_col->len;
    }

    if (lhs_col->type != rhs_type) {
        throw IncompatibleTypeError(coltype2str(lhs_col->type), coltype2str(rhs_type));
    }
    int cmp = rmdb_compare_raw(lhs_col->type, lhs, rhs, std::min(lhs_col->len, rhs_len));
    return rmdb_compare_result(cmp, cond.op);
}

inline bool rmdb_eval_conds(const std::vector<ColMeta> &cols, const RmRecord *rec, const std::vector<Condition> &conds) {
    for (auto &cond : conds) {
        if (!rmdb_eval_cond(cols, rec, cond)) {
            return false;
        }
    }
    return true;
}
