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
#include <cerrno>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <set>
#include <sstream>
#include <string>
#include "optimizer/plan.h"
#include "execution/executor_abstract.h"
#include "execution/executor_nestedloop_join.h"
#include "execution/executor_projection.h"
#include "execution/executor_seq_scan.h"
#include "execution/executor_index_scan.h"
#include "execution/executor_update.h"
#include "execution/executor_insert.h"
#include "execution/executor_delete.h"
#include "execution/execution_sort.h"
#include "common/common.h"
#include "common/config.h"

typedef enum portalTag{
    PORTAL_Invalid_Query = 0,
    PORTAL_ONE_SELECT,
    PORTAL_DML_WITHOUT_SELECT,
    PORTAL_MULTI_QUERY,
    PORTAL_CMD_UTILITY
} portalTag;


struct PortalStmt {
    portalTag tag;
    
    std::vector<TabCol> sel_cols;
    std::unique_ptr<AbstractExecutor> root;
    std::shared_ptr<Plan> plan;
    
    PortalStmt(portalTag tag_, std::vector<TabCol> sel_cols_, std::unique_ptr<AbstractExecutor> root_, std::shared_ptr<Plan> plan_) :
            tag(tag_), sel_cols(std::move(sel_cols_)), root(std::move(root_)), plan(std::move(plan_)) {}
};

class Portal
{
   private:
    SmManager *sm_manager_;

    std::shared_ptr<Plan> unwrap_select_plan(std::shared_ptr<Plan> plan) {
        if (auto dml = std::dynamic_pointer_cast<DMLPlan>(plan)) {
            if (dml->tag == T_select) {
                return dml->subplan_;
            }
        }
        return plan;
    }

    std::string display_table_name(const std::string &tab_name, Context *context) const {
        if (context != nullptr) {
            auto it = context->explain_tab_aliases_.find(tab_name);
            if (it != context->explain_tab_aliases_.end() && !it->second.empty()) {
                return it->second;
            }
        }
        return tab_name;
    }

    std::string display_col_name(const TabCol &col, Context *context) const {
        return display_table_name(col.tab_name, context) + "." + col.col_name;
    }

    std::string value_to_string(const Value &value) const {
        if (value.type == TYPE_INT) {
            return std::to_string(value.int_val);
        }
        if (value.type == TYPE_FLOAT) {
            std::ostringstream os;
            os << std::fixed << std::setprecision(6) << value.float_val;
            return os.str();
        }
        return "'" + value.str_val + "'";
    }

    std::string op_to_string(CompOp op) const {
        switch (op) {
            case OP_EQ: return "=";
            case OP_NE: return "<>";
            case OP_LT: return "<";
            case OP_GT: return ">";
            case OP_LE: return "<=";
            case OP_GE: return ">=";
        }
        return "=";
    }

    std::string condition_to_string(const Condition &cond, Context *context) const {
        std::string rhs = cond.is_rhs_val ? value_to_string(cond.rhs_val) : display_col_name(cond.rhs_col, context);
        return display_col_name(cond.lhs_col, context) + op_to_string(cond.op) + rhs;
    }

    std::string join_strings(const std::vector<std::string> &items, const std::string &sep) const {
        std::string out;
        for (size_t i = 0; i < items.size(); ++i) {
            if (i > 0) {
                out += sep;
            }
            out += items[i];
        }
        return out;
    }

    std::vector<std::string> format_columns(const std::set<TabCol> &cols, Context *context) const {
        std::vector<std::string> result;
        for (auto &col : cols) {
            result.push_back(display_col_name(col, context));
        }
        std::sort(result.begin(), result.end());
        return result;
    }

    std::vector<std::string> format_columns(const std::vector<TabCol> &cols, Context *context) const {
        std::set<TabCol> unique_cols(cols.begin(), cols.end());
        return format_columns(unique_cols, context);
    }

    std::vector<std::string> format_conditions(const std::vector<Condition> &conds, Context *context) const {
        std::vector<std::string> result;
        for (auto &cond : conds) {
            result.push_back(condition_to_string(cond, context));
        }
        std::sort(result.begin(), result.end());
        return result;
    }

    std::set<std::string> collect_table_set(std::shared_ptr<Plan> plan) const {
        std::set<std::string> tables;
        if (auto dml = std::dynamic_pointer_cast<DMLPlan>(plan)) {
            return collect_table_set(dml->subplan_);
        }
        if (auto projection = std::dynamic_pointer_cast<ProjectionPlan>(plan)) {
            return collect_table_set(projection->subplan_);
        }
        if (auto sort = std::dynamic_pointer_cast<SortPlan>(plan)) {
            return collect_table_set(sort->subplan_);
        }
        if (auto scan = std::dynamic_pointer_cast<ScanPlan>(plan)) {
            tables.insert(scan->tab_name_);
            return tables;
        }
        if (auto join = std::dynamic_pointer_cast<JoinPlan>(plan)) {
            auto left_tables = collect_table_set(join->left_);
            auto right_tables = collect_table_set(join->right_);
            tables.insert(left_tables.begin(), left_tables.end());
            tables.insert(right_tables.begin(), right_tables.end());
        }
        return tables;
    }

    bool is_single_scan_table(std::shared_ptr<Plan> plan, const std::string &tab_name) const {
        if (auto scan = std::dynamic_pointer_cast<ScanPlan>(plan)) {
            return scan->tab_name_ == tab_name;
        }
        if (auto projection = std::dynamic_pointer_cast<ProjectionPlan>(plan)) {
            return is_single_scan_table(projection->subplan_, tab_name);
        }
        if (auto sort = std::dynamic_pointer_cast<SortPlan>(plan)) {
            return is_single_scan_table(sort->subplan_, tab_name);
        }
        return false;
    }

    size_t count_plan_rows(std::shared_ptr<Plan> plan, Context *context) {
        plan = unwrap_select_plan(plan);
        if (plan == nullptr) {
            return 0;
        }
        auto executor = convert_plan_executor(plan, context);
        if (executor == nullptr) {
            return 0;
        }
        size_t rows = 0;
        for (executor->beginTuple(); !executor->is_end(); executor->nextTuple()) {
            if (executor->Next() != nullptr) {
                ++rows;
            }
        }
        return rows;
    }

    size_t count_table_rows(const std::string &tab_name) const {
        auto fh = sm_manager_->fhs_.at(tab_name).get();
        RmScan scan(fh);
        size_t rows = 0;
        for (; !scan.is_end(); scan.next()) {
            ++rows;
        }
        return rows;
    }

    void append_explain_line(Context *context, const std::string &line) const {
        std::string text = line + "\n";
        if (context != nullptr && context->data_send_ != nullptr && context->offset_ != nullptr) {
            if (*(context->offset_) < 0) {
                *(context->offset_) = 0;
            }
            int remaining = BUFFER_LENGTH - *(context->offset_) - 1;
            if (remaining > 0) {
                int copy_len = std::min<int>(remaining, static_cast<int>(text.size()));
                memcpy(context->data_send_ + *(context->offset_), text.data(), copy_len);
                *(context->offset_) += copy_len;
                context->data_send_[*(context->offset_)] = '\0';
            }
        }
        if (!output_file_enabled()) {
            return;
        }
        std::fstream outfile;
        outfile.open("output.txt", std::ios::out | std::ios::app);
        outfile << text;
        outfile.close();
    }

    void add_join_cols_to_required(const Condition &cond, const std::set<std::string> &left_tables,
                                   const std::set<std::string> &right_tables, std::set<TabCol> &left_required,
                                   std::set<TabCol> &right_required) const {
        auto add_col = [&](const TabCol &col) {
            if (left_tables.count(col.tab_name) > 0) {
                left_required.insert(col);
            } else if (right_tables.count(col.tab_name) > 0) {
                right_required.insert(col);
            }
        };
        add_col(cond.lhs_col);
        if (!cond.is_rhs_val) {
            add_col(cond.rhs_col);
        }
    }

    bool project_wrap_reduces_columns(std::shared_ptr<Plan> plan, const std::set<TabCol> &required_cols) const {
        if (required_cols.empty()) {
            return false;
        }
        auto tables = collect_table_set(plan);
        size_t total_cols = 0;
        size_t required_in_subtree = 0;
        for (auto &tab_name : tables) {
            total_cols += sm_manager_->db_.get_table(tab_name).cols.size();
        }
        for (auto &col : required_cols) {
            if (tables.count(col.tab_name) > 0) {
                ++required_in_subtree;
            }
        }
        return required_in_subtree > 0 && required_in_subtree < total_cols;
    }

    void explain_plan_node(std::shared_ptr<Plan> plan, Context *context, int depth, size_t multiplier,
                           const std::set<TabCol> &required_cols, bool allow_project_wrap,
                           bool force_index_scan = false, const std::string &force_index_col = "",
                           size_t force_index_rows = 0, bool force_project_all_cols = false) {
        if (plan == nullptr) {
            return;
        }
        std::string indent(static_cast<size_t>(depth), '\t');

        if (auto dml = std::dynamic_pointer_cast<DMLPlan>(plan)) {
            explain_plan_node(dml->subplan_, context, depth, multiplier, required_cols, allow_project_wrap,
                              force_index_scan, force_index_col, force_index_rows, force_project_all_cols);
            return;
        }
        if (auto sort = std::dynamic_pointer_cast<SortPlan>(plan)) {
            auto next_required = required_cols;
            if (!sort->sel_col_.tab_name.empty() || !sort->sel_col_.col_name.empty()) {
                next_required.insert(sort->sel_col_);
            }
            explain_plan_node(sort->subplan_, context, depth, multiplier, next_required, allow_project_wrap,
                              force_index_scan, force_index_col, force_index_rows, force_project_all_cols);
            return;
        }
        if (auto projection = std::dynamic_pointer_cast<ProjectionPlan>(plan)) {
            size_t rows = count_plan_rows(projection, context) * multiplier;
            std::string cols;
            std::set<TabCol> child_required;
            if (context != nullptr && context->explain_select_all_) {
                cols = "*";
            } else {
                auto col_names = format_columns(projection->sel_cols_, context);
                cols = join_strings(col_names, ", ");
                child_required.insert(projection->sel_cols_.begin(), projection->sel_cols_.end());
            }
            append_explain_line(context, indent + "Project(columns=[" + cols + "], rows=" + std::to_string(rows) + ")");
            explain_plan_node(projection->subplan_, context, depth + 1, multiplier, child_required, false);
            return;
        }
        if (auto join = std::dynamic_pointer_cast<JoinPlan>(plan)) {
            size_t rows = count_plan_rows(join, context) * multiplier;
            auto tables = collect_table_set(join);
            if (allow_project_wrap && project_wrap_reduces_columns(plan, required_cols)) {
                auto col_names = format_columns(required_cols, context);
                append_explain_line(context, indent + "Project(columns=[" + join_strings(col_names, ", ") +
                                                    "], rows=" + std::to_string(rows) + ")");
                explain_plan_node(plan, context, depth + 1, multiplier, required_cols, false,
                                  force_index_scan, force_index_col, force_index_rows, force_project_all_cols);
                return;
            }
            std::vector<std::string> table_names(tables.begin(), tables.end());
            std::sort(table_names.begin(), table_names.end());
            auto cond_names = format_conditions(join->conds_, context);
            append_explain_line(context, indent + "Join(tables=[" + join_strings(table_names, ", ") +
                                                "], condition=[" + join_strings(cond_names, ", ") +
                                                "], rows=" + std::to_string(rows) + ")");

            auto left_tables = collect_table_set(join->left_);
            auto right_tables = collect_table_set(join->right_);
            std::set<TabCol> left_required;
            std::set<TabCol> right_required;
            for (auto &col : required_cols) {
                if (left_tables.count(col.tab_name) > 0) {
                    left_required.insert(col);
                } else if (right_tables.count(col.tab_name) > 0) {
                    right_required.insert(col);
                }
            }
            for (auto &cond : join->conds_) {
                add_join_cols_to_required(cond, left_tables, right_tables, left_required, right_required);
            }

            bool allow_child_project = !(context != nullptr && context->explain_select_all_);
            bool child_force_project = allow_child_project && !join->conds_.empty() &&
                                       (force_project_all_cols || tables.size() >= 3);
            explain_plan_node(join->left_, context, depth + 1, multiplier, left_required, allow_child_project,
                              false, "", 0, child_force_project);
            size_t left_rows = count_plan_rows(join->left_, context);
            if (join->use_index_join_ && is_single_scan_table(join->right_, join->index_join_table_)) {
                explain_plan_node(join->right_, context, depth + 1, 1, right_required, allow_child_project,
                                  true, join->index_join_col_, rows, child_force_project);
            } else {
                explain_plan_node(join->right_, context, depth + 1, multiplier * left_rows, right_required, allow_child_project,
                                  false, "", 0, child_force_project);
            }
            return;
        }
        if (auto scan = std::dynamic_pointer_cast<ScanPlan>(plan)) {
            bool use_forced_index = force_index_scan && !force_index_col.empty();
            bool use_plan_index = scan->tag == T_IndexScan && !scan->index_col_names_.empty();
            bool use_index_scan = use_forced_index || use_plan_index;
            std::string index_col = use_forced_index ? force_index_col :
                                    (use_plan_index ? scan->index_col_names_.front() : "");
            size_t filtered_rows = use_forced_index ? force_index_rows : count_plan_rows(scan, context) * multiplier;
            bool should_wrap_scan_project = allow_project_wrap &&
                                            (project_wrap_reduces_columns(plan, required_cols) ||
                                             (!required_cols.empty() && (force_project_all_cols ||
                                                                        !scan->conds_.empty() ||
                                                                        multiplier == 0)));
            if (should_wrap_scan_project) {
                auto col_names = format_columns(required_cols, context);
                append_explain_line(context, indent + "Project(columns=[" + join_strings(col_names, ", ") +
                                                    "], rows=" + std::to_string(filtered_rows) + ")");
                explain_plan_node(scan, context, depth + 1, multiplier, std::set<TabCol>(), false,
                                  force_index_scan, force_index_col, force_index_rows, force_project_all_cols);
                return;
            }

            size_t scan_rows = use_forced_index ? force_index_rows : count_table_rows(scan->tab_name_) * multiplier;
            auto scan_line = [&](const std::string &line_indent, size_t rows) {
                std::string line = line_indent + "Scan(table=" + scan->tab_name_;
                if (use_index_scan) {
                    line += ", type=IndexScan, using_index=(" + index_col + ")";
                } else {
                    line += ", type=SeqScan";
                }
                line += ", rows=" + std::to_string(rows) + ")";
                append_explain_line(context, line);
            };
            if (!scan->conds_.empty()) {
                auto cond_names = format_conditions(scan->conds_, context);
                append_explain_line(context, indent + "Filter(condition=[" + join_strings(cond_names, ", ") +
                                                    "], rows=" + std::to_string(filtered_rows) + ")");
                std::string child_indent(static_cast<size_t>(depth + 1), '\t');
                scan_line(child_indent, scan_rows);
            } else {
                scan_line(indent, scan_rows);
            }
        }
    }
    

   public:
    Portal(SmManager *sm_manager) : sm_manager_(sm_manager){}
    ~Portal(){}

    // 将查询执行计划转换成对应的算子树
    std::shared_ptr<PortalStmt> start(std::shared_ptr<Plan> plan, Context *context)
    {
        // 这里可以将select进行拆分，例如：一个select，带有return的select等
        if (auto x = std::dynamic_pointer_cast<OtherPlan>(plan)) {
            return std::make_shared<PortalStmt>(PORTAL_CMD_UTILITY, std::vector<TabCol>(), std::unique_ptr<AbstractExecutor>(),plan);
        } else if(auto x = std::dynamic_pointer_cast<SetKnobPlan>(plan)) {
            return std::make_shared<PortalStmt>(PORTAL_CMD_UTILITY, std::vector<TabCol>(), std::unique_ptr<AbstractExecutor>(), plan); 
        } else if (auto x = std::dynamic_pointer_cast<DDLPlan>(plan)) {
            return std::make_shared<PortalStmt>(PORTAL_MULTI_QUERY, std::vector<TabCol>(), std::unique_ptr<AbstractExecutor>(),plan);
        } else if (auto x = std::dynamic_pointer_cast<DMLPlan>(plan)) {
            switch(x->tag) {
                case T_select:
                {
                    std::shared_ptr<ProjectionPlan> p = std::dynamic_pointer_cast<ProjectionPlan>(x->subplan_);
                    std::unique_ptr<AbstractExecutor> root= convert_plan_executor(p, context);
                    return std::make_shared<PortalStmt>(PORTAL_ONE_SELECT, std::move(p->sel_cols_), std::move(root), plan);
                }
                    
                case T_Update:
                {
                    std::unique_ptr<AbstractExecutor> scan= convert_plan_executor(x->subplan_, context);
                    std::vector<Rid> rids;
                    for (scan->beginTuple(); !scan->is_end(); scan->nextTuple()) {
                        rids.push_back(scan->rid());
                    }
                    std::unique_ptr<AbstractExecutor> root =std::make_unique<UpdateExecutor>(sm_manager_, 
                                                            x->tab_name_, x->set_clauses_, x->conds_, rids, context);
                    return std::make_shared<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<TabCol>(), std::move(root), plan);
                }
                case T_Delete:
                {
                    std::unique_ptr<AbstractExecutor> scan= convert_plan_executor(x->subplan_, context);
                    std::vector<Rid> rids;
                    for (scan->beginTuple(); !scan->is_end(); scan->nextTuple()) {
                        rids.push_back(scan->rid());
                    }

                    std::unique_ptr<AbstractExecutor> root =
                        std::make_unique<DeleteExecutor>(sm_manager_, x->tab_name_, x->conds_, rids, context);

                    return std::make_shared<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<TabCol>(), std::move(root), plan);
                }

                case T_Insert:
                {
                    std::unique_ptr<AbstractExecutor> root =
                            std::make_unique<InsertExecutor>(sm_manager_, x->tab_name_, x->values_, context);
            
                    return std::make_shared<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<TabCol>(), std::move(root), plan);
                }


                default:
                    throw InternalError("Unexpected field type");
                    break;
            }
        } else {
            throw InternalError("Unexpected field type");
        }
        return nullptr;
    }

    // 遍历算子树并执行算子生成执行结果
    void run(std::shared_ptr<PortalStmt> portal, QlManager* ql, txn_id_t *txn_id, Context *context){
        switch(portal->tag) {
            case PORTAL_ONE_SELECT:
            {
                ql->select_from(std::move(portal->root), std::move(portal->sel_cols), context);
                break;
            }

            case PORTAL_DML_WITHOUT_SELECT:
            {
                ql->run_dml(std::move(portal->root));
                break;
            }
            case PORTAL_MULTI_QUERY:
            {
                ql->run_mutli_query(portal->plan, context);
                break;
            }
            case PORTAL_CMD_UTILITY:
            {
                ql->run_cmd_utility(portal->plan, txn_id, context);
                break;
            }
            default:
            {
                throw InternalError("Unexpected field type");
            }
        }
    }

    // 清空资源
    void drop(){}

    void explain_analyze(std::shared_ptr<Plan> plan, Context *context) {
        std::shared_ptr<Plan> select_plan = unwrap_select_plan(plan);
        if (select_plan == nullptr) {
            return;
        }
        explain_plan_node(select_plan, context, 0, 1, std::set<TabCol>(), false);
    }


    std::unique_ptr<AbstractExecutor> convert_plan_executor(std::shared_ptr<Plan> plan, Context *context)
    {
        if(auto x = std::dynamic_pointer_cast<ProjectionPlan>(plan)){
            return std::make_unique<ProjectionExecutor>(convert_plan_executor(x->subplan_, context), 
                                                        x->sel_cols_);
        } else if(auto x = std::dynamic_pointer_cast<ScanPlan>(plan)) {
            if(x->tag == T_SeqScan) {
                return std::make_unique<SeqScanExecutor>(sm_manager_, x->tab_name_, x->conds_, context);
            }
            else {
                return std::make_unique<IndexScanExecutor>(sm_manager_, x->tab_name_, x->conds_, x->index_col_names_, context);
            } 
        } else if(auto x = std::dynamic_pointer_cast<JoinPlan>(plan)) {
            std::unique_ptr<AbstractExecutor> left = convert_plan_executor(x->left_, context);
            std::unique_ptr<AbstractExecutor> right = convert_plan_executor(x->right_, context);
            std::unique_ptr<AbstractExecutor> join = std::make_unique<NestedLoopJoinExecutor>(
                                std::move(left), 
                                std::move(right), x->conds_);
            return join;
        } else if(auto x = std::dynamic_pointer_cast<SortPlan>(plan)) {
            return std::make_unique<SortExecutor>(convert_plan_executor(x->subplan_, context), 
                                            x->sel_col_, x->is_desc_);
        }
        return nullptr;
    }

};
