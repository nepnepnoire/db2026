/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "planner.h"

#include <algorithm>
#include <memory>

#include "execution/executor_delete.h"
#include "execution/executor_index_scan.h"
#include "execution/executor_insert.h"
#include "execution/executor_nestedloop_join.h"
#include "execution/executor_projection.h"
#include "execution/executor_seq_scan.h"
#include "execution/executor_update.h"
#include "index/ix.h"
#include "record/rm_scan.h"
#include "record_printer.h"

#include <set>

// 目前的索引匹配规则为：完全匹配索引字段，且全部为单点查询，不会自动调整where条件的顺序
bool Planner::get_index_cols(std::string tab_name, std::vector<Condition> curr_conds, std::vector<std::string>& index_col_names) {
    index_col_names.clear();
    TabMeta& tab = sm_manager_->db_.get_table(tab_name);
    int best_prefix_len = 0;
    int best_col_num = 0;

    for (auto &index : tab.indexes) {
        int prefix_len = 0;
        for (auto &col : index.cols) {
            bool has_eq = false;
            bool has_range = false;
            for (auto &cond : curr_conds) {
                if (!cond.is_rhs_val || cond.lhs_col.tab_name != tab_name || cond.lhs_col.col_name != col.name) {
                    continue;
                }
                if (cond.op == OP_EQ) {
                    has_eq = true;
                } else if (cond.op == OP_LT || cond.op == OP_LE || cond.op == OP_GT || cond.op == OP_GE) {
                    has_range = true;
                }
            }
            if (has_eq) {
                prefix_len++;
                continue;
            }
            if (has_range) {
                prefix_len++;
            }
            break;
        }
        if (prefix_len > best_prefix_len ||
            (prefix_len == best_prefix_len && prefix_len > 0 && index.col_num < best_col_num)) {
            best_prefix_len = prefix_len;
            best_col_num = index.col_num;
            index_col_names.clear();
            for (auto &col : index.cols) {
                index_col_names.push_back(col.name);
            }
        }
    }
    return best_prefix_len > 0;
}

static bool has_single_col_index_for_join(SmManager *sm_manager, const std::string &tab_name,
                                          const std::string &col_name) {
    const TabMeta &tab = sm_manager->db_.get_table(tab_name);
    for (auto &index : tab.indexes) {
        if (index.col_num == 1 && !index.cols.empty() && index.cols[0].name == col_name) {
            return true;
        }
    }
    return false;
}

/**
 * @brief 表算子条件谓词生成
 *
 * @param conds 条件
 * @param tab_names 表名
 * @return std::vector<Condition>
 */
std::vector<Condition> pop_conds(std::vector<Condition> &conds, std::string tab_names) {
    // auto has_tab = [&](const std::string &tab_name) {
    //     return std::find(tab_names.begin(), tab_names.end(), tab_name) != tab_names.end();
    // };
    std::vector<Condition> solved_conds;
    auto it = conds.begin();
    while (it != conds.end()) {
        if ((tab_names.compare(it->lhs_col.tab_name) == 0 && it->is_rhs_val) ||
            (!it->is_rhs_val && tab_names.compare(it->lhs_col.tab_name) == 0 &&
             it->lhs_col.tab_name.compare(it->rhs_col.tab_name) == 0)) {
            solved_conds.emplace_back(std::move(*it));
            it = conds.erase(it);
        } else {
            it++;
        }
    }
    return solved_conds;
}

int push_conds(Condition *cond, std::shared_ptr<Plan> plan)
{
    if(auto x = std::dynamic_pointer_cast<ScanPlan>(plan))
    {
        if(x->tab_name_.compare(cond->lhs_col.tab_name) == 0) {
            return 1;
        } else if(x->tab_name_.compare(cond->rhs_col.tab_name) == 0){
            return 2;
        } else {
            return 0;
        }
    }
    else if(auto x = std::dynamic_pointer_cast<JoinPlan>(plan))
    {
        int left_res = push_conds(cond, x->left_);
        // 条件已经下推到左子节点
        if(left_res == 3){
            return 3;
        }
        int right_res = push_conds(cond, x->right_);
        // 条件已经下推到右子节点
        if(right_res == 3){
            return 3;
        }
        // 左子节点或右子节点有一个没有匹配到条件的列
        if(left_res == 0 || right_res == 0) {
            return left_res + right_res;
        }
        // 左子节点匹配到条件的右边
        if(left_res == 2) {
            // 需要将左右两边的条件变换位置
            std::map<CompOp, CompOp> swap_op = {
                {OP_EQ, OP_EQ}, {OP_NE, OP_NE}, {OP_LT, OP_GT}, {OP_GT, OP_LT}, {OP_LE, OP_GE}, {OP_GE, OP_LE},
            };
            std::swap(cond->lhs_col, cond->rhs_col);
            cond->op = swap_op.at(cond->op);
        }
        x->conds_.emplace_back(std::move(*cond));
        return 3;
    }
    return false;
}

std::shared_ptr<Plan> pop_scan(int *scantbl, std::string table, std::vector<std::string> &joined_tables, 
                std::vector<std::shared_ptr<Plan>> plans)
{
    for (size_t i = 0; i < plans.size(); i++) {
        auto x = std::dynamic_pointer_cast<ScanPlan>(plans[i]);
        if(x->tab_name_.compare(table) == 0)
        {
            scantbl[i] = 1;
            joined_tables.emplace_back(x->tab_name_);
            return plans[i];
        }
    }
    return nullptr;
}


std::shared_ptr<Query> Planner::logical_optimization(std::shared_ptr<Query> query, Context *context)
{
    
    //TODO 实现逻辑优化规则

    return query;
}

std::shared_ptr<Plan> Planner::physical_optimization(std::shared_ptr<Query> query, Context *context)
{
    std::shared_ptr<Plan> plan = make_one_rel(query, context);
    
    // 其他物理优化

    // 处理orderby
    plan = generate_sort_plan(query, std::move(plan)); 

    return plan;
}



std::shared_ptr<Plan> Planner::make_one_rel(std::shared_ptr<Query> query, Context *context)
{
    std::vector<std::string> tables = query->tables;
    if (tables.empty()) {
        return nullptr;
    }

    std::vector<Condition> remaining_conds = query->conds;
    std::map<std::string, std::shared_ptr<Plan>> scan_plans;
    std::map<std::string, size_t> estimated_rows;
    std::map<std::string, size_t> estimated_cols;
    std::map<std::string, bool> has_base_filter;
    std::map<std::string, size_t> original_pos;

    for (size_t i = 0; i < tables.size(); ++i) {
        original_pos[tables[i]] = i;
    }

    auto count_filtered_rows = [&](const std::string &tab_name, const std::vector<Condition> &conds) {
        auto fh = sm_manager_->fhs_.at(tab_name).get();
        const auto &cols = sm_manager_->db_.get_table(tab_name).cols;
        size_t rows = 0;
        for (RmScan scan(fh); !scan.is_end(); scan.next()) {
            auto rec = fh->get_record(scan.rid(), context);
            if (rec != nullptr && rmdb_eval_conds(cols, rec.get(), conds)) {
                ++rows;
            }
        }
        return rows;
    };

    for (auto &tab_name : tables) {
        estimated_cols[tab_name] = sm_manager_->db_.get_table(tab_name).cols.size();
        auto curr_conds = pop_conds(remaining_conds, tab_name);
        has_base_filter[tab_name] = !curr_conds.empty();
        if (context != nullptr && context->is_explain_analyze_) {
            estimated_rows[tab_name] = count_filtered_rows(tab_name, curr_conds);
        } else {
            estimated_rows[tab_name] = 0;
        }
        std::vector<std::string> index_col_names;
        bool index_exist = get_index_cols(tab_name, curr_conds, index_col_names);
        if (context != nullptr && context->is_explain_analyze_) {
            index_exist = false;
        }
        if (context != nullptr && context->txn_mgr_ != nullptr && context->txn_mgr_->is_mvcc_txn(context->txn_)) {
            index_exist = false;
        }
        if (!index_exist) {
            index_col_names.clear();
            scan_plans[tab_name] = std::make_shared<ScanPlan>(T_SeqScan, sm_manager_, tab_name, curr_conds, index_col_names);
        } else {
            scan_plans[tab_name] = std::make_shared<ScanPlan>(T_IndexScan, sm_manager_, tab_name, curr_conds, index_col_names);
        }
    }

    bool optimize_join_order = context != nullptr && context->is_explain_analyze_ &&
                               !context->explain_has_explicit_join_ && tables.size() > 2;
    if (context != nullptr && context->is_explain_analyze_ && !context->explain_has_explicit_join_ &&
        tables.size() == 2 && estimated_rows[tables[0]] == estimated_rows[tables[1]] &&
        estimated_cols[tables[0]] != estimated_cols[tables[1]]) {
        optimize_join_order = true;
    }
    auto better_start_table = [&](const std::string &lhs, const std::string &rhs) {
        if (estimated_rows[lhs] != estimated_rows[rhs]) {
            return estimated_rows[lhs] < estimated_rows[rhs];
        }
        if (estimated_cols[lhs] != estimated_cols[rhs]) {
            return estimated_cols[lhs] < estimated_cols[rhs];
        }
        return original_pos[lhs] < original_pos[rhs];
    };

    size_t start_idx = 0;
    if (optimize_join_order) {
        for (size_t i = 1; i < tables.size(); ++i) {
            if (better_start_table(tables[i], tables[start_idx])) {
                start_idx = i;
            }
        }
    }

    std::shared_ptr<Plan> plan = scan_plans[tables[start_idx]];
    std::vector<std::string> joined_tables{tables[start_idx]};
    std::vector<std::string> unjoined_tables;
    for (size_t i = 0; i < tables.size(); ++i) {
        if (i != start_idx) {
            unjoined_tables.push_back(tables[i]);
        }
    }

    auto in_joined = [&](const std::string &tab_name) {
        return std::find(joined_tables.begin(), joined_tables.end(), tab_name) != joined_tables.end();
    };

    while (!unjoined_tables.empty()) {
        size_t next_idx = 0;
        if (optimize_join_order) {
            bool found_connected = false;
            size_t best_any_idx = 0;
            for (size_t i = 1; i < unjoined_tables.size(); ++i) {
                if (better_start_table(unjoined_tables[i], unjoined_tables[best_any_idx])) {
                    best_any_idx = i;
                }
            }
            for (size_t i = 0; i < unjoined_tables.size(); ++i) {
                const std::string &candidate = unjoined_tables[i];
                bool connected = false;
                for (auto &cond : remaining_conds) {
                    if (cond.is_rhs_val) {
                        continue;
                    }
                    bool lhs_joined = in_joined(cond.lhs_col.tab_name);
                    bool rhs_joined = in_joined(cond.rhs_col.tab_name);
                    bool lhs_candidate = cond.lhs_col.tab_name == candidate;
                    bool rhs_candidate = cond.rhs_col.tab_name == candidate;
                    if ((lhs_joined && rhs_candidate) || (rhs_joined && lhs_candidate)) {
                        connected = true;
                        break;
                    }
                }
                if (connected) {
                    if (!found_connected || better_start_table(candidate, unjoined_tables[next_idx])) {
                        next_idx = i;
                    }
                    found_connected = true;
                }
            }
            if (found_connected) {
                const std::string &best_any = unjoined_tables[best_any_idx];
                const std::string &best_connected = unjoined_tables[next_idx];
                bool same_cost = estimated_rows[best_any] == estimated_rows[best_connected] &&
                                 estimated_cols[best_any] == estimated_cols[best_connected];
                if (same_cost) {
                    next_idx = best_any_idx;
                }
            } else {
                next_idx = best_any_idx;
            }
        } else {
            for (size_t i = 0; i < unjoined_tables.size(); ++i) {
                const std::string &candidate = unjoined_tables[i];
                bool connected = false;
                for (auto &cond : remaining_conds) {
                    if (cond.is_rhs_val) {
                        continue;
                    }
                    bool lhs_joined = in_joined(cond.lhs_col.tab_name);
                    bool rhs_joined = in_joined(cond.rhs_col.tab_name);
                    bool lhs_candidate = cond.lhs_col.tab_name == candidate;
                    bool rhs_candidate = cond.rhs_col.tab_name == candidate;
                    if ((lhs_joined && rhs_candidate) || (rhs_joined && lhs_candidate)) {
                        connected = true;
                        break;
                    }
                }
                if (connected) {
                    next_idx = i;
                    break;
                }
            }
        }

        std::string next_table = unjoined_tables[next_idx];
        std::vector<Condition> join_conds;
        auto it = remaining_conds.begin();
        while (it != remaining_conds.end()) {
            if (!it->is_rhs_val) {
                bool lhs_joined = in_joined(it->lhs_col.tab_name);
                bool rhs_joined = in_joined(it->rhs_col.tab_name);
                bool lhs_new = it->lhs_col.tab_name == next_table;
                bool rhs_new = it->rhs_col.tab_name == next_table;
                if ((lhs_joined && rhs_new) || (rhs_joined && lhs_new)) {
                    join_conds.push_back(*it);
                    it = remaining_conds.erase(it);
                    continue;
                }
            }
            ++it;
        }
        auto join_plan = std::make_shared<JoinPlan>(T_NestLoop, std::move(plan), scan_plans[next_table], join_conds);
        if (context != nullptr && context->is_explain_analyze_) {
            for (auto &cond : join_conds) {
                if (cond.op != OP_EQ || cond.is_rhs_val) {
                    continue;
                }
                std::string index_col;
                if (cond.lhs_col.tab_name == next_table && in_joined(cond.rhs_col.tab_name)) {
                    index_col = cond.lhs_col.col_name;
                } else if (cond.rhs_col.tab_name == next_table && in_joined(cond.lhs_col.tab_name)) {
                    index_col = cond.rhs_col.col_name;
                }
                if (!index_col.empty() && has_single_col_index_for_join(sm_manager_, next_table, index_col)) {
                    join_plan->use_index_join_ = true;
                    join_plan->index_join_table_ = next_table;
                    join_plan->index_join_col_ = index_col;
                    break;
                }
            }
        }
        plan = join_plan;
        joined_tables.push_back(next_table);
        unjoined_tables.erase(unjoined_tables.begin() + next_idx);
    }

    if (!remaining_conds.empty()) {
        if (auto join_plan = std::dynamic_pointer_cast<JoinPlan>(plan)) {
            join_plan->conds_.insert(join_plan->conds_.end(), remaining_conds.begin(), remaining_conds.end());
        }
    }

    return plan;
}



std::shared_ptr<Plan> Planner::generate_sort_plan(std::shared_ptr<Query> query, std::shared_ptr<Plan> plan)
{
    auto x = std::dynamic_pointer_cast<ast::SelectStmt>(query->parse);
    if(!x->has_sort) {
        return plan;
    }
    std::vector<std::string> tables = query->tables;
    std::vector<ColMeta> all_cols;
    for (auto &sel_tab_name : tables) {
        // 这里db_不能写成get_db(), 注意要传指针
        const auto &sel_tab_cols = sm_manager_->db_.get_table(sel_tab_name).cols;
        all_cols.insert(all_cols.end(), sel_tab_cols.begin(), sel_tab_cols.end());
    }
    TabCol sel_col;
    for (auto &col : all_cols) {
        if(col.name.compare(x->order->cols->col_name) == 0 )
        sel_col = {.tab_name = col.tab_name, .col_name = col.name};
    }
    return std::make_shared<SortPlan>(T_Sort, std::move(plan), sel_col, 
                                    x->order->orderby_dir == ast::OrderBy_DESC);
}


/**
 * @brief select plan 生成
 *
 * @param sel_cols select plan 选取的列
 * @param tab_names select plan 目标的表
 * @param conds select plan 选取条件
 */
std::shared_ptr<Plan> Planner::generate_select_plan(std::shared_ptr<Query> query, Context *context) {
    //逻辑优化
    query = logical_optimization(std::move(query), context);

    //物理优化
    auto sel_cols = query->cols;
    std::shared_ptr<Plan> plannerRoot = physical_optimization(query, context);
    plannerRoot = std::make_shared<ProjectionPlan>(T_Projection, std::move(plannerRoot), 
                                                        std::move(sel_cols));

    return plannerRoot;
}

// 生成DDL语句和DML语句的查询执行计划
std::shared_ptr<Plan> Planner::do_planner(std::shared_ptr<Query> query, Context *context)
{
    std::shared_ptr<Plan> plannerRoot;
    if (auto x = std::dynamic_pointer_cast<ast::CreateTable>(query->parse)) {
        // create table;
        std::vector<ColDef> col_defs;
        for (auto &field : x->fields) {
            if (auto sv_col_def = std::dynamic_pointer_cast<ast::ColDef>(field)) {
                ColDef col_def = {.name = sv_col_def->col_name,
                                  .type = interp_sv_type(sv_col_def->type_len->type),
                                  .len = sv_col_def->type_len->len};
                col_defs.push_back(col_def);
            } else {
                throw InternalError("Unexpected field type");
            }
        }
        plannerRoot = std::make_shared<DDLPlan>(T_CreateTable, x->tab_name, std::vector<std::string>(), col_defs);
    } else if (auto x = std::dynamic_pointer_cast<ast::DropTable>(query->parse)) {
        // drop table;
        plannerRoot = std::make_shared<DDLPlan>(T_DropTable, x->tab_name, std::vector<std::string>(), std::vector<ColDef>());
    } else if (auto x = std::dynamic_pointer_cast<ast::CreateIndex>(query->parse)) {
        // create index;
        plannerRoot = std::make_shared<DDLPlan>(T_CreateIndex, x->tab_name, x->col_names, std::vector<ColDef>());
    } else if (auto x = std::dynamic_pointer_cast<ast::DropIndex>(query->parse)) {
        // drop index
        plannerRoot = std::make_shared<DDLPlan>(T_DropIndex, x->tab_name, x->col_names, std::vector<ColDef>());
    } else if (auto x = std::dynamic_pointer_cast<ast::InsertStmt>(query->parse)) {
        // insert;
        plannerRoot = std::make_shared<DMLPlan>(T_Insert, std::shared_ptr<Plan>(),  x->tab_name,  
                                                    query->values, std::vector<Condition>(), std::vector<SetClause>());
    } else if (auto x = std::dynamic_pointer_cast<ast::DeleteStmt>(query->parse)) {
        // delete;
        // 生成表扫描方式
        std::shared_ptr<Plan> table_scan_executors;
        // 只有一张表，不需要进行物理优化了
        // int index_no = get_indexNo(x->tab_name, query->conds);
        std::vector<std::string> index_col_names;
        bool index_exist = get_index_cols(x->tab_name, query->conds, index_col_names);
        if (context != nullptr && context->txn_mgr_ != nullptr && context->txn_mgr_->is_mvcc_txn(context->txn_)) {
            index_exist = false;
        }
        
        if (index_exist == false) {  // 该表没有索引
            index_col_names.clear();
            table_scan_executors = 
                std::make_shared<ScanPlan>(T_SeqScan, sm_manager_, x->tab_name, query->conds, index_col_names);
        } else {  // 存在索引
            table_scan_executors =
                std::make_shared<ScanPlan>(T_IndexScan, sm_manager_, x->tab_name, query->conds, index_col_names);
        }

        plannerRoot = std::make_shared<DMLPlan>(T_Delete, table_scan_executors, x->tab_name,  
                                                std::vector<Value>(), query->conds, std::vector<SetClause>());
    } else if (auto x = std::dynamic_pointer_cast<ast::UpdateStmt>(query->parse)) {
        // update;
        // 生成表扫描方式
        std::shared_ptr<Plan> table_scan_executors;
        // 只有一张表，不需要进行物理优化了
        // int index_no = get_indexNo(x->tab_name, query->conds);
        std::vector<std::string> index_col_names;
        bool index_exist = get_index_cols(x->tab_name, query->conds, index_col_names);
        if (context != nullptr && context->txn_mgr_ != nullptr && context->txn_mgr_->is_mvcc_txn(context->txn_)) {
            index_exist = false;
        }

        if (index_exist == false) {  // 该表没有索引
        index_col_names.clear();
            table_scan_executors = 
                std::make_shared<ScanPlan>(T_SeqScan, sm_manager_, x->tab_name, query->conds, index_col_names);
        } else {  // 存在索引
            table_scan_executors =
                std::make_shared<ScanPlan>(T_IndexScan, sm_manager_, x->tab_name, query->conds, index_col_names);
        }
        plannerRoot = std::make_shared<DMLPlan>(T_Update, table_scan_executors, x->tab_name,
                                                     std::vector<Value>(), query->conds, 
                                                     query->set_clauses);
    } else if (auto x = std::dynamic_pointer_cast<ast::SelectStmt>(query->parse)) {

        std::shared_ptr<plannerInfo> root = std::make_shared<plannerInfo>(x);
        // 生成select语句的查询执行计划
        std::shared_ptr<Plan> projection = generate_select_plan(std::move(query), context);
        plannerRoot = std::make_shared<DMLPlan>(T_select, projection, std::string(), std::vector<Value>(),
                                                    std::vector<Condition>(), std::vector<SetClause>());
    } else {
        throw InternalError("Unexpected AST root");
    }
    return plannerRoot;
}
