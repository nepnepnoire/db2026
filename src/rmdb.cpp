/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include <netinet/in.h>
#include <setjmp.h>
#include <signal.h>
#include <unistd.h>
#include <algorithm>
#include <atomic>
#include <cctype>
#include <cstdlib>
#include <condition_variable>
#include <functional>
#include <iomanip>
#include <fstream>
#include <map>
#include <mutex>
#include <set>
#include <sstream>
#include <vector>

#include "errors.h"
#include "optimizer/optimizer.h"
#include "recovery/log_recovery.h"
#include "optimizer/plan.h"
#include "optimizer/planner.h"
#include "portal.h"
#include "analyze/analyze.h"

#define SOCK_PORT 8765
#define MAX_CONN_LIMIT 8

static bool should_exit = false;

// 鏋勫缓鍏ㄥ眬鎵€闇€鐨勭鐞嗗櫒瀵硅薄
auto disk_manager = std::make_unique<DiskManager>();
auto buffer_pool_manager = std::make_unique<BufferPoolManager>(BUFFER_POOL_SIZE, disk_manager.get());
auto rm_manager = std::make_unique<RmManager>(disk_manager.get(), buffer_pool_manager.get());
auto ix_manager = std::make_unique<IxManager>(disk_manager.get(), buffer_pool_manager.get());
auto sm_manager = std::make_unique<SmManager>(disk_manager.get(), buffer_pool_manager.get(), rm_manager.get(), ix_manager.get());
auto lock_manager = std::make_unique<LockManager>();
auto txn_manager = std::make_unique<TransactionManager>(lock_manager.get(), sm_manager.get());
auto planner = std::make_unique<Planner>(sm_manager.get());
auto optimizer = std::make_unique<Optimizer>(sm_manager.get(), planner.get());
auto ql_manager = std::make_unique<QlManager>(sm_manager.get(), txn_manager.get(), nullptr);
auto log_manager = std::make_unique<LogManager>(disk_manager.get());
auto recovery = std::make_unique<RecoveryManager>(disk_manager.get(), buffer_pool_manager.get(), sm_manager.get());
auto portal = std::make_unique<Portal>(sm_manager.get());
auto analyze = std::make_unique<Analyze>(sm_manager.get());
pthread_mutex_t *buffer_mutex;
pthread_mutex_t *sockfd_mutex;

static std::mutex checkpoint_mutex;
static std::condition_variable checkpoint_cv;
static bool checkpoint_in_progress = false;
static int active_sql_requests = 0;
static std::mutex detached_txn_mutex;
static txn_id_t detached_txn_id = INVALID_TXN_ID;

class SqlRequestGuard {
public:
    explicit SqlRequestGuard(bool enabled) : active_(false) {
        if (!enabled) {
            return;
        }
        std::unique_lock<std::mutex> lock(checkpoint_mutex);
        checkpoint_cv.wait(lock, [] { return !checkpoint_in_progress; });
        ++active_sql_requests;
        active_ = true;
    }

    ~SqlRequestGuard() {
        if (!active_) {
            return;
        }
        std::lock_guard<std::mutex> lock(checkpoint_mutex);
        --active_sql_requests;
        if (active_sql_requests == 0) {
            checkpoint_cv.notify_all();
        }
    }

private:
    bool active_;
};

class StaticCheckpointGuard {
public:
    StaticCheckpointGuard() : locked_(false) {
        std::unique_lock<std::mutex> lock(checkpoint_mutex);
        checkpoint_cv.wait(lock, [] { return !checkpoint_in_progress; });
        checkpoint_in_progress = true;
        checkpoint_cv.wait(lock, [] { return active_sql_requests == 0; });
        locked_ = true;
    }

    ~StaticCheckpointGuard() {
        if (!locked_) {
            return;
        }
        std::lock_guard<std::mutex> lock(checkpoint_mutex);
        checkpoint_in_progress = false;
        checkpoint_cv.notify_all();
    }

private:
    bool locked_;
};

static jmp_buf jmpbuf;
void sigint_handler(int signo) {
    should_exit = true;
    log_manager->flush_log_to_disk();
    std::cout << "The Server receive Crtl+C, will been closed\n";
    longjmp(jmpbuf, 1);
}

// 鍒ゆ柇褰撳墠姝ｅ湪鎵ц鐨勬槸鏄惧紡浜嬪姟杩樻槸鍗曟潯SQL璇彞鐨勪簨鍔★紝骞舵洿鏂颁簨鍔D
void SetTransaction(txn_id_t *txn_id, Context *context) {
    context->txn_ = txn_manager->get_transaction(*txn_id);
    if(context->txn_ == nullptr || context->txn_->get_state() == TransactionState::COMMITTED ||
        context->txn_->get_state() == TransactionState::ABORTED) {
        context->txn_ = txn_manager->begin(nullptr, context->log_mgr_);
        *txn_id = context->txn_->get_transaction_id();
        context->txn_->set_txn_mode(false);
        context->txn_->set_isolation_level(context->session_isolation_);
        context->txn_->set_mvcc_enabled(context->session_mvcc_enabled_);
    }
}

static int current_active_sql_requests() {
    std::lock_guard<std::mutex> lock(checkpoint_mutex);
    return active_sql_requests;
}

static void attach_detached_txn_if_needed(txn_id_t *txn_id, bool allow_detached) {
    if (txn_id == nullptr || *txn_id != INVALID_TXN_ID || !allow_detached) {
        return;
    }
    if (current_active_sql_requests() != 1) {
        return;
    }
    std::lock_guard<std::mutex> lock(detached_txn_mutex);
    if (detached_txn_id == INVALID_TXN_ID) {
        return;
    }
    Transaction *txn = txn_manager->get_transaction(detached_txn_id);
    if (txn == nullptr || !txn->get_txn_mode() || txn->get_state() == TransactionState::COMMITTED ||
        txn->get_state() == TransactionState::ABORTED) {
        detached_txn_id = INVALID_TXN_ID;
        return;
    }
    *txn_id = detached_txn_id;
}

static void remember_detached_txn(Transaction *txn) {
    if (txn == nullptr) {
        return;
    }
    std::lock_guard<std::mutex> lock(detached_txn_mutex);
    bool live_explicit_txn = txn->get_txn_mode() &&
                             txn->get_state() != TransactionState::COMMITTED &&
                             txn->get_state() != TransactionState::ABORTED;
    if (live_explicit_txn) {
        detached_txn_id = txn->get_transaction_id();
        return;
    }
    if (detached_txn_id == txn->get_transaction_id()) {
        detached_txn_id = INVALID_TXN_ID;
    }
}

static std::string lower_token(std::string token) {
    std::transform(token.begin(), token.end(), token.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
    return token;
}

static bool try_parse_show_index(const char *sql) {
    std::string text(sql);
    while (!text.empty() && std::isspace(static_cast<unsigned char>(text.back()))) {
        text.pop_back();
    }
    if (!text.empty() && text.back() == ';') {
        text.pop_back();
    }
    std::stringstream ss(text);
    std::string show, index, from, table, extra;
    ss >> show >> index >> from >> table >> extra;
    if (lower_token(show) == "show" && lower_token(index) == "index" &&
        lower_token(from) == "from" && !table.empty() && extra.empty()) {
        ast::parse_tree = std::make_shared<ast::ShowIndex>(table);
        return true;
    }
    return false;
}

static bool sql_ieq(const std::string &lhs, const std::string &rhs) {
    return lower_token(lhs) == lower_token(rhs);
}

static bool is_ident_token(const std::string &token) {
    if (token.empty()) {
        return false;
    }
    unsigned char first = static_cast<unsigned char>(token[0]);
    if (!std::isalpha(first) && token[0] != '_') {
        return false;
    }
    for (char ch : token) {
        unsigned char c = static_cast<unsigned char>(ch);
        if (!std::isalnum(c) && ch != '_') {
            return false;
        }
    }
    return true;
}

static bool is_alias_stop_word(const std::string &token) {
    static const std::vector<std::string> stop_words = {
        "where", "join", "on", "order", "by", "asc", "desc", "inner", "left", "right", "full", "outer",
        "group", "having", "limit", "union", "all"
    };
    std::string lower = lower_token(token);
    return std::find(stop_words.begin(), stop_words.end(), lower) != stop_words.end();
}

static std::vector<std::string> tokenize_sql(const std::string &sql) {
    std::vector<std::string> tokens;
    for (size_t i = 0; i < sql.size();) {
        unsigned char c = static_cast<unsigned char>(sql[i]);
        if (std::isspace(c)) {
            ++i;
            continue;
        }
        if (sql[i] == '-' && i + 1 < sql.size() && sql[i + 1] == '-') {
            i += 2;
            while (i < sql.size() && sql[i] != '\n' && sql[i] != '\r') {
                ++i;
            }
            continue;
        }
        if (sql[i] == '/' && i + 1 < sql.size() && sql[i + 1] == '*') {
            i += 2;
            while (i + 1 < sql.size() && !(sql[i] == '*' && sql[i + 1] == '/')) {
                ++i;
            }
            if (i + 1 < sql.size()) {
                i += 2;
            }
            continue;
        }
        if (sql[i] == '\'') {
            size_t start = i++;
            while (i < sql.size()) {
                if (sql[i] == '\'') {
                    ++i;
                    break;
                }
                ++i;
            }
            tokens.push_back(sql.substr(start, i - start));
            continue;
        }
        if (std::isalpha(c) || sql[i] == '_') {
            size_t start = i++;
            while (i < sql.size()) {
                unsigned char cc = static_cast<unsigned char>(sql[i]);
                if (!std::isalnum(cc) && sql[i] != '_') {
                    break;
                }
                ++i;
            }
            tokens.push_back(sql.substr(start, i - start));
            continue;
        }
        if (std::isdigit(c) ||
            ((sql[i] == '+' || sql[i] == '-') && i + 1 < sql.size() &&
             std::isdigit(static_cast<unsigned char>(sql[i + 1])))) {
            size_t start = i++;
            bool has_dot = false;
            while (i < sql.size()) {
                unsigned char cc = static_cast<unsigned char>(sql[i]);
                if (sql[i] == '.' && !has_dot) {
                    has_dot = true;
                    ++i;
                    continue;
                }
                if (!std::isdigit(cc)) {
                    break;
                }
                ++i;
            }
            tokens.push_back(sql.substr(start, i - start));
            continue;
        }
        if ((sql[i] == '<' || sql[i] == '>') && i + 1 < sql.size() && sql[i + 1] == '=') {
            tokens.push_back(sql.substr(i, 2));
            i += 2;
            continue;
        }
        if (sql[i] == '<' && i + 1 < sql.size() && sql[i + 1] == '>') {
            tokens.push_back(sql.substr(i, 2));
            i += 2;
            continue;
        }
        tokens.push_back(sql.substr(i, 1));
        ++i;
    }
    return tokens;
}

static bool try_set_transaction_isolation(const char *raw_sql, IsolationLevel &session_isolation,
                                          bool &session_mvcc_enabled) {
    std::vector<std::string> tokens = tokenize_sql(raw_sql);
    while (!tokens.empty() && tokens.back() == ";") {
        tokens.pop_back();
    }
    if (tokens.size() == 6 && sql_ieq(tokens[0], "set") && sql_ieq(tokens[1], "transaction") &&
        sql_ieq(tokens[2], "isolation") && sql_ieq(tokens[3], "level") &&
        sql_ieq(tokens[4], "snapshot") && sql_ieq(tokens[5], "isolation")) {
        session_isolation = IsolationLevel::SNAPSHOT_ISOLATION;
        session_mvcc_enabled = true;
        return true;
    }
    if (tokens.size() == 5 && sql_ieq(tokens[0], "set") && sql_ieq(tokens[1], "transaction") &&
        sql_ieq(tokens[2], "isolation") && sql_ieq(tokens[3], "level") &&
        sql_ieq(tokens[4], "serializable")) {
        session_isolation = IsolationLevel::SERIALIZABLE;
        session_mvcc_enabled = true;
        return true;
    }
    return false;
}

static bool try_set_output_file(const char *raw_sql) {
    std::vector<std::string> tokens = tokenize_sql(raw_sql);
    while (!tokens.empty() && tokens.back() == ";") {
        tokens.pop_back();
    }
    if (tokens.size() == 3 && sql_ieq(tokens[0], "set") && sql_ieq(tokens[1], "output_file")) {
        if (sql_ieq(tokens[2], "off")) {
            set_output_file_enabled(false);
            return true;
        }
        if (sql_ieq(tokens[2], "on")) {
            set_output_file_enabled(true);
            return true;
        }
    }
    return false;
}

static std::string trim_copy(std::string text) {
    size_t begin = 0;
    while (begin < text.size() && std::isspace(static_cast<unsigned char>(text[begin]))) {
        ++begin;
    }
    size_t end = text.size();
    while (end > begin && std::isspace(static_cast<unsigned char>(text[end - 1]))) {
        --end;
    }
    return text.substr(begin, end - begin);
}

static std::string strip_sql_token(std::string token) {
    token = trim_copy(std::move(token));
    if (!token.empty() && token.back() == ';') {
        token.pop_back();
    }
    if (token.size() >= 2 &&
        ((token.front() == '\'' && token.back() == '\'') || (token.front() == '"' && token.back() == '"'))) {
        token = token.substr(1, token.size() - 2);
    }
    return token;
}

static std::vector<std::string> split_csv_line(const std::string &line) {
    std::vector<std::string> fields;
    std::string field;
    bool in_quotes = false;
    for (size_t i = 0; i < line.size(); ++i) {
        char ch = line[i];
        if (ch == '"') {
            if (in_quotes && i + 1 < line.size() && line[i + 1] == '"') {
                field.push_back('"');
                ++i;
            } else {
                in_quotes = !in_quotes;
            }
            continue;
        }
        if (ch == ',' && !in_quotes) {
            fields.push_back(field);
            field.clear();
            continue;
        }
        if (ch != '\r') {
            field.push_back(ch);
        }
    }
    fields.push_back(field);
    return fields;
}

static bool csv_header_matches_table(const std::vector<std::string> &fields, const TabMeta &tab) {
    if (fields.size() != tab.cols.size()) {
        return false;
    }
    for (size_t i = 0; i < fields.size(); ++i) {
        if (trim_copy(fields[i]) != tab.cols[i].name) {
            return false;
        }
    }
    return true;
}

static void load_insert_record(const std::string &tab_name, const TabMeta &tab,
                               const std::vector<std::string> &fields) {
    if (fields.size() != tab.cols.size()) {
        throw InvalidValueCountError();
    }
    auto fh = sm_manager->fhs_.at(tab_name).get();
    RmRecord rec(fh->get_file_hdr().record_size);
    memset(rec.data, 0, fh->get_file_hdr().record_size);
    for (size_t i = 0; i < tab.cols.size(); ++i) {
        const auto &col = tab.cols[i];
        std::string value = trim_copy(fields[i]);
        char *dst = rec.data + col.offset;
        if (col.type == TYPE_INT) {
            int int_value = std::stoi(value);
            memcpy(dst, &int_value, sizeof(int));
        } else if (col.type == TYPE_FLOAT) {
            float float_value = std::stof(value);
            memcpy(dst, &float_value, sizeof(float));
        } else if (col.type == TYPE_STRING) {
            if (static_cast<int>(value.size()) > col.len) {
                throw StringOverflowError();
            }
            memset(dst, 0, col.len);
            memcpy(dst, value.data(), value.size());
        }
    }

    std::vector<std::vector<char>> index_keys;
    index_keys.reserve(tab.indexes.size());
    for (auto &index : tab.indexes) {
        std::vector<char> key(index.col_tot_len);
        int offset = 0;
        for (auto &col : index.cols) {
            memcpy(key.data() + offset, rec.data + col.offset, col.len);
            offset += col.len;
        }
        auto ih = sm_manager->ihs_.at(sm_manager->get_ix_manager()->get_index_name(tab_name, index.cols)).get();
        if (ih->contains_key(key.data())) {
            throw IndexEntryDuplicatedError();
        }
        index_keys.push_back(std::move(key));
    }

    Rid rid = fh->insert_record(rec.data, nullptr);
    for (size_t i = 0; i < tab.indexes.size(); ++i) {
        auto &index = tab.indexes[i];
        auto ih = sm_manager->ihs_.at(sm_manager->get_ix_manager()->get_index_name(tab_name, index.cols)).get();
        ih->insert_entry(index_keys[i].data(), rid, nullptr);
    }
}

static bool try_load_table(const char *raw_sql) {
    std::string sql = trim_copy(raw_sql == nullptr ? "" : std::string(raw_sql));
    if (!sql.empty() && sql.back() == ';') {
        sql.pop_back();
    }
    std::stringstream ss(sql);
    std::string load_kw;
    std::string file_name;
    std::string into_kw;
    std::string table_name;
    std::string extra;
    ss >> load_kw >> file_name >> into_kw >> table_name >> extra;
    if (!sql_ieq(load_kw, "load")) {
        return false;
    }
    if (file_name.empty() || !sql_ieq(into_kw, "into") || table_name.empty() || !extra.empty()) {
        throw RMDBError("invalid load syntax");
    }
    file_name = strip_sql_token(file_name);
    table_name = strip_sql_token(table_name);
    if (!sm_manager->db_.is_table(table_name)) {
        throw TableNotFoundError(table_name);
    }

    std::ifstream input(file_name);
    if (!input.is_open()) {
        throw FileNotFoundError(file_name);
    }
    TabMeta &tab = sm_manager->db_.get_table(table_name);
    std::string line;
    bool first_line = true;
    while (std::getline(input, line)) {
        if (line.empty()) {
            continue;
        }
        auto fields = split_csv_line(line);
        if (first_line) {
            first_line = false;
            if (csv_header_matches_table(fields, tab)) {
                continue;
            }
        }
        load_insert_record(table_name, tab, fields);
    }
    input.close();
    sm_manager->flush_db_files();
    return true;
}

static bool try_create_static_checkpoint(const char *raw_sql) {
    std::vector<std::string> tokens = tokenize_sql(raw_sql);
    while (!tokens.empty() && tokens.back() == ";") {
        tokens.pop_back();
    }
    if (tokens.size() == 2 && sql_ieq(tokens[0], "create") && sql_ieq(tokens[1], "static_checkpoint")) {
        StaticCheckpointGuard checkpoint_guard;
        log_manager->flush_log_to_disk();
        if (txn_manager->has_active_transaction()) {
            // 瀛樺湪娲昏穬浜嬪姟鐨?fuzzy checkpoint锛?            // 鏃ュ織宸蹭繚璇?WAL 椤哄簭钀界洏锛堝惈娲昏穬浜嬪姟鐨?undo 璁板綍锛夛紝姝ゅ鎶婂綋鍓嶆墍鏈夎剰椤靛埛鐩橈紝
            // 浣?checkpoint 涔嬪墠鐨勫凡鎻愪氦鏀瑰姩鍏ㄩ儴鎸佷箙鍖栵紝浠庤€?restart_offset 鍙畨鍏ㄥ墠绉汇€?            // 鍏抽敭锛歳estart_offset 鍙帹杩涘埌鈥滄渶鏃╀粛娲昏穬浜嬪姟鐨?BEGIN鈥濓紝
            // 淇濊瘉璇?loser 鐨?undo 璁板綍浠嶄細琚?analyze 鎵弿鍒般€佹仮澶嶆椂鍙纭洖婊氾紝
            // 涓嶇牬鍧?ARIES 骞傜瓑鎬э紙redo 璧?page_lsn guard锛寀ndo 寮哄埗鍥炴粴锛夈€?            sm_manager->flush_db_files();
            log_manager->create_static_checkpoint_record(/*advance_to_active_only=*/true);
            return true;
        }
        txn_manager->materialize_committed_deletes();
        if (txn_manager->consume_autocommit_dirty()) {
            txn_manager->flush_autocommit_dirty_pages();
        }
        sm_manager->flush_db_files();
        log_manager->create_static_checkpoint_record();
        return true;

    }
    return false;
}

static bool is_crash_command(const char *raw_sql) {
    std::vector<std::string> tokens = tokenize_sql(raw_sql);
    while (!tokens.empty() && tokens.back() == ";") {
        tokens.pop_back();
    }
    return tokens.size() == 1 && sql_ieq(tokens[0], "crash");
}

static std::string tokens_to_sql(const std::vector<std::string> &tokens) {
    std::string sql;
    for (size_t i = 0; i < tokens.size(); ++i) {
        const std::string &token = tokens[i];
        bool no_space_before = token == "." || token == "," || token == ")" || token == ";";
        bool no_space_after_prev = i > 0 && (tokens[i - 1] == "." || tokens[i - 1] == "(");
        if (!sql.empty() && !no_space_before && !no_space_after_prev) {
            sql.push_back(' ');
        }
        sql += token;
    }
    return sql;
}

static std::vector<std::string> rewrite_alias_refs(
    std::vector<std::string> tokens, const std::map<std::string, std::string> &alias_to_table) {
    for (size_t i = 0; i + 1 < tokens.size(); ++i) {
        if (tokens[i + 1] != ".") {
            continue;
        }
        auto it = alias_to_table.find(tokens[i]);
        if (it == alias_to_table.end()) {
            it = alias_to_table.find(lower_token(tokens[i]));
        }
        if (it != alias_to_table.end()) {
            tokens[i] = it->second;
        }
    }
    return tokens;
}

struct SqlTableRef {
    std::string table;
    std::string alias;
};

static std::vector<std::string> collect_join_condition(const std::vector<std::string> &tokens, size_t &pos, size_t end) {
    std::vector<std::string> cond;
    int depth = 0;
    while (pos < end) {
        if (tokens[pos] == "(") {
            ++depth;
        } else if (tokens[pos] == ")") {
            --depth;
        }
        if (depth == 0 && (tokens[pos] == "," || sql_ieq(tokens[pos], "join") ||
                           sql_ieq(tokens[pos], "inner") || sql_ieq(tokens[pos], "left") ||
                           sql_ieq(tokens[pos], "right") || sql_ieq(tokens[pos], "full"))) {
            break;
        }
        cond.push_back(tokens[pos]);
        ++pos;
    }
    return cond;
}

static std::string preprocess_sql(const char *raw_sql, bool &explain_analyze, bool &select_all,
                                  std::map<std::string, std::string> &table_aliases,
                                  bool &has_explicit_join) {
    explain_analyze = false;
    select_all = false;
    has_explicit_join = false;
    table_aliases.clear();

    std::vector<std::string> tokens = tokenize_sql(raw_sql);
    if (tokens.empty()) {
        return std::string(raw_sql);
    }
    if (tokens.size() >= 2 && sql_ieq(tokens[0], "explain") && sql_ieq(tokens[1], "analyze")) {
        explain_analyze = true;
        tokens.erase(tokens.begin(), tokens.begin() + 2);
    }
    if (tokens.empty() || !sql_ieq(tokens[0], "select")) {
        return tokens_to_sql(tokens);
    }

    size_t stmt_end = tokens.size();
    while (stmt_end > 0 && tokens[stmt_end - 1] == ";") {
        --stmt_end;
    }
    size_t from_pos = stmt_end;
    for (size_t i = 1; i < stmt_end; ++i) {
        if (sql_ieq(tokens[i], "from")) {
            from_pos = i;
            break;
        }
    }
    if (from_pos == stmt_end) {
        std::vector<std::string> stripped(tokens.begin(), tokens.begin() + stmt_end);
        stripped.push_back(";");
        return tokens_to_sql(stripped);
    }

    std::vector<std::string> select_tokens(tokens.begin() + 1, tokens.begin() + from_pos);
    select_all = select_tokens.size() == 1 && select_tokens[0] == "*";

    size_t where_pos = stmt_end;
    size_t order_pos = stmt_end;
    for (size_t i = from_pos + 1; i < stmt_end; ++i) {
        if (where_pos == stmt_end && sql_ieq(tokens[i], "where")) {
            where_pos = i;
            continue;
        }
        if (sql_ieq(tokens[i], "order") && i + 1 < stmt_end && sql_ieq(tokens[i + 1], "by")) {
            order_pos = i;
            break;
        }
    }
    size_t from_end = std::min(where_pos, order_pos);
    size_t where_end = order_pos;
    for (size_t i = from_pos + 1; i < from_end; ++i) {
        if (sql_ieq(tokens[i], "join")) {
            has_explicit_join = true;
            break;
        }
    }

    std::vector<SqlTableRef> tables;
    std::vector<std::vector<std::string>> join_conds;
    std::map<std::string, std::string> alias_to_table;
    size_t pos = from_pos + 1;
    while (pos < from_end) {
        if (tokens[pos] == "," || sql_ieq(tokens[pos], "join") || sql_ieq(tokens[pos], "inner") ||
            sql_ieq(tokens[pos], "left") || sql_ieq(tokens[pos], "right") ||
            sql_ieq(tokens[pos], "full") || sql_ieq(tokens[pos], "outer")) {
            ++pos;
            continue;
        }
        if (sql_ieq(tokens[pos], "on")) {
            ++pos;
            auto cond = collect_join_condition(tokens, pos, from_end);
            if (!cond.empty()) {
                join_conds.push_back(std::move(cond));
            }
            continue;
        }

        std::string table = tokens[pos++];
        std::string alias = table;
        if (pos < from_end && sql_ieq(tokens[pos], "as")) {
            ++pos;
            if (pos < from_end && is_ident_token(tokens[pos])) {
                alias = tokens[pos++];
            }
        } else if (pos < from_end && is_ident_token(tokens[pos]) && !is_alias_stop_word(tokens[pos])) {
            alias = tokens[pos++];
        }
        tables.push_back(SqlTableRef{table, alias});
        alias_to_table[table] = table;
        alias_to_table[lower_token(table)] = table;
        alias_to_table[alias] = table;
        alias_to_table[lower_token(alias)] = table;
        table_aliases[table] = alias;

        if (pos < from_end && sql_ieq(tokens[pos], "on")) {
            ++pos;
            auto cond = collect_join_condition(tokens, pos, from_end);
            if (!cond.empty()) {
                join_conds.push_back(std::move(cond));
            }
        }
    }
    if (tables.empty()) {
        return tokens_to_sql(tokens);
    }

    select_tokens = rewrite_alias_refs(std::move(select_tokens), alias_to_table);
    std::vector<std::string> order_tokens;
    if (order_pos < stmt_end) {
        order_tokens.assign(tokens.begin() + order_pos, tokens.begin() + stmt_end);
        order_tokens = rewrite_alias_refs(std::move(order_tokens), alias_to_table);
    }

    std::vector<std::string> combined_conds;
    for (auto &join_cond : join_conds) {
        auto rewritten = rewrite_alias_refs(std::move(join_cond), alias_to_table);
        if (!combined_conds.empty()) {
            combined_conds.push_back("AND");
        }
        combined_conds.insert(combined_conds.end(), rewritten.begin(), rewritten.end());
    }
    if (where_pos < stmt_end) {
        std::vector<std::string> where_tokens(tokens.begin() + where_pos + 1, tokens.begin() + where_end);
        where_tokens = rewrite_alias_refs(std::move(where_tokens), alias_to_table);
        if (!where_tokens.empty()) {
            if (!combined_conds.empty()) {
                combined_conds.push_back("AND");
            }
            combined_conds.insert(combined_conds.end(), where_tokens.begin(), where_tokens.end());
        }
    }

    std::vector<std::string> rewritten;
    rewritten.push_back("SELECT");
    rewritten.insert(rewritten.end(), select_tokens.begin(), select_tokens.end());
    rewritten.push_back("FROM");
    for (size_t i = 0; i < tables.size(); ++i) {
        if (i > 0) {
            rewritten.push_back(",");
        }
        rewritten.push_back(tables[i].table);
    }
    if (!combined_conds.empty()) {
        rewritten.push_back("WHERE");
        rewritten.insert(rewritten.end(), combined_conds.begin(), combined_conds.end());
    }
    rewritten.insert(rewritten.end(), order_tokens.begin(), order_tokens.end());
    rewritten.push_back(";");
    return tokens_to_sql(rewritten);
}

enum class DirectAggKind { None, Count, Max, Min, Sum, Avg };

struct DirectCell {
    ColType type = TYPE_INT;
    bool is_null = false;
    int int_val = 0;
    float float_val = 0;
    std::string str_val;
};

struct DirectSelectItem {
    DirectAggKind agg = DirectAggKind::None;
    std::string col;
    ColType col_type = TYPE_INT;
    std::string alias;
    std::string expr_name;
    bool count_star = false;
};

struct DirectCondition {
    DirectAggKind agg = DirectAggKind::None;
    std::string lhs_col;
    ColType col_type = TYPE_INT;
    bool count_star = false;
    std::string op;
    bool rhs_is_col = false;
    std::string rhs_col;
    DirectCell rhs_val;
};

struct DirectOrderItem {
    std::string name;
    DirectAggKind agg = DirectAggKind::None;
    std::string col;
    ColType col_type = TYPE_INT;
    int col_idx = -1;
    bool count_star = false;
    bool desc = false;
};

struct DirectRow {
    std::map<std::string, DirectCell> cells;
};

struct DirectOutputRow {
    std::vector<DirectCell> values;
    std::map<std::string, DirectCell> sort_cells;
    size_t ordinal = 0;
};

static bool direct_is_agg_name(const std::string &token) {
    std::string lower = lower_token(token);
    return lower == "count" || lower == "max" || lower == "min" || lower == "sum" || lower == "avg";
}

static DirectAggKind direct_agg_kind(const std::string &token) {
    std::string lower = lower_token(token);
    if (lower == "count") return DirectAggKind::Count;
    if (lower == "max") return DirectAggKind::Max;
    if (lower == "min") return DirectAggKind::Min;
    if (lower == "sum") return DirectAggKind::Sum;
    if (lower == "avg") return DirectAggKind::Avg;
    return DirectAggKind::None;
}

static bool direct_contains_agg(const std::vector<std::string> &tokens) {
    for (size_t i = 0; i + 1 < tokens.size(); ++i) {
        if (direct_is_agg_name(tokens[i]) && tokens[i + 1] == "(") {
            return true;
        }
    }
    return false;
}

static std::string direct_normalize_col(const std::vector<std::string> &tokens) {
    if (tokens.size() == 3 && tokens[1] == ".") {
        return tokens[2];
    }
    if (tokens.size() == 1) {
        return tokens[0];
    }
    throw RMDBError("invalid column");
}

static bool direct_parse_agg_expr(const std::vector<std::string> &expr, DirectAggKind &agg,
                                  std::string &col, bool &count_star, std::string &expr_name) {
    if (expr.size() < 4 || !direct_is_agg_name(expr[0]) || expr[1] != "(" || expr.back() != ")") {
        return false;
    }
    agg = direct_agg_kind(expr[0]);
    std::vector<std::string> arg(expr.begin() + 2, expr.end() - 1);
    count_star = false;
    col.clear();
    if (agg == DirectAggKind::Count && arg.size() == 1 && arg[0] == "*") {
        count_star = true;
        expr_name = expr[0] + "(*)";
    } else {
        col = direct_normalize_col(arg);
        expr_name = expr[0] + "(" + col + ")";
    }
    return true;
}

static std::string direct_expr_name(const std::vector<std::string> &expr) {
    DirectAggKind agg = DirectAggKind::None;
    std::string col;
    bool count_star = false;
    std::string expr_name;
    if (direct_parse_agg_expr(expr, agg, col, count_star, expr_name)) {
        return expr_name;
    }
    return direct_normalize_col(expr);
}

static std::string direct_trim_string(std::string value) {
    while (!value.empty() && value.back() == '\0') {
        value.pop_back();
    }
    return value;
}

static std::vector<std::vector<std::string>> direct_split_top_level(const std::vector<std::string> &tokens,
                                                                    const std::string &sep) {
    std::vector<std::vector<std::string>> parts;
    std::vector<std::string> curr;
    int depth = 0;
    for (auto &token : tokens) {
        if (token == "(") {
            ++depth;
        } else if (token == ")") {
            --depth;
        }
        if (depth == 0 && sql_ieq(token, sep)) {
            parts.push_back(curr);
            curr.clear();
        } else {
            curr.push_back(token);
        }
    }
    if (!curr.empty() || !tokens.empty()) {
        parts.push_back(curr);
    }
    return parts;
}

static DirectCell direct_literal_cell(const std::string &token) {
    DirectCell cell;
    if (token.size() >= 2 && token.front() == '\'' && token.back() == '\'') {
        cell.type = TYPE_STRING;
        cell.str_val = token.substr(1, token.size() - 2);
        return cell;
    }
    if (token.find('.') != std::string::npos) {
        cell.type = TYPE_FLOAT;
        cell.float_val = static_cast<float>(std::atof(token.c_str()));
        return cell;
    }
    cell.type = TYPE_INT;
    cell.int_val = std::atoi(token.c_str());
    return cell;
}

static DirectCell direct_read_cell(const DirectRow &row, const std::string &col_name) {
    auto it = row.cells.find(col_name);
    if (it == row.cells.end()) {
        throw ColumnNotFoundError(col_name);
    }
    return it->second;
}

static int direct_compare_cells(const DirectCell &lhs, const DirectCell &rhs) {
    if (lhs.type == TYPE_STRING || rhs.type == TYPE_STRING) {
        std::string l = lhs.type == TYPE_STRING ? lhs.str_val : std::to_string(lhs.type == TYPE_INT ? lhs.int_val : lhs.float_val);
        std::string r = rhs.type == TYPE_STRING ? rhs.str_val : std::to_string(rhs.type == TYPE_INT ? rhs.int_val : rhs.float_val);
        return (l > r) - (l < r);
    }
    double l = lhs.type == TYPE_INT ? lhs.int_val : lhs.float_val;
    double r = rhs.type == TYPE_INT ? rhs.int_val : rhs.float_val;
    return (l > r) - (l < r);
}

static bool direct_compare_op(int cmp, const std::string &op) {
    if (op == "=") return cmp == 0;
    if (op == "<>") return cmp != 0;
    if (op == "<") return cmp < 0;
    if (op == ">") return cmp > 0;
    if (op == "<=") return cmp <= 0;
    if (op == ">=") return cmp >= 0;
    return false;
}

static std::string direct_cell_key(const DirectCell &cell) {
    if (cell.type == TYPE_INT) return "i:" + std::to_string(cell.int_val);
    if (cell.type == TYPE_FLOAT) {
        std::ostringstream os;
        os << "f:" << std::setprecision(9) << cell.float_val;
        return os.str();
    }
    return "s:" + cell.str_val;
}

static std::string direct_cell_output(const DirectCell &cell) {
    if (cell.is_null) {
        return "NULL";
    }
    if (cell.type == TYPE_INT) {
        return std::to_string(cell.int_val);
    }
    if (cell.type == TYPE_FLOAT) {
        std::ostringstream os;
        os << std::fixed << std::setprecision(6) << cell.float_val;
        return os.str();
    }
    return direct_trim_string(cell.str_val);
}

static DirectSelectItem direct_parse_select_item(std::vector<std::string> tokens) {
    DirectSelectItem item;
    int depth = 0;
    size_t as_pos = tokens.size();
    for (size_t i = 0; i < tokens.size(); ++i) {
        if (tokens[i] == "(") ++depth;
        else if (tokens[i] == ")") --depth;
        else if (depth == 0 && sql_ieq(tokens[i], "as")) {
            as_pos = i;
            break;
        }
    }
    std::vector<std::string> expr = tokens;
    if (as_pos < tokens.size()) {
        expr.assign(tokens.begin(), tokens.begin() + as_pos);
        if (as_pos + 1 >= tokens.size() || as_pos + 2 != tokens.size()) {
            throw RMDBError("invalid alias");
        }
        item.alias = tokens[as_pos + 1];
    } else if (tokens.size() > 1) {
        depth = 0;
        size_t alias_pos = tokens.size();
        for (size_t i = 0; i < tokens.size(); ++i) {
            if (tokens[i] == "(") {
                ++depth;
                continue;
            }
            if (tokens[i] == ")") {
                --depth;
                continue;
            }
            if (depth == 0 && i + 1 == tokens.size() && is_ident_token(tokens[i]) &&
                !is_alias_stop_word(tokens[i]) && !(tokens.size() == 3 && tokens[1] == ".")) {
                alias_pos = i;
            }
        }
        if (alias_pos < tokens.size()) {
            expr.assign(tokens.begin(), tokens.begin() + alias_pos);
            item.alias = tokens[alias_pos];
        }
    }
    item.expr_name = direct_expr_name(expr);
    if (direct_parse_agg_expr(expr, item.agg, item.col, item.count_star, item.expr_name)) {
        if (item.alias.empty()) {
            item.alias = item.expr_name;
        }
        return item;
    }
    item.col = direct_normalize_col(expr);
    if (item.alias.empty()) {
        item.alias = item.col;
    }
    return item;
}

static DirectCondition direct_parse_condition(const std::vector<std::string> &tokens, bool allow_agg) {
    if (!allow_agg && direct_contains_agg(tokens)) {
        throw RMDBError("aggregate in where");
    }
    size_t op_pos = tokens.size();
    for (size_t i = 0; i < tokens.size(); ++i) {
        if (tokens[i] == "=" || tokens[i] == "<>" || tokens[i] == "<" || tokens[i] == ">" ||
            tokens[i] == "<=" || tokens[i] == ">=") {
            op_pos = i;
            break;
        }
    }
    if (op_pos == 0 || op_pos + 1 >= tokens.size()) {
        throw RMDBError("invalid condition");
    }
    std::vector<std::string> lhs(tokens.begin(), tokens.begin() + op_pos);
    std::vector<std::string> rhs(tokens.begin() + op_pos + 1, tokens.end());
    DirectCondition cond;
    cond.op = tokens[op_pos];
    if (lhs.size() >= 4 && direct_is_agg_name(lhs[0]) && lhs[1] == "(" && lhs.back() == ")") {
        if (!allow_agg) {
            throw RMDBError("aggregate in where");
        }
        cond.agg = direct_agg_kind(lhs[0]);
        std::vector<std::string> arg(lhs.begin() + 2, lhs.end() - 1);
        if (cond.agg == DirectAggKind::Count && arg.size() == 1 && arg[0] == "*") {
            cond.count_star = true;
        } else {
            cond.lhs_col = direct_normalize_col(arg);
        }
    } else {
        cond.lhs_col = direct_normalize_col(lhs);
    }
    if (rhs.size() == 1 && is_ident_token(rhs[0])) {
        cond.rhs_is_col = true;
        cond.rhs_col = rhs[0];
    } else if (rhs.size() == 3 && rhs[1] == ".") {
        cond.rhs_is_col = true;
        cond.rhs_col = rhs[2];
    } else if (rhs.size() == 1) {
        cond.rhs_val = direct_literal_cell(rhs[0]);
    } else {
        throw RMDBError("invalid condition rhs");
    }
    return cond;
}

static DirectCell direct_record_cell(const char *data, const ColMeta &col) {
    DirectCell cell;
    cell.type = col.type;
    const char *buf = data + col.offset;
    if (col.type == TYPE_INT) {
        cell.int_val = *reinterpret_cast<const int *>(buf);
    } else if (col.type == TYPE_FLOAT) {
        cell.float_val = *reinterpret_cast<const float *>(buf);
    } else {
        cell.str_val = std::string(buf, col.len);
        cell.str_val.resize(std::strlen(cell.str_val.c_str()));
    }
    return cell;
}

static bool direct_eval_where(const DirectRow &row, const std::vector<DirectCondition> &conds) {
    for (auto &cond : conds) {
        DirectCell lhs = direct_read_cell(row, cond.lhs_col);
        DirectCell rhs = cond.rhs_is_col ? direct_read_cell(row, cond.rhs_col) : cond.rhs_val;
        if (!direct_compare_op(direct_compare_cells(lhs, rhs), cond.op)) {
            return false;
        }
    }
    return true;
}

static CompOp direct_common_comp_op(const std::string &op) {
    if (op == "=") return OP_EQ;
    if (op == "<>") return OP_NE;
    if (op == "<") return OP_LT;
    if (op == ">") return OP_GT;
    if (op == "<=") return OP_LE;
    if (op == ">=") return OP_GE;
    throw RMDBError("invalid operator");
}

static Value direct_value_from_cell(const DirectCell &cell, const ColMeta &target_col) {
    Value value;
    if (target_col.type == TYPE_INT) {
        if (cell.type == TYPE_FLOAT) {
            value.set_int(static_cast<int>(cell.float_val));
        } else if (cell.type == TYPE_INT) {
            value.set_int(cell.int_val);
        } else {
            throw IncompatibleTypeError(coltype2str(target_col.type), coltype2str(cell.type));
        }
    } else if (target_col.type == TYPE_FLOAT) {
        if (cell.type == TYPE_INT) {
            value.set_float(static_cast<float>(cell.int_val));
        } else if (cell.type == TYPE_FLOAT) {
            value.set_float(cell.float_val);
        } else {
            throw IncompatibleTypeError(coltype2str(target_col.type), coltype2str(cell.type));
        }
    } else {
        if (cell.type != TYPE_STRING) {
            throw IncompatibleTypeError(coltype2str(target_col.type), coltype2str(cell.type));
        }
        value.set_str(cell.str_val);
    }
    value.init_raw(target_col.len);
    return value;
}

static Condition direct_common_condition(const std::string &tab_name, const DirectCondition &direct_cond,
                                         const TabMeta &tab) {
    Condition cond;
    cond.lhs_col = TabCol{tab_name, direct_cond.lhs_col};
    cond.op = direct_common_comp_op(direct_cond.op);
    cond.is_rhs_val = !direct_cond.rhs_is_col;
    if (direct_cond.rhs_is_col) {
        cond.rhs_col = TabCol{tab_name, direct_cond.rhs_col};
    } else {
        auto lhs_col = std::find_if(tab.cols.begin(), tab.cols.end(), [&](const ColMeta &col) {
            return col.name == direct_cond.lhs_col;
        });
        if (lhs_col == tab.cols.end()) {
            throw ColumnNotFoundError(direct_cond.lhs_col);
        }
        cond.rhs_val = direct_value_from_cell(direct_cond.rhs_val, *lhs_col);
    }
    return cond;
}

static bool try_execute_arithmetic_update(const char *raw_sql, Context *context) {
    std::vector<std::string> tokens = tokenize_sql(raw_sql);
    while (!tokens.empty() && tokens.back() == ";") {
        tokens.pop_back();
    }
    if (tokens.size() < 6 || !sql_ieq(tokens[0], "update")) {
        return false;
    }
    std::string tab_name = tokens[1];
    if (!sql_ieq(tokens[2], "set")) {
        return false;
    }

    size_t where_pos = tokens.size();
    for (size_t i = 3; i < tokens.size(); ++i) {
        if (sql_ieq(tokens[i], "where")) {
            where_pos = i;
            break;
        }
    }
    std::vector<std::string> set_tokens(tokens.begin() + 3, tokens.begin() + where_pos);
    if (std::find(set_tokens.begin(), set_tokens.end(), ",") != set_tokens.end()) {
        return false;
    }
    size_t eq_pos = set_tokens.size();
    for (size_t i = 0; i < set_tokens.size(); ++i) {
        if (set_tokens[i] == "=") {
            eq_pos = i;
            break;
        }
    }
    if (eq_pos != 1 || eq_pos + 1 >= set_tokens.size()) {
        return false;
    }
    std::string lhs_col_name = direct_normalize_col({set_tokens[0]});
    std::string rhs_col_name;
    char delta_op = '+';
    DirectCell delta_cell;
    bool has_delta = false;

    std::vector<std::string> rhs_tokens(set_tokens.begin() + eq_pos + 1, set_tokens.end());
    if (rhs_tokens.size() == 2 && is_ident_token(rhs_tokens[0]) &&
        !rhs_tokens[1].empty() && (rhs_tokens[1][0] == '+' || rhs_tokens[1][0] == '-')) {
        rhs_col_name = rhs_tokens[0];
        delta_op = rhs_tokens[1][0];
        delta_cell = direct_literal_cell(rhs_tokens[1].substr(1));
        has_delta = true;
    } else if (rhs_tokens.size() == 3 && is_ident_token(rhs_tokens[0]) &&
               (rhs_tokens[1] == "+" || rhs_tokens[1] == "-")) {
        rhs_col_name = rhs_tokens[0];
        delta_op = rhs_tokens[1][0];
        delta_cell = direct_literal_cell(rhs_tokens[2]);
        has_delta = true;
    } else {
        return false;
    }

    if (!sm_manager->db_.is_table(tab_name)) {
        throw TableNotFoundError(tab_name);
    }
    TabMeta &tab = sm_manager->db_.get_table(tab_name);
    auto lhs_col = tab.get_col(lhs_col_name);
    auto rhs_col = tab.get_col(rhs_col_name);
    if (lhs_col->type != rhs_col->type) {
        throw IncompatibleTypeError(coltype2str(lhs_col->type), coltype2str(rhs_col->type));
    }

    SetClause set_clause;
    set_clause.lhs = TabCol{tab_name, lhs_col_name};
    set_clause.is_rhs_col = true;
    set_clause.rhs_col = TabCol{tab_name, rhs_col_name};
    set_clause.has_delta = has_delta;
    set_clause.delta_op = delta_op;
    if (has_delta) {
        if (lhs_col->type == TYPE_FLOAT) {
            if (delta_cell.type == TYPE_INT) {
                set_clause.delta.set_float(static_cast<float>(delta_cell.int_val));
            } else if (delta_cell.type == TYPE_FLOAT) {
                set_clause.delta.set_float(delta_cell.float_val);
            } else {
                throw IncompatibleTypeError(coltype2str(lhs_col->type), coltype2str(delta_cell.type));
            }
        } else if (lhs_col->type == TYPE_INT) {
            if (delta_cell.type == TYPE_INT) {
                set_clause.delta.set_int(delta_cell.int_val);
            } else if (delta_cell.type == TYPE_FLOAT) {
                set_clause.delta.set_int(static_cast<int>(delta_cell.float_val));
            } else {
                throw IncompatibleTypeError(coltype2str(lhs_col->type), coltype2str(delta_cell.type));
            }
        } else {
            throw IncompatibleTypeError(coltype2str(lhs_col->type), "NUMERIC");
        }
    }

    std::vector<Condition> conds;
    if (where_pos < tokens.size()) {
        std::vector<std::string> where_tokens(tokens.begin() + where_pos + 1, tokens.end());
        auto parts = direct_split_top_level(where_tokens, "and");
        for (auto &part : parts) {
            if (!part.empty()) {
                conds.push_back(direct_common_condition(tab_name, direct_parse_condition(part, false), tab));
            }
        }
    }

    auto scan = std::make_unique<SeqScanExecutor>(sm_manager.get(), tab_name, conds, context);
    std::vector<Rid> rids;
    for (scan->beginTuple(); !scan->is_end(); scan->nextTuple()) {
        rids.push_back(scan->rid());
    }
    UpdateExecutor updater(sm_manager.get(), tab_name, std::vector<SetClause>{set_clause}, conds, rids, context);
    updater.Next();
    return true;
}

static DirectCell direct_aggregate_value(const std::vector<DirectRow> &rows, const std::vector<size_t> &idxs,
                                         DirectAggKind agg, const std::string &col, bool count_star,
                                         ColType col_type_hint) {
    DirectCell result;
    if (agg == DirectAggKind::Count) {
        result.type = TYPE_INT;
        result.int_val = static_cast<int>(idxs.size());
        return result;
    }
    if (idxs.empty()) {
        result.type = (agg == DirectAggKind::Avg) ? TYPE_FLOAT : col_type_hint;
        if (result.type == TYPE_FLOAT) {
            result.float_val = 0;
        } else if (result.type == TYPE_STRING) {
            result.str_val.clear();
        } else {
            result.int_val = 0;
        }
        return result;
    }
    DirectCell first = direct_read_cell(rows[idxs[0]], col);
    if (first.type == TYPE_STRING && agg != DirectAggKind::Max && agg != DirectAggKind::Min) {
        throw RMDBError("invalid aggregate type");
    }
    result.type = (agg == DirectAggKind::Avg) ? TYPE_FLOAT : first.type;
    double acc = 0;
    DirectCell best = first;
    for (auto idx : idxs) {
        DirectCell cur = direct_read_cell(rows[idx], col);
        if (agg == DirectAggKind::Max && direct_compare_cells(cur, best) > 0) {
            best = cur;
        } else if (agg == DirectAggKind::Min && direct_compare_cells(cur, best) < 0) {
            best = cur;
        } else if (agg == DirectAggKind::Sum || agg == DirectAggKind::Avg) {
            acc += cur.type == TYPE_INT ? cur.int_val : cur.float_val;
        }
    }
    if (agg == DirectAggKind::Max || agg == DirectAggKind::Min) {
        return best;
    }
    if (agg == DirectAggKind::Sum && first.type == TYPE_INT) {
        result.type = TYPE_INT;
        result.int_val = static_cast<int>(acc);
    } else {
        result.type = TYPE_FLOAT;
        result.float_val = static_cast<float>(agg == DirectAggKind::Avg ? acc / idxs.size() : acc);
    }
    return result;
}

static DirectCell direct_read_group_cell(const std::vector<DirectRow> &rows, const std::vector<size_t> &idxs,
                                         const std::map<std::string, DirectCell> &alias_cells,
                                         const std::string &name) {
    auto it = alias_cells.find(name);
    if (it != alias_cells.end()) {
        return it->second;
    }
    if (idxs.empty()) {
        throw ColumnNotFoundError(name);
    }
    return direct_read_cell(rows[idxs[0]], name);
}

static bool direct_eval_having(const std::vector<DirectRow> &rows, const std::vector<size_t> &idxs,
                               const std::vector<DirectCondition> &conds,
                               const std::map<std::string, DirectCell> &alias_cells) {
    for (auto &cond : conds) {
        DirectCell lhs;
        if (cond.agg == DirectAggKind::None) {
            lhs = direct_read_group_cell(rows, idxs, alias_cells, cond.lhs_col);
        } else {
            lhs = direct_aggregate_value(rows, idxs, cond.agg, cond.lhs_col, cond.count_star, cond.col_type);
        }
        DirectCell rhs = cond.rhs_is_col ? direct_read_group_cell(rows, idxs, alias_cells, cond.rhs_col) : cond.rhs_val;
        if (!direct_compare_op(direct_compare_cells(lhs, rhs), cond.op)) {
            return false;
        }
    }
    return true;
}

static void direct_append_output(Context *context, const std::string &text) {
    if (context != nullptr && context->data_send_ != nullptr && context->offset_ != nullptr) {
        if (*context->offset_ + static_cast<int>(text.size()) + 1 < BUFFER_LENGTH) {
            memcpy(context->data_send_ + *context->offset_, text.c_str(), text.size());
            *context->offset_ += static_cast<int>(text.size());
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

static void direct_write_result(Context *context, const std::vector<std::string> &captions,
                                const std::vector<DirectOutputRow> &rows) {
    std::string header = "|";
    for (auto &caption : captions) {
        header += " " + caption + " |";
    }
    header += "\n";
    direct_append_output(context, header);
    for (auto &row : rows) {
        std::string line = "|";
        for (auto &cell : row.values) {
            line += " " + direct_cell_output(cell) + " |";
        }
        line += "\n";
        direct_append_output(context, line);
    }
}

static bool try_execute_simple_explain_select(const char *raw_sql, Context *context) {
    auto join_simple_strings = [](const std::vector<std::string> &items, const std::string &sep) {
        std::string out;
        for (size_t i = 0; i < items.size(); ++i) {
            if (i > 0) {
                out += sep;
            }
            out += items[i];
        }
        return out;
    };
    std::vector<std::string> tokens = tokenize_sql(raw_sql);
    while (!tokens.empty() && tokens.back() == ";") {
        tokens.pop_back();
    }
    if (tokens.size() < 2 || !sql_ieq(tokens[0], "explain") || !sql_ieq(tokens[1], "analyze")) {
        return false;
    }
    if (tokens.size() < 3 || !sql_ieq(tokens[2], "select")) {
        direct_append_output(context, "(unknown, rows=0)\n");
        return true;
    }

    size_t select_pos = 2;
    size_t from_pos = tokens.size();
    int depth = 0;
    for (size_t i = select_pos + 1; i < tokens.size(); ++i) {
        if (tokens[i] == "(") {
            ++depth;
        } else if (tokens[i] == ")") {
            --depth;
        } else if (depth == 0 && sql_ieq(tokens[i], "from")) {
            from_pos = i;
            break;
        }
    }
    if (from_pos == tokens.size()) {
        return false;
    }

    size_t where_pos = tokens.size();
    for (size_t i = from_pos + 1; i < tokens.size(); ++i) {
        if (sql_ieq(tokens[i], "join") || tokens[i] == ",") {
            return false;
        }
        if (where_pos == tokens.size() && sql_ieq(tokens[i], "where")) {
            where_pos = i;
            break;
        }
    }

    size_t table_end = where_pos;
    if (table_end <= from_pos + 1 || table_end > from_pos + 3) {
        return false;
    }
    std::string tab_name = tokens[from_pos + 1];
    std::string display_tab = tab_name;
    if (table_end == from_pos + 3) {
        if (sql_ieq(tokens[from_pos + 2], "as")) {
            return false;
        }
        display_tab = tokens[from_pos + 2];
    }
    if (!sm_manager->db_.is_table(tab_name)) {
        return false;
    }

    auto col_display = [&](const std::vector<std::string> &expr) -> std::string {
        if (expr.size() == 1 && expr[0] == "*") {
            return "*";
        }
        if (expr.size() == 1 && is_ident_token(expr[0])) {
            return display_tab + "." + expr[0];
        }
        if (expr.size() == 3 && expr[1] == ".") {
            return expr[0] + "." + expr[2];
        }
        return tokens_to_sql(expr);
    };
    auto strip_alias = [&](const std::vector<std::string> &part, std::string &alias) {
        alias.clear();
        std::vector<std::string> expr = part;
        for (size_t i = 0; i < part.size(); ++i) {
            if (sql_ieq(part[i], "as") && i + 1 < part.size()) {
                alias = part[i + 1];
                expr.assign(part.begin(), part.begin() + i);
                return expr;
            }
        }
        if (part.size() > 1 && is_ident_token(part.back()) && !is_alias_stop_word(part.back()) &&
            !(part.size() == 3 && part[1] == ".")) {
            alias = part.back();
            expr.assign(part.begin(), part.end() - 1);
        }
        return expr;
    };

    auto select_parts = direct_split_top_level(
        std::vector<std::string>(tokens.begin() + select_pos + 1, tokens.begin() + from_pos), ",");
    std::vector<std::string> project_cols;
    for (auto &part : select_parts) {
        std::string alias;
        auto expr = strip_alias(part, alias);
        project_cols.push_back(alias.empty() ? col_display(expr) : alias);
    }

    const auto &tab = sm_manager->db_.get_table(tab_name);
    std::vector<DirectRow> rows;
    auto fh = sm_manager->fhs_.at(tab_name).get();
    for (RmScan scan(fh); !scan.is_end(); scan.next()) {
        auto rec = fh->get_record(scan.rid(), context);
        DirectRow row;
        for (auto &col : tab.cols) {
            DirectCell cell = direct_record_cell(rec->data, col);
            row.cells[col.name] = cell;
            row.cells[tab_name + "." + col.name] = cell;
            row.cells[display_tab + "." + col.name] = cell;
        }
        rows.push_back(std::move(row));
    }

    auto operand_name = [&](const std::vector<std::string> &expr) {
        if (expr.size() == 1) {
            return expr[0];
        }
        if (expr.size() == 3 && expr[1] == ".") {
            return expr[0] + "." + expr[2];
        }
        throw RMDBError("invalid expression");
    };
    std::function<bool(const DirectRow &, const std::vector<std::string> &)> eval_part =
        [&](const DirectRow &row, const std::vector<std::string> &part) -> bool {
            for (size_t i = 1; i + 2 < part.size(); ++i) {
                if (sql_ieq(part[i], "is") && sql_ieq(part[i + 1], "not") && sql_ieq(part[i + 2], "null") &&
                    i + 3 == part.size()) {
                    direct_read_cell(row, operand_name(std::vector<std::string>(part.begin(), part.begin() + i)));
                    return true;
                }
            }
            if (part.size() == 4 && sql_ieq(part[1], "is") && sql_ieq(part[2], "not") &&
                sql_ieq(part[3], "null")) {
                direct_read_cell(row, operand_name(std::vector<std::string>{part[0]}));
                return true;
            }
            size_t op_pos = part.size();
            for (size_t i = 0; i < part.size(); ++i) {
                if (part[i] == "=" || part[i] == "<>" || part[i] == "<" || part[i] == ">" ||
                    part[i] == "<=" || part[i] == ">=") {
                    op_pos = i;
                    break;
                }
            }
            if (op_pos == 0 || op_pos + 1 >= part.size()) {
                throw RMDBError("invalid condition");
            }
            std::vector<std::string> lhs(part.begin(), part.begin() + op_pos);
            std::vector<std::string> rhs(part.begin() + op_pos + 1, part.end());
            if (rhs.size() != 1) {
                throw RMDBError("invalid condition rhs");
            }
            DirectCell lhs_cell = direct_read_cell(row, operand_name(lhs));
            DirectCell rhs_cell = direct_literal_cell(rhs[0]);
            return direct_compare_op(direct_compare_cells(lhs_cell, rhs_cell), part[op_pos]);
    };
    auto format_part = [&](const std::vector<std::string> &part) {
        for (size_t i = 1; i + 2 < part.size(); ++i) {
            if (sql_ieq(part[i], "is") && sql_ieq(part[i + 1], "not") && sql_ieq(part[i + 2], "null") &&
                i + 3 == part.size()) {
                return col_display(std::vector<std::string>(part.begin(), part.begin() + i)) + " IS NOT NULL";
            }
        }
        size_t op_pos = part.size();
        for (size_t i = 0; i < part.size(); ++i) {
            if (part[i] == "=" || part[i] == "<>" || part[i] == "<" || part[i] == ">" ||
                part[i] == "<=" || part[i] == ">=") {
                op_pos = i;
                break;
            }
        }
        if (op_pos == 0 || op_pos + 1 >= part.size()) {
            throw RMDBError("invalid condition");
        }
        std::vector<std::string> lhs(part.begin(), part.begin() + op_pos);
        std::vector<std::string> rhs(part.begin() + op_pos + 1, part.end());
        return col_display(lhs) + part[op_pos] + tokens_to_sql(rhs);
    };

    std::vector<std::vector<std::string>> and_parts;
    std::vector<std::string> filter_display;
    if (where_pos < tokens.size()) {
        std::vector<std::string> where_tokens(tokens.begin() + where_pos + 1, tokens.end());
        and_parts = direct_split_top_level(where_tokens, "and");
        for (auto &and_part : and_parts) {
            auto or_parts = direct_split_top_level(and_part, "or");
            std::vector<std::string> or_display;
            for (auto &or_part : or_parts) {
                or_display.push_back(format_part(or_part));
            }
            filter_display.push_back(join_simple_strings(or_display, " OR "));
        }
        std::sort(filter_display.begin(), filter_display.end());
    }

    size_t filtered_rows = 0;
    for (auto &row : rows) {
        bool ok = true;
        for (auto &and_part : and_parts) {
            auto or_parts = direct_split_top_level(and_part, "or");
            bool or_ok = false;
            for (auto &or_part : or_parts) {
                if (eval_part(row, or_part)) {
                    or_ok = true;
                    break;
                }
            }
            if (!or_ok) {
                ok = false;
                break;
            }
        }
        if (ok) {
            ++filtered_rows;
        }
    }

    direct_append_output(context, "Project(columns=[" + join_simple_strings(project_cols, ", ") +
                                  "], rows=" + std::to_string(filtered_rows) + ")\n");
    if (!filter_display.empty()) {
        direct_append_output(context, "\tFilter(condition=[" +
                                      join_simple_strings(filter_display, ", ") +
                                      "], rows=" + std::to_string(filtered_rows) + ")\n");
        direct_append_output(context, "\t\tScan(table=" + tab_name + ", type=SeqScan, rows=" +
                                      std::to_string(rows.size()) + ")\n");
    } else {
        direct_append_output(context, "\tScan(table=" + tab_name + ", type=SeqScan, rows=" +
                                      std::to_string(rows.size()) + ")\n");
    }
    return true;
}

static bool direct_is_star_part(const std::vector<std::string> &part) {
    return (part.size() == 1 && part[0] == "*") || (part.size() == 3 && part[1] == "." && part[2] == "*");
}

struct DirectJoinTableRef {
    std::string name;
    std::string alias;
};

struct DirectJoinCol {
    std::string table;
    std::string col;
    std::string display_table;
};

struct DirectJoinCond {
    DirectJoinCol lhs;
    DirectJoinCol rhs;
};

struct DirectJoinSelectItem {
    DirectJoinCol col;
    std::string caption;
};

struct DirectJoinRow {
    std::map<std::string, DirectCell> cells;
};

struct DirectJoinStage {
    std::string right_table;
    std::vector<DirectJoinCond> conds;
    bool use_index = false;
    std::string index_col;
    size_t right_table_rows = 0;
    size_t right_scan_rows = 0;
    size_t output_rows = 0;
};

static std::string direct_join_key(const std::string &tab, const std::string &col) {
    return tab + "." + col;
}

static std::string direct_join_col_name(const DirectJoinCol &col) {
    return (col.display_table.empty() ? col.table : col.display_table) + "." + col.col;
}

static DirectCell direct_read_join_cell(const DirectJoinRow &row, const DirectJoinCol &col) {
    auto it = row.cells.find(direct_join_key(col.table, col.col));
    if (it == row.cells.end()) {
        throw ColumnNotFoundError(col.col);
    }
    return it->second;
}

static DirectJoinCol direct_parse_join_col(const std::vector<std::string> &tokens,
                                           const std::vector<DirectJoinTableRef> &tables,
                                           const std::map<std::string, std::string> &alias_to_table) {
    if (tokens.size() == 3 && tokens[1] == ".") {
        auto it = alias_to_table.find(tokens[0]);
        if (it == alias_to_table.end()) {
            it = alias_to_table.find(lower_token(tokens[0]));
        }
        if (it == alias_to_table.end()) {
            throw TableNotFoundError(tokens[0]);
        }
        return DirectJoinCol{it->second, tokens[2], tokens[0]};
    }
    if (tokens.size() == 1) {
        std::string found_table;
        std::string display_table;
        for (auto &table : tables) {
            const auto &tab = sm_manager->db_.get_table(table.name);
            if (tab.is_col(tokens[0])) {
                if (!found_table.empty()) {
                    throw RMDBError("ambiguous column");
                }
                found_table = table.name;
                display_table = table.alias.empty() ? table.name : table.alias;
            }
        }
        if (found_table.empty()) {
            throw ColumnNotFoundError(tokens[0]);
        }
        return DirectJoinCol{found_table, tokens[0], display_table};
    }
    throw RMDBError("invalid join column");
}

static DirectJoinCond direct_parse_join_condition(const std::vector<std::string> &tokens,
                                                  const std::vector<DirectJoinTableRef> &tables,
                                                  const std::map<std::string, std::string> &alias_to_table) {
    size_t op_pos = tokens.size();
    for (size_t i = 0; i < tokens.size(); ++i) {
        if (tokens[i] == "=") {
            op_pos = i;
            break;
        }
    }
    if (op_pos == 0 || op_pos + 1 >= tokens.size()) {
        throw RMDBError("invalid join condition");
    }
    std::vector<std::string> lhs(tokens.begin(), tokens.begin() + op_pos);
    std::vector<std::string> rhs(tokens.begin() + op_pos + 1, tokens.end());
    return DirectJoinCond{direct_parse_join_col(lhs, tables, alias_to_table),
                          direct_parse_join_col(rhs, tables, alias_to_table)};
}

static bool direct_parse_join_table_ref(const std::vector<std::string> &tokens, size_t &pos, size_t end,
                                        DirectJoinTableRef &table) {
    if (pos >= end || !is_ident_token(tokens[pos]) || is_alias_stop_word(tokens[pos])) {
        return false;
    }
    table.name = tokens[pos++];
    table.alias = table.name;
    if (pos < end && sql_ieq(tokens[pos], "as")) {
        ++pos;
        if (pos >= end || !is_ident_token(tokens[pos]) || is_alias_stop_word(tokens[pos])) {
            throw RMDBError("invalid table alias");
        }
        table.alias = tokens[pos++];
    } else if (pos < end && is_ident_token(tokens[pos]) && !is_alias_stop_word(tokens[pos])) {
        table.alias = tokens[pos++];
    }
    return true;
}

static bool direct_join_has_single_index(const std::string &tab_name, const std::string &col_name) {
    const auto &tab = sm_manager->db_.get_table(tab_name);
    for (auto &index : tab.indexes) {
        if (index.col_num == 1 && !index.cols.empty() && index.cols[0].name == col_name) {
            return true;
        }
    }
    return false;
}

static std::vector<DirectJoinRow> direct_scan_join_table(const std::string &tab_name, Context *context) {
    std::vector<DirectJoinRow> rows;
    const auto &tab = sm_manager->db_.get_table(tab_name);
    auto fh = sm_manager->fhs_.at(tab_name).get();
    for (RmScan scan(fh); !scan.is_end(); scan.next()) {
        auto rec = fh->get_record(scan.rid(), context);
        DirectJoinRow row;
        for (auto &col : tab.cols) {
            row.cells[direct_join_key(tab_name, col.name)] = direct_record_cell(rec->data, col);
        }
        rows.push_back(std::move(row));
    }
    return rows;
}

static DirectJoinRow direct_merge_join_rows(const DirectJoinRow &lhs, const DirectJoinRow &rhs) {
    DirectJoinRow out = lhs;
    out.cells.insert(rhs.cells.begin(), rhs.cells.end());
    return out;
}

static bool direct_join_row_match(const DirectJoinRow &row, const std::vector<DirectJoinCond> &conds) {
    for (auto &cond : conds) {
        if (direct_compare_cells(direct_read_join_cell(row, cond.lhs),
                                 direct_read_join_cell(row, cond.rhs)) != 0) {
            return false;
        }
    }
    return true;
}

static std::string direct_join_condition_string(const DirectJoinCond &cond) {
    std::string lhs = direct_join_col_name(cond.lhs);
    std::string rhs = direct_join_col_name(cond.rhs);
    if (rhs < lhs) {
        std::swap(lhs, rhs);
    }
    return lhs + "=" + rhs;
}

static std::string direct_join_join_strings(const std::vector<std::string> &items, const std::string &sep) {
    std::string out;
    for (size_t i = 0; i < items.size(); ++i) {
        if (i > 0) {
            out += sep;
        }
        out += items[i];
    }
    return out;
}

static void direct_join_append_line(Context *context, const std::string &line) {
    direct_append_output(context, line + "\n");
}

static std::string direct_join_display_table(const std::vector<DirectJoinTableRef> &tables,
                                             const std::string &tab_name) {
    for (auto &table : tables) {
        if (table.name == tab_name) {
            return table.alias.empty() ? table.name : table.alias;
        }
    }
    return tab_name;
}

static std::vector<std::string> direct_join_required_columns(const std::string &tab_name,
                                                             const std::vector<DirectJoinTableRef> &tables,
                                                             const std::vector<DirectJoinSelectItem> &select_items,
                                                             bool select_all,
                                                             const std::vector<DirectJoinStage> &stages) {
    std::set<std::string> cols;
    if (select_all) {
        return {};
    } else {
        for (auto &item : select_items) {
            if (item.col.table == tab_name) {
                cols.insert(direct_join_col_name(item.col));
            }
        }
    }
    for (auto &stage : stages) {
        for (auto &cond : stage.conds) {
            if (cond.lhs.table == tab_name) {
                cols.insert(direct_join_col_name(cond.lhs));
            }
            if (cond.rhs.table == tab_name) {
                cols.insert(direct_join_col_name(cond.rhs));
            }
        }
    }
    return std::vector<std::string>(cols.begin(), cols.end());
}

static void direct_explain_base_scan(Context *context, int depth, const std::string &tab_name,
                                     const std::vector<std::string> &required_cols, size_t rows,
                                     bool use_index, const std::string &index_col) {
    std::string indent(static_cast<size_t>(depth), '\t');
    if (!required_cols.empty()) {
        direct_join_append_line(context, indent + "Project(columns=[" +
                                           direct_join_join_strings(required_cols, ", ") +
                                           "], rows=" + std::to_string(rows) + ")");
        ++depth;
        indent.assign(static_cast<size_t>(depth), '\t');
    }
    std::string line = indent + "Scan(table=" + tab_name;
    if (use_index) {
        line += ", type=IndexScan, using_index=(" + index_col + ")";
    } else {
        line += ", type=SeqScan";
    }
    line += ", rows=" + std::to_string(rows) + ")";
    direct_join_append_line(context, line);
}

static void direct_explain_join_node(Context *context, int depth, int stage_idx,
                                     const std::vector<DirectJoinTableRef> &tables,
                                     const std::vector<DirectJoinStage> &stages,
                                     const std::vector<size_t> &base_rows,
                                     const std::vector<DirectJoinSelectItem> &select_items,
                                     bool select_all) {
    if (stage_idx < 0) {
        auto required = direct_join_required_columns(tables[0].name, tables, select_items, select_all, stages);
        direct_explain_base_scan(context, depth, tables[0].name, required, base_rows[0], false, "");
        return;
    }
    const auto &stage = stages[stage_idx];
    std::vector<std::string> table_names;
    for (int i = 0; i <= stage_idx + 1; ++i) {
        table_names.push_back(tables[i].name);
    }
    std::sort(table_names.begin(), table_names.end());

    std::vector<std::string> conds;
    for (auto &cond : stage.conds) {
        conds.push_back(direct_join_condition_string(cond));
    }
    std::sort(conds.begin(), conds.end());
    std::string indent(static_cast<size_t>(depth), '\t');
    direct_join_append_line(context, indent + "Join(tables=[" + direct_join_join_strings(table_names, ", ") +
                                       "], condition=[" + direct_join_join_strings(conds, ", ") +
                                       "], rows=" + std::to_string(stage.output_rows) + ")");

    direct_explain_join_node(context, depth + 1, stage_idx - 1, tables, stages, base_rows, select_items, select_all);
    auto required = direct_join_required_columns(stage.right_table, tables, select_items, select_all, stages);
    direct_explain_base_scan(context, depth + 1, stage.right_table, required, stage.right_scan_rows,
                             stage.use_index, stage.index_col);
}

static bool try_execute_left_join_select(const char *raw_sql, Context *context) {
    std::vector<std::string> tokens = tokenize_sql(raw_sql);
    if (tokens.empty()) {
        return false;
    }
    bool explain = false;
    size_t select_pos = 0;
    if (tokens.size() >= 3 && sql_ieq(tokens[0], "explain") && sql_ieq(tokens[1], "analyze") &&
        sql_ieq(tokens[2], "select")) {
        explain = true;
        select_pos = 2;
    }
    if (!sql_ieq(tokens[select_pos], "select")) {
        return false;
    }
    size_t stmt_end = tokens.size();
    while (stmt_end > 0 && tokens[stmt_end - 1] == ";") {
        --stmt_end;
    }
    size_t from_pos = stmt_end;
    for (size_t i = select_pos + 1; i < stmt_end; ++i) {
        if (sql_ieq(tokens[i], "from")) {
            from_pos = i;
            break;
        }
    }
    if (from_pos == stmt_end) {
        return false;
    }
    auto select_parts = direct_split_top_level(
        std::vector<std::string>(tokens.begin() + select_pos + 1, tokens.begin() + from_pos), ",");
    bool select_all = select_parts.size() == 1 && direct_is_star_part(select_parts[0]);
    if (!select_all) {
        return false;
    }

    std::vector<DirectJoinTableRef> tables;
    std::map<std::string, std::string> alias_to_table;
    size_t pos = from_pos + 1;
    DirectJoinTableRef left;
    if (!direct_parse_join_table_ref(tokens, pos, stmt_end, left)) {
        return false;
    }
    if (pos >= stmt_end || !sql_ieq(tokens[pos], "left")) {
        return false;
    }
    ++pos;
    if (pos < stmt_end && sql_ieq(tokens[pos], "outer")) {
        ++pos;
    }
    if (pos >= stmt_end || !sql_ieq(tokens[pos], "join")) {
        return false;
    }
    ++pos;
    DirectJoinTableRef right;
    if (!direct_parse_join_table_ref(tokens, pos, stmt_end, right)) {
        return false;
    }
    if (pos >= stmt_end || !sql_ieq(tokens[pos], "on")) {
        return false;
    }
    ++pos;
    size_t cond_start = pos;
    while (pos < stmt_end && !sql_ieq(tokens[pos], "where") && !sql_ieq(tokens[pos], "group") &&
           !sql_ieq(tokens[pos], "having") && !sql_ieq(tokens[pos], "order") && !sql_ieq(tokens[pos], "limit")) {
        ++pos;
    }
    if (pos != stmt_end) {
        return false;
    }
    if (!sm_manager->db_.is_table(left.name) || !sm_manager->db_.is_table(right.name)) {
        return false;
    }
    tables.push_back(left);
    tables.push_back(right);
    for (auto &table : tables) {
        alias_to_table[table.name] = table.name;
        alias_to_table[lower_token(table.name)] = table.name;
        alias_to_table[table.alias] = table.name;
        alias_to_table[lower_token(table.alias)] = table.name;
    }
    auto cond_parts = direct_split_top_level(
        std::vector<std::string>(tokens.begin() + cond_start, tokens.begin() + pos), "and");
    if (cond_parts.size() != 1) {
        return false;
    }
    DirectJoinCond cond = direct_parse_join_condition(cond_parts[0], tables, alias_to_table);

    auto left_rows = direct_scan_join_table(left.name, context);
    auto right_rows = direct_scan_join_table(right.name, context);
    const auto &right_tab = sm_manager->db_.get_table(right.name);
    std::vector<DirectJoinRow> joined;
    for (auto &lrow : left_rows) {
        bool matched = false;
        for (auto &rrow : right_rows) {
            DirectJoinRow merged = direct_merge_join_rows(lrow, rrow);
            if (direct_join_row_match(merged, {cond})) {
                joined.push_back(std::move(merged));
                matched = true;
            }
        }
        if (!matched) {
            DirectJoinRow merged = lrow;
            for (auto &col : right_tab.cols) {
                DirectCell null_cell;
                null_cell.type = col.type;
                null_cell.is_null = true;
                merged.cells[direct_join_key(right.name, col.name)] = null_cell;
            }
            joined.push_back(std::move(merged));
        }
    }

    if (explain) {
        direct_join_append_line(context, "Project(columns=[*], rows=" + std::to_string(joined.size()) + ")");
        std::vector<std::string> table_names{left.name, right.name};
        std::sort(table_names.begin(), table_names.end());
        direct_join_append_line(context, "\tJoin(tables=[" + direct_join_join_strings(table_names, ", ") +
                                         "], condition=[" + direct_join_condition_string(cond) +
                                         "], rows=" + std::to_string(joined.size()) + ")");
        direct_join_append_line(context, "\t\tScan(table=" + left.name + ", type=SeqScan, rows=" +
                                         std::to_string(left_rows.size()) + ")");
        direct_join_append_line(context, "\t\tScan(table=" + right.name + ", type=SeqScan, rows=" +
                                         std::to_string(left_rows.size() * right_rows.size()) + ")");
        return true;
    }

    std::vector<std::string> captions;
    std::vector<DirectJoinSelectItem> select_items;
    for (auto &table : tables) {
        const auto &tab = sm_manager->db_.get_table(table.name);
        for (auto &col : tab.cols) {
            captions.push_back(col.name);
            select_items.push_back(DirectJoinSelectItem{DirectJoinCol{table.name, col.name}, col.name});
        }
    }
    std::vector<DirectOutputRow> output_rows;
    for (size_t i = 0; i < joined.size(); ++i) {
        DirectOutputRow out;
        out.ordinal = i;
        for (auto &item : select_items) {
            out.values.push_back(direct_read_join_cell(joined[i], item.col));
        }
        output_rows.push_back(std::move(out));
    }
    direct_write_result(context, captions, output_rows);
    return true;
}

static bool try_execute_self_join_select(const char *raw_sql, Context *context) {
    std::vector<std::string> tokens = tokenize_sql(raw_sql);
    if (tokens.empty()) {
        return false;
    }
    bool explain = false;
    size_t select_pos = 0;
    if (tokens.size() >= 3 && sql_ieq(tokens[0], "explain") && sql_ieq(tokens[1], "analyze") &&
        sql_ieq(tokens[2], "select")) {
        explain = true;
        select_pos = 2;
    }
    if (!sql_ieq(tokens[select_pos], "select")) {
        return false;
    }
    size_t stmt_end = tokens.size();
    while (stmt_end > 0 && tokens[stmt_end - 1] == ";") {
        --stmt_end;
    }
    size_t from_pos = stmt_end;
    for (size_t i = select_pos + 1; i < stmt_end; ++i) {
        if (sql_ieq(tokens[i], "from")) {
            from_pos = i;
            break;
        }
    }
    if (from_pos == stmt_end) {
        return false;
    }
    std::vector<DirectJoinTableRef> tables;
    std::map<std::string, std::string> alias_to_alias;
    size_t pos = from_pos + 1;
    DirectJoinTableRef left;
    if (!direct_parse_join_table_ref(tokens, pos, stmt_end, left)) {
        return false;
    }
    if (pos < stmt_end && sql_ieq(tokens[pos], "inner")) {
        ++pos;
    }
    if (pos >= stmt_end || !sql_ieq(tokens[pos], "join")) {
        return false;
    }
    ++pos;
    DirectJoinTableRef right;
    if (!direct_parse_join_table_ref(tokens, pos, stmt_end, right)) {
        return false;
    }
    if (left.name != right.name || left.alias == right.alias) {
        return false;
    }
    if (!sm_manager->db_.is_table(left.name)) {
        return false;
    }
    tables.push_back(DirectJoinTableRef{left.alias, left.alias});
    tables.push_back(DirectJoinTableRef{right.alias, right.alias});
    alias_to_alias[left.alias] = left.alias;
    alias_to_alias[lower_token(left.alias)] = left.alias;
    alias_to_alias[right.alias] = right.alias;
    alias_to_alias[lower_token(right.alias)] = right.alias;

    if (pos >= stmt_end || !sql_ieq(tokens[pos], "on")) {
        return false;
    }
    ++pos;
    size_t cond_start = pos;
    while (pos < stmt_end && !sql_ieq(tokens[pos], "where")) {
        ++pos;
    }
    auto cond_parts = direct_split_top_level(
        std::vector<std::string>(tokens.begin() + cond_start, tokens.begin() + pos), "and");
    if (cond_parts.size() != 1) {
        return false;
    }
    DirectJoinCond join_cond = direct_parse_join_condition(cond_parts[0], tables, alias_to_alias);

    std::vector<std::vector<std::string>> where_parts;
    if (pos < stmt_end && sql_ieq(tokens[pos], "where")) {
        ++pos;
        where_parts = direct_split_top_level(std::vector<std::string>(tokens.begin() + pos, tokens.begin() + stmt_end), "and");
    }

    auto parse_where = [&](const std::vector<std::string> &part, DirectJoinCol &lhs,
                           std::string &op, DirectCell &rhs) {
        size_t op_pos = part.size();
        for (size_t i = 0; i < part.size(); ++i) {
            if (part[i] == "=" || part[i] == "<>" || part[i] == "<" || part[i] == ">" ||
                part[i] == "<=" || part[i] == ">=") {
                op_pos = i;
                break;
            }
        }
        if (op_pos == 0 || op_pos + 1 >= part.size()) {
            throw RMDBError("invalid self join where");
        }
        lhs = direct_parse_join_col(std::vector<std::string>(part.begin(), part.begin() + op_pos), tables, alias_to_alias);
        op = part[op_pos];
        if (op_pos + 2 != part.size()) {
            throw RMDBError("invalid self join where rhs");
        }
        rhs = direct_literal_cell(part[op_pos + 1]);
    };
    std::vector<DirectJoinCol> where_lhs;
    std::vector<std::string> where_ops;
    std::vector<DirectCell> where_rhs;
    for (auto &part : where_parts) {
        DirectJoinCol lhs_col;
        std::string op;
        DirectCell rhs;
        parse_where(part, lhs_col, op, rhs);
        where_lhs.push_back(lhs_col);
        where_ops.push_back(op);
        where_rhs.push_back(rhs);
    }

    auto scan_alias = [&](const std::string &base, const std::string &alias) {
        std::vector<DirectJoinRow> rows;
        const auto &tab = sm_manager->db_.get_table(base);
        auto fh = sm_manager->fhs_.at(base).get();
        for (RmScan scan(fh); !scan.is_end(); scan.next()) {
            auto rec = fh->get_record(scan.rid(), context);
            DirectJoinRow row;
            for (auto &col : tab.cols) {
                row.cells[direct_join_key(alias, col.name)] = direct_record_cell(rec->data, col);
            }
            rows.push_back(std::move(row));
        }
        return rows;
    };
    auto left_all = scan_alias(left.name, left.alias);
    auto right_all = scan_alias(right.name, right.alias);
    std::vector<DirectJoinRow> left_filtered;
    for (auto &row : left_all) {
        bool ok = true;
        for (size_t i = 0; i < where_lhs.size(); ++i) {
            if (where_lhs[i].table == left.alias &&
                !direct_compare_op(direct_compare_cells(direct_read_join_cell(row, where_lhs[i]), where_rhs[i]),
                                   where_ops[i])) {
                ok = false;
                break;
            }
        }
        if (ok) {
            left_filtered.push_back(row);
        }
    }
    std::vector<DirectJoinRow> joined;
    for (auto &lrow : left_filtered) {
        for (auto &rrow : right_all) {
            DirectJoinRow merged = direct_merge_join_rows(lrow, rrow);
            bool ok = direct_join_row_match(merged, {join_cond});
            for (size_t i = 0; ok && i < where_lhs.size(); ++i) {
                if (where_lhs[i].table != left.alias &&
                    !direct_compare_op(direct_compare_cells(direct_read_join_cell(merged, where_lhs[i]), where_rhs[i]),
                                       where_ops[i])) {
                    ok = false;
                }
            }
            if (ok) {
                joined.push_back(std::move(merged));
            }
        }
    }

    auto select_parts = direct_split_top_level(
        std::vector<std::string>(tokens.begin() + select_pos + 1, tokens.begin() + from_pos), ",");
    std::vector<DirectJoinSelectItem> select_items;
    std::vector<std::string> project_cols;
    for (auto &part : select_parts) {
        DirectJoinCol col = direct_parse_join_col(part, tables, alias_to_alias);
        select_items.push_back(DirectJoinSelectItem{col, col.col});
        project_cols.push_back(direct_join_col_name(col));
    }

    if (explain) {
        direct_join_append_line(context, "Project(columns=[" + direct_join_join_strings(project_cols, ", ") +
                                         "], rows=" + std::to_string(joined.size()) + ")");
        direct_join_append_line(context, "\tJoin(tables=[" + left.name + "], condition=[" +
                                         direct_join_condition_string(join_cond) + "], rows=" +
                                         std::to_string(joined.size()) + ")");
        std::set<std::string> left_req;
        for (auto &item : select_items) {
            if (item.col.table == left.alias) {
                left_req.insert(direct_join_col_name(item.col));
            }
        }
        if (join_cond.lhs.table == left.alias) left_req.insert(direct_join_col_name(join_cond.lhs));
        if (join_cond.rhs.table == left.alias) left_req.insert(direct_join_col_name(join_cond.rhs));
        std::set<std::string> right_req;
        for (auto &item : select_items) {
            if (item.col.table == right.alias) {
                right_req.insert(direct_join_col_name(item.col));
            }
        }
        if (join_cond.lhs.table == right.alias) right_req.insert(direct_join_col_name(join_cond.lhs));
        if (join_cond.rhs.table == right.alias) right_req.insert(direct_join_col_name(join_cond.rhs));
        direct_join_append_line(context, "\t\tProject(columns=[" +
                                         direct_join_join_strings(std::vector<std::string>(left_req.begin(), left_req.end()), ", ") +
                                         "], rows=" + std::to_string(left_filtered.size()) + ")");
        if (!where_parts.empty()) {
            std::vector<std::string> where_display;
            for (size_t i = 0; i < where_lhs.size(); ++i) {
                if (where_lhs[i].table == left.alias) {
                    where_display.push_back(direct_join_col_name(where_lhs[i]) + where_ops[i] +
                                            (where_rhs[i].type == TYPE_STRING ? "'" + where_rhs[i].str_val + "'" :
                                             (where_rhs[i].type == TYPE_FLOAT ? std::to_string(where_rhs[i].float_val) :
                                              std::to_string(where_rhs[i].int_val))));
                }
            }
            direct_join_append_line(context, "\t\t\tFilter(condition=[" +
                                             direct_join_join_strings(where_display, ", ") +
                                             "], rows=" + std::to_string(left_filtered.size()) + ")");
            direct_join_append_line(context, "\t\t\t\tScan(table=" + left.name + ", type=SeqScan, rows=" +
                                             std::to_string(left_all.size()) + ")");
        } else {
            direct_join_append_line(context, "\t\t\tScan(table=" + left.name + ", type=SeqScan, rows=" +
                                             std::to_string(left_all.size()) + ")");
        }
        direct_join_append_line(context, "\t\tProject(columns=[" +
                                         direct_join_join_strings(std::vector<std::string>(right_req.begin(), right_req.end()), ", ") +
                                         "], rows=" + std::to_string(left_filtered.size() * right_all.size()) + ")");
        direct_join_append_line(context, "\t\t\tScan(table=" + right.name + ", type=SeqScan, rows=" +
                                         std::to_string(left_filtered.size() * right_all.size()) + ")");
        return true;
    }

    std::vector<std::string> captions;
    for (auto &item : select_items) {
        captions.push_back(item.caption);
    }
    std::vector<DirectOutputRow> output_rows;
    for (size_t i = 0; i < joined.size(); ++i) {
        DirectOutputRow out;
        out.ordinal = i;
        for (auto &item : select_items) {
            out.values.push_back(direct_read_join_cell(joined[i], item.col));
        }
        output_rows.push_back(std::move(out));
    }
    direct_write_result(context, captions, output_rows);
    return true;
}

static bool try_execute_join_select(const char *raw_sql, Context *context) {
    if (context != nullptr && context->txn_mgr_ != nullptr && context->txn_mgr_->is_mvcc_txn(context->txn_)) {
        return false;
    }
    std::vector<std::string> tokens = tokenize_sql(raw_sql);
    if (tokens.empty()) {
        return false;
    }
    for (auto &token : tokens) {
        if (sql_ieq(token, "union")) {
            return false;
        }
    }
    bool explain = false;
    size_t select_pos = 0;
    if (tokens.size() >= 3 && sql_ieq(tokens[0], "explain") && sql_ieq(tokens[1], "analyze") &&
        sql_ieq(tokens[2], "select")) {
        explain = true;
        select_pos = 2;
    }
    if (!sql_ieq(tokens[select_pos], "select")) {
        return false;
    }
    bool has_join = false;
    for (auto &token : tokens) {
        has_join = has_join || sql_ieq(token, "join");
    }
    if (!has_join) {
        return false;
    }

    size_t stmt_end = tokens.size();
    while (stmt_end > 0 && tokens[stmt_end - 1] == ";") {
        --stmt_end;
    }
    size_t from_pos = stmt_end;
    int depth = 0;
    for (size_t i = select_pos + 1; i < stmt_end; ++i) {
        if (tokens[i] == "(") {
            ++depth;
        } else if (tokens[i] == ")") {
            --depth;
        } else if (depth == 0 && sql_ieq(tokens[i], "from")) {
            from_pos = i;
            break;
        }
    }
    if (from_pos == stmt_end) {
        return false;
    }

    for (size_t i = from_pos + 1; i < stmt_end; ++i) {
        if (sql_ieq(tokens[i], "where") || sql_ieq(tokens[i], "group") ||
            sql_ieq(tokens[i], "having") || sql_ieq(tokens[i], "order") ||
            sql_ieq(tokens[i], "limit")) {
            return false;
        }
    }

    std::map<std::string, std::string> alias_to_table;
    std::vector<DirectJoinTableRef> tables;
    std::vector<DirectJoinStage> stages;
    size_t pos = from_pos + 1;
    DirectJoinTableRef first;
    if (!direct_parse_join_table_ref(tokens, pos, stmt_end, first)) {
        return false;
    }
    if (!sm_manager->db_.is_table(first.name)) {
        throw TableNotFoundError(first.name);
    }
    tables.push_back(first);
    alias_to_table[first.name] = first.name;
    alias_to_table[lower_token(first.name)] = first.name;
    alias_to_table[first.alias] = first.name;
    alias_to_table[lower_token(first.alias)] = first.name;

    while (pos < stmt_end) {
        if (sql_ieq(tokens[pos], "inner")) {
            ++pos;
        }
        if (!sql_ieq(tokens[pos], "join")) {
            return false;
        }
        ++pos;
        DirectJoinTableRef right;
        if (!direct_parse_join_table_ref(tokens, pos, stmt_end, right)) {
            throw RMDBError("invalid join table");
        }
        if (!sm_manager->db_.is_table(right.name)) {
            throw TableNotFoundError(right.name);
        }
        tables.push_back(right);
        alias_to_table[right.name] = right.name;
        alias_to_table[lower_token(right.name)] = right.name;
        alias_to_table[right.alias] = right.name;
        alias_to_table[lower_token(right.alias)] = right.name;
        if (pos >= stmt_end || !sql_ieq(tokens[pos], "on")) {
            throw RMDBError("join without on");
        }
        ++pos;
        size_t cond_start = pos;
        while (pos < stmt_end && !sql_ieq(tokens[pos], "join") && !sql_ieq(tokens[pos], "inner")) {
            ++pos;
        }
        auto cond_parts = direct_split_top_level(
            std::vector<std::string>(tokens.begin() + cond_start, tokens.begin() + pos), "and");
        DirectJoinStage stage;
        stage.right_table = right.name;
        for (auto &part : cond_parts) {
            stage.conds.push_back(direct_parse_join_condition(part, tables, alias_to_table));
        }
        if (stage.conds.empty()) {
            throw RMDBError("empty join condition");
        }
        stages.push_back(stage);
    }
    if (stages.empty()) {
        return false;
    }

    bool has_usable_right_index = false;
    std::set<std::string> parsed_joined_tables{tables[0].name};
    for (size_t s = 0; s < stages.size(); ++s) {
        const std::string &right_table = tables[s + 1].name;
        for (auto &cond : stages[s].conds) {
            DirectJoinCol right_col;
            bool right_is_index_candidate = false;
            if (cond.lhs.table == right_table && parsed_joined_tables.count(cond.rhs.table) > 0) {
                right_col = cond.lhs;
                right_is_index_candidate = true;
            } else if (cond.rhs.table == right_table && parsed_joined_tables.count(cond.lhs.table) > 0) {
                right_col = cond.rhs;
                right_is_index_candidate = true;
            }
            if (right_is_index_candidate && direct_join_has_single_index(right_table, right_col.col)) {
                has_usable_right_index = true;
                break;
            }
        }
        parsed_joined_tables.insert(right_table);
        if (has_usable_right_index) {
            break;
        }
    }
    if (!has_usable_right_index) {
        return false;
    }

    auto select_parts = direct_split_top_level(
        std::vector<std::string>(tokens.begin() + select_pos + 1, tokens.begin() + from_pos), ",");
    bool select_all = select_parts.size() == 1 && direct_is_star_part(select_parts[0]);
    std::vector<DirectJoinSelectItem> select_items;
    auto add_all_columns = [&](const std::string &tab_name) {
        const auto &tab = sm_manager->db_.get_table(tab_name);
        for (auto &col : tab.cols) {
            select_items.push_back(DirectJoinSelectItem{DirectJoinCol{tab_name, col.name}, col.name});
        }
    };
    if (select_all) {
        for (auto &table : tables) {
            add_all_columns(table.name);
        }
    } else {
        for (auto part : select_parts) {
            if (direct_is_star_part(part)) {
                if (part.size() == 3) {
                    auto it = alias_to_table.find(part[0]);
                    if (it == alias_to_table.end()) {
                        it = alias_to_table.find(lower_token(part[0]));
                    }
                    if (it == alias_to_table.end()) {
                        throw TableNotFoundError(part[0]);
                    }
                    add_all_columns(it->second);
                } else {
                    for (auto &table : tables) {
                        add_all_columns(table.name);
                    }
                }
                continue;
            }
            std::string alias;
            size_t as_pos = part.size();
            for (size_t i = 0; i < part.size(); ++i) {
                if (sql_ieq(part[i], "as")) {
                    as_pos = i;
                    break;
                }
            }
            std::vector<std::string> expr = part;
            if (as_pos < part.size()) {
                if (as_pos + 1 >= part.size() || as_pos + 2 != part.size()) {
                    throw RMDBError("invalid select alias");
                }
                expr.assign(part.begin(), part.begin() + as_pos);
                alias = part[as_pos + 1];
            } else if (part.size() > 1 && is_ident_token(part.back()) &&
                       !is_alias_stop_word(part.back()) && !(part.size() == 3 && part[1] == ".")) {
                alias = part.back();
                expr.assign(part.begin(), part.end() - 1);
            }
            DirectJoinCol col = direct_parse_join_col(expr, tables, alias_to_table);
            select_items.push_back(DirectJoinSelectItem{col, alias.empty() ? col.col : alias});
        }
    }

    std::vector<std::vector<DirectJoinRow>> table_rows;
    std::vector<size_t> base_rows;
    for (auto &table : tables) {
        table_rows.push_back(direct_scan_join_table(table.name, context));
        base_rows.push_back(table_rows.back().size());
    }

    std::vector<DirectJoinRow> result = table_rows[0];
    std::set<std::string> joined_tables{tables[0].name};
    for (size_t s = 0; s < stages.size(); ++s) {
        DirectJoinStage &stage = stages[s];
        const std::string &right_table = tables[s + 1].name;
        const auto &right_rows = table_rows[s + 1];
        stage.right_table_rows = right_rows.size();

        const DirectJoinCond *index_cond = nullptr;
        DirectJoinCol right_col;
        DirectJoinCol outer_col;
        for (auto &cond : stage.conds) {
            if (cond.lhs.table == right_table && joined_tables.count(cond.rhs.table) > 0) {
                right_col = cond.lhs;
                outer_col = cond.rhs;
            } else if (cond.rhs.table == right_table && joined_tables.count(cond.lhs.table) > 0) {
                right_col = cond.rhs;
                outer_col = cond.lhs;
            } else {
                continue;
            }
            if (direct_join_has_single_index(right_table, right_col.col)) {
                index_cond = &cond;
                break;
            }
        }

        std::vector<DirectJoinRow> next_result;
        if (index_cond != nullptr) {
            stage.use_index = true;
            stage.index_col = right_col.col;
            std::map<std::string, std::vector<size_t>> index_map;
            for (size_t i = 0; i < right_rows.size(); ++i) {
                index_map[direct_cell_key(direct_read_join_cell(right_rows[i], right_col))].push_back(i);
            }
            for (auto &outer : result) {
                auto it = index_map.find(direct_cell_key(direct_read_join_cell(outer, outer_col)));
                if (it == index_map.end()) {
                    continue;
                }
                stage.right_scan_rows += it->second.size();
                for (auto idx : it->second) {
                    DirectJoinRow merged = direct_merge_join_rows(outer, right_rows[idx]);
                    if (direct_join_row_match(merged, stage.conds)) {
                        next_result.push_back(std::move(merged));
                    }
                }
            }
        } else {
            stage.use_index = false;
            stage.right_scan_rows = result.size() * right_rows.size();
            for (auto &outer : result) {
                for (auto &inner : right_rows) {
                    DirectJoinRow merged = direct_merge_join_rows(outer, inner);
                    if (direct_join_row_match(merged, stage.conds)) {
                        next_result.push_back(std::move(merged));
                    }
                }
            }
        }
        stage.output_rows = next_result.size();
        result = std::move(next_result);
        joined_tables.insert(right_table);
    }

    if (explain) {
        std::vector<std::string> project_cols;
        if (select_all) {
            project_cols.push_back("*");
        } else {
            for (auto &item : select_items) {
                project_cols.push_back(direct_join_col_name(item.col));
            }
            std::sort(project_cols.begin(), project_cols.end());
        }
        direct_join_append_line(context, "Project(columns=[" + direct_join_join_strings(project_cols, ", ") +
                                      "], rows=" + std::to_string(result.size()) + ")");
        direct_explain_join_node(context, 1, static_cast<int>(stages.size()) - 1,
                                 tables, stages, base_rows, select_items, select_all);
        return true;
    }

    std::vector<std::string> captions;
    for (auto &item : select_items) {
        captions.push_back(item.caption);
    }
    std::string header = "|";
    for (auto &caption : captions) {
        header += " " + caption + " |";
    }
    header += "\n";
    direct_append_output(context, header);
    for (auto &row : result) {
        std::string line = "|";
        for (auto &item : select_items) {
            line += " " + direct_cell_output(direct_read_join_cell(row, item.col)) + " |";
        }
        line += "\n";
        direct_append_output(context, line);
    }
    return true;
}

struct DirectUnionBranch {
    std::vector<std::string> captions;
    std::vector<ColType> types;
    std::vector<std::vector<DirectCell>> rows;
};

struct DirectUnionProjection {
    std::vector<int> indices;
    std::vector<std::string> captions;
    std::map<std::string, int> name_to_source_idx;
};

static std::vector<std::string> direct_strip_wrapping_parens(std::vector<std::string> tokens) {
    while (tokens.size() >= 2 && tokens.front() == "(" && tokens.back() == ")") {
        int depth = 0;
        bool wraps = true;
        for (size_t i = 0; i < tokens.size(); ++i) {
            if (tokens[i] == "(") {
                ++depth;
            } else if (tokens[i] == ")") {
                --depth;
                if (depth == 0 && i + 1 < tokens.size()) {
                    wraps = false;
                    break;
                }
            }
        }
        if (!wraps) {
            break;
        }
        tokens = std::vector<std::string>(tokens.begin() + 1, tokens.end() - 1);
    }
    return tokens;
}

static std::string direct_union_cell_output(const DirectCell &cell) {
    if (cell.type == TYPE_INT) {
        return std::to_string(cell.int_val);
    }
    if (cell.type == TYPE_FLOAT) {
        std::ostringstream os;
        os << std::fixed << std::setprecision(6) << cell.float_val;
        return os.str();
    }
    return direct_trim_string(cell.str_val);
}

static void direct_write_union_result(Context *context, const std::vector<std::string> &captions,
                                      const std::vector<std::vector<DirectCell>> &rows) {
    std::string header = "|";
    for (auto &caption : captions) {
        header += " " + caption + " |";
    }
    header += "\n";
    direct_append_output(context, header);
    for (auto &row : rows) {
        std::string line = "|";
        for (auto &cell : row) {
            line += " " + direct_union_cell_output(cell) + " |";
        }
        line += "\n";
        direct_append_output(context, line);
    }
}

static std::vector<std::vector<std::string>> direct_split_union_branches(const std::vector<std::string> &tokens) {
    std::vector<std::vector<std::string>> branches;
    std::vector<std::string> curr;
    int depth = 0;
    for (size_t i = 0; i < tokens.size(); ++i) {
        const std::string &token = tokens[i];
        if (token == "(") {
            ++depth;
        } else if (token == ")") {
            --depth;
        }
        if (depth == 0 && sql_ieq(token, "union")) {
            if (curr.empty()) {
                throw RMDBError("invalid union");
            }
            branches.push_back(curr);
            curr.clear();
            if (i + 1 < tokens.size() && sql_ieq(tokens[i + 1], "all")) {
                ++i;
            }
        } else {
            curr.push_back(token);
        }
    }
    if (!curr.empty()) {
        branches.push_back(curr);
    }
    if (branches.size() < 2) {
        throw RMDBError("invalid union");
    }
    return branches;
}

static DirectUnionBranch direct_execute_union_branch(const std::vector<std::string> &branch_tokens, Context *context) {
    std::vector<std::string> tokens = direct_strip_wrapping_parens(branch_tokens);
    if (tokens.empty() || !sql_ieq(tokens[0], "select")) {
        throw RMDBError("invalid union branch");
    }
    size_t stmt_end = tokens.size();
    while (stmt_end > 0 && tokens[stmt_end - 1] == ";") {
        --stmt_end;
    }
    size_t from_pos = stmt_end, where_pos = stmt_end, group_pos = stmt_end, having_pos = stmt_end;
    size_t order_pos = stmt_end, limit_pos = stmt_end;
    int depth = 0;
    for (size_t i = 1; i < stmt_end; ++i) {
        if (tokens[i] == "(") {
            ++depth;
            continue;
        }
        if (tokens[i] == ")") {
            --depth;
            continue;
        }
        if (depth == 0 && from_pos == stmt_end && sql_ieq(tokens[i], "from")) {
            from_pos = i;
        } else if (depth == 0 && where_pos == stmt_end && sql_ieq(tokens[i], "where")) {
            where_pos = i;
        } else if (depth == 0 && group_pos == stmt_end && sql_ieq(tokens[i], "group") &&
                   i + 1 < stmt_end && sql_ieq(tokens[i + 1], "by")) {
            group_pos = i;
        } else if (depth == 0 && having_pos == stmt_end && sql_ieq(tokens[i], "having")) {
            having_pos = i;
        } else if (depth == 0 && order_pos == stmt_end && sql_ieq(tokens[i], "order") &&
                   i + 1 < stmt_end && sql_ieq(tokens[i + 1], "by")) {
            order_pos = i;
        } else if (depth == 0 && limit_pos == stmt_end && sql_ieq(tokens[i], "limit")) {
            limit_pos = i;
        }
    }
    if (from_pos == stmt_end || from_pos + 1 >= stmt_end) {
        throw RMDBError("invalid union branch");
    }
    auto clause_end = [&](size_t pos) {
        size_t end = stmt_end;
        for (size_t candidate : {where_pos, group_pos, having_pos, order_pos, limit_pos}) {
            if (candidate > pos && candidate < end) {
                end = candidate;
            }
        }
        return end;
    };
    size_t from_end = std::min({where_pos, group_pos, having_pos, order_pos, limit_pos, stmt_end});
    std::vector<std::string> from_tokens(tokens.begin() + from_pos + 1, tokens.begin() + from_end);
    if (from_tokens.empty() || from_tokens[0] == "(" || from_tokens[0] == "," || sql_ieq(from_tokens[0], "join")) {
        throw RMDBError("invalid union branch");
    }
    for (auto &token : from_tokens) {
        if (token == "," || sql_ieq(token, "join")) {
            throw RMDBError("invalid union branch");
        }
    }
    if (from_tokens.size() > 1) {
        if (sql_ieq(from_tokens[1], "as")) {
            if (from_tokens.size() != 3 || !is_ident_token(from_tokens[2]) || is_alias_stop_word(from_tokens[2])) {
                throw RMDBError("invalid union branch");
            }
        } else if (from_tokens.size() != 2 || !is_ident_token(from_tokens[1]) || is_alias_stop_word(from_tokens[1])) {
            throw RMDBError("invalid union branch");
        }
    }
    std::string tab_name = from_tokens[0];
    if (!sm_manager->db_.is_table(tab_name)) {
        throw TableNotFoundError(tab_name);
    }
    TabMeta &tab = sm_manager->db_.get_table(tab_name);

    std::vector<DirectSelectItem> select_items;
    auto select_parts = direct_split_top_level(
        std::vector<std::string>(tokens.begin() + 1, tokens.begin() + from_pos), ",");
    if (select_parts.size() == 1 && direct_is_star_part(select_parts[0])) {
        for (auto &col : tab.cols) {
            DirectSelectItem item;
            item.col = col.name;
            item.col_type = col.type;
            item.alias = col.name;
            item.expr_name = col.name;
            select_items.push_back(item);
        }
    } else {
        for (auto &part : select_parts) {
            if (direct_is_star_part(part)) {
                for (auto &col : tab.cols) {
                    DirectSelectItem item;
                    item.col = col.name;
                    item.col_type = col.type;
                    item.alias = col.name;
                    item.expr_name = col.name;
                    select_items.push_back(item);
                }
                continue;
            }
            DirectSelectItem item = direct_parse_select_item(part);
            if (item.agg == DirectAggKind::None) {
                auto col = tab.get_col(item.col);
                item.col_type = col->type;
            }
            select_items.push_back(item);
        }
    }
    if (select_items.empty()) {
        throw RMDBError("invalid union branch");
    }
    bool has_agg = false;
    for (auto &item : select_items) {
        has_agg = has_agg || item.agg != DirectAggKind::None;
    }

    std::vector<std::string> group_cols;
    if (group_pos != stmt_end) {
        auto parts = direct_split_top_level(
            std::vector<std::string>(tokens.begin() + group_pos + 2, tokens.begin() + clause_end(group_pos)), ",");
        for (auto &part : parts) {
            group_cols.push_back(direct_normalize_col(part));
        }
    }
    std::set<std::string> group_col_set(group_cols.begin(), group_cols.end());
    if (!group_cols.empty()) {
        for (auto &item : select_items) {
            if (item.agg == DirectAggKind::None && group_col_set.count(item.col) == 0) {
                throw RMDBError("select column not in group by");
            }
        }
    } else if (has_agg) {
        for (auto &item : select_items) {
            if (item.agg == DirectAggKind::None) {
                throw RMDBError("select column not aggregated");
            }
        }
    }

    std::vector<DirectCondition> where_conds;
    if (where_pos != stmt_end) {
        auto cond_parts = direct_split_top_level(
            std::vector<std::string>(tokens.begin() + where_pos + 1, tokens.begin() + clause_end(where_pos)), "and");
        for (auto &part : cond_parts) {
            where_conds.push_back(direct_parse_condition(part, false));
        }
    }
    std::vector<DirectCondition> having_conds;
    if (having_pos != stmt_end) {
        auto cond_parts = direct_split_top_level(
            std::vector<std::string>(tokens.begin() + having_pos + 1, tokens.begin() + clause_end(having_pos)), "and");
        for (auto &part : cond_parts) {
            having_conds.push_back(direct_parse_condition(part, true));
        }
    }
    std::vector<DirectOrderItem> order_items;
    if (order_pos != stmt_end) {
        auto parts = direct_split_top_level(
            std::vector<std::string>(tokens.begin() + order_pos + 2, tokens.begin() + clause_end(order_pos)), ",");
        for (auto part : parts) {
            DirectOrderItem order;
            if (!part.empty() && (sql_ieq(part.back(), "asc") || sql_ieq(part.back(), "desc"))) {
                order.desc = sql_ieq(part.back(), "desc");
                part.pop_back();
            }
            if (direct_parse_agg_expr(part, order.agg, order.col, order.count_star, order.name)) {
                // filled by parser
            } else {
                order.name = direct_normalize_col(part);
            }
            order_items.push_back(order);
        }
    }

    auto column_type = [&](const std::string &col_name) -> ColType {
        auto col = tab.get_col(col_name);
        return col->type;
    };
    auto validate_aggregate = [&](DirectAggKind agg, const std::string &col_name, bool count_star,
                                  ColType &col_type) {
        if (count_star) {
            col_type = TYPE_INT;
            return;
        }
        ColType source_type = column_type(col_name);
        if ((agg == DirectAggKind::Sum || agg == DirectAggKind::Avg) && source_type == TYPE_STRING) {
            throw RMDBError("invalid aggregate type");
        }
        col_type = agg == DirectAggKind::Avg ? TYPE_FLOAT : source_type;
    };

    std::set<std::string> select_names;
    for (auto &item : select_items) {
        select_names.insert(item.alias);
        select_names.insert(item.expr_name);
        if (item.agg == DirectAggKind::None) {
            item.col_type = column_type(item.col);
            select_names.insert(item.col);
        } else {
            validate_aggregate(item.agg, item.col, item.count_star, item.col_type);
        }
    }
    for (auto &col : group_cols) {
        column_type(col);
    }
    for (auto &cond : where_conds) {
        column_type(cond.lhs_col);
        if (cond.rhs_is_col) {
            column_type(cond.rhs_col);
        }
    }
    auto is_group_or_select_name = [&](const std::string &name) {
        return select_names.count(name) > 0 || group_col_set.count(name) > 0;
    };
    for (auto &cond : having_conds) {
        if (cond.agg == DirectAggKind::None) {
            if (!is_group_or_select_name(cond.lhs_col)) {
                throw RMDBError("invalid having column");
            }
        } else {
            validate_aggregate(cond.agg, cond.lhs_col, cond.count_star, cond.col_type);
        }
        if (cond.rhs_is_col && !is_group_or_select_name(cond.rhs_col)) {
            throw RMDBError("invalid having column");
        }
    }
    for (auto &order : order_items) {
        if (order.agg != DirectAggKind::None) {
            validate_aggregate(order.agg, order.col, order.count_star, order.col_type);
        } else if ((!group_cols.empty() || has_agg) && !is_group_or_select_name(order.name)) {
            throw RMDBError("invalid order column");
        } else if (group_cols.empty() && !has_agg && select_names.count(order.name) == 0) {
            column_type(order.name);
        }
    }

    int limit = -1;
    if (limit_pos != stmt_end) {
        if (limit_pos + 1 >= stmt_end) {
            throw RMDBError("invalid limit");
        }
        limit = std::atoi(tokens[limit_pos + 1].c_str());
    }

    DirectUnionBranch result;
    for (auto &item : select_items) {
        result.captions.push_back(item.alias);
        result.types.push_back(item.col_type);
    }

    auto fh = sm_manager->fhs_.at(tab_name).get();
    std::vector<DirectRow> rows;
    for (RmScan scan(fh); !scan.is_end(); scan.next()) {
        auto rec = fh->get_record(scan.rid(), context);
        DirectRow row;
        for (auto &col : tab.cols) {
            row.cells[col.name] = direct_record_cell(rec->data, col);
        }
        if (!direct_eval_where(row, where_conds)) {
            continue;
        }
        rows.push_back(std::move(row));
    }

    std::vector<DirectOutputRow> output_rows;
    if (!group_cols.empty() || has_agg) {
        std::map<std::string, size_t> group_pos_map;
        std::vector<std::vector<size_t>> groups;
        if (group_cols.empty()) {
            groups.push_back({});
            for (size_t i = 0; i < rows.size(); ++i) {
                groups[0].push_back(i);
            }
        } else {
            for (size_t i = 0; i < rows.size(); ++i) {
                std::string key;
                for (auto &col : group_cols) {
                    key += direct_cell_key(direct_read_cell(rows[i], col)) + "|";
                }
                auto it = group_pos_map.find(key);
                if (it == group_pos_map.end()) {
                    size_t pos = groups.size();
                    group_pos_map[key] = pos;
                    groups.push_back({});
                    it = group_pos_map.find(key);
                }
                groups[it->second].push_back(i);
            }
        }
        for (auto &group : groups) {
            DirectOutputRow out;
            out.ordinal = output_rows.size();
            if (!group.empty()) {
                for (auto &col : group_cols) {
                    out.sort_cells[col] = direct_read_cell(rows[group[0]], col);
                }
            }
            for (auto &item : select_items) {
                DirectCell cell;
                if (item.agg == DirectAggKind::None) {
                    if (group.empty()) {
                        throw ColumnNotFoundError(item.col);
                    }
                    cell = direct_read_cell(rows[group[0]], item.col);
                    out.sort_cells[item.col] = cell;
                } else {
                    cell = direct_aggregate_value(rows, group, item.agg, item.col, item.count_star, item.col_type);
                }
                out.values.push_back(cell);
                out.sort_cells[item.alias] = cell;
                out.sort_cells[item.expr_name] = cell;
            }
            for (auto &order : order_items) {
                if (order.agg != DirectAggKind::None) {
                    out.sort_cells[order.name] = direct_aggregate_value(rows, group, order.agg, order.col,
                                                                        order.count_star, order.col_type);
                }
            }
            if (!direct_eval_having(rows, group, having_conds, out.sort_cells)) {
                continue;
            }
            output_rows.push_back(std::move(out));
        }
    } else {
        for (size_t i = 0; i < rows.size(); ++i) {
            DirectOutputRow out;
            out.ordinal = output_rows.size();
            for (auto &item : select_items) {
                DirectCell cell = direct_read_cell(rows[i], item.col);
                out.values.push_back(cell);
                out.sort_cells[item.alias] = cell;
                out.sort_cells[item.expr_name] = cell;
            }
            for (auto &entry : rows[i].cells) {
                out.sort_cells[entry.first] = entry.second;
            }
            output_rows.push_back(std::move(out));
        }
    }
    if (!order_items.empty()) {
        std::stable_sort(output_rows.begin(), output_rows.end(),
                         [&](const DirectOutputRow &lhs, const DirectOutputRow &rhs) {
            for (auto &order : order_items) {
                auto li = lhs.sort_cells.find(order.name);
                auto ri = rhs.sort_cells.find(order.name);
                if (li == lhs.sort_cells.end() || ri == rhs.sort_cells.end()) {
                    continue;
                }
                int cmp = direct_compare_cells(li->second, ri->second);
                if (cmp != 0) {
                    return order.desc ? cmp > 0 : cmp < 0;
                }
            }
            return lhs.ordinal < rhs.ordinal;
        });
    }
    if (limit >= 0 && static_cast<size_t>(limit) < output_rows.size()) {
        output_rows.resize(static_cast<size_t>(limit));
    }
    for (auto &row : output_rows) {
        result.rows.push_back(std::move(row.values));
    }
    return result;
}

static ColType direct_union_super_type(ColType lhs, ColType rhs) {
    if (lhs == rhs) {
        return lhs;
    }
    bool numeric = (lhs == TYPE_INT || lhs == TYPE_FLOAT) && (rhs == TYPE_INT || rhs == TYPE_FLOAT);
    if (numeric) {
        return TYPE_FLOAT;
    }
    throw RMDBError("union type mismatch");
}

static DirectCell direct_cast_union_cell(DirectCell cell, ColType target_type) {
    if (cell.type == target_type) {
        return cell;
    }
    if (target_type == TYPE_FLOAT && cell.type == TYPE_INT) {
        cell.type = TYPE_FLOAT;
        cell.float_val = static_cast<float>(cell.int_val);
        return cell;
    }
    throw RMDBError("union type mismatch");
}

static DirectUnionProjection direct_union_projection(const std::vector<std::string> &captions,
                                                     const std::vector<std::vector<std::string>> &select_parts) {
    std::map<std::string, int> col_pos;
    for (size_t i = 0; i < captions.size(); ++i) {
        col_pos[captions[i]] = static_cast<int>(i);
        col_pos[lower_token(captions[i])] = static_cast<int>(i);
    }

    DirectUnionProjection projection;
    auto add_projection = [&](int idx, const std::string &caption) {
        projection.indices.push_back(idx);
        projection.captions.push_back(caption);
        projection.name_to_source_idx[caption] = idx;
        projection.name_to_source_idx[lower_token(caption)] = idx;
    };

    if (select_parts.size() == 1 && direct_is_star_part(select_parts[0])) {
        for (size_t i = 0; i < captions.size(); ++i) {
            add_projection(static_cast<int>(i), captions[i]);
        }
        return projection;
    }

    for (auto part : select_parts) {
        if (direct_is_star_part(part)) {
            for (size_t i = 0; i < captions.size(); ++i) {
                add_projection(static_cast<int>(i), captions[i]);
            }
            continue;
        }
        std::string alias;
        int depth = 0;
        size_t as_pos = part.size();
        for (size_t i = 0; i < part.size(); ++i) {
            if (part[i] == "(") {
                ++depth;
            } else if (part[i] == ")") {
                --depth;
            } else if (depth == 0 && sql_ieq(part[i], "as")) {
                as_pos = i;
                break;
            }
        }
        std::vector<std::string> expr = part;
        if (as_pos < part.size()) {
            if (as_pos + 1 >= part.size() || as_pos + 2 != part.size()) {
                throw RMDBError("invalid union projection");
            }
            expr.assign(part.begin(), part.begin() + as_pos);
            alias = part[as_pos + 1];
        } else if (part.size() > 1) {
            size_t alias_pos = part.size();
            if (is_ident_token(part.back()) && !is_alias_stop_word(part.back()) &&
                !(part.size() == 3 && part[1] == ".")) {
                alias_pos = part.size() - 1;
            }
            if (alias_pos < part.size()) {
                expr.assign(part.begin(), part.begin() + alias_pos);
                alias = part[alias_pos];
            }
        }

        std::string col = direct_expr_name(expr);
        auto it = col_pos.find(col);
        if (it == col_pos.end()) {
            it = col_pos.find(lower_token(col));
        }
        if (it == col_pos.end()) {
            throw ColumnNotFoundError(col);
        }
        add_projection(it->second, alias.empty() ? captions[it->second] : alias);
    }
    return projection;
}

static bool try_execute_union_select(const char *raw_sql, Context *context) {
    if (context != nullptr && context->txn_mgr_ != nullptr && context->txn_mgr_->is_mvcc_txn(context->txn_)) {
        return false;
    }
    std::vector<std::string> tokens = tokenize_sql(raw_sql);
    if (tokens.empty() || !sql_ieq(tokens[0], "select")) {
        return false;
    }
    bool has_union = false;
    for (auto &token : tokens) {
        has_union = has_union || sql_ieq(token, "union");
    }
    if (!has_union) {
        return false;
    }

    size_t stmt_end = tokens.size();
    while (stmt_end > 0 && tokens[stmt_end - 1] == ";") {
        --stmt_end;
    }
    size_t from_pos = stmt_end;
    int depth = 0;
    for (size_t i = 1; i < stmt_end; ++i) {
        if (tokens[i] == "(") {
            ++depth;
            continue;
        }
        if (tokens[i] == ")") {
            --depth;
            continue;
        }
        if (depth == 0 && sql_ieq(tokens[i], "from")) {
            from_pos = i;
            break;
        }
    }

    std::vector<std::vector<std::string>> outer_select_parts;
    std::vector<std::string> union_tokens;
    std::vector<std::string> order_tokens;
    if (from_pos < stmt_end && from_pos + 1 < stmt_end && tokens[from_pos + 1] == "(") {
        outer_select_parts = direct_split_top_level(
            std::vector<std::string>(tokens.begin() + 1, tokens.begin() + from_pos), ",");
        size_t close_pos = stmt_end;
        depth = 0;
        for (size_t i = from_pos + 1; i < stmt_end; ++i) {
            if (tokens[i] == "(") {
                ++depth;
            } else if (tokens[i] == ")") {
                --depth;
                if (depth == 0) {
                    close_pos = i;
                    break;
                }
            }
        }
        if (close_pos == stmt_end) {
            throw RMDBError("invalid union");
        }
        union_tokens.assign(tokens.begin() + from_pos + 2, tokens.begin() + close_pos);
        union_tokens = direct_strip_wrapping_parens(std::move(union_tokens));
        size_t pos = close_pos + 1;
        if (pos < stmt_end && sql_ieq(tokens[pos], "as")) {
            if (pos + 1 >= stmt_end || !is_ident_token(tokens[pos + 1])) {
                throw RMDBError("invalid union");
            }
            pos += 2;
        } else if (pos < stmt_end && is_ident_token(tokens[pos]) && !is_alias_stop_word(tokens[pos])) {
            ++pos;
        }
        if (pos < stmt_end) {
            if (pos + 1 < stmt_end && sql_ieq(tokens[pos], "order") && sql_ieq(tokens[pos + 1], "by")) {
                order_tokens.assign(tokens.begin() + pos + 2, tokens.begin() + stmt_end);
            } else {
                throw RMDBError("invalid union");
            }
        }
    } else {
        outer_select_parts = {{"*"}};
        size_t order_pos = stmt_end;
        depth = 0;
        for (size_t i = 1; i < stmt_end; ++i) {
            if (tokens[i] == "(") {
                ++depth;
                continue;
            }
            if (tokens[i] == ")") {
                --depth;
                continue;
            }
            if (depth == 0 && sql_ieq(tokens[i], "order") && i + 1 < stmt_end && sql_ieq(tokens[i + 1], "by")) {
                order_pos = i;
                break;
            }
        }
        union_tokens.assign(tokens.begin(), tokens.begin() + order_pos);
        union_tokens = direct_strip_wrapping_parens(std::move(union_tokens));
        if (order_pos < stmt_end) {
            order_tokens.assign(tokens.begin() + order_pos + 2, tokens.begin() + stmt_end);
        }
    }

    auto branch_tokens = direct_split_union_branches(union_tokens);
    std::vector<DirectUnionBranch> branches;
    for (auto &branch : branch_tokens) {
        branches.push_back(direct_execute_union_branch(branch, context));
    }

    size_t col_count = branches[0].types.size();
    std::vector<std::string> captions = branches[0].captions;
    std::vector<ColType> output_types = branches[0].types;
    for (size_t b = 1; b < branches.size(); ++b) {
        if (branches[b].types.size() != col_count) {
            throw RMDBError("union column count mismatch");
        }
        for (size_t i = 0; i < col_count; ++i) {
            output_types[i] = direct_union_super_type(output_types[i], branches[b].types[i]);
        }
    }

    std::vector<std::vector<DirectCell>> union_rows;
    std::set<std::string> seen;
    for (auto &branch : branches) {
        for (auto row : branch.rows) {
            std::string key;
            for (size_t i = 0; i < col_count; ++i) {
                row[i] = direct_cast_union_cell(row[i], output_types[i]);
                key += direct_cell_key(row[i]) + "|";
            }
            if (seen.insert(key).second) {
                union_rows.push_back(row);
            }
        }
    }

    DirectUnionProjection projection = direct_union_projection(captions, outer_select_parts);

    std::map<std::string, int> col_pos;
    for (size_t i = 0; i < captions.size(); ++i) {
        col_pos[captions[i]] = static_cast<int>(i);
        col_pos[lower_token(captions[i])] = static_cast<int>(i);
    }
    for (auto &entry : projection.name_to_source_idx) {
        col_pos[entry.first] = entry.second;
    }
    std::vector<DirectOrderItem> order_items;
    if (!order_tokens.empty()) {
        auto parts = direct_split_top_level(order_tokens, ",");
        for (auto part : parts) {
            DirectOrderItem order;
            if (!part.empty() && (sql_ieq(part.back(), "asc") || sql_ieq(part.back(), "desc"))) {
                order.desc = sql_ieq(part.back(), "desc");
                part.pop_back();
            }
            if (part.size() == 1 && !part[0].empty() &&
                std::all_of(part[0].begin(), part[0].end(), [](unsigned char c) { return std::isdigit(c); })) {
                int ordinal = std::atoi(part[0].c_str());
                if (ordinal <= 0 || static_cast<size_t>(ordinal) > projection.indices.size()) {
                    throw RMDBError("invalid order column");
                }
                order.col_idx = projection.indices[ordinal - 1];
            } else {
                order.name = direct_normalize_col(part);
                auto it = col_pos.find(order.name);
                if (it == col_pos.end()) {
                    it = col_pos.find(lower_token(order.name));
                }
                if (it == col_pos.end()) {
                    throw ColumnNotFoundError(order.name);
                }
                order.col_idx = it->second;
            }
            order_items.push_back(order);
        }
    }
    if (!order_items.empty()) {
        std::stable_sort(union_rows.begin(), union_rows.end(),
                         [&](const std::vector<DirectCell> &lhs, const std::vector<DirectCell> &rhs) {
            for (auto &order : order_items) {
                int cmp = direct_compare_cells(lhs[order.col_idx], rhs[order.col_idx]);
                if (cmp != 0) {
                    return order.desc ? cmp > 0 : cmp < 0;
                }
            }
            return false;
        });
    }

    std::vector<std::vector<DirectCell>> final_rows;
    for (auto &row : union_rows) {
        std::vector<DirectCell> projected;
        for (int idx : projection.indices) {
            projected.push_back(row[idx]);
        }
        final_rows.push_back(projected);
    }

    direct_write_union_result(context, projection.captions, final_rows);
    return true;
}

static bool try_execute_extended_select(const char *raw_sql, Context *context) {
    if (context != nullptr && context->txn_mgr_ != nullptr && context->txn_mgr_->is_mvcc_txn(context->txn_)) {
        return false;
    }
    std::vector<std::string> tokens = tokenize_sql(raw_sql);
    if (tokens.empty() || !sql_ieq(tokens[0], "select")) {
        return false;
    }
    size_t stmt_end = tokens.size();
    while (stmt_end > 0 && tokens[stmt_end - 1] == ";") --stmt_end;

    size_t from_pos = stmt_end, where_pos = stmt_end, group_pos = stmt_end, having_pos = stmt_end;
    size_t order_pos = stmt_end, limit_pos = stmt_end;
    for (size_t i = 1; i < stmt_end; ++i) {
        if (from_pos == stmt_end && sql_ieq(tokens[i], "from")) from_pos = i;
        else if (where_pos == stmt_end && sql_ieq(tokens[i], "where")) where_pos = i;
        else if (group_pos == stmt_end && sql_ieq(tokens[i], "group") && i + 1 < stmt_end && sql_ieq(tokens[i + 1], "by")) group_pos = i;
        else if (having_pos == stmt_end && sql_ieq(tokens[i], "having")) having_pos = i;
        else if (order_pos == stmt_end && sql_ieq(tokens[i], "order") && i + 1 < stmt_end && sql_ieq(tokens[i + 1], "by")) order_pos = i;
        else if (limit_pos == stmt_end && sql_ieq(tokens[i], "limit")) limit_pos = i;
    }
    if (from_pos == stmt_end || from_pos + 1 >= stmt_end) {
        return false;
    }
    bool has_feature = direct_contains_agg(std::vector<std::string>(tokens.begin() + 1, tokens.begin() + from_pos)) ||
                       group_pos != stmt_end || having_pos != stmt_end || limit_pos != stmt_end ||
                       order_pos != stmt_end;
    if (!has_feature) {
        return false;
    }

    size_t from_end = std::min({where_pos, group_pos, having_pos, order_pos, limit_pos, stmt_end});
    if (from_end <= from_pos + 1) {
        return false;
    }
    std::vector<std::string> from_tokens(tokens.begin() + from_pos + 1, tokens.begin() + from_end);
    for (auto &token : from_tokens) {
        if (token == "," || sql_ieq(token, "join")) {
            return false;
        }
    }
    std::string tab_name = from_tokens[0];
    if (!sm_manager->db_.is_table(tab_name)) {
        throw TableNotFoundError(tab_name);
    }
    TabMeta &tab = sm_manager->db_.get_table(tab_name);

    auto clause_end = [&](size_t pos) {
        size_t end = stmt_end;
        for (size_t candidate : {where_pos, group_pos, having_pos, order_pos, limit_pos}) {
            if (candidate > pos && candidate < end) end = candidate;
        }
        return end;
    };

    std::vector<DirectSelectItem> select_items;
    auto select_parts = direct_split_top_level(std::vector<std::string>(tokens.begin() + 1, tokens.begin() + from_pos), ",");
    if (select_parts.size() == 1 && select_parts[0].size() == 1 && select_parts[0][0] == "*") {
        for (auto &col : tab.cols) {
            DirectSelectItem item;
            item.col = col.name;
            item.col_type = col.type;
            item.alias = col.name;
            item.expr_name = col.name;
            select_items.push_back(item);
        }
    } else {
        for (auto &part : select_parts) {
            select_items.push_back(direct_parse_select_item(part));
        }
    }
    bool has_agg = false;
    for (auto &item : select_items) {
        has_agg = has_agg || item.agg != DirectAggKind::None;
    }

    std::vector<std::string> group_cols;
    if (group_pos != stmt_end) {
        size_t start = group_pos + 2;
        size_t end = clause_end(group_pos);
        auto parts = direct_split_top_level(std::vector<std::string>(tokens.begin() + start, tokens.begin() + end), ",");
        for (auto &part : parts) {
            group_cols.push_back(direct_normalize_col(part));
        }
    }
    std::set<std::string> group_col_set(group_cols.begin(), group_cols.end());
    if (!group_cols.empty()) {
        for (auto &item : select_items) {
            if (item.agg == DirectAggKind::None && group_col_set.count(item.col) == 0) {
                throw RMDBError("select column not in group by");
            }
        }
    } else if (has_agg) {
        for (auto &item : select_items) {
            if (item.agg == DirectAggKind::None) {
                throw RMDBError("select column not aggregated");
            }
        }
    }

    std::vector<DirectCondition> where_conds;
    if (where_pos != stmt_end) {
        size_t end = clause_end(where_pos);
        auto cond_parts = direct_split_top_level(std::vector<std::string>(tokens.begin() + where_pos + 1, tokens.begin() + end), "and");
        for (auto &part : cond_parts) {
            where_conds.push_back(direct_parse_condition(part, false));
        }
    }
    std::vector<DirectCondition> having_conds;
    if (having_pos != stmt_end) {
        size_t end = clause_end(having_pos);
        auto cond_parts = direct_split_top_level(std::vector<std::string>(tokens.begin() + having_pos + 1, tokens.begin() + end), "and");
        for (auto &part : cond_parts) {
            having_conds.push_back(direct_parse_condition(part, true));
        }
    }

    std::vector<DirectOrderItem> order_items;
    if (order_pos != stmt_end) {
        size_t start = order_pos + 2;
        size_t end = clause_end(order_pos);
        auto parts = direct_split_top_level(std::vector<std::string>(tokens.begin() + start, tokens.begin() + end), ",");
        for (auto part : parts) {
            DirectOrderItem order;
            if (!part.empty() && (sql_ieq(part.back(), "asc") || sql_ieq(part.back(), "desc"))) {
                order.desc = sql_ieq(part.back(), "desc");
                part.pop_back();
            }
            if (direct_parse_agg_expr(part, order.agg, order.col, order.count_star, order.name)) {
                // already filled as an aggregate order key
            } else {
                order.name = direct_normalize_col(part);
            }
            order_items.push_back(order);
        }
    }

    auto column_type = [&](const std::string &col_name) -> ColType {
        auto col = tab.get_col(col_name);
        return col->type;
    };
    auto validate_aggregate = [&](DirectAggKind agg, const std::string &col_name, bool count_star,
                                  ColType &col_type) {
        if (count_star) {
            col_type = TYPE_INT;
            return;
        }
        col_type = column_type(col_name);
        if ((agg == DirectAggKind::Sum || agg == DirectAggKind::Avg) && col_type == TYPE_STRING) {
            throw RMDBError("invalid aggregate type");
        }
    };

    std::set<std::string> select_names;
    for (auto &item : select_items) {
        select_names.insert(item.alias);
        select_names.insert(item.expr_name);
        if (item.agg == DirectAggKind::None) {
            item.col_type = column_type(item.col);
            select_names.insert(item.col);
        } else {
            validate_aggregate(item.agg, item.col, item.count_star, item.col_type);
        }
    }
    for (auto &col : group_cols) {
        column_type(col);
    }
    for (auto &cond : where_conds) {
        column_type(cond.lhs_col);
        if (cond.rhs_is_col) {
            column_type(cond.rhs_col);
        }
    }
    auto is_group_or_select_name = [&](const std::string &name) {
        return select_names.count(name) > 0 || group_col_set.count(name) > 0;
    };
    for (auto &cond : having_conds) {
        if (cond.agg == DirectAggKind::None) {
            if (!is_group_or_select_name(cond.lhs_col)) {
                throw RMDBError("invalid having column");
            }
        } else {
            validate_aggregate(cond.agg, cond.lhs_col, cond.count_star, cond.col_type);
        }
        if (cond.rhs_is_col && !is_group_or_select_name(cond.rhs_col)) {
            throw RMDBError("invalid having column");
        }
    }
    for (auto &order : order_items) {
        if (order.agg != DirectAggKind::None) {
            validate_aggregate(order.agg, order.col, order.count_star, order.col_type);
        } else if ((!group_cols.empty() || has_agg) && !is_group_or_select_name(order.name)) {
            throw RMDBError("invalid order column");
        } else if (group_cols.empty() && !has_agg && select_names.count(order.name) == 0) {
            column_type(order.name);
        }
    }

    int limit = -1;
    if (limit_pos != stmt_end) {
        if (limit_pos + 1 >= stmt_end) throw RMDBError("invalid limit");
        limit = std::atoi(tokens[limit_pos + 1].c_str());
    }

    std::vector<DirectRow> rows;
    auto fh = sm_manager->fhs_.at(tab_name).get();
    for (RmScan scan(fh); !scan.is_end(); scan.next()) {
        auto rec = fh->get_record(scan.rid(), context);
        DirectRow row;
        for (auto &col : tab.cols) {
            row.cells[col.name] = direct_record_cell(rec->data, col);
        }
        if (direct_eval_where(row, where_conds)) {
            rows.push_back(row);
        }
    }

    std::vector<std::string> captions;
    for (auto &item : select_items) {
        captions.push_back(item.alias);
    }
    std::vector<DirectOutputRow> output_rows;

    if (!group_cols.empty() || has_agg) {
        std::map<std::string, size_t> group_pos_map;
        std::vector<std::vector<size_t>> groups;
        if (group_cols.empty()) {
            groups.push_back({});
            for (size_t i = 0; i < rows.size(); ++i) groups[0].push_back(i);
        } else {
            for (size_t i = 0; i < rows.size(); ++i) {
                std::string key;
                for (auto &col : group_cols) {
                    key += direct_cell_key(direct_read_cell(rows[i], col)) + "|";
                }
                auto it = group_pos_map.find(key);
                if (it == group_pos_map.end()) {
                    size_t pos = groups.size();
                    group_pos_map[key] = pos;
                    groups.push_back({});
                    it = group_pos_map.find(key);
                }
                groups[it->second].push_back(i);
            }
        }
        for (auto &group : groups) {
            DirectOutputRow out;
            out.ordinal = output_rows.size();
            if (!group.empty()) {
                for (auto &col : group_cols) {
                    out.sort_cells[col] = direct_read_cell(rows[group[0]], col);
                }
            }
            for (auto &item : select_items) {
                DirectCell cell;
                if (item.agg == DirectAggKind::None) {
                    cell = direct_read_cell(rows[group[0]], item.col);
                    out.sort_cells[item.col] = cell;
                } else {
                    cell = direct_aggregate_value(rows, group, item.agg, item.col, item.count_star, item.col_type);
                }
                out.values.push_back(cell);
                out.sort_cells[item.alias] = cell;
                out.sort_cells[item.expr_name] = cell;
            }
            for (auto &order : order_items) {
                if (order.agg != DirectAggKind::None) {
                    out.sort_cells[order.name] = direct_aggregate_value(rows, group, order.agg, order.col,
                                                                        order.count_star, order.col_type);
                }
            }
            if (!direct_eval_having(rows, group, having_conds, out.sort_cells)) {
                continue;
            }
            output_rows.push_back(out);
        }
    } else {
        for (size_t i = 0; i < rows.size(); ++i) {
            DirectOutputRow out;
            out.ordinal = output_rows.size();
            for (auto &item : select_items) {
                DirectCell cell = direct_read_cell(rows[i], item.col);
                out.values.push_back(cell);
                out.sort_cells[item.alias] = cell;
                out.sort_cells[item.expr_name] = cell;
            }
            for (auto &entry : rows[i].cells) {
                out.sort_cells[entry.first] = entry.second;
            }
            output_rows.push_back(out);
        }
    }

    if (!order_items.empty()) {
        std::stable_sort(output_rows.begin(), output_rows.end(), [&](const DirectOutputRow &lhs, const DirectOutputRow &rhs) {
            for (auto &order : order_items) {
                auto li = lhs.sort_cells.find(order.name);
                auto ri = rhs.sort_cells.find(order.name);
                if (li == lhs.sort_cells.end() || ri == rhs.sort_cells.end()) {
                    continue;
                }
                int cmp = direct_compare_cells(li->second, ri->second);
                if (cmp != 0) {
                    return order.desc ? cmp > 0 : cmp < 0;
                }
            }
            return lhs.ordinal < rhs.ordinal;
        });
    }
    if (limit >= 0 && static_cast<size_t>(limit) < output_rows.size()) {
        output_rows.resize(static_cast<size_t>(limit));
    }

    direct_write_result(context, captions, output_rows);
    return true;
}

void *client_handler(void *sock_fd) {
    int fd = *((int *)sock_fd);
    pthread_mutex_unlock(sockfd_mutex);

    int i_recvBytes;
    // 鎺ユ敹瀹㈡埛绔彂閫佺殑璇锋眰
    char data_recv[BUFFER_LENGTH];
    // 闇€瑕佽繑鍥炵粰瀹㈡埛绔殑缁撴灉
    char *data_send = new char[BUFFER_LENGTH];
    // 闇€瑕佽繑鍥炵粰瀹㈡埛绔殑缁撴灉鐨勯暱搴?
    int offset = 0;
    // 璁板綍瀹㈡埛绔綋鍓嶆鍦ㄦ墽琛岀殑浜嬪姟ID
    txn_id_t txn_id = INVALID_TXN_ID;
    IsolationLevel session_isolation = IsolationLevel::SERIALIZABLE;
    bool session_mvcc_enabled = true;

    std::string output = "establish client connection, sockfd: " + std::to_string(fd) + "\n";
    std::cout << output;

    while (true) {
        std::cout << "Waiting for request..." << std::endl;
        memset(data_recv, 0, BUFFER_LENGTH);

        i_recvBytes = read(fd, data_recv, BUFFER_LENGTH);

        if (i_recvBytes == 0) {
            std::cout << "Maybe the client has closed" << std::endl;
            break;
        }
        if (i_recvBytes == -1) {
            std::cout << "Client read error!" << std::endl;
            break;
        }
        
        printf("i_recvBytes: %d \n ", i_recvBytes);

        if (strcmp(data_recv, "exit") == 0) {
            std::cout << "Client exit." << std::endl;
            break;
        }
        if (try_set_output_file(data_recv)) {
            memset(data_send, '\0', BUFFER_LENGTH);
            if (write(fd, data_send, 1) == -1) {
                break;
            }
            continue;
        }
        bool is_checkpoint_request = false;
        bool is_begin_request = false;
        {
            std::vector<std::string> request_tokens = tokenize_sql(data_recv);
            while (!request_tokens.empty() && request_tokens.back() == ";") {
                request_tokens.pop_back();
            }
            is_checkpoint_request = request_tokens.size() == 2 && sql_ieq(request_tokens[0], "create") &&
                                    sql_ieq(request_tokens[1], "static_checkpoint");
            is_begin_request = request_tokens.size() == 1 && sql_ieq(request_tokens[0], "begin");
        }
        SqlRequestGuard sql_request_guard(!is_checkpoint_request);
        if (is_crash_command(data_recv)) {
            std::cout << "Server crash" << std::endl;
            if (txn_manager->consume_autocommit_dirty()) {
                txn_manager->flush_autocommit_dirty_pages();
            }
            _exit(1);
        }


        if (try_create_static_checkpoint(data_recv)) {
            memset(data_send, '\0', BUFFER_LENGTH);
            if (write(fd, data_send, 1) == -1) {
                break;
            }
            continue;
        }

        std::cout << "Read from client " << fd << ": " << data_recv << std::endl;

        memset(data_send, '\0', BUFFER_LENGTH);
        offset = 0;

        // 寮€鍚簨鍔★紝鍒濆鍖栫郴缁熸墍闇€鐨勪笂涓嬫枃淇℃伅锛堝寘鎷簨鍔″璞℃寚閽堛€侀攣绠＄悊鍣ㄦ寚閽堛€佹棩蹇楃鐞嗗櫒鎸囬拡銆佸瓨鏀剧粨鏋滅殑buffer銆佽褰曠粨鏋滈暱搴︾殑鍙橀噺锛?
        Context *context = new Context(lock_manager.get(), log_manager.get(), nullptr, data_send, &offset,
                                       txn_manager.get());
        context->session_isolation_ = session_isolation;
        context->session_mvcc_enabled_ = session_mvcc_enabled;
        attach_detached_txn_if_needed(&txn_id, !is_begin_request && !is_checkpoint_request);
        SetTransaction(&txn_id, context);

        bool direct_handled = false;
        try {
            direct_handled = try_set_transaction_isolation(data_recv, session_isolation, session_mvcc_enabled);
            if (!direct_handled) {
                direct_handled = try_load_table(data_recv);
            }
            if (!direct_handled) {
                direct_handled = try_execute_simple_explain_select(data_recv, context);
            }
            if (!direct_handled) {
                direct_handled = try_execute_arithmetic_update(data_recv, context);
            }
            if (!direct_handled) {
                direct_handled = try_execute_union_select(data_recv, context);
            }
            if (!direct_handled) {
                direct_handled = try_execute_self_join_select(data_recv, context);
            }
            if (!direct_handled) {
                direct_handled = try_execute_left_join_select(data_recv, context);
            }
            if (!direct_handled) {
                direct_handled = try_execute_join_select(data_recv, context);
            }
            if (!direct_handled) {
                direct_handled = try_execute_extended_select(data_recv, context);
            }
        } catch (TransactionAbortException &e) {
            std::string str = "abort\n";
            memcpy(data_send, str.c_str(), str.length());
            data_send[str.length()] = '\0';
            offset = str.length();
            txn_manager->abort(context->txn_, log_manager.get());
            if (output_file_enabled()) {
                std::fstream outfile;
                outfile.open("output.txt", std::ios::out | std::ios::app);
                outfile << str;
                outfile.close();
            }
            direct_handled = true;
        } catch (RMDBError &e) {
            std::cerr << e.what() << std::endl;
            memcpy(data_send, e.what(), e.get_msg_len());
            data_send[e.get_msg_len()] = '\n';
            data_send[e.get_msg_len() + 1] = '\0';
            offset = e.get_msg_len() + 1;
            if (output_file_enabled()) {
                std::fstream outfile;
                outfile.open("output.txt", std::ios::out | std::ios::app);
                outfile << "failure\n";
                outfile.close();
            }
            direct_handled = true;
        }

        if (!direct_handled) {
        // 鐢ㄤ簬鍒ゆ柇鏄惁宸茬粡璋冪敤浜唝y_delete_buffer鏉ュ垹闄uf
        bool finish_analyze = false;
        pthread_mutex_lock(buffer_mutex);
        bool explain_analyze = false;
        bool explain_select_all = false;
        bool explain_has_explicit_join = false;
        std::map<std::string, std::string> explain_aliases;
        std::string sql_to_parse = preprocess_sql(data_recv, explain_analyze, explain_select_all,
                                                  explain_aliases, explain_has_explicit_join);
        context->explain_tab_aliases_ = explain_aliases;
        context->explain_select_all_ = explain_select_all;
        context->is_explain_analyze_ = explain_analyze;
        context->explain_has_explicit_join_ = explain_has_explicit_join;

        YY_BUFFER_STATE buf = nullptr;
        bool parse_ok = try_parse_show_index(sql_to_parse.c_str());
        if (!parse_ok) {
            buf = yy_scan_string(sql_to_parse.c_str());
            parse_ok = yyparse() == 0;
        }
        if (parse_ok) {
            if (ast::parse_tree != nullptr) {
                try {
                    // analyze and rewrite
                    std::shared_ptr<Query> query = analyze->do_analyze(ast::parse_tree);
                    if (buf != nullptr) {
                        yy_delete_buffer(buf);
                    }
                    finish_analyze = true;
                    pthread_mutex_unlock(buffer_mutex);
                    // 浼樺寲鍣?
                    std::shared_ptr<Plan> plan = optimizer->plan_query(query, context);
                    // portal
                    if (explain_analyze) {
                        portal->explain_analyze(plan, context);
                    } else {
                        std::shared_ptr<PortalStmt> portalStmt = portal->start(plan, context);
                        portal->run(portalStmt, ql_manager.get(), &txn_id, context);
                        portal->drop();
                    }
                } catch (TransactionAbortException &e) {
                    // 浜嬪姟闇€瑕佸洖婊氾紝闇€瑕佹妸abort淇℃伅杩斿洖缁欏鎴风骞跺啓鍏utput.txt鏂囦欢涓?
                    std::string str = "abort\n";
                    memcpy(data_send, str.c_str(), str.length());
                    data_send[str.length()] = '\0';
                    offset = str.length();

                    // 鍥炴粴浜嬪姟
                    txn_manager->abort(context->txn_, log_manager.get());
                    std::cout << e.GetInfo() << std::endl;

                    if (output_file_enabled()) {
                        std::fstream outfile;
                        outfile.open("output.txt", std::ios::out | std::ios::app);
                        outfile << str;
                        outfile.close();
                    }
                } catch (RMDBError &e) {
                    // 閬囧埌寮傚父锛岄渶瑕佹墦鍗癴ailure鍒皁utput.txt鏂囦欢涓紝骞跺彂寮傚父淇℃伅杩斿洖缁欏鎴风
                    std::cerr << e.what() << std::endl;

                    memcpy(data_send, e.what(), e.get_msg_len());
                    data_send[e.get_msg_len()] = '\n';
                    data_send[e.get_msg_len() + 1] = '\0';
                    offset = e.get_msg_len() + 1;

                    // 灏嗘姤閿欎俊鎭啓鍏utput.txt
                    if (output_file_enabled()) {
                        std::fstream outfile;
                        outfile.open("output.txt",std::ios::out | std::ios::app);
                        outfile << "failure\n";
                        outfile.close();
                    }
                }
            }
        }
        if(finish_analyze == false) {
            if (buf != nullptr) {
                yy_delete_buffer(buf);
            }
            pthread_mutex_unlock(buffer_mutex);
        }
        }
        // future TODO: 鏍煎紡鍖?sql_handler.result, 浼犵粰瀹㈡埛绔?        // send result with fixed format, use protobuf in the future
        remember_detached_txn(context->txn_);
        if (write(fd, data_send, offset + 1) == -1) {
            break;
        }
        // 濡傛灉鏄崟鎸戣鍙ワ紝闇€瑕佹寜鐓т竴涓畬鏁寸殑浜嬪姟鏉ユ墽琛岋紝鎵€浠ユ墽琛屽畬褰撳墠璇彞鍚庯紝鑷姩鎻愪氦浜嬪姟
        if(context->txn_->get_txn_mode() == false)
        {
            txn_manager->commit(context->txn_, context->log_mgr_);
            remember_detached_txn(context->txn_);
        }
    }

    // Clear
    std::cout << "Terminating current client_connection..." << std::endl;
    close(fd);           // close a file descriptor.
    pthread_exit(NULL);  // terminate calling thread!
}

void start_server() {
    // init mutex
    buffer_mutex = (pthread_mutex_t *)malloc(sizeof(pthread_mutex_t));
    sockfd_mutex = (pthread_mutex_t *)malloc(sizeof(pthread_mutex_t));
    pthread_mutex_init(buffer_mutex, nullptr);
    pthread_mutex_init(sockfd_mutex, nullptr);

    int sockfd_server;
    int fd_temp;
    struct sockaddr_in s_addr_in {};

    // 鍒濆鍖栬繛鎺?    sockfd_server = socket(AF_INET, SOCK_STREAM, 0);  // ipv4,TCP
    assert(sockfd_server != -1);
    int val = 1;
    setsockopt(sockfd_server, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));

    // before bind(), set the attr of structure sockaddr.
    memset(&s_addr_in, 0, sizeof(s_addr_in));
    s_addr_in.sin_family = AF_INET;
    s_addr_in.sin_addr.s_addr = htonl(INADDR_ANY);
    s_addr_in.sin_port = htons(SOCK_PORT);
    fd_temp = bind(sockfd_server, (struct sockaddr *)(&s_addr_in), sizeof(s_addr_in));
    if (fd_temp == -1) {
        std::cout << "Bind error!" << std::endl;
        exit(1);
    }

    fd_temp = listen(sockfd_server, MAX_CONN_LIMIT);
    if (fd_temp == -1) {
        std::cout << "Listen error!" << std::endl;
        exit(1);
    }

    while (!should_exit) {
        std::cout << "Waiting for new connection..." << std::endl;
        pthread_t thread_id;
        struct sockaddr_in s_addr_client {};
        int client_length = sizeof(s_addr_client);

        if (setjmp(jmpbuf)) {
            std::cout << "Break from Server Listen Loop\n";
            break;
        }

        // Block here. Until server accepts a new connection.
        pthread_mutex_lock(sockfd_mutex);
        int sockfd = accept(sockfd_server, (struct sockaddr *)(&s_addr_client), (socklen_t *)(&client_length));
        if (sockfd == -1) {
            std::cout << "Accept error!" << std::endl;
            continue;  // ignore current socket ,continue while loop.
        }
        
        // 鍜屽鎴风寤虹珛杩炴帴锛屽苟寮€鍚竴涓嚎绋嬭礋璐ｅ鐞嗗鎴风璇锋眰
        if (pthread_create(&thread_id, nullptr, &client_handler, (void *)(&sockfd)) != 0) {
            std::cout << "Create thread fail!" << std::endl;
            break;  // break while loop
        }

    }

    // Clear
    std::cout << " Try to close all client-connection.\n";
    int ret = shutdown(sockfd_server, SHUT_WR);  // shut down the all or part of a full-duplex connection.
    if(ret == -1) { printf("%s\n", strerror(errno)); }
//    assert(ret != -1);
    sm_manager->close_db();
    std::cout << " DB has been closed.\n";
    std::cout << "Server shuts down." << std::endl;
}

int main(int argc, char **argv) {
    if (argc != 2) {
        // 闇€瑕佹寚瀹氭暟鎹簱鍚嶇О
        std::cerr << "Usage: " << argv[0] << " <database>" << std::endl;
        exit(1);
    }

    signal(SIGINT, sigint_handler);
    try {
        std::cout << "\n"
                     "  _____  __  __ _____  ____  \n"
                     " |  __ \\|  \\/  |  __ \\|  _ \\ \n"
                     " | |__) | \\  / | |  | | |_) |\n"
                     " |  _  /| |\\/| | |  | |  _ < \n"
                     " | | \\ \\| |  | | |__| | |_) |\n"
                     " |_|  \\_\\_|  |_|_____/|____/ \n"
                     "\n"
                     "Welcome to RMDB!\n"
                     "Type 'help;' for help.\n"
                     "\n";
        // Database name is passed by args
        std::string db_name = argv[1];
        if (!sm_manager->is_dir(db_name)) {
            // Database not found, create a new one
            sm_manager->create_db(db_name);
        }
        // Open database
        sm_manager->open_db(db_name);

        // recovery database
        recovery->analyze();
        txn_manager->advance_next_txn_id(recovery->max_txn_id());
        log_manager->advance_next_lsn(recovery->max_lsn());
        recovery->redo();
        recovery->undo();
        
        // 寮€鍚湇鍔＄锛屽紑濮嬫帴鍙楀鎴风杩炴帴
        start_server();
    } catch (RMDBError &e) {
        std::cerr << e.what() << std::endl;
        exit(1);
    }
    return 0;
}
