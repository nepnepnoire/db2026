/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sm_manager.h"

#include <sys/stat.h>
#include <unistd.h>

#include <cstring>
#include <fstream>
#include <sstream>

#include "index/ix.h"
#include "record/rm.h"
#include "record_printer.h"

namespace {
std::vector<char> build_index_key(const IndexMeta &index, const RmRecord &rec) {
    std::vector<char> key(index.col_tot_len);
    int offset = 0;
    for (auto &col : index.cols) {
        memcpy(key.data() + offset, rec.data + col.offset, col.len);
        offset += col.len;
    }
    return key;
}

std::string format_index_cols(const IndexMeta &index) {
    std::string cols = "(";
    for (int i = 0; i < index.col_num; ++i) {
        if (i > 0) {
            cols += ",";
        }
        cols += index.cols[i].name;
    }
    cols += ")";
    return cols;
}

void append_context(Context *context, const std::string &line) {
    if (context == nullptr || context->data_send_ == nullptr || context->offset_ == nullptr || *context->offset_ < 0) {
        return;
    }
    memcpy(context->data_send_ + *context->offset_, line.data(), line.size());
    *context->offset_ += static_cast<int>(line.size());
}
}

/**
 * @description: 判断是否为一个文件夹
 * @return {bool} 返回是否为一个文件夹
 * @param {string&} db_name 数据库文件名称，与文件夹同名
 */
bool SmManager::is_dir(const std::string& db_name) {
    struct stat st;
    return stat(db_name.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

/**
 * @description: 创建数据库，所有的数据库相关文件都放在数据库同名文件夹下
 * @param {string&} db_name 数据库名称
 */
void SmManager::create_db(const std::string& db_name) {
    if (is_dir(db_name)) {
        throw DatabaseExistsError(db_name);
    }
    //为数据库创建一个子目录
    std::string cmd = "mkdir " + db_name;
    if (system(cmd.c_str()) < 0) {  // 创建一个名为db_name的目录
        throw UnixError();
    }
    if (chdir(db_name.c_str()) < 0) {  // 进入名为db_name的目录
        throw UnixError();
    }
    //创建系统目录
    DbMeta *new_db = new DbMeta();
    new_db->name_ = db_name;

    // 注意，此处ofstream会在当前目录创建(如果没有此文件先创建)和打开一个名为DB_META_NAME的文件
    std::ofstream ofs(DB_META_NAME);

    // 将new_db中的信息，按照定义好的operator<<操作符，写入到ofs打开的DB_META_NAME文件中
    ofs << *new_db;  // 注意：此处重载了操作符<<

    delete new_db;

    // 创建日志文件
    disk_manager_->create_file(LOG_FILE_NAME);

    std::ofstream output("output.txt", std::ios::out | std::ios::trunc);
    output.close();

    // 回到根目录
    if (chdir("..") < 0) {
        throw UnixError();
    }
}

/**
 * @description: 删除数据库，同时需要清空相关文件以及数据库同名文件夹
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::drop_db(const std::string& db_name) {
    if (!is_dir(db_name)) {
        throw DatabaseNotFoundError(db_name);
    }
    std::string cmd = "rm -r " + db_name;
    if (system(cmd.c_str()) < 0) {
        throw UnixError();
    }
}

/**
 * @description: 打开数据库，找到数据库对应的文件夹，并加载数据库元数据和相关文件
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::open_db(const std::string& db_name) {
    if (!is_dir(db_name)) {
        throw DatabaseNotFoundError(db_name);
    }
    if (chdir(db_name.c_str()) < 0) {
        throw UnixError();
    }

    std::ifstream ifs(DB_META_NAME);
    if (!ifs.is_open()) {
        throw FileNotFoundError(DB_META_NAME);
    }
    ifs >> db_;
    ifs.close();

    fhs_.clear();
    ihs_.clear();
    for (auto &entry : db_.tabs_) {
        const std::string &tab_name = entry.first;
        fhs_.emplace(tab_name, rm_manager_->open_file(tab_name));
        for (auto &index : entry.second.indexes) {
            std::string ix_name = ix_manager_->get_index_name(tab_name, index.cols);
            ihs_.emplace(ix_name, ix_manager_->open_index(tab_name, index.cols));
            rebuild_index(tab_name, index, nullptr);
        }
    }

}

/**
 * @description: 把数据库相关的元数据刷入磁盘中
 */
void SmManager::flush_meta() {
    // 默认清空文件
    std::ofstream ofs(DB_META_NAME);
    ofs << db_;
}

void SmManager::flush_db_files() {
    flush_meta();
    for (auto &entry : fhs_) {
        if (entry.second != nullptr) {
            entry.second->flush();
        }
    }
    for (auto &entry : ihs_) {
        if (entry.second != nullptr) {
            entry.second->flush();
        }
    }
}

void SmManager::rebuild_all_indexes(Context* context) {
    for (auto &entry : db_.tabs_) {
        const std::string &tab_name = entry.first;
        for (auto &index : entry.second.indexes) {
            rebuild_index(tab_name, index, context);
        }
    }
}

/**
 * @description: 关闭数据库并把数据落盘
 */
void SmManager::close_db() {
    flush_db_files();

    for (auto &entry : ihs_) {
        ix_manager_->close_index(entry.second.get());
    }
    ihs_.clear();

    for (auto &entry : fhs_) {
        rm_manager_->close_file(entry.second.get());
    }
    fhs_.clear();

    if (chdir("..") < 0) {
        throw UnixError();
    }
}

/**
 * @description: 显示所有的表,通过测试需要将其结果写入到output.txt,详情看题目文档
 * @param {Context*} context 
 */
void SmManager::show_tables(Context* context) {
    std::fstream outfile;
    bool write_output_file = output_file_enabled();
    if (write_output_file) {
        outfile.open("output.txt", std::ios::out | std::ios::app);
        outfile << "| Tables |\n";
    }
    RecordPrinter printer(1);
    printer.print_separator(context);
    printer.print_record({"Tables"}, context);
    printer.print_separator(context);
    for (auto &entry : db_.tabs_) {
        auto &tab = entry.second;
        printer.print_record({tab.name}, context);
        if (write_output_file) {
            outfile << "| " << tab.name << " |\n";
        }
    }
    printer.print_separator(context);
    if (write_output_file) {
        outfile.close();
    }
}

void SmManager::show_index(const std::string& tab_name, Context* context) {
    if (!db_.is_table(tab_name)) {
        throw TableNotFoundError(tab_name);
    }
    TabMeta &tab = db_.get_table(tab_name);
    std::fstream outfile;
    bool write_output_file = output_file_enabled();
    if (write_output_file) {
        outfile.open("output.txt", std::ios::out | std::ios::app);
    }
    for (auto &index : tab.indexes) {
        std::string line = "| " + tab.name + " | unique | " + format_index_cols(index) + " |\n";
        if (write_output_file) {
            outfile << line;
        }
        append_context(context, line);
    }
    if (write_output_file) {
        outfile.close();
    }
}

/**
 * @description: 显示表的元数据
 * @param {string&} tab_name 表名称
 * @param {Context*} context 
 */
void SmManager::desc_table(const std::string& tab_name, Context* context) {
    TabMeta &tab = db_.get_table(tab_name);

    std::vector<std::string> captions = {"Field", "Type", "Index"};
    RecordPrinter printer(captions.size());
    // Print header
    printer.print_separator(context);
    printer.print_record(captions, context);
    printer.print_separator(context);
    // Print fields
    for (auto &col : tab.cols) {
        std::vector<std::string> field_info = {col.name, coltype2str(col.type), col.index ? "YES" : "NO"};
        printer.print_record(field_info, context);
    }
    // Print footer
    printer.print_separator(context);
}

/**
 * @description: 创建表
 * @param {string&} tab_name 表的名称
 * @param {vector<ColDef>&} col_defs 表的字段
 * @param {Context*} context 
 */
void SmManager::create_table(const std::string& tab_name, const std::vector<ColDef>& col_defs, Context* context) {
    if (db_.is_table(tab_name)) {
        throw TableExistsError(tab_name);
    }
    // Create table meta
    int curr_offset = 0;
    TabMeta tab;
    tab.name = tab_name;
    for (auto &col_def : col_defs) {
        ColMeta col = {.tab_name = tab_name,
                       .name = col_def.name,
                       .type = col_def.type,
                       .len = col_def.len,
                       .offset = curr_offset,
                       .index = false};
        curr_offset += col_def.len;
        tab.cols.push_back(col);
    }
    // Create & open record file
    int record_size = curr_offset;  // record_size就是col meta所占的大小（表的元数据也是以记录的形式进行存储的）
    rm_manager_->create_file(tab_name, record_size);
    db_.tabs_[tab_name] = tab;
    // fhs_[tab_name] = rm_manager_->open_file(tab_name);
    fhs_.emplace(tab_name, rm_manager_->open_file(tab_name));

    flush_meta();
}

/**
 * @description: 删除表
 * @param {string&} tab_name 表的名称
 * @param {Context*} context
 */
void SmManager::drop_table(const std::string& tab_name, Context* context) {
    if (!db_.is_table(tab_name)) {
        throw TableNotFoundError(tab_name);
    }

    TabMeta &tab = db_.get_table(tab_name);
    for (auto &index : tab.indexes) {
        std::string ix_name = ix_manager_->get_index_name(tab_name, index.cols);
        auto ih_it = ihs_.find(ix_name);
        if (ih_it != ihs_.end()) {
            ix_manager_->close_index(ih_it->second.get());
            ihs_.erase(ih_it);
        }
        if (ix_manager_->exists(tab_name, index.cols)) {
            ix_manager_->destroy_index(tab_name, index.cols);
        }
    }

    auto fh_it = fhs_.find(tab_name);
    if (fh_it != fhs_.end()) {
        rm_manager_->close_file(fh_it->second.get());
        fhs_.erase(fh_it);
    }
    rm_manager_->destroy_file(tab_name);
    db_.tabs_.erase(tab_name);
    flush_meta();
}

/**
 * @description: 创建索引
 * @param {string&} tab_name 表的名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::create_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context) {
    if (!db_.is_table(tab_name)) {
        throw TableNotFoundError(tab_name);
    }
    TabMeta &tab = db_.get_table(tab_name);
    if (tab.is_index(col_names)) {
        throw IndexExistsError(tab_name, col_names);
    }

    std::vector<ColMeta> cols;
    int col_tot_len = 0;
    for (auto &col_name : col_names) {
        auto col = tab.get_col(col_name);
        cols.push_back(*col);
        col_tot_len += col->len;
    }

    ix_manager_->create_index(tab_name, cols);
    std::string ix_name = ix_manager_->get_index_name(tab_name, cols);
    ihs_.emplace(ix_name, ix_manager_->open_index(tab_name, cols));

    IndexMeta index_meta;
    index_meta.tab_name = tab_name;
    index_meta.col_tot_len = col_tot_len;
    index_meta.col_num = static_cast<int>(cols.size());
    index_meta.cols = cols;

    RmFileHandle *fh = fhs_.at(tab_name).get();
    IxIndexHandle *ih = ihs_.at(ix_name).get();
    try {
        for (RmScan scan(fh); !scan.is_end(); scan.next()) {
            Rid rid = scan.rid();
            auto rec = fh->get_record(rid, context);
            auto key = build_index_key(index_meta, *rec);
            ih->insert_entry(key.data(), rid, context == nullptr ? nullptr : context->txn_);
        }
    } catch (...) {
        ix_manager_->close_index(ih);
        ihs_.erase(ix_name);
        ix_manager_->destroy_index(tab_name, cols);
        throw;
    }

    tab.indexes.push_back(index_meta);
    for (auto &col_name : col_names) {
        tab.get_col(col_name)->index = true;
    }
    flush_meta();
}

/**
 * @description: 删除索引
 * @param {string&} tab_name 表名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::drop_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context) {
    if (!db_.is_table(tab_name)) {
        throw TableNotFoundError(tab_name);
    }
    TabMeta &tab = db_.get_table(tab_name);
    auto index = tab.get_index_meta(col_names);
    std::vector<ColMeta> cols = index->cols;
    std::string ix_name = ix_manager_->get_index_name(tab_name, cols);
    auto ih_it = ihs_.find(ix_name);
    if (ih_it != ihs_.end()) {
        ix_manager_->close_index(ih_it->second.get());
        ihs_.erase(ih_it);
    }
    ix_manager_->destroy_index(tab_name, cols);
    tab.indexes.erase(index);

    for (auto &col_name : col_names) {
        bool still_indexed = false;
        for (auto &idx : tab.indexes) {
            for (auto &col : idx.cols) {
                if (col.name == col_name) {
                    still_indexed = true;
                    break;
                }
            }
            if (still_indexed) break;
        }
        tab.get_col(col_name)->index = still_indexed;
    }
    flush_meta();
}

/**
 * @description: 删除索引
 * @param {string&} tab_name 表名称
 * @param {vector<ColMeta>&} 索引包含的字段元数据
 * @param {Context*} context
 */
void SmManager::drop_index(const std::string& tab_name, const std::vector<ColMeta>& cols, Context* context) {
    std::vector<std::string> col_names;
    col_names.reserve(cols.size());
    for (auto &col : cols) {
        col_names.push_back(col.name);
    }
    drop_index(tab_name, col_names, context);
}

void SmManager::rebuild_index(const std::string& tab_name, const IndexMeta& index, Context* context) {
    RmFileHandle *fh = fhs_.at(tab_name).get();
    auto ih = ihs_.at(ix_manager_->get_index_name(tab_name, index.cols)).get();
    ih->clear_entries();
    for (RmScan scan(fh); !scan.is_end(); scan.next()) {
        Rid rid = scan.rid();
        auto rec = fh->get_record(rid, context);
        auto key = build_index_key(index, *rec);
        ih->insert_entry(key.data(), rid, context == nullptr ? nullptr : context->txn_);
    }
}
