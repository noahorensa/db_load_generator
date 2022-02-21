#pragma once

#include <vector>
#include <stdlib.h>

enum class DataType {
    UINT8,
    UINT16,
    UINT32,
    UINT64,
    INT8,
    INT16,
    INT32,
    INT64,
    FLOAT32,
    FLOAT64,
    STRING,
    MYSQL_DATE,
};

struct ColumnChunk {
    DataType type;
    void *data;
    size_t size;
};

struct ColumnarTableChunk {
    std::vector<ColumnChunk> columns;

    ColumnarTableChunk(const std::vector<ColumnChunk> &columns)
    :   columns(columns)
    { }

    ColumnarTableChunk(const ColumnarTableChunk &) = delete;

    ColumnarTableChunk(ColumnarTableChunk &&) = delete;

    ~ColumnarTableChunk() {
        for (auto &c : columns) {
            switch (c.type) {
            case DataType::STRING:
                free(static_cast<char **>(c.data)[0]);
                free(static_cast<char **>(c.data));
                break;

            default:
                free(c.data);
            }
        }
    }

    ColumnarTableChunk & operator=(const ColumnarTableChunk &) = delete;

    ColumnarTableChunk & operator=(ColumnarTableChunk &&) = delete;

    size_t size() const {
        return columns.front().size;
    }

    size_t numColumns() const {
        return columns.size();
    }
};
 