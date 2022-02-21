#pragma once

#include <types.h>
#include <vector>

struct CSVField {
    DataType type;
    size_t size = 0;

    CSVField(DataType type)
    :   type(type),
        size(0)
    { }

    CSVField(DataType type, size_t size)
    :   type(type),
        size(size)
    { }
};

struct CSVOptions {
    char delimiter = ',';

    bool header = true;

    size_t maxChunkSize = 16 * 1024 * 1024;

    std::vector<CSVField> fields;

    CSVOptions(const std::vector<CSVField> &fields)
    :   fields(fields)
    { }
};

class CSV {

public:

    static std::vector<ColumnarTableChunk *> read(
        const char *path,
        const CSVOptions &options
    );

};
