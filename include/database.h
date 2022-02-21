#pragma once

#include <types.h>
#include <string>

class Database {

public:

    virtual ~Database() = default;

    virtual void query(const std::string &sql) const = 0;

    virtual void loadIntoTable(
        const std::string &table,
        const ColumnarTableChunk *chunk
    ) const = 0;
};
