#pragma once

#include <database.h>
#include <csv.h>
#include <mysql.h>
#include <exception.h>

using namespace spl;

class MySQLDatabase
:   public Database
{

private:

    MYSQL _mysql;

    MySQLDatabase() {
        if (! mysql_init(&_mysql)) {
            throw RuntimeError("Insufficient memory");
        }
    }

    MYSQL * _conn() const {
        return (MYSQL *) &_mysql;
    }

public:

    MySQLDatabase(
        const char *host,
        const char *user,
        const char *password,
        const char *db,
        unsigned int port = 0
    );

    ~MySQLDatabase();

    void query(const std::string &sql) const override;

    void loadIntoTable(
        const std::string &table,
        const ColumnarTableChunk *chunk
    ) const override;

private:

    static class __Init {
    public:
        __Init() {
            mysql_library_init(0, NULL, NULL);
        }

        ~__Init() {
            mysql_library_end();
        }
    } __init;
};
