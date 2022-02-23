#include <mysql_database.h>
#include <sstream>

MySQLDatabase::MySQLDatabase(
    const char *host,
    const char *user,
    const char *password,
    const char *db,
    unsigned int port
):  MySQLDatabase()
{
    if (! mysql_real_connect(_conn(), host, user, password, db, port, NULL, 0)) {
        throw DynamicMessageError(mysql_error(_conn()));
    }
}

MySQLDatabase::~MySQLDatabase() {
    mysql_close(_conn());
}

void MySQLDatabase::query(const std::string &sql) const {
    if (mysql_query(_conn(), sql.c_str())) {
        auto e = DynamicMessageError(mysql_error(_conn()));
        mysql_reset_connection(_conn());
        throw e;
    }
    else {
        auto result = mysql_store_result(_conn());
        if (result) {
            mysql_free_result(result);
        }
        else if (mysql_field_count(_conn()) != 0) {
            auto e = DynamicMessageError(mysql_error(_conn()));
            mysql_reset_connection(_conn());
            throw e;
        }
    }
}

void MySQLDatabase::loadIntoTable(
    const std::string &table,
    const ColumnarTableChunk *chunk
) const {

    mysql_query(_conn(), "SET autocommit=0");
    mysql_query(_conn(), "SET unique_checks=0");
    mysql_query(_conn(), "SET foreign_key_checks=0");

    MYSQL_STMT *stmt = mysql_stmt_init(_conn());
    if (! stmt) {
        throw RuntimeError("Insufficient memory");
    }

    std::stringstream sql;
    sql << "INSERT INTO " << table << " VALUES (?";
    for (size_t i = 0; i < chunk->numColumns() - 1; ++i) sql << ",?";
    sql << ')';

    if (mysql_stmt_prepare(stmt, sql.str().c_str(), -1)) {
        auto e = DynamicMessageError(mysql_stmt_error(stmt));
        mysql_stmt_close(stmt);
        throw e;
    }

    MYSQL_BIND *bind = new MYSQL_BIND[chunk->numColumns()];
    memset(bind, 0, chunk->numColumns() * sizeof(MYSQL_BIND));
    std::vector<size_t> inc(chunk->numColumns());

    for (size_t i = 0; i < chunk->numColumns(); ++i) {
        switch (chunk->columns[i].type) {
        case DataType::UINT8:
        case DataType::INT8:
            bind[i].buffer = chunk->columns[i].data;
            bind[i].buffer_type = MYSQL_TYPE_TINY;
            inc[i] = 1;
            break;

        case DataType::UINT16:
        case DataType::INT16:
            bind[i].buffer = chunk->columns[i].data;
            bind[i].buffer_type = MYSQL_TYPE_SHORT;
            inc[i] = 2;
            break;

        case DataType::UINT32:
        case DataType::INT32:
            bind[i].buffer = chunk->columns[i].data;
            bind[i].buffer_type = MYSQL_TYPE_LONG;
            inc[i] = 4;
            break;

        case DataType::UINT64:
        case DataType::INT64:
            bind[i].buffer = chunk->columns[i].data;
            bind[i].buffer_type = MYSQL_TYPE_LONGLONG;
            inc[i] = 8;
            break;

        case DataType::FLOAT32:
            bind[i].buffer = chunk->columns[i].data;
            bind[i].buffer_type = MYSQL_TYPE_FLOAT;
            inc[i] = 4;
            break;

        case DataType::FLOAT64:
            bind[i].buffer = chunk->columns[i].data;
            bind[i].buffer_type = MYSQL_TYPE_DOUBLE;
            inc[i] = 8;
            break;

        case DataType::STRING:
            bind[i].buffer = chunk->columns[i].data;
            bind[i].buffer_type = MYSQL_TYPE_STRING;
            inc[i] += sizeof(char *);
            break;

        case DataType::MYSQL_DATE:
            bind[i].buffer = chunk->columns[i].data;
            bind[i].buffer_type = MYSQL_TYPE_DATE;
            inc[i] += sizeof(MYSQL_TIME);
            break;
        }
    }

    size_t chunkSize = chunk->size();
    size_t numColumns = chunk->numColumns();
    for (size_t i = 0; i < chunkSize; ++i) {
        mysql_stmt_bind_param(stmt, bind);

        if (mysql_stmt_execute(stmt)) {
            auto e = DynamicMessageError(mysql_stmt_error(stmt));

            delete[] bind;

            mysql_stmt_close(stmt);

            throw e;
        }

        for (size_t j = 0; j < numColumns; ++j) {
            *((char **) (&bind[j].buffer)) += inc[j];
        }
    }

    delete[] bind;

    mysql_stmt_close(stmt);

    mysql_query(_conn(), "SET foreign_key_checks=0");
    mysql_query(_conn(), "SET unique_checks=0");
    mysql_query(_conn(), "SET autocommit=0");
    mysql_query(_conn(), "COMMIT");
}

MySQLDatabase::__Init MySQLDatabase::__init;
