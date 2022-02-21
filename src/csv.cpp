#include <csv.h>
#include <file.h>
#include <string_conversions.h>
#include <mysql.h>

using namespace spl;

static size_t size(const CSVField &field) {
    switch (field.type) {
    case DataType::UINT8:
    case DataType::INT8:
        return 1;

    case DataType::UINT16:
    case DataType::INT16:
        return 2;

    case DataType::UINT32:
    case DataType::INT32:
    case DataType::FLOAT32:
        return 4;
        break;

    case DataType::UINT64:
    case DataType::INT64:
    case DataType::FLOAT64:
        return 8;
        break;

    case DataType::STRING:
        return field.size;
        break;

    case DataType::MYSQL_DATE:
        return sizeof(MYSQL_TIME);
        break;
    }

    return 0;
}

static size_t estimatedRowSize(const CSVOptions &options) {
    size_t sz = 0;

    for (const auto &f : options.fields) sz += size(f);

    return sz;
}

static std::vector<ColumnChunk> allocateColumns(const CSVOptions &options, size_t length) {
    std::vector<ColumnChunk> columns(options.fields.size());

    for (size_t i = 0; i < options.fields.size(); ++i) {
        switch (options.fields[i].type) {
        case DataType::STRING: {
            auto width = size(options.fields[i]) + 1;
            char *buf = (char *) calloc(length, width);
            char **ptr = (char **) calloc(length, sizeof(char *));
            char **pp = ptr;
            for (auto p = buf; p != buf + length * width; p += width, ++pp) {
                *pp = p;
            }
            columns[i] = {
                options.fields[i].type,
                ptr,
                length
            };
        }
        break;

        default:
            columns[i] = {
                options.fields[i].type,
                calloc(length, size(options.fields[i])),
                length
            };
        }
    }

    return columns;
}

std::vector<ColumnarTableChunk *> CSV::read(
    const char *path,
    const CSVOptions &options
) {
    size_t rowSize = estimatedRowSize(options);
    size_t maxRows = options.maxChunkSize / rowSize;
    size_t numColumns = options.fields.size();

    File f(path);
    char *buffer = new char[f.info().length()];
    auto sz = f.read(buffer, f.info().length());

    size_t i = 0;
    std::vector<ColumnChunk> columns = allocateColumns(options, maxRows);
    std::vector<ColumnarTableChunk *> chunks;

    char *delim, *p = buffer, *end = buffer + sz;

    if (options.header) {
        while (p != end && *p != '\n') ++p;
        if (p != end) ++p;
    }

    while (p < end) {
        if (i == maxRows) {
            chunks.push_back(new ColumnarTableChunk(columns));
            columns = allocateColumns(options, maxRows);
            i = 0;
        }

        for (size_t j = 0; j < numColumns; ++j) {
            delim = p;
            while (delim != end && *delim != options.delimiter && *delim != '\n') ++delim;
            if (delim == end) {
                throw RuntimeError("Error reading CSV file");
            }
            *delim = '\0';

            switch (options.fields[j].type) {
                case DataType::UINT8: {
                    static_cast<uint8 *>(columns[j].data)[i] =
                        StringConversions::str_to_unsigned_int<uint8>(p);
                }
                break;

                case DataType::UINT16: {
                    static_cast<uint16 *>(columns[j].data)[i] =
                        StringConversions::str_to_unsigned_int<uint16>(p);
                }
                break;

                case DataType::UINT32: {
                    static_cast<uint32 *>(columns[j].data)[i] =
                        StringConversions::str_to_unsigned_int<uint32>(p);
                }
                break;

                case DataType::UINT64: {
                    static_cast<uint64 *>(columns[j].data)[i] =
                        StringConversions::str_to_unsigned_int<uint64>(p);
                }
                break;

                case DataType::INT8: {
                    static_cast<int8 *>(columns[j].data)[i] =
                        StringConversions::str_to_int<int8>(p);
                }
                break;

                case DataType::INT16: {
                    static_cast<int16 *>(columns[j].data)[i] =
                        StringConversions::str_to_int<int16>(p);
                }
                break;

                case DataType::INT32: {
                    static_cast<int32 *>(columns[j].data)[i] =
                        StringConversions::str_to_int<int32>(p);
                }
                break;

                case DataType::INT64: {
                    static_cast<int64 *>(columns[j].data)[i] =
                        StringConversions::str_to_int<int64>(p);
                }
                break;

                case DataType::FLOAT32: {
                    static_cast<float32 *>(columns[j].data)[i] =
                        StringConversions::str_to_float<float32>(p);
                }
                break;

                case DataType::FLOAT64: {
                    static_cast<float64 *>(columns[j].data)[i] =
                        StringConversions::str_to_float<float64>(p);
                }
                break;

                case DataType::STRING: {
                    strcpy(static_cast<char **>(columns[j].data)[i], p);
                }
                break;

                case DataType::MYSQL_DATE: {
                    auto dt = strtok(p, "-");
                    static_cast<MYSQL_TIME *>(columns[j].data)[i].year =
                        StringConversions::str_to_unsigned_int<unsigned int>(dt);

                    dt = strtok(nullptr, "-");
                    static_cast<MYSQL_TIME *>(columns[j].data)[i].month =
                        StringConversions::str_to_unsigned_int<unsigned int>(dt);

                    dt = strtok(nullptr, "-");
                    static_cast<MYSQL_TIME *>(columns[j].data)[i].day =
                        StringConversions::str_to_unsigned_int<unsigned int>(dt);
                }
                break;
            }

            p = delim + 1;
        }

        ++i;
    }

    for (size_t j = 0; j < numColumns; ++j) {
        columns[j].size = i;
    }
    chunks.push_back(new ColumnarTableChunk(columns));

    delete[] buffer;
    return chunks;
}
